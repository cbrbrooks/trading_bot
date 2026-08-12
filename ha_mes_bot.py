"""
ha_mes_bot.py — LIVE Heikin-Ashi 2-min bot for TopstepX (MES, 3 contracts)
==========================================================================
Trades the strategy in ha_strategy.py live on your Topstep account via the
ProjectX/TopstepX Gateway API + SignalR tick feed. All strategy DECISIONS come
from ha_strategy.py, so this bot and ha_replay.py can never disagree about what a
trade is — if the replay says "enter here", so does this.

Defaults: MES, 3 contracts, BE@30 (stop moves to breakeven at 30% of target).

⚠️  READ BEFORE RUNNING LIVE  ⚠️
--------------------------------
DRY_RUN defaults to TRUE. In DRY_RUN the bot connects, streams ticks, builds
candles, and logs every signal / entry / breakeven-trigger it WOULD act on — but
places NO orders. Run it in DRY_RUN for a full session first and confirm in the
log that entries fire at the same prices ha_replay.py shows. THEN:

The one piece that could NOT be verified offline is the live breakeven move. Your
existing bots use a STATIC broker bracket, which cannot move a stop mid-trade
(see the note at the top of choch_bot_live.py). To get BE@30 this bot cancels the
resting broker STOP child and places a fresh stop at breakeven — using only the
/Order/cancel and /Order/place endpoints your code already relies on. But the
exact OCO-linkage behavior when you cancel one bracket child (does the broker keep
the target working? orphan the position?) is broker-specific and I could not test
it against your account. VERIFY THE BREAKEVEN MOVE ON YOUR EVAL/PRACTICE ACCOUNT
before running it on the funded account. Set BE_ENABLED=False to run pure
fixed-bracket (no BE) if you want the simplest possible live path first.

Requires the same .env as your other bots: TOPSTEP_USERNAME, TOPSTEP_API_KEY.
Uses the pinned signalrcore==1.0.2 / websocket-client==1.8.0 from requirements.txt.
"""

import asyncio
import csv
import logging
import os
import threading
from collections import deque
from datetime import datetime, time, timedelta, date
from zoneinfo import ZoneInfo

import aiohttp
from dotenv import load_dotenv
from signalrcore.hub_connection_builder import HubConnectionBuilder

import ha_strategy as S

load_dotenv()

# ── Config ───────────────────────────────────────────────────────────────────
DRY_RUN      = True            # ← place NO orders until you flip this to False
BE_ENABLED   = True            # ← False = pure fixed bracket (no breakeven move)

INSTRUMENT   = os.getenv("INSTRUMENT", "MES").upper()
CONTRACTS    = int(os.getenv("CONTRACTS", "3"))
POINT_VALUE  = 5.0 if INSTRUMENT == "MES" else 50.0
MES_SYMBOL   = os.getenv("CONTRACT_SYMBOL", "")   # optional exact front-month, e.g. "MESU6"
FEE_PER_RT   = float(os.getenv("FEE_PER_RT", "1.34" if INSTRUMENT == "MES" else "3.78"))

BASE_URL     = "https://api.topstepx.com/api"
MARKET_HUB   = "https://rtc.topstepx.com/hubs/market"
USERNAME     = os.getenv("TOPSTEP_USERNAME")
API_KEY      = os.getenv("TOPSTEP_API_KEY")

PT = ZoneInfo("America/Los_Angeles")
CT = ZoneInfo("America/Chicago")

ORDER_TYPE_LIMIT  = 1
ORDER_TYPE_MARKET = 2
ORDER_TYPE_STOP   = 4          # per ProjectX docs — same const choch_bot_live.py uses

# safety gates
MAX_TRADES_PER_DAY      = int(os.getenv("MAX_TRADES_PER_DAY", "12"))
MAX_CONSEC_LOSSES       = int(os.getenv("MAX_CONSEC_LOSSES", "4"))
DAILY_LOSS_LIMIT        = float(os.getenv("DAILY_LOSS_LIMIT", "500"))   # dollars

FORWARD_LOG = os.getenv("RTH_FORWARD_LOG", "ha_rth_forward_log.csv")
_LOG_COLS = ["date", "entry_time", "exit_time", "dir", "entry", "stop", "target",
             "exit", "reason", "points", "net", "mode"]

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s",
                    handlers=[logging.StreamHandler(),
                              logging.FileHandler("ha_mes_bot.log")])
log = logging.getLogger("ha_mes")

# ── Telegram alerts (same .env keys as your CHOCH bot) ───────────────────────
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID")


class TelegramNotifier:
    """Push alerts to Telegram. Fail-safe: any network/token error logs a warning and
    returns, so a Telegram problem NEVER interrupts trading. Reads the same
    TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID your CHOCH bot already uses."""
    API_BASE = "https://api.telegram.org"

    def __init__(self, token, chat_id):
        self.token = token; self.chat_id = chat_id
        self.enabled = bool(token and chat_id)
        log.info("Telegram alerts ENABLED." if self.enabled else
                 "Telegram alerts DISABLED (set TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID in .env).")

    async def send(self, session, msg):
        if not self.enabled:
            return
        try:
            url = f"{self.API_BASE}/bot{self.token}/sendMessage"
            async with session.post(url, json={"chat_id": self.chat_id, "text": msg}) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    log.warning(f"Telegram send got HTTP {resp.status}: {body[:200]} — trading unaffected.")
        except Exception as e:
            log.warning(f"Telegram send failed ({type(e).__name__}: {e}) — trading unaffected.")


# ── Rolling candle builder (2-min signal bars + 1-min target bars) ───────────
class Bars:
    """Builds 1-min and 2-min OHLCV bars from the tick stream, chains Heikin-Ashi
    on the 2-min series, and keeps session VWAP + a 1-min wick history for targets."""

    def __init__(self):
        self.cur2 = None; self.cur2_key = None           # 2-min in progress
        self.cur1 = None; self.cur1_key = None           # 1-min in progress
        self.prev_ha = None                               # (ha_open, ha_close) of last closed 2-min
        self.last2 = None                                 # last CLOSED 2-min HA dict
        self.prev2 = None                                 # the one before that
        self.vwap_num = 0.0; self.vwap_den = 0.0; self.vwap = None
        self.upper_wicks = []; self.lower_wicks = []      # 1-min wick extremes (closed bars)

    @staticmethod
    def _key(ts_pt, minutes):
        # Return a plain tuple with NO sub-second component. Using
        # ts.replace(second=0, microsecond=0) is unsafe: on a pandas Timestamp it
        # leaves the nanosecond field intact, so every tick would get a unique key
        # and close a fresh bar. A tuple keyed only to the quantized minute is
        # robust for datetime AND pandas Timestamp inputs alike.
        return (ts_pt.year, ts_pt.month, ts_pt.day, ts_pt.hour,
                (ts_pt.minute // minutes) * minutes)

    def add_tick(self, ts_pt, price, size):
        """Feed one tick. Returns a just-closed 2-min HA dict when a 2-min bar rolls
        over (else None). The caller evaluates the signal on that returned bar."""
        closed2 = None
        # 1-min bar
        k1 = self._key(ts_pt, 1)
        if self.cur1_key != k1:
            if self.cur1 is not None:
                self._close_1min()
            self.cur1 = {"o": price, "h": price, "l": price, "c": price}
            self.cur1_key = k1
        else:
            self.cur1["h"] = max(self.cur1["h"], price)
            self.cur1["l"] = min(self.cur1["l"], price)
            self.cur1["c"] = price
        # 2-min bar
        k2 = self._key(ts_pt, 2)
        if self.cur2_key != k2:
            if self.cur2 is not None:
                closed2 = self._close_2min()
            self.cur2 = {"o": price, "h": price, "l": price, "c": price, "v": size}
            self.cur2_key = k2
        else:
            self.cur2["h"] = max(self.cur2["h"], price)
            self.cur2["l"] = min(self.cur2["l"], price)
            self.cur2["c"] = price
            self.cur2["v"] += size
        return closed2

    def _close_1min(self):
        b = self.cur1
        if (b["h"] - max(b["o"], b["c"])) >= S.MIN_WICK:
            self.upper_wicks.append(b["h"])
        if (min(b["o"], b["c"]) - b["l"]) >= S.MIN_WICK:
            self.lower_wicks.append(b["l"])

    def _close_2min(self):
        b = self.cur2
        ha_o, ha_h, ha_l, ha_c = S.heikin(b["o"], b["h"], b["l"], b["c"],
                                          self.prev_ha[0] if self.prev_ha else None,
                                          self.prev_ha[1] if self.prev_ha else None)
        self.prev_ha = (ha_o, ha_c)
        # session VWAP (typical price * volume)
        tp = (b["h"] + b["l"] + b["c"]) / 3.0
        self.vwap_num += tp * b["v"]; self.vwap_den += b["v"]
        self.vwap = self.vwap_num / self.vwap_den if self.vwap_den else b["c"]
        hb = {"ha_open": ha_o, "ha_high": ha_h, "ha_low": ha_l, "ha_close": ha_c,
              "raw_high": b["h"], "raw_low": b["l"], "vwap": self.vwap}
        self.prev2 = self.last2
        self.last2 = hb
        return hb

    def reset_session(self):
        self.__init__()


# ── The bot ──────────────────────────────────────────────────────────────────
class HAMesBot:
    def __init__(self):
        self.token = None
        self.account_id = None
        self.contract_id = None
        self.bars = Bars()
        self.ticks = deque()          # incoming queue, DRAINED each loop for bar-building
        self.recent = deque()         # rolling ~90s of (ts, price) for the consolidation window
        self.tick_lock = threading.Lock()
        self._signalr = None
        # position state
        self.in_trade = False
        self.direction = None
        self.entry = self.stop = self.target = None
        self.be_trigger = self.be_stop = None
        self.be_armed = False
        self.entry_oid = None
        self.stop_child_id = None
        self.trades_today = 0
        self.consec_losses = 0
        self.daily_pnl = 0.0
        self.session_date = None
        self.seeded_date = None
        self.entry_time_str = ""
        self.notifier = TelegramNotifier(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID)

    async def _alert(self, session, msg):
        """Fire-and-forget Telegram push; never raises into the trading path."""
        try:
            await self.notifier.send(session, msg)
        except Exception:
            pass

    # ---- auth / account / contract (payloads match your proven bots) ----
    async def _auth(self, session):
        r = await session.post(f"{BASE_URL}/Auth/loginKey",
                               json={"userName": USERNAME, "apiKey": API_KEY})
        d = await r.json()
        if not d.get("success"):
            raise SystemExit(f"Auth failed: {d}")
        self.token = d["token"]
        log.info("Authenticated OK")

    def _hdr(self):
        return {"Authorization": f"Bearer {self.token}"}

    async def _get_account(self, session):
        r = await session.post(f"{BASE_URL}/Account/search", headers=self._hdr(),
                               json={"onlyActiveAccounts": True})
        accts = (await r.json()).get("accounts", [])
        if not accts:
            raise SystemExit("No active accounts.")
        want = os.getenv("ACCOUNT_ID")
        acct = next((a for a in accts if str(a.get("id")) == want), accts[0]) if want else accts[0]
        self.account_id = acct["id"]
        log.info(f"Account: {acct.get('name')} (id={self.account_id}) balance={acct.get('balance')}")

    async def _get_contract(self, session):
        r = await session.post(f"{BASE_URL}/Contract/search", headers=self._hdr(),
                               json={"searchText": INSTRUMENT, "live": False})
        cs = [c for c in (await r.json()).get("contracts", []) if "-" not in c.get("name", "")]
        if MES_SYMBOL:
            exact = [c for c in cs if c.get("name") == MES_SYMBOL]
            if exact: cs = exact
        matches = [c for c in cs if c.get("name", "").startswith(INSTRUMENT)]
        if not matches:
            raise SystemExit(f"{INSTRUMENT} contract not found. Set CONTRACT_SYMBOL in .env.")
        contract = sorted(matches, key=lambda c: c.get("name", ""))[0]
        self.contract_id = contract["id"]
        log.info(f"Contract: {contract['name']} (id={self.contract_id})")

    # ---- SignalR tick feed (same hub/handler as intrabar_es_bot.py) ----
    def _on_trade_tick(self, args):
        try:
            payload = args[1] if isinstance(args, list) and len(args) > 1 else args
            trades = payload if isinstance(payload, list) else [payload]
            for tr in trades:
                price = tr.get("price") if isinstance(tr, dict) else None
                if price is None:
                    continue
                size = tr.get("volume") or tr.get("size") or 0
                ts_pt = datetime.now(PT)
                with self.tick_lock:
                    self.ticks.append((ts_pt, float(price), float(size)))
                    self.recent.append((ts_pt, float(price)))     # rolling window for entries
                    cut = ts_pt - timedelta(seconds=90)
                    while self.recent and self.recent[0][0] < cut:
                        self.recent.popleft()
        except Exception as e:
            log.warning(f"tick parse error: {e}")

    def _open_market_hub(self):
        conn = (HubConnectionBuilder()
                .with_url(f"{MARKET_HUB}?access_token={self.token}",
                          options={"skip_negotiation": True,
                                   "headers": {"Authorization": f"Bearer {self.token}"}})
                .with_automatic_reconnect({"type": "raw", "keep_alive_interval": 10,
                                           "reconnect_interval": 5})
                .build())
        conn.on("GatewayTrade", self._on_trade_tick)
        conn.on_open(lambda: (log.info("SignalR open — subscribing"),
                              conn.send("SubscribeContractTrades", [self.contract_id])))
        conn.on_close(lambda: log.warning("SignalR closed"))
        conn.start()
        self._signalr = conn

    # ---- orders (payload shapes copied from choch_bot_live.py) ----
    async def _place_bracket_entry(self, session, action, stop_ticks, target_ticks):
        side = 0 if action == "buy" else 1
        if abs(stop_ticks) < 4:
            log.error(f"REFUSE: stop {abs(stop_ticks)}t < 4t broker min."); return None, None
        if (stop_ticks > 0) == (target_ticks > 0):
            log.error("REFUSE: stop/target share a sign."); return None, None
        payload = {"accountId": self.account_id, "contractId": self.contract_id,
                   "type": ORDER_TYPE_MARKET, "side": side, "size": CONTRACTS,
                   "limitPrice": None, "stopPrice": None, "trailPrice": None,
                   "customTag": None, "linkedOrderId": None,
                   "stopLossBracket": {"ticks": int(stop_ticks), "type": ORDER_TYPE_STOP},
                   "takeProfitBracket": {"ticks": int(target_ticks), "type": ORDER_TYPE_LIMIT}}
        if DRY_RUN:
            log.info(f"[DRY] bracket entry {action} {CONTRACTS}x {INSTRUMENT} stop={stop_ticks}t tgt={target_ticks}t")
            return "DRY", None
        r = await session.post(f"{BASE_URL}/Order/place", headers=self._hdr(), json=payload)
        d = await r.json()
        if not d.get("success"):
            log.error(f"Bracket rejected: {d}"); return None, None
        oid = d.get("orderId")
        for _ in range(3):
            await asyncio.sleep(1)
            st, fill = await self._order_status(session, oid)
            if st == "filled":
                log.info(f"Bracket entry FILLED @ {fill} id={oid}"); return oid, fill
            if st == "rejected":
                return None, None
        log.warning("Entry fill inconclusive — check account."); return oid, None

    async def _order_status(self, session, oid):
        try:
            start = (datetime.now(CT) - timedelta(minutes=15)).isoformat()
            r = await session.post(f"{BASE_URL}/Order/search", headers=self._hdr(),
                                   json={"accountId": self.account_id, "startTimestamp": start})
            o = next((x for x in (await r.json()).get("orders", []) if x.get("id") == oid), None)
            if o is None: return "pending", None
            if o.get("status") == 2: return "filled", o.get("filledPrice") or o.get("limitPrice")
            if o.get("status") in (4, 5): return "rejected", None
            return "pending", None
        except Exception as e:
            log.warning(f"status check failed: {e}"); return "pending", None

    async def _find_stop_child(self, session):
        """Locate the resting broker STOP child of the current bracket, so BE can move it."""
        try:
            start = (datetime.now(CT) - timedelta(minutes=15)).isoformat()
            r = await session.post(f"{BASE_URL}/Order/search", headers=self._hdr(),
                                   json={"accountId": self.account_id, "startTimestamp": start})
            for o in (await r.json()).get("orders", []):
                if (o.get("contractId") == self.contract_id and o.get("type") == ORDER_TYPE_STOP
                        and o.get("status") in (0, 1, 6)):        # working/accepted
                    return o.get("id")
        except Exception as e:
            log.warning(f"stop-child lookup failed: {e}")
        return None

    async def _cancel_order(self, session, oid):
        if DRY_RUN:
            log.info(f"[DRY] cancel order {oid}"); return True
        r = await session.post(f"{BASE_URL}/Order/cancel", headers=self._hdr(),
                               json={"accountId": self.account_id, "orderId": oid})
        return (await r.json()).get("success", False)

    async def _place_stop(self, session, action, stop_price):
        """Standalone protective STOP at an absolute price (used for the breakeven move)."""
        side = 0 if action == "buy" else 1     # action = side that CLOSES the position
        payload = {"accountId": self.account_id, "contractId": self.contract_id,
                   "type": ORDER_TYPE_STOP, "side": side, "size": CONTRACTS,
                   "limitPrice": None, "stopPrice": round(stop_price, 2), "trailPrice": None,
                   "customTag": "ha_be_stop", "linkedOrderId": None}
        if DRY_RUN:
            log.info(f"[DRY] place BE stop {action} {CONTRACTS}x @ {stop_price}"); return "DRY"
        r = await session.post(f"{BASE_URL}/Order/place", headers=self._hdr(), json=payload)
        d = await r.json()
        if not d.get("success"):
            log.error(f"BE stop place failed: {d}"); return None
        return d.get("orderId")

    async def _flatten(self, session, reason=""):
        log.info(f"FLATTEN — {reason}")
        if DRY_RUN:
            log.info("[DRY] would Position/closeAll"); return
        await session.post(f"{BASE_URL}/Position/closeAll", headers=self._hdr(),
                           json={"accountId": self.account_id})

    async def _position_open(self, session):
        try:
            r = await session.post(f"{BASE_URL}/Position/searchOpen", headers=self._hdr(),
                                   json={"accountId": self.account_id})
            return any(p.get("contractId") == self.contract_id
                       for p in (await r.json()).get("positions", []))
        except Exception:
            return None   # unknown

    async def _lookup_exit_fill(self, session):
        """After the broker closes the position, find the child order that actually
        filled (stop, breakeven-stop, or target) and return (exit_price, reason).
        Returns (None, 'unknown') if the fill can't be read."""
        try:
            start = (datetime.now(CT) - timedelta(minutes=30)).isoformat()
            r = await session.post(f"{BASE_URL}/Order/search", headers=self._hdr(),
                                   json={"accountId": self.account_id, "startTimestamp": start})
            orders = (await r.json()).get("orders", [])
            filled = [o for o in orders
                      if o.get("contractId") == self.contract_id and o.get("status") == 2
                      and o.get("filledPrice") is not None
                      and o.get("type") in (ORDER_TYPE_STOP, ORDER_TYPE_LIMIT)]
            if not filled:
                return None, "unknown"
            closer = max(filled, key=lambda o: o.get("updateTimestamp") or o.get("creationTimestamp") or "")
            return closer.get("filledPrice"), ("target" if closer.get("type") == ORDER_TYPE_LIMIT else "stop/BE")
        except Exception as e:
            log.warning(f"exit-fill lookup failed: {e}")
            return None, "unknown"

    # ---- startup seeding: build VWAP/HA/wicks from 6:30->now so a late start is OK ----
    async def _fetch_1m(self, session, a_ct, b_ct):
        r = await session.post(f"{BASE_URL}/History/retrieveBars", headers=self._hdr(),
                               json={"contractId": self.contract_id, "live": False, "unit": 2,
                                     "unitNumber": 1, "limit": 5000, "includePartialBar": False,
                                     "startTime": a_ct.isoformat(), "endTime": b_ct.isoformat()})
        d = await r.json()
        out = []
        for x in d.get("bars") or d.get("data") or []:
            ts = x.get("t") or x.get("timestamp")
            o = x.get("o", x.get("open"))
            if ts is None or o is None:
                continue
            t = datetime.fromisoformat(str(ts).replace("Z", "+00:00")).astimezone(PT)
            out.append({"ts": t, "o": float(o), "h": float(x.get("h", x.get("high"))),
                        "l": float(x.get("l", x.get("low"))), "c": float(x.get("c", x.get("close"))),
                        "v": float(x.get("v", x.get("volume", 0)) or 0)})
        out.sort(key=lambda z: z["ts"])
        return out

    async def _seed_session(self, session, day):
        """Replay today's 6:30->now 1-min bars into the candle builder so VWAP, the HA
        chain and the wick history are correct even if the bot starts after 6:30. Signals
        are NOT emitted here (returns are discarded) — pure state build."""
        self.bars.reset_session()
        anchor = datetime.combine(day, S.SESSION_OPEN, tzinfo=PT).astimezone(CT)
        try:
            mins = await self._fetch_1m(session, anchor, datetime.now(CT))
        except Exception as e:
            log.warning(f"seed fetch failed ({e}) — VWAP will build live from first ticks"); return
        n = 0
        for m in mins:
            if not (S.SESSION_OPEN <= m["ts"].time() < S.SESSION_CLOSE):
                continue
            self.bars.add_tick(m["ts"], m["o"], m["v"])      # feed o,h,l,c so 2-min OHLC builds
            self.bars.add_tick(m["ts"], m["h"], 0)
            self.bars.add_tick(m["ts"], m["l"], 0)
            self.bars.add_tick(m["ts"], m["c"], 0)
            n += 1
        log.info(f"Seeded RTH {day}: {n} 1-min bars, VWAP={self.bars.vwap}, "
                 f"{len(self.bars.upper_wicks)+len(self.bars.lower_wicks)} wick levels")

    # ---- DRY_RUN paper fills (live mode uses the broker bracket + _manage instead) ----
    def _record_trade(self, reason, exit_price, exit_time_str):
        is_long = self.direction == "long"
        pts = (exit_price - self.entry) if is_long else (self.entry - exit_price)
        net = round(pts * POINT_VALUE * CONTRACTS - FEE_PER_RT * CONTRACTS, 2)
        row = {"date": str(self.session_date), "entry_time": getattr(self, "entry_time_str", ""),
               "exit_time": exit_time_str, "dir": self.direction, "entry": round(self.entry, 2),
               "stop": round(self.stop, 2), "target": round(self.target, 2), "exit": round(exit_price, 2),
               "reason": reason, "points": round(pts, 2), "net": net, "mode": "DRY" if DRY_RUN else "LIVE"}
        new = not os.path.exists(FORWARD_LOG)
        with open(FORWARD_LOG, "a", newline="") as f:
            w = csv.DictWriter(f, fieldnames=_LOG_COLS)
            if new: w.writeheader()
            w.writerow(row)
        return net, pts

    async def _paper_manage(self, session, last_price):
        """Simulate the trade tick-by-tick in DRY_RUN: arm BE at 30%, resolve target/stop,
        record the paper trade + P&L. No broker calls."""
        if not self.in_trade:
            return
        is_long = self.direction == "long"
        if BE_ENABLED and not self.be_armed:
            if (last_price >= self.be_trigger) if is_long else (last_price <= self.be_trigger):
                self.be_armed = True
                log.info(f"[PAPER] breakeven armed @ {last_price} (stop -> {self.be_stop})")
        cur_stop = self.be_stop if self.be_armed else self.stop
        reason = xp = None
        if (last_price >= self.target) if is_long else (last_price <= self.target):
            reason, xp = "target", self.target
        elif (last_price <= cur_stop) if is_long else (last_price >= cur_stop):
            reason, xp = ("be" if self.be_armed else "stop"), cur_stop
        if reason:
            net, pts = self._record_trade(reason, xp, datetime.now(PT).strftime("%H:%M"))
            self.daily_pnl += net
            self.consec_losses = 0 if net > 0 else self.consec_losses + 1
            icon = {"target": "✅", "be": "➖", "stop": "🛑"}.get(reason, "⚪")
            log.info(f"[PAPER] {reason.upper()} @ {xp} | {pts:+.2f}pt ${net:+.0f} | day ${self.daily_pnl:+.0f}")
            self._go_flat()
            await self._alert(session, f"{icon} PAPER {reason.upper()} {INSTRUMENT} {pts:+.2f}pt ${net:+.0f}"
                              f"\nday P&L ${self.daily_pnl:+.0f}")

    # ---- the breakeven move ----
    async def _move_to_breakeven(self, session):
        """Cancel the resting broker stop child and place a fresh stop at breakeven.
        ⚠ This is the piece to verify on a practice account first (see file header)."""
        close_action = "sell" if self.direction == "long" else "buy"
        child = self.stop_child_id or await self._find_stop_child(session)
        if child:
            ok = await self._cancel_order(session, child)
            if not ok:
                log.warning("Could not cancel broker stop child — leaving original stop in place, NOT arming BE.")
                return False
        new_stop = await self._place_stop(session, close_action, self.be_stop)
        if new_stop is None:
            log.error("BE stop placement FAILED after cancelling old stop — position may be UNPROTECTED. "
                      "Flattening for safety.")
            await self._flatten(session, "BE stop placement failed")
            self._go_flat()
            return False
        log.info(f"BREAKEVEN ARMED — stop moved to {self.be_stop} (was {self.stop})")
        self.be_armed = True
        return True

    def _go_flat(self):
        self.in_trade = False; self.direction = None
        self.entry = self.stop = self.target = None
        self.be_trigger = self.be_stop = None; self.be_armed = False
        self.entry_oid = self.stop_child_id = None

    # ---- signal handling on a closed 2-min bar ----
    async def _on_bar_close(self, session, closed_bar, now_pt):
        if self.in_trade or self.bars.prev2 is None:
            return
        if now_pt.time() > S.ENTRY_CUTOFF or now_pt.time() < S.SESSION_OPEN:
            return
        if self.trades_today >= MAX_TRADES_PER_DAY:
            return
        # NOTE: consecutive-loss cutoff intentionally DISABLED per user preference —
        # a losing streak does NOT stop the bot. consec_losses is still tracked and
        # shown in the Telegram alert for info; only the daily-loss limit gates entries.
        if -self.daily_pnl >= DAILY_LOSS_LIMIT:
            log.info(f"Safety gate active (daily loss ${-self.daily_pnl:.0f} >= ${DAILY_LOSS_LIMIT:.0f}) — no new entries.")
            return

        sig = S.entry_signal(self.bars.prev2, closed_bar, closed_bar["vwap"])
        if sig is None:
            return
        is_long = sig == "long"

        # entry = last-30s consolidation extreme (bottom for long / top for short),
        # read from the ROLLING recent-tick buffer (self.ticks is drained each loop and
        # would be empty here).
        cutoff = datetime.now(PT) - timedelta(seconds=S.CONSOL_WINDOW_SEC)
        with self.tick_lock:
            recent = [p for (t, p) in self.recent if t >= cutoff]
        entry = S.consolidation_entry(recent, is_long)
        if entry is None:
            log.info(f"{sig.upper()} signal but no consolidation ticks — skip."); return

        stop = S.initial_stop(closed_bar["raw_low"], closed_bar["raw_high"], is_long)
        tips = self.bars.upper_wicks if is_long else self.bars.lower_wicks
        tgt = S.cluster_target(list(tips), entry, is_long)
        if tgt is None:
            log.info(f"{sig.upper()} signal but no key-level target ≥{S.MIN_TP_POINTS}pt — skip."); return
        level, target = tgt

        stop_pts = abs(entry - stop); tgt_pts = abs(target - entry)
        stop_ticks_mag = round(stop_pts / S.TICK)
        tgt_ticks_mag = round(tgt_pts / S.TICK)
        # signed ticks: long -> stop below (neg), target above (pos); short -> mirror
        stop_ticks = -stop_ticks_mag if is_long else stop_ticks_mag
        target_ticks = tgt_ticks_mag if is_long else -tgt_ticks_mag

        self.be_trigger, self.be_stop = S.breakeven_stop(entry, target, is_long)
        log.info(f"SIGNAL {sig.upper()} | entry~{entry} stop {stop} tgt {target} "
                 f"(level {level}) | risk {stop_pts:.2f}pt reward {tgt_pts:.2f}pt "
                 f"| BE trigger {self.be_trigger}->{self.be_stop}")

        action = "buy" if is_long else "sell"
        oid, fill = await self._place_bracket_entry(session, action, stop_ticks, target_ticks)
        if oid is None:
            return
        self.in_trade = True; self.direction = sig
        self.entry = fill if fill else entry
        self.stop = stop; self.target = target
        self.be_armed = False; self.entry_oid = oid
        self.stop_child_id = None if DRY_RUN else await self._find_stop_child(session)
        self.trades_today += 1
        self.entry_time_str = now_pt.strftime("%H:%M")
        await self._alert(session, f"{'🟢 LONG' if is_long else '🔴 SHORT'} {INSTRUMENT} x{CONTRACTS}"
                          f"{' [DRY]' if DRY_RUN else ''}\nentry {self.entry}  stop {stop}  target {target}")

    # ---- manage an open position (LIVE mode; broker owns the exits) ----
    async def _manage(self, session, last_price):
        if not self.in_trade:
            return
        is_long = self.direction == "long"
        # 1) detect broker close FIRST — if the position is already gone, do NOT try to
        #    move a stop (that could leave an orphan stop order that opens a new position).
        opened = await self._position_open(session)
        if opened is None:
            return   # couldn't read position this cycle — try again next loop, don't act blind
        if opened is True:
            # still open: arm breakeven if price reached the trigger
            if BE_ENABLED and not self.be_armed:
                hit = (last_price >= self.be_trigger) if is_long else (last_price <= self.be_trigger)
                if hit:
                    await self._move_to_breakeven(session)
            return
        # opened is False → position closed by the broker
        if True:
            # sweep any orphan working orders, then look up the REAL exit fill for P&L
            child = await self._find_stop_child(session)
            if child: await self._cancel_order(session, child)
            exit_px, reason = await self._lookup_exit_fill(session)
            direction = self.direction
            if exit_px is not None:
                is_long = direction == "long"
                pts = (exit_px - self.entry) if is_long else (self.entry - exit_px)
                net = round(pts * POINT_VALUE * CONTRACTS - FEE_PER_RT * CONTRACTS, 2)
                self.daily_pnl += net                       # feed the safety gates with REAL P&L
                self.consec_losses = 0 if net > 0 else self.consec_losses + 1
                icon = "✅" if net > 0 else ("🛑" if net < 0 else "➖")
                log.info(f"Position closed ({reason}) | entry {self.entry} -> exit {exit_px} "
                         f"| {pts:+.2f}pt ${net:+.0f} | day ${self.daily_pnl:+.0f} consecL {self.consec_losses}")
                await self._alert(session,
                                  f"{icon} {INSTRUMENT} {direction.upper()} CLOSED ({reason})"
                                  f"{' [DRY]' if DRY_RUN else ''}\n"
                                  f"entry {self.entry} → exit {exit_px}   {pts:+.2f}pt   ${net:+.0f}\n"
                                  f"day P&L ${self.daily_pnl:+.0f}   consec losses {self.consec_losses}")
            else:
                log.info(f"Position closed but exit fill unavailable. direction={direction}")
                await self._alert(session, f"⚪ {INSTRUMENT} {direction.upper()} closed — "
                                  f"couldn't read the exit fill; check the platform for P&L")
            self._go_flat()


    # ---- main loop ----
    async def run(self):
        if not USERNAME or not API_KEY:
            raise SystemExit("Set TOPSTEP_USERNAME / TOPSTEP_API_KEY in .env")
        log.info("=" * 60)
        log.info(f"  HA 2-MIN LIVE BOT — {INSTRUMENT} x{CONTRACTS} | BE@{int(S.BE_TRIGGER_FRAC*100)} "
                 f"{'ENABLED' if BE_ENABLED else 'OFF'} | DRY_RUN={DRY_RUN}")
        log.info("=" * 60)
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20)) as session:
            await self._auth(session)
            await self._get_account(session)
            await self._get_contract(session)
            self._open_market_hub()
            self.session_date = datetime.now(PT).date()
            await self._seed_session(session, self.session_date)   # VWAP/HA/wicks from 6:30->now
            self.seeded_date = self.session_date
            await self._alert(session, f"🤖 HA RTH bot started — {INSTRUMENT} x{CONTRACTS} | "
                              f"BE@{int(S.BE_TRIGGER_FRAC*100)} | DRY_RUN={DRY_RUN}\n"
                              f"You'll get a ping on every entry and exit.")

            last_price = None
            last_hb = datetime.now(PT); tick_count = 0; last_mgmt = datetime.now(PT)
            while True:
                now = datetime.now(PT)
                # new day → reset + re-seed (so VWAP re-anchors at 6:30)
                if now.date() != self.session_date:
                    self.trades_today = 0; self.consec_losses = 0; self.daily_pnl = 0.0
                    self.session_date = now.date()
                    if self.in_trade: self._go_flat()
                    self.seeded_date = None
                    log.info(f"New session {self.session_date}")
                if self.seeded_date != self.session_date and now.time() >= S.SESSION_OPEN:
                    await self._seed_session(session, self.session_date); self.seeded_date = self.session_date

                # drain ticks
                with self.tick_lock:
                    batch = list(self.ticks); self.ticks.clear()
                for ts_pt, price, size in batch:
                    last_price = price; tick_count += 1
                    if not (S.SESSION_OPEN <= ts_pt.time() < S.SESSION_CLOSE):
                        continue
                    if DRY_RUN and self.in_trade:                 # paper-manage every tick
                        await self._paper_manage(session, price)
                    closed = self.bars.add_tick(ts_pt, price, size)
                    if closed is not None and not self.in_trade:
                        await self._on_bar_close(session, closed, ts_pt)

                # LIVE: broker owns exits; poll at most ~1/sec to respect the rate limit
                if not DRY_RUN and self.in_trade and (now - last_mgmt).total_seconds() >= 1.0:
                    await self._manage(session, last_price); last_mgmt = now

                # force flat at session close
                if now.time() >= S.SESSION_CLOSE and self.in_trade:
                    if DRY_RUN:
                        self._record_trade("flat_eod", last_price, now.strftime("%H:%M")); self._go_flat()
                    else:
                        await self._flatten(session, "Session close force-flat"); self._go_flat()

                # heartbeat every ~60s so you can see it's alive
                if (now - last_hb).total_seconds() >= 60:
                    inwin = S.SESSION_OPEN <= now.time() <= S.ENTRY_CUTOFF
                    st = ("IN TRADE" if self.in_trade else "watching 6:30-12:30" if inwin else "idle (outside window)")
                    vw = f"{self.bars.vwap:.2f}" if self.bars.vwap else "-"
                    log.info(f"♥ {now.strftime('%H:%M:%S')} | last {last_price} | {tick_count} ticks/min "
                             f"| vwap {vw} | trades {self.trades_today} day ${self.daily_pnl:+.0f} | {st}")
                    last_hb = now; tick_count = 0

                await asyncio.sleep(0.25)


if __name__ == "__main__":
    try:
        asyncio.run(HAMesBot().run())
    except KeyboardInterrupt:
        log.info("Stopped by user.")
