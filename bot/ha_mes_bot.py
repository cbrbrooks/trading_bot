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
DRY_RUN is the default — always. Run `python ha_mes_bot.py` and it connects,
streams ticks, builds candles, and logs every signal / entry / breakeven-trigger
it WOULD act on — but places NO orders. Run it in DRY_RUN for a full session
first and confirm in the log that entries fire at the same prices ha_replay.py
shows. THEN, to place real orders: `python ha_mes_bot.py --live`

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

import argparse
import asyncio
import csv
import logging
import os
import sys
import threading
from collections import deque
from datetime import datetime, time, timedelta, date
from zoneinfo import ZoneInfo

import aiohttp
from dotenv import load_dotenv
from signalrcore.hub_connection_builder import HubConnectionBuilder

import ha_strategy as S
import ha_entry_model as EM

load_dotenv()

# ── CLI ──────────────────────────────────────────────────────────────────────
# Safe-by-default: the bot is ALWAYS paper/DRY_RUN unless you explicitly pass
# --live. This is deliberate — a missing/forgotten flag should never be able
# to place real orders. To go live: `python ha_mes_bot.py --live`
_parser = argparse.ArgumentParser(description="HA NY/RTH bot (MES/ES)")
_parser.add_argument("--live", action="store_true",
                      help="Place REAL orders on the broker. Omit this to run in "
                           "DRY_RUN/paper mode (default).")
_args = _parser.parse_args()

# ── Config ───────────────────────────────────────────────────────────────────
DRY_RUN      = not _args.live  # ← True unless launched with --live
BE_ENABLED   = True            # ← False = pure fixed bracket (no breakeven move)
# Skip the breakeven move entirely on trades whose REWARD is under this many points.
# Reasoning: BE_MIN_TRIGGER_PTS is 1.5, so on a 2.0pt target breakeven arms 75% of the
# way to the target and any wobble scratches a trade that was nearly paid.
#
# NOTE, and this matters: S.MIN_TP_POINTS is also 2.0, so a target is never under 2.0
# points to begin with. With CHASE_AFTER_SEC = 0 the fill is always AT the limit price,
# so the reward cannot shrink below the floor either -- which makes this gate INERT at
# 2.0. Verified on tick data: be_min_tp 2.0 returns numbers identical to BE always on.
# It only bites at a higher threshold. Measured, no chase, limit valid 240s:
#            6/15-7/21        4/22-5/21
#   off       -$408            -$1,020
#   2.0pt     -$408            -$1,020   <- identical, as expected
#   2.5pt     -$367            -$1,012
#   3.0pt     -$232            -$1,012
#   4.0pt     -$149              -$821
# Set to 3.0 or 4.0 if the intent is to actually stop BE scratching small targets.
BE_MIN_TP_PTS = 2.0
# Keep the stop and target at their ABSOLUTE structural prices (prior-bar extreme and
# wick-cluster key level) instead of letting the broker anchor them to the actual fill.
# The bracket API takes TICK DISTANCES, so a slipped fill slides both exits with it —
# but those levels are set by market structure and do not move because our entry was a
# tick off. Backtest at 2-tick slippage (6/16-7/21): fill-anchored exits cost $479 (28%)
# and 5.2pts of win rate vs. absolute. False = old fill-anchored behavior.
BRACKET_ABSOLUTE = True

# ── ENTRY STYLE ──────────────────────────────────────────────────────────────
# "limit"  — rest a LIMIT at the extreme of the last CONSOL_WINDOW_SEC of the signal
#            bar and only trade if price comes BACK to it. This is the validated
#            mechanism: 26 tick-days 6/16-7/21 gave PF 2.60, +$2,836, max DD -$185
#            even when price is required to trade a full tick THROUGH the level.
# "market" — take a market order at the bar close. Tested at PF 0.73 / -$1,022 on
#            the same data: the strategy LOSES MONEY entered this way. Do not use.
ENTRY_STYLE      = "limit"
# Cancel the resting entry if unfilled after this long. Cut 360 -> 240 (4 min) on
# 2026-08-13 at Ruth's request. Measured cost of the shorter window, no chase,
# 6/15-7/21: -$210 at 360s vs -$408 at 240s (167 fills vs 163). Fill timings say why:
# only 56% of limits that ever fill do so within 60s, and 23% take longer than 2 min.
LIMIT_VALID_SEC  = 240
# CHASE: if the limit has not filled after this many seconds, take the trade at MARKET
# instead of losing it.
#
# RE-ENABLED 2026-08-13. It was switched off on 08-12 on the reasoning that the
# original $9,776 model never chased. That reasoning does not hold: that model did
# not need to chase because it assumed a fill at a price only knowable AFTER the bar
# closed. A real resting limit misses precisely the signals where price runs away --
# and those are the winners. Of the 62 signals whose limit never filled in the
# 6/16-7/21 sample, 58 were winners.
#
# Measured across all 48 tick days (4/22-5/21 and 6/15-7/21), MES x3, 1-tick chase
# slippage, exits resolved from the fill:
#     chase 30s : 310 trades  PF 0.98   -$106
#     no chase  : 269 trades  PF 0.80 -$1,254
# DISABLED AGAIN 2026-08-13 at Ruth's request. Recording the cost so the trade-off
# stays visible: measured on all 48 tick days the chase was worth about $1,100
# (chase 30s -$106 vs no chase -$1,254), and a sweep of the delay confirmed 30s was
# the best value tested (45s -$248, 60s -$371, 90s -$663). Set back to 30 to re-enable.
CHASE_AFTER_SEC  = 0

# ── SHADOW LOGGING (measurement only — changes NOTHING about trading) ─────────
# The offset-entry backtest (rest the limit SHADOW_OFFSET_PTS better than the price
# at the bar close, chase at market if unfilled) tested at PF 8.24 / +$5,090 / DD
# -$108 on 26 tick-days, vs PF 2.80 / +$3,455 / -$189 for the entry we actually
# trade. The entire difference rests on one unverified claim: that a limit resting
# 3 points from market FILLS 54% OF THE TIME. That is a microstructure question a
# backtest cannot answer, so this records where such an order WOULD have sat and
# whether price ever reached it. No order is sent. Nothing about the live trade
# changes. Writes ha_shadow_log.csv.
SHADOW_LOG        = True
# Retained only so an old log can still be read; the shadow no longer uses a flat
# offset. See _open_shadow for why it now records the ATR-scaled model level.
SHADOW_OFFSET_PTS = 3.0
SHADOW_VALID_SEC  = 360
LIMIT_WINDOW_SEC = 60      # seconds before the bar close that define the entry level

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

SHADOW_LOG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ha_shadow_log.csv")
_SHADOW_COLS = ["date", "signal_time", "dir", "close_px", "live_level", "shadow_level",
                "atr", "pred_depth", "offset_pts", "shadow_filled", "shadow_wait_s",
                "stop", "target", "shadow_risk_pts", "shadow_rew_pts"]

# ── Logging — same setup as choch_bot_live.py, which runs clean on this machine ──
# The Windows console defaults to cp1252, which cannot encode characters outside
# Latin-1. Any non-ASCII in a log line (the old heartbeat used a heart glyph) makes
# logging raise UnicodeEncodeError on EVERY emit and dump a traceback instead of the
# line. Two fixes, both needed: reconfigure the console to UTF-8, and open the log
# FILE with an explicit encoding (FileHandler otherwise uses the same cp1252 locale
# default). Log strings are also kept ASCII-only so redirected output can't reintroduce
# the problem.
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

# Absolute path so the log lands next to the script no matter what directory the bot
# is launched from — ha_watchdog.py looks for it there.
LOG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ha_mes_bot.log")

# A dedicated, non-propagating logger rather than logging.basicConfig(): basicConfig()
# only takes effect on the FIRST call in a process, so if any imported module calls it
# first, these settings are silently ignored and the lines ride on that module's handler.
log = logging.getLogger("ha_mes")
log.setLevel(logging.INFO)
log.propagate = False
if not log.handlers:
    _formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    _fh = logging.FileHandler(LOG_PATH, encoding="utf-8")
    _fh.setFormatter(_formatter)
    _sh = logging.StreamHandler(stream=sys.stdout)
    _sh.setFormatter(_formatter)
    log.addHandler(_fh)
    log.addHandler(_sh)

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

    async def send(self, session, text, prefix=""):
        """Fire-and-forget alert. Never raises. `prefix` is a short bracketed tag
        ([ENTRY], [EXIT], [SUMMARY]...) so the message is scannable on a phone
        lock screen — same convention as choch_bot_live.py."""
        if not self.enabled:
            return
        msg = f"{prefix} {text}".strip()
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
        self.hist1m = deque(maxlen=60)                    # closed 1-min OHLC, for ATR

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
        # Keep a short 1-min OHLC history so ATR can be computed live. Only the
        # last ~60 bars are needed for a 14-period ATR; the deque discards the
        # rest so this cannot grow across a long session.
        self.hist1m.append({"h": b["h"], "l": b["l"], "c": b["c"]})

    def atr_1min(self, period=14):
        """14-bar ATR from closed 1-min bars, or None until enough have closed."""
        return EM.atr_from_bars(list(self.hist1m), period)

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

    def seed_vwap(self, typical_price, volume):
        """Add one pre-RTH 1-min bar to the VWAP accumulator ONLY — no HA chain, no
        wick levels, no 2-min bars. Used for the CME (15:00 PT prior day) anchor so
        the VWAP line matches the TopstepX chart, while the Heikin-Ashi chain and the
        wick-cluster target levels stay strictly RTH as the strategy was validated."""
        if volume <= 0:
            return
        self.vwap_num += typical_price * volume
        self.vwap_den += volume
        self.vwap = self.vwap_num / self.vwap_den

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
        self.entry_theo = None
        self.be_trigger = self.be_stop = None
        self.be_armed = False
        self.be_active = True      # False when reward < BE_MIN_TP_PTS for this trade
        self.entry_oid = None
        self.pending = None        # resting entry limit awaiting a fill (see _check_pending)
        self.shadows = []          # measurement-only offset limits (see _update_shadows)
        self.stop_child_id = None
        self.trades_today = 0
        self.consec_losses = 0
        self.daily_pnl = 0.0
        # per-day tallies for the [SUMMARY] alert at session close
        self.day_wins = 0
        self.day_losses = 0
        self.day_scratches = 0
        self.day_fees = 0.0
        self.summary_sent = False
        self.limit_alerted = False
        self.session_date = None
        self.seeded_date = None
        self.entry_time_str = ""
        self.notifier = TelegramNotifier(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID)

    async def _alert(self, session, msg, prefix=""):
        """Fire-and-forget Telegram push; never raises into the trading path."""
        try:
            await self.notifier.send(session, msg, prefix=prefix)
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
    async def _place_limit_bracket(self, session, action, limit_price, stop_ticks, target_ticks):
        """Resting LIMIT entry with stop/target brackets attached.

        A limit entry also fixes the bracket-anchoring problem for free: the fill
        price IS the limit price, so the tick distances resolve to exactly the
        structural stop and target with no re-anchoring needed."""
        side = 0 if action == "buy" else 1
        if abs(stop_ticks) < 4:
            log.error(f"REFUSE: stop {abs(stop_ticks)}t < 4t broker min."); return None
        if (stop_ticks > 0) == (target_ticks > 0):
            log.error("REFUSE: stop/target share a sign."); return None
        payload = {"accountId": self.account_id, "contractId": self.contract_id,
                   "type": ORDER_TYPE_LIMIT, "side": side, "size": CONTRACTS,
                   "limitPrice": round(limit_price, 2), "stopPrice": None, "trailPrice": None,
                   "customTag": "ha_entry", "linkedOrderId": None,
                   "stopLossBracket": {"ticks": int(stop_ticks), "type": ORDER_TYPE_STOP},
                   "takeProfitBracket": {"ticks": int(target_ticks), "type": ORDER_TYPE_LIMIT}}
        if DRY_RUN:
            log.info(f"[DRY] resting LIMIT {action} {CONTRACTS}x {INSTRUMENT} @ {limit_price} "
                     f"stop={stop_ticks}t tgt={target_ticks}t (valid {LIMIT_VALID_SEC}s)")
            return "DRY"
        r = await session.post(f"{BASE_URL}/Order/place", headers=self._hdr(), json=payload)
        d = await r.json()
        if not d.get("success"):
            log.error(f"Limit entry rejected: {d}"); return None
        return d.get("orderId")

    async def _check_pending(self, session, last_price, now):
        """Watch a resting entry limit: promote it to a live trade when it fills, or
        cancel it once it expires. In DRY_RUN the fill is simulated the moment a tick
        trades at or through the level."""
        p = self.pending
        if not p:
            return
        is_long = p["dir"] == "long"

        age = (now - p["placed"]).total_seconds()

        # ---- CHASE: unfilled limit -> market entry ----
        if CHASE_AFTER_SEC and not p.get("chased") and age >= CHASE_AFTER_SEC:
            p["chased"] = True
            if not DRY_RUN and p["oid"]:
                await self._cancel_order(session, p["oid"])   # pull the resting limit first
            stop_pts = abs(p["entry"] - p["stop"]); tgt_pts = abs(p["target"] - p["entry"])
            st_t = round(stop_pts / S.TICK); tg_t = round(tgt_pts / S.TICK)
            oid, fill = await self._place_bracket_entry(
                session, "buy" if is_long else "sell",
                -st_t if is_long else st_t, tg_t if is_long else -tg_t)
            if oid is None:
                log.warning("Chase market entry failed - dropping the setup.")
                self.pending = None
                return
            px = fill if fill else (last_price if last_price is not None else p["entry"])
            self.in_trade = True; self.direction = p["dir"]
            self.entry_theo = p["entry"]; self.entry = px
            self.stop = p["stop"]; self.target = p["target"]
            self.be_trigger, self.be_stop = S.breakeven_stop(px, p["target"], is_long)
            self.be_active = abs(p["target"] - px) >= BE_MIN_TP_PTS
            self.be_armed = False; self.entry_oid = oid
            self.stop_child_id = None if DRY_RUN else await self._find_stop_child(session)
            self.trades_today += 1
            self.entry_time_str = now.strftime("%H:%M")
            self.pending = None
            if BRACKET_ABSOLUTE and not DRY_RUN:
                if not await self._align_brackets(session, fill):
                    return
            log.info(f"CHASED {p['dir'].upper()} at market @ {px} after {CHASE_AFTER_SEC}s "
                     f"(limit {p['entry']} never filled) | stop {p['stop']} target {p['target']}")
            await self._alert(session,
                f"{'BUY' if is_long else 'SELL'} {CONTRACTS} {INSTRUMENT} @ {px}"
                f"{' [DRY]' if DRY_RUN else ''} - CHASED\n"
                f"Limit {p['entry']} never filled in {CHASE_AFTER_SEC}s, took it at market.\n"
                f"Stop {p['stop']} ({abs(px-p['stop']):.2f}pt) / "
                f"Target {p['target']} ({abs(p['target']-px):.2f}pt)\n"
                f"BE arms at {self.be_trigger}\n"
                f"Trade {self.trades_today}/{MAX_TRADES_PER_DAY} today | "
                f"day P&L ${self.daily_pnl:+,.2f}", prefix="[ENTRY-CHASE]")
            return

        if age >= LIMIT_VALID_SEC:
            if not DRY_RUN and p["oid"]:
                await self._cancel_order(session, p["oid"])
            log.info(f"Entry limit @ {p['entry']} EXPIRED unfilled after {LIMIT_VALID_SEC}s - level dead.")
            await self._alert(session,
                f"{p['dir'].upper()} setup expired - price never returned to {p['entry']}. "
                f"No trade taken.", prefix="[NO FILL]")
            self.pending = None
            return

        filled = False
        fill_px = None
        if DRY_RUN:
            if last_price is not None:
                filled = (last_price <= p["entry"]) if is_long else (last_price >= p["entry"])
                fill_px = p["entry"]          # a limit fills at its price, never worse
        else:
            st, fp = await self._order_status(session, p["oid"])
            if st == "rejected":
                log.error("Resting entry was REJECTED by the broker."); self.pending = None; return
            filled = (st == "filled")
            # Use the ACTUAL fill. A limit can fill BETTER than its price if the market
            # gaps through it - and when it does the broker anchors the bracket children
            # to that real fill, sliding stop and target off the structural levels.
            fill_px = float(fp) if (filled and fp) else p["entry"]
        if not filled:
            return

        self.in_trade = True; self.direction = p["dir"]
        self.entry_theo = p["entry"]
        self.entry = fill_px if fill_px else p["entry"]
        self.stop = p["stop"]; self.target = p["target"]
        # BE is measured from the REAL entry, not the level we asked for.
        self.be_trigger, self.be_stop = S.breakeven_stop(self.entry, p["target"], is_long)
        self.be_active = abs(p["target"] - self.entry) >= BE_MIN_TP_PTS
        self.be_armed = False; self.entry_oid = p["oid"]
        self.stop_child_id = None if DRY_RUN else await self._find_stop_child(session)
        self.trades_today += 1
        self.entry_time_str = now.strftime("%H:%M")
        waited = (now - p["placed"]).total_seconds()
        self.pending = None

        # Hold stop/target at the structural prices regardless of where we filled.
        if BRACKET_ABSOLUTE and not DRY_RUN:
            if not await self._align_brackets(session, self.entry):
                return

        imp = ""
        if abs(self.entry - p["entry"]) >= S.TICK:
            imp = (f"\nFilled {abs(self.entry-p['entry'])/S.TICK:.0f}t BETTER than the "
                   f"{p['entry']} limit (exits held at the key levels)")
        log.info(f"ENTRY FILLED {p['dir'].upper()} @ {self.entry} after {waited:.0f}s | "
                 f"stop {p['stop']} target {p['target']}")
        await self._alert(session,
            f"{'BUY' if is_long else 'SELL'} {CONTRACTS} {INSTRUMENT} @ {self.entry}"
            f"{' [DRY]' if DRY_RUN else ''}\n"
            f"Stop {p['stop']} ({abs(self.entry-p['stop']):.2f}pt) / "
            f"Target {p['target']} ({abs(p['target']-self.entry):.2f}pt)"
            f"{imp}\n"
            f"Limit filled after {waited:.0f}s\n"
            f"BE arms at {self.be_trigger}\n"
            f"Trade {self.trades_today}/{MAX_TRADES_PER_DAY} today | "
            f"day P&L ${self.daily_pnl:+,.2f}", prefix="[ENTRY]")

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

    async def _place_limit(self, session, action, limit_price):
        """Standalone TAKE-PROFIT limit at an absolute price (used to re-anchor the
        target to the wick-cluster key level after a slipped fill)."""
        side = 0 if action == "buy" else 1     # action = side that CLOSES the position
        payload = {"accountId": self.account_id, "contractId": self.contract_id,
                   "type": ORDER_TYPE_LIMIT, "side": side, "size": CONTRACTS,
                   "limitPrice": round(limit_price, 2), "stopPrice": None, "trailPrice": None,
                   "customTag": "ha_tp_abs", "linkedOrderId": None}
        if DRY_RUN:
            log.info(f"[DRY] place TP limit {action} {CONTRACTS}x @ {limit_price}"); return "DRY"
        r = await session.post(f"{BASE_URL}/Order/place", headers=self._hdr(), json=payload)
        d = await r.json()
        if not d.get("success"):
            log.error(f"TP limit place failed: {d}"); return None
        return d.get("orderId")

    async def _find_child(self, session, order_type):
        """Locate a resting bracket child of the given type (STOP or LIMIT)."""
        try:
            start = (datetime.now(CT) - timedelta(minutes=15)).isoformat()
            r = await session.post(f"{BASE_URL}/Order/search", headers=self._hdr(),
                                   json={"accountId": self.account_id, "startTimestamp": start})
            for o in (await r.json()).get("orders", []):
                if (o.get("contractId") == self.contract_id and o.get("type") == order_type
                        and o.get("status") in (0, 1, 6)):        # working/accepted
                    return o.get("id")
        except Exception as e:
            log.warning(f"child lookup failed: {e}")
        return None

    async def _align_brackets(self, session, fill):
        """Re-anchor the broker's stop/target to the ABSOLUTE structural levels.

        WHY: the bracket children are submitted as TICK DISTANCES, so the broker
        anchors them to the actual fill. If the fill slips, both exits slide with it —
        but the prior-bar extreme and the wick-cluster level are fixed prices set by
        market structure, and they do not move just because our entry was a tick or
        two off. Backtest at 2-tick slippage, 6/16-7/21: leaving them fill-anchored
        cost $479 (28%) and 5.2 points of win rate, most of it from the STOP sitting
        tighter than the structural invalidation and getting hit on noise.

        Returns True if the position is still safely protected.

        ORDERING IS DELIBERATE: cancel-then-replace the stop (never place a second
        stop first — two working stops of full size would flip the position short if
        both triggered). If the replacement stop fails to place we FLATTEN rather
        than sit unprotected. The target is adjusted after the stop is secure; a
        failed target leaves the trade stop-protected, which is degraded but safe.

        NOTE: this uses the same cancel/replace mechanic as the breakeven move, which
        is STILL UNVERIFIED against a real account (see the file header). Verify both
        on the eval account before running funded."""
        if DRY_RUN or fill is None:
            return True
        drift = abs(fill - self.entry_theo)
        if drift < S.TICK:
            return True                      # filled where we expected — nothing to fix
        close_action = "sell" if self.direction == "long" else "buy"
        log.info(f"Fill slipped {drift:.2f}pt ({drift/S.TICK:.0f}t) from {self.entry_theo} to {fill} "
                 f"- re-anchoring brackets to stop {self.stop} / target {self.target}")

        # ---- STOP: cancel, then replace at the absolute prior-bar extreme ----
        old_stop = await self._find_child(session, ORDER_TYPE_STOP)
        if old_stop and not await self._cancel_order(session, old_stop):
            log.warning("Could not cancel the fill-anchored stop - leaving brackets as the broker set them.")
            return True                      # still protected by the original stop
        new_stop = await self._place_stop(session, close_action, self.stop)
        if not new_stop:
            log.error("CRITICAL: replacement stop FAILED to place after cancelling the old one - "
                      "position is unprotected. Flattening now.")
            await self._flatten(session, "no protective stop after bracket re-anchor")
            self._go_flat()
            await self._alert(session,
                f"Could not re-place the protective stop after a slipped fill, so the bot "
                f"FLATTENED the position immediately rather than sit unprotected. "
                f"Check the account.", prefix="[ERROR]")
            return False
        self.stop_child_id = new_stop

        # ---- TARGET: cancel, then replace at the absolute key level ----
        old_tp = await self._find_child(session, ORDER_TYPE_LIMIT)
        if old_tp and not await self._cancel_order(session, old_tp):
            log.warning("Could not cancel the fill-anchored target - it stays where the broker put it.")
            return True
        if not await self._place_limit(session, close_action, self.target):
            log.warning("Replacement target FAILED to place. Position is still stop-protected, "
                        "but there is no resting take-profit - it will ride to the stop or the "
                        "session-close flatten.")
            await self._alert(session,
                f"Re-anchored the stop after a slipped fill, but the take-profit failed to "
                f"re-place. Trade is stop-protected with NO target. Check the account.",
                prefix="[ALERT]")
        return True

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

        # Optional CME-anchored VWAP: accumulate 15:00 PT (prior day) -> 06:30 into the
        # VWAP only, so the line already has a value at the open and matches the TopstepX
        # chart. HA chain / wick levels are deliberately NOT fed from this window.
        pre_n = 0
        if S.VWAP_ANCHOR == "CME":
            cme_start = (datetime.combine(day, S.CME_OPEN, tzinfo=PT) - timedelta(days=1)).astimezone(CT)
            rth_start = datetime.combine(day, S.SESSION_OPEN, tzinfo=PT).astimezone(CT)
            try:
                pre = await self._fetch_1m(session, cme_start, rth_start)
                for m in pre:
                    if m["ts"].time() >= S.SESSION_OPEN and m["ts"].date() == day:
                        continue                      # RTH bars are handled below
                    self.bars.seed_vwap((m["h"] + m["l"] + m["c"]) / 3.0, m["v"])
                    pre_n += 1
                log.info(f"VWAP anchored at CME open 15:00 PT: seeded {pre_n} overnight "
                         f"1-min bars, VWAP={self.bars.vwap}")
            except Exception as e:
                log.warning(f"overnight VWAP seed failed ({e}) - falling back to an RTH-anchored VWAP")

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
        log.info(f"Seeded RTH {day}: {n} 1-min bars (VWAP anchor {S.VWAP_ANCHOR}), VWAP={self.bars.vwap}, "
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
        if BE_ENABLED and self.be_active and not self.be_armed:
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
            self._tally(net)
            entry_px, direction = self.entry, self.direction
            word = {"target": "Target hit", "be": "Breakeven stop",
                    "stop": "Stopped out", "flat_eod": "Session-close flat"}.get(reason, reason)
            log.info(f"[PAPER] {reason.upper()} @ {xp} | {pts:+.2f}pt ${net:+.0f} | day ${self.daily_pnl:+.0f}")
            self._go_flat()
            await self._alert(session,
                f"{word} | ${net:+,.2f} (after ${FEE_PER_RT*CONTRACTS:.2f} fees)\n"
                f"{direction.upper()} {entry_px} -> {xp}  ({pts:+.2f}pt)\n"
                f"Day so far: ${self.daily_pnl:+,.2f} | "
                f"trades {self.trades_today}/{MAX_TRADES_PER_DAY}",
                prefix="[EXIT-DRY]")

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
        self.entry_theo = None
        self.be_trigger = self.be_stop = None; self.be_armed = False
        self.be_active = True
        self.entry_oid = self.stop_child_id = None

    # ---- shadow logging: measurement only, never touches an order ----
    def _open_shadow(self, sig, close_px, stop, target, now):
        """Record where the PREDICTED-DEPTH entry limit would have rested.

        CHANGED 2026-08-13. This used to log a flat SHADOW_OFFSET_PTS (3.0) better
        than the close. Two reasons that was measuring the wrong thing:

          1. A flat 3-point offset is un-tradeable about half the time. The median
             stop on this strategy is 4.25pt, so moving the entry 3pt better leaves
             44.9% of signals with a stop under the broker's 4-tick minimum, and
             30.2% with the entry on the far side of the stop entirely. The first
             shadow row ever written (08-13, short 7808.75 vs stop 7809.50) had a
             0.75pt stop -- 3 ticks -- and could never have been placed.
          2. The flat-offset variant has already been tested and discarded. The
             candidate actually worth forward-testing is the volatility-scaled
             level: depth ~ 1.22 * ATR14, entered at ALPHA of that.

        So the shadow now records the model level, skips any signal where it breaks
        the 4-tick minimum, and logs the ATR that produced it so the prediction can
        be scored against what price actually did. Still measurement only -- no
        order is placed and nothing about the live trade changes."""
        if not SHADOW_LOG or close_px is None:
            return
        try:
            is_long = sig == "long"
            atr = self.bars.atr_1min()
            lvl = EM.entry_level(close_px, atr, is_long)
            if lvl is None:
                log.info(f"[SHADOW] skipped - ATR unusable ({atr}) so no model level.")
                return
            # Same guard the live order path applies: a bracket inside 4 ticks is
            # rejected by the broker, so logging it would measure a fill that could
            # never have been traded.
            if abs(lvl - stop) < 4 * S.TICK:
                log.info(f"[SHADOW] skipped - model level {lvl} leaves only "
                         f"{abs(lvl-stop):.2f}pt to the stop ({stop}), under the 4-tick minimum.")
                return
            self.shadows.append({"date": str(self.session_date), "signal_time": now.strftime("%H:%M:%S"),
                                 "dir": sig, "close_px": round(close_px, 2),
                                 "live_level": round(self.pending["entry"], 2) if self.pending else "",
                                 "shadow_level": lvl, "placed": now,
                                 "atr": round(atr, 2),
                                 "pred_depth": round(EM.predicted_depth(atr), 2),
                                 "offset_used": round(abs(close_px - lvl), 2),
                                 "stop": stop, "target": target})
        except Exception as e:
            log.warning(f"shadow open failed ({e}) - trading unaffected")

    def _update_shadows(self, last_price, now):
        """Check every pending shadow level against the live tape. Pure measurement:
        wrapped so a failure here can never reach the trading path."""
        if not self.shadows:
            return
        try:
            still = []
            for sh in self.shadows:
                age = (now - sh["placed"]).total_seconds()
                hit = False
                if last_price is not None:
                    hit = (last_price <= sh["shadow_level"]) if sh["dir"] == "long" \
                          else (last_price >= sh["shadow_level"])
                if hit:
                    self._write_shadow(sh, True, age)
                elif age >= SHADOW_VALID_SEC:
                    self._write_shadow(sh, False, age)
                else:
                    still.append(sh)
            self.shadows = still
        except Exception as e:
            log.warning(f"shadow update failed ({e}) - trading unaffected")
            self.shadows = []

    def _write_shadow(self, sh, filled, wait_s):
        try:
            lvl = sh["shadow_level"]
            row = {"date": sh["date"], "signal_time": sh["signal_time"], "dir": sh["dir"],
                   "close_px": sh["close_px"], "live_level": sh["live_level"], "shadow_level": lvl,
                   "atr": sh.get("atr", ""), "pred_depth": sh.get("pred_depth", ""),
                   "offset_pts": sh.get("offset_used", ""), "shadow_filled": int(filled),
                   "shadow_wait_s": round(wait_s, 1), "stop": sh["stop"], "target": sh["target"],
                   "shadow_risk_pts": round(abs(lvl - sh["stop"]), 2),
                   "shadow_rew_pts": round(abs(sh["target"] - lvl), 2)}
            # The shadow schema changed on 2026-08-13 (atr / pred_depth added). A file
            # written under the OLD header would silently take rows with more fields
            # than the header declares, giving a CSV that reads back with shifted
            # columns -- and the entire point of this file is to be analysable later.
            # So if the header on disk does not match, rotate the old file aside and
            # start a clean one rather than corrupting either.
            new = not os.path.exists(SHADOW_LOG_PATH)
            if not new:
                try:
                    with open(SHADOW_LOG_PATH, "r", encoding="utf-8") as fh:
                        hdr = fh.readline().strip().split(",")
                    if hdr and hdr != _SHADOW_COLS:
                        n = 1
                        while os.path.exists(f"{SHADOW_LOG_PATH}.old{n}"):
                            n += 1
                        os.rename(SHADOW_LOG_PATH, f"{SHADOW_LOG_PATH}.old{n}")
                        log.info(f"[SHADOW] schema changed - previous log moved to "
                                 f"{os.path.basename(SHADOW_LOG_PATH)}.old{n}, starting a fresh file.")
                        new = True
                except Exception as e:
                    log.warning(f"shadow log rotate check failed ({e}) - appending anyway")
            with open(SHADOW_LOG_PATH, "a", newline="", encoding="utf-8") as fh:
                wtr = csv.DictWriter(fh, fieldnames=_SHADOW_COLS)
                if new:
                    wtr.writeheader()
                wtr.writerow(row)
            log.info(f"[SHADOW] {sh['dir']} offset limit {lvl} "
                     f"{'FILLED after %.0fs' % wait_s if filled else 'never reached (%.0fs)' % wait_s}")
        except Exception as e:
            log.warning(f"shadow write failed ({e}) - trading unaffected")

    # ---- per-day tallies + end-of-session summary ----
    def _tally(self, net):
        """Book one closed trade into the day counters that feed the [SUMMARY] alert.
        A breakeven scratch (fees only) is counted separately from a real loss so the
        summary doesn't read as 10 losing trades when 8 of them were flat."""
        self.day_fees += FEE_PER_RT * CONTRACTS
        scratch_floor = FEE_PER_RT * CONTRACTS + 0.01
        if net > 0.01:
            self.day_wins += 1
        elif net >= -scratch_floor:
            self.day_scratches += 1
        else:
            self.day_losses += 1

    async def _send_session_summary(self, session):
        """One [SUMMARY] message at session close so you get the day's result without
        adding up individual alerts. Sent once per day; safe to call repeatedly."""
        if self.summary_sent:
            return
        self.summary_sent = True
        if self.trades_today == 0:
            await self._alert(session, f"Session Complete — {self.session_date}\n"
                                       f"No trades taken today.", prefix="[SUMMARY]")
            log.info("Session summary sent | no trades")
            return
        await self._alert(session,
            f"Session Complete — {self.session_date}{' [DRY]' if DRY_RUN else ''}\n"
            f"Trades: {self.trades_today}/{MAX_TRADES_PER_DAY}\n"
            f"Wins: {self.day_wins}\n"
            f"Losses: {self.day_losses}\n"
            f"Breakeven: {self.day_scratches}\n"
            f"Fees: -${self.day_fees:,.2f}\n"
            f"Net: ${self.daily_pnl:+,.2f}", prefix="[SUMMARY]")
        log.info(f"Session summary sent | trades={self.trades_today} W={self.day_wins} "
                 f"L={self.day_losses} BE={self.day_scratches} net=${self.daily_pnl:+,.2f}")

    def _reset_day(self, new_date):
        self.trades_today = 0; self.consec_losses = 0; self.daily_pnl = 0.0
        self.day_wins = 0; self.day_losses = 0; self.day_scratches = 0
        self.day_fees = 0.0; self.summary_sent = False; self.limit_alerted = False
        self.session_date = new_date

    # ---- signal handling on a closed 2-min bar ----
    async def _on_bar_close(self, session, closed_bar, now_pt):
        if self.in_trade or self.pending or self.bars.prev2 is None:
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
            if not self.limit_alerted:          # alert ONCE, not on every bar close
                self.limit_alerted = True
                await self._alert(session,
                    f"DAILY LOSS LIMIT hit: ${self.daily_pnl:+,.2f} "
                    f"(limit -${DAILY_LOSS_LIMIT:,.0f}). No more trades today.\n"
                    f"Trades taken: {self.trades_today}/{MAX_TRADES_PER_DAY}",
                    prefix="[SAFETY STOP]")
            return

        sig = S.entry_signal(self.bars.prev2, closed_bar, closed_bar["vwap"])
        if sig is None:
            return
        is_long = sig == "long"

        # entry = last-30s consolidation extreme (bottom for long / top for short),
        # read from the ROLLING recent-tick buffer (self.ticks is drained each loop and
        # would be empty here).
        win = LIMIT_WINDOW_SEC if ENTRY_STYLE == "limit" else S.CONSOL_WINDOW_SEC
        cutoff = datetime.now(PT) - timedelta(seconds=win)
        with self.tick_lock:
            recent = [p for (t, p) in self.recent if t >= cutoff]
        entry = S.consolidation_entry(recent, is_long)
        if entry is None:
            log.info(f"{sig.upper()} signal but no consolidation ticks — skip."); return

        stop = S.initial_stop(closed_bar["raw_low"], closed_bar["raw_high"], is_long)
        tips = self.bars.upper_wicks if is_long else self.bars.lower_wicks
        tgt = S.cluster_target(list(tips), entry, is_long)
        if tgt is None:
            log.info(f"{sig.upper()} signal but no key-level target >={S.MIN_TP_POINTS}pt - skip."); return
        level, target = tgt

        stop_pts = abs(entry - stop); tgt_pts = abs(target - entry)
        stop_ticks_mag = round(stop_pts / S.TICK)
        tgt_ticks_mag = round(tgt_pts / S.TICK)
        # signed ticks: long -> stop below (neg), target above (pos); short -> mirror
        stop_ticks = -stop_ticks_mag if is_long else stop_ticks_mag
        target_ticks = tgt_ticks_mag if is_long else -tgt_ticks_mag

        self.be_trigger, self.be_stop = S.breakeven_stop(entry, target, is_long)
        self.be_active = abs(target - entry) >= BE_MIN_TP_PTS
        log.info(f"SIGNAL {sig.upper()} | entry~{entry} stop {stop} tgt {target} "
                 f"(level {level}) | risk {stop_pts:.2f}pt reward {tgt_pts:.2f}pt "
                 f"| BE trigger {self.be_trigger}->{self.be_stop}")

        action = "buy" if is_long else "sell"

        if ENTRY_STYLE == "limit":
            # Rest the order at the consolidation extreme and WAIT for price to come
            # back. The level is already known (the window has closed), so this is a
            # real order sitting in the book — not an assumed fill.
            oid = await self._place_limit_bracket(session, action, entry, stop_ticks, target_ticks)
            if oid is None:
                return
            self.pending = {"oid": oid, "dir": sig, "entry": entry, "stop": stop,
                            "target": target, "be_trigger": self.be_trigger,
                            "be_stop": self.be_stop, "placed": now_pt, "chased": False}
            log.info(f"Resting {sig.upper()} limit @ {entry} | stop {stop} target {target} "
                     f"| valid {LIMIT_VALID_SEC}s")
            # measurement only — records where an offset limit WOULD have sat
            with self.tick_lock:
                _last = self.recent[-1][1] if self.recent else None
            self._open_shadow(sig, _last, stop, target, now_pt)
            await self._alert(session,
                f"{sig.upper()} setup - resting limit @ {entry}{' [DRY]' if DRY_RUN else ''}\n"
                f"Stop {stop} / Target {target}\n"
                f"Fills if price returns"+ (f", else market at {CHASE_AFTER_SEC}s." if CHASE_AFTER_SEC else f" within {LIMIT_VALID_SEC//60} min."),
                prefix="[SETUP]")
            return

        # ---- legacy market entry (validated at PF 0.73 — kept only for comparison) ----
        oid, fill = await self._place_bracket_entry(session, action, stop_ticks, target_ticks)
        if oid is None:
            return
        self.in_trade = True; self.direction = sig
        self.entry_theo = entry
        self.entry = fill if fill else entry
        self.stop = stop; self.target = target
        self.be_armed = False; self.entry_oid = oid
        self.stop_child_id = None if DRY_RUN else await self._find_stop_child(session)
        self.trades_today += 1
        self.entry_time_str = now_pt.strftime("%H:%M")
        if BRACKET_ABSOLUTE and not DRY_RUN:
            if not await self._align_brackets(session, fill):
                return
        slip_note = ""
        if fill and abs(fill - entry) >= S.TICK:
            slip_note = (f"\nSlipped {abs(fill-entry)/S.TICK:.0f}t from {entry} "
                         f"(exits held at the key levels)")
        await self._alert(session,
            f"{'BUY' if is_long else 'SELL'} {CONTRACTS} {INSTRUMENT} @ {self.entry}"
            f"{' [DRY]' if DRY_RUN else ''}\n"
            f"Stop {stop} ({abs(self.entry-stop):.2f}pt) / Target {target} ({abs(target-self.entry):.2f}pt)"
            f"{slip_note}\n"
            f"BE arms at {self.be_trigger}\n"
            f"Trade {self.trades_today}/{MAX_TRADES_PER_DAY} today | "
            f"day P&L ${self.daily_pnl:+,.2f}", prefix="[ENTRY]")

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
            if BE_ENABLED and self.be_active and not self.be_armed:
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
                self._tally(net)
                word = {"target": "Target hit", "stop/BE": "Stopped out / breakeven"}.get(reason, reason)
                log.info(f"Position closed ({reason}) | entry {self.entry} -> exit {exit_px} "
                         f"| {pts:+.2f}pt ${net:+.0f} | day ${self.daily_pnl:+.0f} consecL {self.consec_losses}")
                await self._alert(session,
                    f"{word} | ${net:+,.2f} (after ${FEE_PER_RT*CONTRACTS:.2f} fees)\n"
                    f"{direction.upper()} {self.entry} -> {exit_px}  ({pts:+.2f}pt)\n"
                    f"Day so far: ${self.daily_pnl:+,.2f} | "
                    f"trades {self.trades_today}/{MAX_TRADES_PER_DAY}",
                    prefix="[EXIT]")
            else:
                log.info(f"Position closed but exit fill unavailable. direction={direction}")
                await self._alert(session,
                    f"{direction.upper()} {INSTRUMENT} closed — but the bot couldn't capture the "
                    f"real fill price. Check TopstepX for the actual result.\n"
                    f"Trade {self.trades_today}/{MAX_TRADES_PER_DAY} today.",
                    prefix="[EXIT?]")
            self._go_flat()


    # ---- main loop ----
    async def run(self):
        if not USERNAME or not API_KEY:
            raise SystemExit("Set TOPSTEP_USERNAME / TOPSTEP_API_KEY in .env")
        log.info("=" * 60)
        log.info(f"  HA 2-MIN LIVE BOT - {INSTRUMENT} x{CONTRACTS}")
        log.info(f"  MODE      : {'PAPER (DRY_RUN - no orders)' if DRY_RUN else '*** LIVE - REAL ORDERS ***'}")
        if ENTRY_STYLE == "limit":
            log.info(f"  ENTRY     : LIMIT at the {LIMIT_WINDOW_SEC}s extreme, valid {LIMIT_VALID_SEC}s ({LIMIT_VALID_SEC//60} min)"
                     + (f", CHASE at market after {CHASE_AFTER_SEC}s" if CHASE_AFTER_SEC else ", no chase"))
        else:
            log.info(f"  ENTRY     : MARKET at bar close  <-- tested at PF 0.73, NOT recommended")
        log.info(f"  BRACKETS  : {'ABSOLUTE (held at key levels)' if BRACKET_ABSOLUTE else 'fill-anchored'}")
        log.info(f"  BREAKEVEN : {'BE@%d, min %.2fpt from entry, skipped when reward < %.2fpt' % (int(S.BE_TRIGGER_FRAC*100), S.BE_MIN_TRIGGER_PTS, BE_MIN_TP_PTS) if BE_ENABLED else 'OFF'}")
        log.info(f"  TARGET    : nearest wick cluster >= {S.MIN_TP_POINTS}pt | MIN_WICK {S.MIN_WICK}pt")
        log.info(f"  VWAP      : anchored {S.VWAP_ANCHOR}")
        log.info(f"  SHADOW    : {'ON, predicted-depth level %.2f x ATR (measurement only)' % (EM.ALPHA*EM.DEPTH_PER_ATR) if SHADOW_LOG else 'off'}")
        log.info(f"  GATES     : max {MAX_TRADES_PER_DAY} trades/day | daily loss limit ${DAILY_LOSS_LIMIT:.0f}")
        log.info("=" * 60)
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20)) as session:
            await self._auth(session)
            await self._get_account(session)
            await self._get_contract(session)
            self._open_market_hub()
            self.session_date = datetime.now(PT).date()
            await self._seed_session(session, self.session_date)   # VWAP/HA/wicks from 6:30->now
            self.seeded_date = self.session_date
            await self._alert(session,
                f"HA RTH bot started — {INSTRUMENT} x{CONTRACTS} | "
                f"BE@{int(S.BE_TRIGGER_FRAC*100)} {'ON' if BE_ENABLED else 'OFF'} | "
                f"{'PAPER (no orders)' if DRY_RUN else 'LIVE — REAL ORDERS'}\n"
                f"Session {S.SESSION_OPEN:%H:%M}-{S.SESSION_CLOSE:%H:%M} PT, "
                f"no new entries after {S.ENTRY_CUTOFF:%H:%M}.\n"
                f"Alerts are working. You'll get messages for entries, exits, the daily-loss "
                f"stop, and an end-of-session summary.", prefix="[STARTUP]")

            last_price = None
            last_hb = datetime.now(PT); tick_count = 0; last_mgmt = datetime.now(PT)
            try:
                while True:
                    now = datetime.now(PT)
                    # new day → reset + re-seed (so VWAP re-anchors at 6:30)
                    if now.date() != self.session_date:
                        # send yesterday's summary before wiping the counters (covers the case
                        # where the bot ran past midnight without hitting the SESSION_CLOSE branch)
                        await self._send_session_summary(session)
                        self._reset_day(now.date())
                        self.pending = None
                        self.shadows = []
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
                        if self.shadows:
                            self._update_shadows(last_price, now)
                        if self.pending:
                            await self._check_pending(session, last_price, now)
                        if DRY_RUN and self.in_trade:                 # paper-manage every tick
                            await self._paper_manage(session, price)
                        closed = self.bars.add_tick(ts_pt, price, size)
                        if closed is not None and not self.in_trade:
                            await self._on_bar_close(session, closed, ts_pt)

                    # LIVE: broker owns exits; poll at most ~1/sec to respect the rate limit
                    if not DRY_RUN and self.in_trade and (now - last_mgmt).total_seconds() >= 1.0:
                        await self._manage(session, last_price); last_mgmt = now

                    # force flat at session close
                    if now.time() >= S.ENTRY_CUTOFF and self.pending:
                        if not DRY_RUN and self.pending.get("oid"):
                            await self._cancel_order(session, self.pending["oid"])
                        log.info("Entry cutoff reached - cancelling the resting entry limit.")
                        self.pending = None

                    if now.time() >= S.SESSION_CLOSE and self.in_trade:
                        if DRY_RUN:
                            # BUGFIX: this path previously recorded the trade but never fed its P&L
                            # into daily_pnl, so an open-at-the-bell trade silently vanished from the
                            # day's total (and from the safety gates). Now tallied and alerted.
                            net, pts = self._record_trade("flat_eod", last_price, now.strftime("%H:%M"))
                            self.daily_pnl += net
                            self.consec_losses = 0 if net > 0 else self.consec_losses + 1
                            self._tally(net)
                            entry_px, direction = self.entry, self.direction
                            self._go_flat()
                            await self._alert(session,
                                f"Session-close flat | ${net:+,.2f} (after ${FEE_PER_RT*CONTRACTS:.2f} fees)\n"
                                f"{direction.upper()} {entry_px} -> {last_price}  ({pts:+.2f}pt)\n"
                                f"Day so far: ${self.daily_pnl:+,.2f} | "
                                f"trades {self.trades_today}/{MAX_TRADES_PER_DAY}",
                                prefix="[EXIT-DRY]")
                        else:
                            await self._flatten(session, "Session close force-flat"); self._go_flat()

                    # end-of-session summary (once per day, after the force-flat above)
                    # Summary once trading is DONE for the day: past the entry cutoff and
                    # flat. Previously this only fired at SESSION_CLOSE (13:00), so stopping
                    # the bot any time before that meant no summary ever arrived.
                    if (now.time() >= S.ENTRY_CUTOFF and not self.in_trade
                            and not self.pending and not self.summary_sent):
                        await self._send_session_summary(session)

                    # heartbeat every ~60s so you can see it's alive
                    if (now - last_hb).total_seconds() >= 60:
                        inwin = S.SESSION_OPEN <= now.time() <= S.ENTRY_CUTOFF
                        st = ("IN TRADE" if self.in_trade else "watching 6:30-12:30" if inwin else "idle (outside window)")
                        vw = f"{self.bars.vwap:.2f}" if self.bars.vwap else "-"
                        log.info(f"HB {now.strftime('%H:%M:%S')} | last {last_price} | {tick_count} ticks/min "
                                 f"| vwap {vw} | trades {self.trades_today} day ${self.daily_pnl:+.0f} | {st}")
                        last_hb = now; tick_count = 0

                    await asyncio.sleep(0.25)
            except (KeyboardInterrupt, asyncio.CancelledError):
                # Ctrl-C used to be caught at module level, outside this instance, so
                # the day's summary was never sent unless the bot happened to still be
                # running at SESSION_CLOSE. Send it on the way out.
                log.info("Stopping - sending session summary.")
                if not DRY_RUN and self.pending and self.pending.get("oid"):
                    await self._cancel_order(session, self.pending["oid"])
                    log.info("Cancelled the resting entry limit on shutdown.")
                try:
                    await self._send_session_summary(session)
                except Exception as e:
                    log.warning(f"summary on shutdown failed: {e}")
                raise


if __name__ == "__main__":
    try:
        asyncio.run(HAMesBot().run())
    except KeyboardInterrupt:
        log.info("Stopped by user.")
