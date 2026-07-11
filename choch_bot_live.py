"""
CHOCH/FVG Bot — LIVE trading version for TopstepX
==================================================
Drives CHOCHFVGEngine / CHOCHFVGReplayWrapper (choch_fvg_bot.py) against
real-time 1-minute ES bars from TopstepX, placing real market orders on
entry/exit signals. This imports the strategy engine directly — it does not
reimplement any CHOCH/FVG/fib/breakeven logic — so what trades live is
exactly what was backtested in choch_databento_replay.py.

Architecture (auth, contract search, order placement, fill confirmation,
watchdog, daily reset) is mirrored from topstep_bot_v5.py's proven Dual Trail
live loop, adapted to CHOCH's simpler single-session structure:
  - No LevelEngine / 15-min lookback — CHOCH builds its structure purely from
    the 1-min candle stream itself. One fresh CHOCHFVGReplayWrapper per
    trading day (matches the engine's own "one instance per day" design).
  - No Session 2 — continuous monitoring from SESSION_OPEN (6:30 AM PT)
    through FORCE_FLAT_TIME (1:10 PM PT). New ENTRIES only happen 6:30-8:00
    AM PT — that gating is already inside CHOCHFVGReplayWrapper.add_candle,
    not duplicated here.
  - Per-trade/day risk gating (MAX_TRADES_PER_DAY, MAX_CONSECUTIVE_LOSSES,
    DAILY_LOSS_LIMIT, ATR filter) is already enforced INSIDE CHOCHFVGEngine —
    this loop doesn't re-check any of it, it just acts on whatever
    add_candle() returns (None = do nothing, entry/exit = place an order).

READ BEFORE RUNNING WITH REAL CAPITAL:
  - INSTRUMENT is force-set to "ES" at the top of this file, regardless of
    what's in your .env. This strategy's risk constants (CONTRACTS=1,
    MAX_TRADE_RISK_DOLLARS=75, MAX_STOP_POINTS=1.5) are calibrated
    specifically for ES's $50/point. If your .env is shared with
    topstep_bot_v5.py and set to MES, that's fine for that bot — it would
    silently break this one, so this file overrides it locally.
  - Orders are MARKET orders (type=2) for both entry and exit. A LIMIT-order
    entry approach was tried and reverted: it fixed the chasing problem on
    paper but introduced a worse one — it filters on whether price returns to
    the entry level after the signal, which turned out to correlate with
    LOSING trades, not fast ones (backtested: 61 of 77 filtered trades would
    have been winners). The actual fix is MIN_STOP_POINTS=0.25 in
    choch_fvg_bot.py — a floor that rejects setups too tight to survive
    execution chasing (confirmed: July 8, 0.50pt risk, kept 87.5% of
    theoretical edge after a real chase; July 9, 0.12pt risk, kept 0%) without
    conditioning on anything that happens after the signal fires. CHOCH's
    remaining stops are still tight — watch early fills against what the log
    says the signal price was, same as always.
  - Like topstep_bot_v5.py, this bot assumes it starts each day FLAT and
    does not reconcile against an existing open broker position on restart.
    Start it fresh each morning before 6:30 AM PT. Don't restart it mid-trade.
  - Uses its own log/analytics files (choch_bot.log, choch_trades.json) so
    it won't collide with Dual Trail's bot.log/trades.json even run from the
    same folder. But if both bots trade the SAME TopstepX account at the
    same time, they compound risk against shared daily-loss/margin limits —
    know that before running both together.
  - Run with --dry-run first. It authenticates and polls real bars, and logs
    every decision the strategy makes, but never calls Order/place. No real
    orders happen. Strongly recommended before pointing this at real capital,
    especially given the margin-rejection incident already seen on this
    account with the Dual Trail bot.

Requirements:
  pip install aiohttp numpy python-dotenv

Setup:
  .env in the same folder, with:
    TOPSTEP_USERNAME=your_username
    TOPSTEP_API_KEY=your_api_key

Usage:
  python choch_bot_live.py             # LIVE — places real orders
  python choch_bot_live.py --dry-run   # paper mode — logs signals, no orders
"""

import asyncio
import json
import logging
import os
import sys
import threading
from datetime import datetime, time, timedelta

# Force ES regardless of .env — see docstring above. Must happen before
# topstep_bot_v5 is imported, since POINT_VALUE/CONTRACTS/MES_SYMBOL are
# derived from this env var at import time.
os.environ["INSTRUMENT"] = "ES"

from dotenv import load_dotenv
load_dotenv()

from topstep_bot_v5 import Candle, PT, CT, BASE_URL, USERNAME, API_KEY, MES_SYMBOL, INSTRUMENT, POINT_VALUE
from choch_fvg_bot import (
    CHOCHFVGReplayWrapper, SESSION_OPEN, SESSION_CLOSE, FORCE_FLAT_TIME,
    CONTRACTS, RR_MULTIPLE, EARLY_BREAKEVEN_PCT, MAX_STOP_POINTS, MIN_STOP_POINTS,
    MAX_TRADE_RISK_DOLLARS, MAX_TRADES_PER_DAY, DAILY_LOSS_LIMIT, MAX_CONSECUTIVE_LOSSES,
    USE_BREAKEVEN,
)

# ── Bracket-order execution ─────────────────────────────────────────────────
# When True (the default), the ENTRY market order carries a broker-side
# stopLossBracket (type 4 = Stop) and takeProfitBracket (type 1 = Limit),
# confirmed supported by the TopstepX/ProjectX Order/place API. The exchange
# then manages BOTH exits off live ticks — no polling-loop lag, and crucially
# the stop is a real resting stop, not a market order the bot fires late into a
# fast move (which cost -$325 on July 10 2026). This is the whole point of the
# rewrite. Requires USE_BREAKEVEN=False in choch_fvg_bot.py — a static bracket
# can't do a mid-trade breakeven move, and we deliberately dropped breakeven
# after it caused that loss. Guarded below so the two can't silently conflict.
#
# When False, falls back to the legacy behavior: engine detects stop/target on
# closed candles and the bot fires a market exit. Kept as an escape hatch in
# case bracket orders misbehave live (e.g. account restriction we didn't foresee).
USE_BRACKET_ORDERS = True

TICK_SIZE = 0.25          # ES tick
ORDER_TYPE_LIMIT = 1
ORDER_TYPE_MARKET = 2
ORDER_TYPE_STOP = 4       # per ProjectX Order/place docs: type 4 = Stop

if USE_BRACKET_ORDERS and USE_BREAKEVEN:
    raise SystemExit(
        "CONFIG CONFLICT: USE_BRACKET_ORDERS=True requires USE_BREAKEVEN=False in "
        "choch_fvg_bot.py. A broker-side static bracket cannot perform the mid-trade "
        "breakeven stop-move; the breakeven mechanism was intentionally removed after it "
        "caused a market-order-chase loss. Set USE_BREAKEVEN=False, or set "
        "USE_BRACKET_ORDERS=False to use legacy market exits with breakeven."
    )

# Arizona doesn't observe DST, unlike the rest of the Pacific-linked US, so the AZ-local
# equivalent of every PT session boundary shifts by an hour twice a year (7:30 AM AZ in winter
# PST vs. 6:30 AM AZ in summer PDT, for a 6:30 AM PT session open). PT itself needs no fix —
# it's ZoneInfo("America/Los_Angeles"), already DST-correct for aligning to the real market
# open — this is purely a display convenience for reading the log against an AZ wall clock.
from zoneinfo import ZoneInfo
AZ = ZoneInfo("America/Phoenix")


def _pt_to_az_str(t: time) -> str:
    """Convert a PT time-of-day to today's AZ-local equivalent for display."""
    today_pt = datetime.now(PT).replace(hour=t.hour, minute=t.minute, second=0, microsecond=0)
    return today_pt.astimezone(AZ).strftime("%I:%M %p")

if POINT_VALUE != 50.0 or INSTRUMENT != "ES":
    raise SystemExit(f"ERROR: INSTRUMENT={INSTRUMENT!r}, POINT_VALUE={POINT_VALUE} — expected "
                      f"ES/50.0. This should be impossible since INSTRUMENT is force-set above; "
                      f"check topstep_bot_v5.py's INSTRUMENT block hasn't changed.")

DRY_RUN = "--dry-run" in sys.argv

# ─── Logging — separate files from topstep_bot_v5.py so both bots can run
# from the same folder without clobbering each other's logs ─────────────────
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

LOG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "choch_bot.log")
ANALYTICS_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "choch_trades.json")

# NOT logging.basicConfig() — topstep_bot_v5.py (imported above) calls basicConfig()
# itself at module level, pointed at its own "bot.log". basicConfig() only takes
# effect on the FIRST call in a process; a second call is silently a no-op. Since
# that import happens before this file's own logging setup would otherwise run,
# every CHOCH log line was riding along on Dual Trail's handler instead of its own
# file — choch_bot.log stayed empty while everything actually landed in bot.log.
# A dedicated, explicitly-named, non-propagating logger sidesteps that entirely,
# regardless of what any imported module does to the root logger.
log = logging.getLogger("choch_bot")
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

# ─── Config ───────────────────────────────────────────────────────────────
POLL_SECONDS = 15
BAR_LOOKBACK_MINUTES = 90       # matches topstep_bot_v5.py's fetch window
SESSION_STOP_FETCHING = time(13, 20)   # stop polling for the day a bit after force-flat
WATCHDOG_TIMEOUT = 30
BAR_GAP_WARN_SECONDS = 150       # warn if the newest bar is >2.5 min newer than the last one processed


# ─── Analytics ──────────────────────────────────────────────────────────────
class ChochAnalytics:
    """Logs every live trade to choch_trades.json for post-session review.
    Separate from topstep_bot_v5.py's Analytics/trades.json."""

    def __init__(self, path: str):
        self.path = path
        self.trades = self._load()

    def _load(self):
        try:
            with open(self.path) as f:
                return json.load(f)
        except Exception:
            return []

    def log_trade(self, entry_price: float, direction: str, exit_price: float,
                  reason: str, pnl: float, actual_entry: float = None,
                  actual_exit: float = None, actual_pnl: float = None):
        record = {
            "date": datetime.now(PT).strftime("%Y-%m-%d"),
            "time_of_day": datetime.now(PT).strftime("%H:%M"),
            "direction": direction,
            "theoretical_entry_price": entry_price,
            "theoretical_exit_price": exit_price,
            "theoretical_pnl": round(pnl, 2),
            "actual_entry_price": actual_entry,
            "actual_exit_price": actual_exit,
            "actual_pnl": round(actual_pnl, 2) if actual_pnl is not None else None,
            "exit_reason": reason,
            # Headline pnl/result_r use the REAL fill-based number when we have it —
            # falls back to theoretical only if a fill price couldn't be captured.
            "pnl": round(actual_pnl, 2) if actual_pnl is not None else round(pnl, 2),
            "result_r": round((actual_pnl if actual_pnl is not None else pnl) / MAX_TRADE_RISK_DOLLARS, 2)
                        if MAX_TRADE_RISK_DOLLARS else None,
            "rr_multiple": RR_MULTIPLE,
            "dry_run": DRY_RUN,
        }
        self.trades.append(record)
        try:
            with open(self.path, "w") as f:
                json.dump(self.trades, f, indent=2)
        except Exception as e:
            log.warning(f"Analytics write failed: {e}")
        log.info(f"Trade logged: {record}")


# ─── Live bot ────────────────────────────────────────────────────────────
class ChochLiveBot:
    def __init__(self):
        self.wrapper: CHOCHFVGReplayWrapper | None = None
        self.current_day = None
        self.account_id = None
        self.contract_id = None
        self._last_bar_fp = None
        self._last_bar_ts = None
        self._token_refreshed_at = None
        self._live_entry_fill = None  # actual broker fill price for the currently-open trade
        self._awaiting_bracket_close = False  # True while a bracket position is live and the
                                               # broker owns the exit; the loop polls for its close
        self._bracket_entry_signal = None     # the entry signal, kept for logging at broker close
        self.analytics = ChochAnalytics(ANALYTICS_PATH)

    # ── Auth / account / contract — same endpoints & fallback logic as
    # topstep_bot_v5.py's proven implementation ─────────────────────────────
    async def _authenticate(self, session):
        resp = await session.post(f"{BASE_URL}/Auth/loginKey",
                                   json={"userName": USERNAME, "apiKey": API_KEY})
        data = await resp.json()
        if not data.get("success"):
            raise RuntimeError(f"Auth failed: {data}")
        log.info("Authenticated with TopstepX OK")
        return data["token"]

    async def _get_account(self, session, token):
        resp = await session.post(f"{BASE_URL}/Account/search",
                                   headers={"Authorization": f"Bearer {token}"},
                                   json={"onlyActiveAccounts": True})
        data = await resp.json()
        accounts = data.get("accounts", [])
        if not accounts:
            raise RuntimeError("No active accounts found.")
        account = accounts[0]
        log.info(f"Account: {account['name']} (id={account['id']})")
        return account["id"]

    async def _get_contract(self, session, token):
        resp = await session.post(f"{BASE_URL}/Contract/search",
                                   headers={"Authorization": f"Bearer {token}"},
                                   json={"searchText": INSTRUMENT, "live": False})
        data = await resp.json()
        contracts = data.get("contracts", [])
        exact = [c for c in contracts if c.get("name") == MES_SYMBOL]
        if exact:
            contract = exact[0]
        else:
            matches = [c for c in contracts if c.get("name", "").startswith(INSTRUMENT)]
            if not matches:
                raise RuntimeError(f"{INSTRUMENT} contract not found.")
            contract = sorted(matches, key=lambda c: c.get("name", ""))[0]
            log.warning(f"{MES_SYMBOL} not found — using {contract.get('name')}. Update MES_SYMBOL.")
        log.info(f"Using contract: {contract['name']} (id={contract['id']})")
        return contract["id"]

    # ── Orders — same payload shape as topstep_bot_v5.py's proven _place_order,
    # sized to CHOCH's CONTRACTS. Both entries and exits are MARKET orders
    # (type=2) — a LIMIT-order entry path was built and tested (type=1), then
    # reverted after backtesting showed it filtered out disproportionately
    # winning trades (see choch_fvg_bot.py's MIN_STOP_POINTS comment for the
    # full story). The order_type/limit_price parameters are left in place —
    # harmless when unused, and available if a limit-order approach is ever
    # revisited with a better-designed selection mechanism.
    #
    # MARKET orders: submit, then poll for up to ~3s and return (order_id,
    # fill_price) once confirmed via _check_order_status(). LIMIT orders (not
    # currently used, but supported) would return immediately as (order_id,
    # None) instead, since a limit entry can sit unfilled far longer than a
    # market order ever would — the caller would need to poll for a fill on
    # its own schedule rather than blocking here.
    async def _place_order(self, session, token, action: str,
                            order_type: str = "market", limit_price: float = None):
        side = 0 if action == "buy" else 1
        type_code = 1 if order_type == "limit" else 2
        order_payload = {"accountId": self.account_id, "contractId": self.contract_id,
                          "type": type_code, "side": side, "size": CONTRACTS,
                          "limitPrice": limit_price if order_type == "limit" else None,
                          "stopPrice": None, "trailPrice": None,
                          "customTag": None, "linkedOrderId": None}
        resp = await session.post(
            f"{BASE_URL}/Order/place",
            headers={"Authorization": f"Bearer {token}"},
            json=order_payload,
        )
        data = await resp.json()
        if not data.get("success"):
            log.error(f"Order rejected | HTTP {resp.status} | request={order_payload} | response={data}")
            return None, None
        oid = data.get("orderId")
        price_note = f" @ {limit_price}" if order_type == "limit" else ""
        log.info(f"Order submitted: {order_type.upper()} {action.upper()} {CONTRACTS}x {INSTRUMENT}"
                  f"{price_note} | id={oid}")

        if order_type == "limit":
            return oid, None  # caller polls _check_order_status() going forward

        for attempt in range(3):
            await asyncio.sleep(1)
            status, fill = await self._check_order_status(session, token, oid)
            if status == "filled":
                log.info(f"Order FILLED @ {fill} | id={oid}")
                return oid, fill
            elif status == "rejected":
                log.error(f"Order NOT filled (rejected/cancelled) | id={oid}")
                return None, None

        log.warning("Fill status inconclusive after 3 order-status attempts — checking open positions directly.")
        try:
            pos_resp = await session.post(f"{BASE_URL}/Position/searchOpen",
                                           headers={"Authorization": f"Bearer {token}"},
                                           json={"accountId": self.account_id})
            pos_data = await pos_resp.json()
            positions = pos_data.get("positions", [])
            match = next((p for p in positions if p.get("contractId") == self.contract_id), None)
            if match:
                fill = match.get("averagePrice")
                log.warning(f"Position CONFIRMED open via Position/searchOpen: "
                            f"size={match.get('size')} avgPrice={fill} — "
                            f"treating order as FILLED despite inconclusive status poll.")
                return oid, fill
        except Exception as e:
            log.error(f"Position fallback check failed: {e}")

        log.error("Could not confirm fill via order status OR open positions. "
                   "MANUALLY CHECK YOUR ACCOUNT — the bot cannot safely assume flat.")
        return None, None

    async def _check_order_status(self, session, token, order_id):
        """Single non-blocking status check. Returns ('filled', price), ('pending', None),
        or ('rejected', None). Used both by _place_order's market-order wait loop and by
        the main loop's ongoing poll of a resting limit entry order."""
        try:
            start_ts = (datetime.now(CT) - timedelta(minutes=15)).isoformat()
            chk = await session.post(f"{BASE_URL}/Order/search",
                                      headers={"Authorization": f"Bearer {token}"},
                                      json={"accountId": self.account_id, "startTimestamp": start_ts})
            chkd = await chk.json()
            order = next((o for o in chkd.get("orders", []) if o.get("id") == order_id), None)
            if order is None:
                return "pending", None
            status = order.get("status", "")
            if status == 2:
                return "filled", order.get("filledPrice") or order.get("limitPrice")
            elif status in (4, 5):
                return "rejected", None
            return "pending", None
        except Exception as e:
            log.warning(f"Order status check failed: {e}")
            return "pending", None

    # Not currently called anywhere — was used by the limit-order pending-entry logic
    # that got reverted. Kept since it's small, self-contained, and correct; harmless
    # to leave in place for if a resting-order approach is ever revisited.
    async def _cancel_order(self, session, token, order_id):
        try:
            resp = await session.post(f"{BASE_URL}/Order/cancel",
                                       headers={"Authorization": f"Bearer {token}"},
                                       json={"accountId": self.account_id, "orderId": order_id})
            data = await resp.json()
            if data.get("success"):
                log.info(f"Order cancelled | id={order_id}")
                return True
            log.warning(f"Order cancel failed | id={order_id} | response={data}")
            return False
        except Exception as e:
            log.error(f"Order cancel request failed: {e}")
            return False

    async def _place_bracket_entry(self, session, token, action: str,
                                     stop_ticks: int, target_ticks: int):
        """Place a MARKET entry carrying a broker-side stop-loss bracket (type 4 = Stop)
        and take-profit bracket (type 1 = Limit). The exchange manages both exits from
        the moment of fill — this is the whole point of the bracket rewrite. Returns
        (order_id, fill_price) for the ENTRY. stop_ticks/target_ticks are distances from
        the fill, in ticks, as the API expects (not absolute prices)."""
        side = 0 if action == "buy" else 1
        order_payload = {
            "accountId": self.account_id, "contractId": self.contract_id,
            "type": ORDER_TYPE_MARKET, "side": side, "size": CONTRACTS,
            "limitPrice": None, "stopPrice": None, "trailPrice": None,
            "customTag": None, "linkedOrderId": None,
            "stopLossBracket": {"ticks": int(stop_ticks), "type": ORDER_TYPE_STOP},
            "takeProfitBracket": {"ticks": int(target_ticks), "type": ORDER_TYPE_LIMIT},
        }
        resp = await session.post(f"{BASE_URL}/Order/place",
                                   headers={"Authorization": f"Bearer {token}"},
                                   json=order_payload)
        data = await resp.json()
        if not data.get("success"):
            log.error(f"Bracket entry rejected | HTTP {resp.status} | request={order_payload} | response={data}")
            return None, None
        oid = data.get("orderId")
        log.info(f"Bracket entry submitted: MARKET {action.upper()} {CONTRACTS}x {INSTRUMENT} "
                  f"| stop={stop_ticks}t / target={target_ticks}t | id={oid}")

        for attempt in range(3):
            await asyncio.sleep(1)
            status, fill = await self._check_order_status(session, token, oid)
            if status == "filled":
                log.info(f"Bracket entry FILLED @ {fill} | id={oid} | broker now managing stop+target")
                return oid, fill
            elif status == "rejected":
                log.error(f"Bracket entry NOT filled (rejected) | id={oid}")
                return None, None
        try:
            pos_resp = await session.post(f"{BASE_URL}/Position/searchOpen",
                                           headers={"Authorization": f"Bearer {token}"},
                                           json={"accountId": self.account_id})
            pos_data = await pos_resp.json()
            match = next((p for p in pos_data.get("positions", [])
                          if p.get("contractId") == self.contract_id), None)
            if match:
                fill = match.get("averagePrice")
                log.warning(f"Bracket entry fill inconclusive but position CONFIRMED open "
                            f"@ {fill} — proceeding, broker managing exits.")
                return oid, fill
        except Exception as e:
            log.error(f"Bracket entry position fallback failed: {e}")
        log.error("Bracket entry fill UNCONFIRMED. MANUALLY CHECK ACCOUNT — a bracket order may "
                   "be resting with no confirmed position.")
        return None, None

    async def _check_position_closed(self, session, token):
        """With brackets, the BROKER closes the position when stop or target hits — the bot
        doesn't fire the exit. Polls open positions; when our contract is no longer open, the
        trade has closed. Returns (fill_price, reason, order_id) of the closing order, or
        (None, None, None) if still open. Reason is inferred from which bracket child filled
        (type 4 Stop vs type 1 Limit)."""
        try:
            pos_resp = await session.post(f"{BASE_URL}/Position/searchOpen",
                                           headers={"Authorization": f"Bearer {token}"},
                                           json={"accountId": self.account_id})
            pos_data = await pos_resp.json()
            still_open = any(p.get("contractId") == self.contract_id
                             for p in pos_data.get("positions", []))
            if still_open:
                return None, None, None
        except Exception as e:
            log.warning(f"Open-position check failed: {e}")
            return None, None, None

        try:
            start_ts = (datetime.now(CT) - timedelta(minutes=30)).isoformat()
            chk = await session.post(f"{BASE_URL}/Order/search",
                                      headers={"Authorization": f"Bearer {token}"},
                                      json={"accountId": self.account_id, "startTimestamp": start_ts})
            orders = (await chk.json()).get("orders", [])
            filled = [o for o in orders if o.get("status") == 2 and o.get("filledPrice") is not None]
            if filled:
                closer = max(filled, key=lambda o: o.get("updateTimestamp") or o.get("creationTimestamp") or "")
                fill = closer.get("filledPrice")
                reason = ("Stop bracket hit" if closer.get("type") == ORDER_TYPE_STOP
                          else "Target bracket hit" if closer.get("type") == ORDER_TYPE_LIMIT
                          else "Position closed")
                return fill, reason, closer.get("id")
        except Exception as e:
            log.warning(f"Closing-order lookup failed: {e}")
        return "unknown", "Position closed (fill price unavailable)", None

    async def _finalize_bracket_trade(self, exit_fill, close_reason):
        """Called once the broker has closed a bracket position. Records the trade with real
        entry/exit fills, reconciles the engine's safety-gate tracking to actual P&L, and
        clears the awaiting-close state. This is the bracket equivalent of what the old
        market-order exit path did inline — moved here because under brackets the close is a
        broker event detected by polling, not an order the bot fires."""
        signal = getattr(self, "_bracket_entry_signal", None)
        entry_fill = getattr(self, "_live_entry_fill", None)
        direction = signal["direction"] if signal else (self.wrapper.engine.position or "long")
        theoretical_pnl = signal["pnl"] if signal and "pnl" in signal else None

        # The engine may or may not have already fired its own theoretical exit signal (and
        # thus booked a theoretical pnl into daily_pnl) by the time the broker close is
        # detected. _last_theoretical_exit_pnl holds whatever it last booked; if it booked
        # nothing for this trade, reconciling against 0 is correct (nothing to back out).
        eng = self.wrapper.engine
        theo_for_reconcile = getattr(eng, "_last_theoretical_exit_pnl", 0.0)

        if entry_fill is not None and exit_fill not in (None, "unknown"):
            try:
                real_pnl = ((float(exit_fill) - float(entry_fill)) if direction == "long"
                            else (float(entry_fill) - float(exit_fill))) * POINT_VALUE * CONTRACTS
                real_pnl = round(real_pnl, 2)
            except (TypeError, ValueError):
                real_pnl = None
        else:
            real_pnl = None

        theo_entry = signal["entry_price"] if signal else None
        log.info(f"Bracket trade CLOSED by broker | {close_reason} | "
                 f"entry_fill={entry_fill} exit_fill={exit_fill} | "
                 f"ACTUAL P&L=${real_pnl:.2f}" if real_pnl is not None
                 else f"Bracket trade CLOSED by broker | {close_reason} | "
                      f"entry_fill={entry_fill} exit_fill={exit_fill} | actual P&L unavailable")

        self.analytics.log_trade(
            theo_entry, direction,
            (signal.get("stop") if close_reason and "Stop" in close_reason else signal.get("target")) if signal else None,
            close_reason, theoretical_pnl if theoretical_pnl is not None else 0.0,
            actual_entry=entry_fill, actual_exit=(float(exit_fill) if exit_fill not in (None, "unknown") else None),
            actual_pnl=real_pnl,
        )

        if real_pnl is not None:
            self.wrapper.engine._reconcile_actual_pnl(theo_for_reconcile, real_pnl)
            log.info(f"Daily tracking reconciled to actual bracket fill | "
                     f"real daily P&L=${self.wrapper.engine.daily_pnl:.2f} | "
                     f"consecutive losses={self.wrapper.engine.consecutive_losses} | "
                     f"(gates: DAILY_LOSS_LIMIT=${DAILY_LOSS_LIMIT:.0f}, MAX_CONSEC_LOSSES={MAX_CONSECUTIVE_LOSSES})")
        else:
            log.warning("Bracket close: could not capture real fills — safety gates remain on "
                        "theoretical P&L. Watch your account directly.")

        # Clear engine + bracket state so the day can continue (up to MAX_TRADES_PER_DAY).
        self._awaiting_bracket_close = False
        self._bracket_entry_signal = None
        self._live_entry_fill = None
        self.wrapper.engine.in_trade = False
        self.wrapper.engine.position = None

    async def _flatten_all(self, session, token, reason=""):
        log.info(f"FLATTEN ALL — {reason}")
        if DRY_RUN:
            log.info("[DRY RUN] Would call Position/closeAll here — skipped.")
            return
        resp = await session.post(f"{BASE_URL}/Position/closeAll",
                                   headers={"Authorization": f"Bearer {token}"},
                                   json={"accountId": self.account_id})
        data = await resp.json()
        if data.get("success"):
            log.info("All positions closed OK")
        else:
            log.error(f"Flatten failed: {data.get('errorMessage')}")

    # ── Watchdog — same pattern as topstep_bot_v5.py ────────────────────────
    async def _watchdog_check(self):
        confirmed = threading.Event()

        def prompt():
            try:
                print("\n" + "=" * 55)
                print("  WATCHDOG CHECK — Are you still watching?")
                print(f"  Press Enter within {WATCHDOG_TIMEOUT}s to continue.")
                print("=" * 55)
                sys.stdin.readline()
                confirmed.set()
            except Exception:
                pass

        t = threading.Thread(target=prompt, daemon=True)
        t.start()
        t.join(timeout=WATCHDOG_TIMEOUT)
        if confirmed.is_set():
            log.info("Watchdog: presence confirmed OK")
        else:
            log.warning("Watchdog: no response — PAUSED. Press Enter to resume.")
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, sys.stdin.readline)
            log.info("Watchdog: resumed.")

    # ── Daily reset ──────────────────────────────────────────────────────
    def _ensure_today_wrapper(self):
        today = datetime.now(PT).date()
        if self.current_day != today:
            self.current_day = today
            self.wrapper = CHOCHFVGReplayWrapper()
            self._last_bar_fp = None
            self._last_bar_ts = None
            log.info(f"New trading day {today} — fresh CHOCH engine.")

    # ── Signal handling ──────────────────────────────────────────────────
    async def _handle_signal(self, session, token, signal):
        if signal is None:
            return

        if signal["type"] == "entry":
            action = "buy" if signal["direction"] == "long" else "sell"
            # Convert the engine's point-based stop/target distances to ticks for the bracket.
            # The engine sets stop_price/target_price as absolute prices off the theoretical
            # entry; distance in points / TICK_SIZE = ticks. Round to nearest whole tick.
            entry_theo = signal["entry_price"]
            stop_pts = abs(entry_theo - signal["stop"])
            target_pts = abs(signal["target"] - entry_theo)
            stop_ticks = max(1, round(stop_pts / TICK_SIZE))
            target_ticks = max(1, round(target_pts / TICK_SIZE))

            if DRY_RUN:
                log.info(f"[DRY RUN] Would place BRACKET {action.upper()} entry | {signal['reason']} "
                         f"| stop={stop_ticks}t ({stop_pts:.2f}pt) / target={target_ticks}t ({target_pts:.2f}pt)")
                return
            oid, fill = await self._place_bracket_entry(session, token, action, stop_ticks, target_ticks)
            if oid:
                self._live_entry_fill = fill
                self._awaiting_bracket_close = True   # broker now owns the exit; loop will detect close
                self._bracket_entry_signal = signal   # kept for logging theoretical vs actual at close
                log.info(f"Entry #{self.wrapper.engine.trades_today} confirmed (bracket) | {signal['reason']} | "
                         f"theoretical entry={entry_theo} | actual fill={fill} | "
                         f"broker managing stop {stop_ticks}t / target {target_ticks}t")
            else:
                self._live_entry_fill = None
                self._awaiting_bracket_close = False
                log.error("Bracket entry failed/unconfirmed, but the strategy engine now thinks "
                           "it's in a trade. MANUALLY CHECK YOUR ACCOUNT — engine and broker may "
                           "be out of sync.")

        elif signal["type"] == "exit":
            # With brackets, the BROKER closes the position, not us. The engine still emits an
            # exit signal from its own (theoretical) stop/target check — but we must NOT fire a
            # real order here, or we'd double-exit / try to sell a position the broker already
            # closed. The real close is detected in the main loop via _check_position_closed,
            # which is where actual P&L, analytics, and safety-gate reconciliation happen. Here
            # we only note that the engine's theoretical view says the trade is done.
            log.info(f"[engine exit signal — informational only under brackets] {signal['reason']} "
                     f"| theoretical P&L=${signal['pnl']:.2f}. Broker owns the real exit; "
                     f"awaiting broker close confirmation.")

    # ── Main loop ────────────────────────────────────────────────────────
    async def run(self):
        import aiohttp
        timeout = aiohttp.ClientTimeout(total=20)   # any single request hangs no longer than this
        async with aiohttp.ClientSession(timeout=timeout) as session:
            token = await self._authenticate(session)
            self.account_id = await self._get_account(session, token)
            self.contract_id = await self._get_contract(session, token)

            log.info(f"CHOCH/FVG live bot ready.{'  [DRY RUN — no real orders]' if DRY_RUN else ''}")
            log.info(f"  Entry window: {SESSION_OPEN.strftime('%I:%M %p')}-{SESSION_CLOSE.strftime('%I:%M %p')} PT "
                      f"| Force flat: {FORCE_FLAT_TIME.strftime('%I:%M %p')} PT")
            log.info(f"  Same, in Arizona local time (AZ doesn't observe DST, so this shifts "
                      f"seasonally relative to PT): Entry window: {_pt_to_az_str(SESSION_OPEN)}-"
                      f"{_pt_to_az_str(SESSION_CLOSE)} AZ | Force flat: {_pt_to_az_str(FORCE_FLAT_TIME)} AZ")
            log.info(f"  RR={RR_MULTIPLE} | Early breakeven at {EARLY_BREAKEVEN_PCT*100:.0f}% of risk covered "
                      f"| Max stop={MAX_STOP_POINTS}pts | Max risk/trade=${MAX_TRADE_RISK_DOLLARS}")
            log.info(f"  Contracts: {CONTRACTS}x {INSTRUMENT} | Orders: MARKET | Min stop filter: {MIN_STOP_POINTS}pts")

            # Watchdog runs ONCE, here, at startup only — no periodic re-check in the loop
            # below. This trades away the "still-watching" safety net after the initial
            # confirmation: if the process hangs or misbehaves hours into the session, there's
            # no longer a mechanism that pauses it waiting for a human response. That's a real
            # reduction in the safety margin versus topstep_bot_v5.py's live bot, which still
            # re-checks every WATCHDOG_INTERVAL — by design, per request, not an oversight.
            await self._watchdog_check()

            while True:
                now_pt = datetime.now(PT)
                pt_time = now_pt.time()

                self._ensure_today_wrapper()

                if now_pt.weekday() >= 5:
                    await asyncio.sleep(300)
                    continue

                # Token refresh, throttled to ~20h
                if (self._token_refreshed_at is None or
                        (now_pt - self._token_refreshed_at).total_seconds() > 72000):
                    try:
                        r = await session.post(f"{BASE_URL}/Auth/validate",
                                                headers={"Authorization": f"Bearer {token}"})
                        vd = await r.json()
                        if vd.get("success") and vd.get("newToken"):
                            token = vd["newToken"]
                            self._token_refreshed_at = now_pt
                            log.info("JWT token refreshed.")
                    except Exception as e:
                        log.warning(f"Token refresh failed: {e}")

                # ── Bracket-close monitoring ──────────────────────────────────
                # When a bracket trade is live, the BROKER closes it (stop or target hits on
                # live ticks). Poll for that close every iteration and finalize once detected:
                # record real P&L, reconcile safety gates, and clear engine state. This runs
                # BEFORE the force-flat backstop so a normally-closed bracket is handled cleanly.
                if getattr(self, "_awaiting_bracket_close", False) and not DRY_RUN:
                    exit_fill, close_reason, _ = await self._check_position_closed(session, token)
                    if exit_fill is not None:
                        await self._finalize_bracket_trade(exit_fill, close_reason)

                # Defense-in-depth: if we're past force-flat and the engine still
                # thinks it's in a trade, the normal signal path should already have
                # closed it — this is a backstop in case that candle's poll was missed.
                if pt_time >= FORCE_FLAT_TIME and self.wrapper.engine.in_trade:
                    await self._flatten_all(session, token, "Force-flat backstop — normal exit signal didn't fire")
                    if getattr(self, "_awaiting_bracket_close", False):
                        # A bracket was still resting past force-flat; closeAll cancels it. Finalize
                        # on the theoretical figure since we couldn't capture a clean bracket fill.
                        self._awaiting_bracket_close = False
                        self._bracket_entry_signal = None
                    self.wrapper.engine.in_trade = False
                    self.wrapper.engine.position = None

                if pt_time < SESSION_OPEN or pt_time > SESSION_STOP_FETCHING:
                    await asyncio.sleep(60)
                    continue

                # Fetch 1-min bars
                end_b = datetime.now(CT)
                start_b = end_b - timedelta(minutes=BAR_LOOKBACK_MINUTES)
                try:
                    resp = await session.post(
                        f"{BASE_URL}/History/retrieveBars",
                        headers={"Authorization": f"Bearer {token}"},
                        json={"contractId": self.contract_id, "live": False,
                              "unit": 2, "unitNumber": 1, "limit": BAR_LOOKBACK_MINUTES,
                              "includePartialBar": False,
                              "startTime": start_b.isoformat(), "endTime": end_b.isoformat()},
                    )
                    bdata = await resp.json()
                except Exception as e:
                    log.warning(f"Bar fetch failed ({type(e).__name__}: {e}) — retrying in 15s.")
                    await asyncio.sleep(15)
                    continue
                bars = bdata.get("bars") or bdata.get("d") or bdata.get("data") or []
                if bars:
                    bars.sort(key=lambda b: b.get("t") or b.get("time") or "")
                if not bars or len(bars) < 1:
                    log.warning("Bar fetch returned no bars — retrying in 15s.")
                    await asyncio.sleep(15)
                    continue

                latest_bar = bars[-1]
                bar_ts = datetime.fromisoformat(
                    latest_bar.get("t") or latest_bar.get("time") or "2000-01-01T00:00:00+00:00"
                )
                bar_fp = (bar_ts,
                          latest_bar.get("o") or latest_bar.get("open") or 0,
                          latest_bar.get("h") or latest_bar.get("high") or 0,
                          latest_bar.get("l") or latest_bar.get("low") or 0,
                          latest_bar.get("c") or latest_bar.get("close") or 0)
                if bar_fp == self._last_bar_fp:
                    await asyncio.sleep(15)
                    continue

                if self._last_bar_ts is not None:
                    gap = (bar_ts - self._last_bar_ts).total_seconds()
                    if gap > BAR_GAP_WARN_SECONDS:
                        log.warning(f"Bar gap detected: {gap:.0f}s since last processed bar "
                                    f"({self._last_bar_ts} -> {bar_ts}). CHOCH/FVG structure "
                                    f"detection depends on consecutive candles — a skipped bar "
                                    f"can throw off swing/FVG state for the rest of the day. "
                                    f"If this happens often, check network/loop latency.")

                self._last_bar_fp = bar_fp
                self._last_bar_ts = bar_ts

                candle = Candle.from_bar(latest_bar, bar_ts)
                candle_pt_time = candle.ts.astimezone(PT).time()

                signal = self.wrapper.add_candle(candle, candle_pt_time, False)

                # Heartbeat — one line per processed bar regardless of whether it produced a
                # signal, so silence never means "is this stuck or just quiet" ambiguity.
                eng = self.wrapper.engine
                log.info(f"{candle_pt_time.strftime('%H:%M')} bar O={candle.open:.2f} H={candle.high:.2f} "
                         f"L={candle.low:.2f} C={candle.close:.2f} | in_trade={eng.in_trade} | "
                         f"trades_today={eng.trades_today}/{MAX_TRADES_PER_DAY} | "
                         f"daily_pnl=${eng.daily_pnl:.2f} | signal={signal['type'] if signal else 'none'}")

                await self._handle_signal(session, token, signal)

                await asyncio.sleep(POLL_SECONDS)


if __name__ == "__main__":
    if not USERNAME or not API_KEY:
        raise SystemExit("ERROR: Set TOPSTEP_USERNAME and TOPSTEP_API_KEY in your .env file.")
    mode = "DRY RUN (no real orders will be placed)" if DRY_RUN else "LIVE — will place real orders"
    print(f"Starting CHOCH/FVG bot — {mode}")
    asyncio.run(ChochLiveBot().run())
