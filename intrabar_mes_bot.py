"""
CHOCH/FVG Intrabar Tick-Level Bot — MES 1 Contract

Architecture:
  * Bar-polling loop (as in choch_bot_live.py) continues to run. It maintains
    the engine state: CHOCH detection, FVG tracking, VWAP, 9-SMA, session
    P&L / trade count / safety gates. This loop DOES NOT place entry orders.
  * SignalR trade-tick loop runs concurrently. On every trade print for the
    active MES contract, it checks:
      (1) engine has an armed FVG midpoint waiting to be touched
      (2) the current tick price touches/crosses the midpoint
      (3) Version B filter passes (uses PRIOR-bar close as filter price,
          PRIOR-bar VWAP/9-SMA — the design validated OOS + 2025-2026)
      (4) all safety gates permit the entry
    If all true, submits a MARKET bracket entry with fill-relative stop and
    target derived from the engine's signal (converted to point offsets from
    the fill via the broker's bracket-in-ticks semantics).
  * Broker manages exits via the bracket. No polled-exit path — this
    intentionally excludes USE_BREAKEVEN, matching what choch_bot_live.py
    already does for bracket orders.

Design choices explicit in this file:
  * MES only. CONTRACTS = 1 hardcoded here regardless of what
    topstep_bot_v5.py's env-dispatched CONTRACTS says. This bot is for
    smaller-size live testing of the tick-level entry path.
  * Version B filter timing (validated design).
  * Bar loop still runs entries as a fallback IF the tick loop hasn't
    fired within a bar of the signal — same design as Phase 2 from our
    plan. This gives resilience during WebSocket drops.
  * All existing safety gates enforced: MAX_TRADES_PER_DAY,
    MAX_CONSECUTIVE_LOSSES, DAILY_LOSS_LIMIT, session window, Topstep hard
    flatten at 3:10pm CT.

WARNING: This bot places REAL market orders on your funded TopstepX account.
Every entry costs money if it fails. The dollar risk per trade is small (~$5
per point on MES, 1-contract) but is REAL. Never run this unattended on the
first day. Watch it, kill it (Ctrl-C) at the first sign of anything weird.
"""

import asyncio
import json
import logging
import os
import sys
from datetime import datetime, time, timedelta, date
from pathlib import Path
from zoneinfo import ZoneInfo

# ─── Force MES + 1 contract BEFORE importing anything that reads env ────────
# The parent bot dispatches CONTRACTS and POINT_VALUE from these env vars at
# import time, so we set them explicitly for this file.
os.environ["INSTRUMENT"] = "MES"
os.environ.setdefault("MES_CONTRACTS", "1")

from dotenv import load_dotenv
load_dotenv()

import aiohttp
from signalrcore.hub_connection_builder import HubConnectionBuilder

# Parent bot state (auth, contract resolution, order placement)
from topstep_bot_v5 import Candle, PT, CT, BASE_URL, USERNAME, API_KEY, INSTRUMENT, POINT_VALUE, MES_SYMBOL
import choch_fvg_bot
from choch_fvg_bot import (
    CHOCHFVGReplayWrapper, SESSION_OPEN, SESSION_CLOSE, FORCE_FLAT_TIME,
    RR_MULTIPLE, MAX_STOP_POINTS, MIN_STOP_POINTS,
    MAX_TRADE_RISK_DOLLARS, MAX_TRADES_PER_DAY, DAILY_LOSS_LIMIT,
    MAX_CONSECUTIVE_LOSSES, FEE_PER_ROUND_TURN,
)

# HARD-CODED for this bot: 1 MES contract, no matter what parent config says
CONTRACTS = 1
INSTRUMENT_NAME = "MES"

# Bracket geometry — same as parent bot
ORDER_TYPE_MARKET = 2
ORDER_TYPE_LIMIT  = 1
ORDER_TYPE_STOP   = 4

# Market data hub
MARKET_HUB_URL = "https://rtc.topstepx.com/hubs/market"

# Poll cadence for the bar loop
POLL_SECONDS = 15
BAR_LOOKBACK_MINUTES = 150

# Session end for reading bars (matches choch_bot_live.py)
REPLAY_WINDOW_END = time(13, 15)

# ─── Telegram alerts — matches choch_bot_live.py exactly ────────────────────
# Reads TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID from .env. Fail-safe: any
# failure logs a warning and is swallowed so trading is never affected.
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# ─── Logging ─────────────────────────────────────────────────────────────────
log = logging.getLogger("intrabar_mes")
log.setLevel(logging.INFO)
_fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
_stream = logging.StreamHandler(sys.stdout); _stream.setFormatter(_fmt); log.addHandler(_stream)
_file = logging.FileHandler("intrabar_mes.log", mode="a"); _file.setFormatter(_fmt); log.addHandler(_file)


class TelegramNotifier:
    """Same class as choch_bot_live.py — copied verbatim so a Telegram outage
    or bad config cannot raise into the trading loop."""

    API_BASE = "https://api.telegram.org"

    def __init__(self, token, chat_id):
        self.token = token
        self.chat_id = chat_id
        self.enabled = bool(token and chat_id)
        if not self.enabled:
            log.info("Telegram alerts DISABLED (TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID not set). "
                     "Trading is unaffected; you just won't get push messages.")
        else:
            log.info("Telegram alerts ENABLED.")

    async def send(self, session, text, prefix=""):
        if not self.enabled:
            return
        msg = f"{prefix} {text}".strip()
        try:
            url = f"{self.API_BASE}/bot{self.token}/sendMessage"
            async with session.post(url, json={"chat_id": self.chat_id, "text": msg},
                                     timeout=10) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    log.warning(f"Telegram send got HTTP {resp.status}: {body[:200]} (trading unaffected)")
        except Exception as e:
            log.warning(f"Telegram send failed ({type(e).__name__}: {e}) — trading unaffected.")

    async def send_startup_test(self, session):
        await self.send(session,
                        "Intrabar MES bot started. Alerts are working. You'll get messages for "
                        "entries, exits, safety stops, and errors.",
                        prefix="[STARTUP]")


class IntrabarMESBot:
    """
    Single bot instance that runs bar-polling + tick streaming concurrently.
    """

    def __init__(self):
        self.wrapper = CHOCHFVGReplayWrapper()
        self.account_id = None
        self.contract_id = None
        self.contract_name = None
        self.token = None
        self._token_last_refresh = None

        # Telegram alerts
        self.notifier = TelegramNotifier(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID)

        # Session state
        self.trades_today = 0
        self.consecutive_losses = 0
        self.daily_pnl = 0.0
        self._last_pnl_refresh = None

        # In-trade state
        self.in_trade = False
        self._trade_lock = asyncio.Lock()   # serializes entry decisions across tick+bar loops
        self._last_bar_fp = None            # de-dupes bar polling
        self._last_bar_ts = None            # timestamp of the last bar processed — for staleness check

        # Bracket trade tracking for real-fill reconciliation (matches ES bot pattern)
        self._live_entry_fill = None        # actual fill price of the current entry
        self._live_entry_direction = None   # "long"/"short" of current entry
        self._live_entry_signal = None      # dict — signal that produced this entry
        self._awaiting_bracket_close = False  # True while broker is managing a live bracket

        # Filter state that intrabar needs to read
        self._prev_bar_close = None         # updated whenever a NEW bar closes
        self._prev_bar_vwap  = None         # vwap value BEFORE current bar contribution
        self._prev_bar_ema9  = None         # 9-sma value BEFORE current bar contribution

        # Signal state (updated on every completed bar) — cached FVG midpoints
        # the engine is armed on, so the tick loop can react without re-running
        # the whole CHOCH pipeline every millisecond.
        self._armed_signal = None           # dict or None
        # Format: {"direction":"long"|"short", "midpoint": price,
        #          "stop": price, "target": price, "signal_bar_time": datetime,
        #          "bar_fired": False}  — bar_fired True means bar loop already
        # fired an entry on this signal, so tick loop should stand down.

        # Tick loop plumbing
        self._connection = None
        self._loop = None

        # Metrics
        self._trades_placed = 0
        self._ticks_received = 0
        self._last_tick_log = datetime.now(PT)

    # ═══════════════════════════════════════════════════════════════════════
    # AUTH + RESOLUTION
    # ═══════════════════════════════════════════════════════════════════════
    async def _authenticate(self, session):
        resp = await session.post(f"{BASE_URL}/Auth/loginKey",
                                  json={"userName": USERNAME, "apiKey": API_KEY})
        data = await resp.json()
        if not data.get("success"):
            raise RuntimeError(f"Auth failed: {data}")
        log.info("Authenticated with TopstepX OK")
        self._token_last_refresh = datetime.now(PT)
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
        log.info(f"Account: {account['name']} (id={account['id']})  balance={account.get('balance', '?')}")
        return account["id"]

    async def _get_contract(self, session, token):
        resp = await session.post(f"{BASE_URL}/Contract/search",
                                  headers={"Authorization": f"Bearer {token}"},
                                  json={"searchText": "MES", "live": False})
        data = await resp.json()
        contracts = data.get("contracts", [])
        # Prefer exact MES front-month
        exact = [c for c in contracts if c.get("name") == MES_SYMBOL]
        if exact:
            contract = exact[0]
        else:
            matches = [c for c in contracts if c.get("name", "").startswith("MES")]
            if not matches:
                raise RuntimeError("MES contract not found.")
            contract = sorted(matches, key=lambda c: c.get("name", ""))[0]
            log.warning(f"{MES_SYMBOL} not exact match — using {contract.get('name')}. Update MES_SYMBOL.")
        log.info(f"Using contract: {contract['name']} (id={contract['id']})")
        return contract["id"], contract["name"]

    # ═══════════════════════════════════════════════════════════════════════
    # SAFETY GATES
    # ═══════════════════════════════════════════════════════════════════════
    def _within_session(self, now_pt):
        t = now_pt.time()
        return SESSION_OPEN <= t <= SESSION_CLOSE

    def _past_force_flat(self, now_pt):
        return now_pt.time() >= FORCE_FLAT_TIME

    def _safety_gates_pass(self):
        """Return (True, "") if all gates permit entry, else (False, reason)."""
        if self.trades_today >= MAX_TRADES_PER_DAY:
            return False, f"MAX_TRADES_PER_DAY reached ({self.trades_today}/{MAX_TRADES_PER_DAY})"
        if self.consecutive_losses >= MAX_CONSECUTIVE_LOSSES:
            return False, f"MAX_CONSECUTIVE_LOSSES reached ({self.consecutive_losses}/{MAX_CONSECUTIVE_LOSSES})"
        if self.daily_pnl <= -abs(DAILY_LOSS_LIMIT):
            return False, f"DAILY_LOSS_LIMIT breached (pnl=${self.daily_pnl:.2f})"
        if self.in_trade:
            return False, "already in_trade"
        return True, ""

    async def _refresh_daily_pnl(self, session, token):
        now = datetime.now(PT)
        if self._last_pnl_refresh and (now - self._last_pnl_refresh).total_seconds() < 10:
            return self.daily_pnl
        self._last_pnl_refresh = now
        try:
            resp = await session.post(f"{BASE_URL}/Account/search",
                                      headers={"Authorization": f"Bearer {token}"},
                                      json={"onlyActiveAccounts": True})
            data = await resp.json()
            for account in data.get("accounts", []):
                if account.get("id") != self.account_id:
                    continue
                for key in ("dailyPnl", "dailyPnL", "dayPnl", "dayPnL", "realizedPnl", "pnl"):
                    v = account.get(key)
                    if isinstance(v, (int, float)):
                        self.daily_pnl = float(v)
                        return self.daily_pnl
        except Exception as e:
            log.warning(f"P&L refresh failed: {e}")
        return self.daily_pnl

    # ═══════════════════════════════════════════════════════════════════════
    # ORDER PLACEMENT — same shape as parent bot's proven code
    # ═══════════════════════════════════════════════════════════════════════
    async def _place_bracket(self, session, token, action, stop_ticks, target_ticks, signal_meta):
        """action = 'buy' or 'sell'.
        stop_ticks and target_ticks are broker offsets in TICKS relative to fill.
        For MES: 1 tick = 0.25pt.
        Sign convention: LONG entry → stop_ticks negative, target_ticks positive.
                         SHORT entry → stop_ticks positive, target_ticks negative.
        """
        side = 0 if action == "buy" else 1

        # Belt-and-suspenders validation matching parent bot exactly
        if abs(stop_ticks) < 4:
            log.error(f"REFUSING to submit: stop magnitude {abs(stop_ticks)} < 4 ticks (broker minimum).")
            return None, None, "rejected"
        if (stop_ticks > 0) == (target_ticks > 0):
            log.error(f"REFUSING to submit: stop({stop_ticks}) and target({target_ticks}) share a sign.")
            return None, None, "rejected"
        expected_stop_sign = -1 if side == 0 else +1
        if (stop_ticks > 0) != (expected_stop_sign > 0):
            log.error(f"REFUSING to submit: {action} order but stop_ticks={stop_ticks} sign wrong.")
            return None, None, "rejected"

        order_payload = {
            "accountId": self.account_id, "contractId": self.contract_id,
            "type": ORDER_TYPE_MARKET, "side": side, "size": CONTRACTS,
            "limitPrice": None, "stopPrice": None, "trailPrice": None,
            "customTag": None, "linkedOrderId": None,
            "stopLossBracket": {"ticks": int(stop_ticks), "type": ORDER_TYPE_STOP},
            "takeProfitBracket": {"ticks": int(target_ticks), "type": ORDER_TYPE_LIMIT},
        }
        log.info(f"SUBMITTING BRACKET | {action.upper()} {CONTRACTS}x MES | "
                 f"stop={stop_ticks}t / target={target_ticks}t | signal={signal_meta}")
        resp = await session.post(f"{BASE_URL}/Order/place",
                                   headers={"Authorization": f"Bearer {token}"},
                                   json=order_payload)
        data = await resp.json()
        if not data.get("success"):
            log.error(f"Bracket entry rejected | HTTP {resp.status} | request={order_payload} | response={data}")
            return None, None, "rejected"
        oid = data.get("orderId")
        log.info(f"Bracket entry ACCEPTED by broker | id={oid}")
        self._trades_placed += 1

        # Wait up to 3 seconds for fill confirmation
        for attempt in range(3):
            await asyncio.sleep(1)
            status, fill = await self._check_order_status(session, token, oid)
            if status == "filled":
                log.info(f"Bracket entry FILLED @ {fill} | id={oid} | broker managing stop+target")
                return oid, fill, "filled"
            elif status == "rejected":
                log.error(f"Bracket entry NOT filled (rejected) | id={oid}")
                return None, None, "rejected"
        # Fill inconclusive — check positions
        try:
            pos_resp = await session.post(f"{BASE_URL}/Position/searchOpen",
                                           headers={"Authorization": f"Bearer {token}"},
                                           json={"accountId": self.account_id})
            pos_data = await pos_resp.json()
            match = next((p for p in pos_data.get("positions", [])
                          if p.get("contractId") == self.contract_id), None)
            if match:
                fill = match.get("averagePrice")
                log.warning(f"Bracket entry fill inconclusive but position CONFIRMED @ {fill}")
                return oid, fill, "filled"
        except Exception as e:
            log.error(f"Position check failed after inconclusive fill: {e}")
        log.error(f"Bracket entry status UNKNOWN | id={oid} — human review needed")
        return oid, None, "unknown"

    async def _check_order_status(self, session, token, oid):
        """Uses INTEGER status codes matching ES bot pattern:
             2 = filled
             4 or 5 = rejected/cancelled
             anything else = still pending
        Returns (status_str, fill_price) tuple."""
        try:
            start_ct = (datetime.now(CT) - timedelta(minutes=15)).isoformat()
            resp = await session.post(f"{BASE_URL}/Order/search",
                                       headers={"Authorization": f"Bearer {token}"},
                                       json={"accountId": self.account_id,
                                             "startTimestamp": start_ct})
            data = await resp.json()
            orders = data.get("orders", [])
            match = next((o for o in orders if o.get("id") == oid), None)
            if match is None:
                return "pending", None
            status = match.get("status", "")
            # Status is an integer code — not a string
            if status == 2:
                return "filled", match.get("filledPrice") or match.get("limitPrice")
            if status in (4, 5):
                return "rejected", None
            return "pending", None
        except Exception as e:
            log.warning(f"Order status check failed: {e}")
            return "pending", None

    # ═══════════════════════════════════════════════════════════════════════
    # BAR-POLLING LOOP — maintains engine state, fires as fallback
    # ═══════════════════════════════════════════════════════════════════════
    async def _bar_polling_loop(self, session):
        log.info(f"Bar polling loop started (interval={POLL_SECONDS}s)")
        while True:
            try:
                await self._process_new_bars(session)
            except Exception as e:
                log.error(f"Bar loop exception: {e}", exc_info=True)
            await asyncio.sleep(POLL_SECONDS)

    async def _process_new_bars(self, session):
        now_pt = datetime.now(PT)
        if now_pt.weekday() >= 5:  # weekend
            return
        if not self._within_session(now_pt):
            return

        # Refresh P&L opportunistically
        await self._refresh_daily_pnl(session, self.token)

        # Fetch latest bars
        end_ct = datetime.now(CT).isoformat()
        start_ct = (datetime.now(CT) - timedelta(minutes=BAR_LOOKBACK_MINUTES)).isoformat()
        resp = await session.post(
            f"{BASE_URL}/History/retrieveBars",
            headers={"Authorization": f"Bearer {self.token}"},
            json={"contractId": self.contract_id, "live": False,
                  "startTime": start_ct, "endTime": end_ct,
                  "unit": 2, "unitNumber": 1, "limit": BAR_LOOKBACK_MINUTES,
                  "includePartialBar": False},
        )
        data = await resp.json()
        if not data.get("success"):
            log.warning(f"retrieveBars failed: {data}")
            return
        bars = data.get("bars", [])
        if not bars:
            return
        bar = bars[-1]  # newest COMPLETED bar
        bar_fp = f"{bar.get('t')}|{bar.get('c')}"
        if bar_fp == self._last_bar_fp:
            return  # already processed
        self._last_bar_fp = bar_fp

        # Convert to Candle
        ts_str = bar.get("t") or bar.get("timestamp")
        ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00")).astimezone(PT)
        candle = Candle(bar["o"], bar["h"], bar["l"], bar["c"],
                        bar.get("v", 0), ts)

        # ─── BAR STALENESS CHECK ─────────────────────────────────────────────
        # A completed 1-min bar timestamped at time T represents the interval
        # [T, T+60s), so it "closed" at T+60s. If NOW is more than
        # MAX_BAR_STALENESS_SECS beyond that close, the API is serving old data
        # (session degraded, network stalled, or auth silently expired). Refuse
        # to trade on stale bars — the July 17 morning bug where a bar-loop
        # entry fired ~15pts off signal was exactly this failure mode.
        MAX_BAR_STALENESS_SECS = 90
        bar_close_time = ts + timedelta(seconds=60)
        now_pt2 = datetime.now(PT)
        staleness = (now_pt2 - bar_close_time).total_seconds()
        if staleness > MAX_BAR_STALENESS_SECS:
            if self._last_bar_ts != ts:  # only warn once per stale bar
                log.warning(f"STALE BAR REJECTED | bar_close={bar_close_time.isoformat()} "
                            f"now={now_pt2.isoformat()} staleness={staleness:.0f}s "
                            f"(> {MAX_BAR_STALENESS_SECS}s). Skipping to protect from bad fills.")
                self._last_bar_ts = ts
                # Alert on first-stale-bar per session
                if self.notifier.enabled:
                    try:
                        await self.notifier.send(
                            session,
                            f"Bar API returning STALE data ({staleness:.0f}s old). "
                            f"Bot is refusing to trade on this bar. May indicate API session "
                            f"has degraded — consider checking TopstepX / restarting bot.",
                            prefix="[STALE BAR]",
                        )
                    except Exception:
                        pass
            return
        self._last_bar_ts = ts

        # Snapshot PRIOR-bar filter state BEFORE feeding this candle to engine
        prior_vwap = self.wrapper.engine.vwap
        prior_ema9 = self.wrapper.engine.ema9
        candle_pt_time = candle.ts.astimezone(PT).time()

        # Advance the engine
        signal = self.wrapper.add_candle(candle, candle_pt_time, is_last_candle_of_session=False,
                                          session_vwap=None)

        # After the engine has processed the bar, update prev_bar state for
        # the NEXT bar's intrabar tick decisions.
        # The bar that just closed IS the "prev bar" for anything that happens
        # during the bar that starts NOW.
        self._prev_bar_close = candle.close
        self._prev_bar_vwap  = self.wrapper.engine.vwap
        self._prev_bar_ema9  = self.wrapper.engine.ema9

        # Track whether the engine has an armed FVG midpoint to watch
        self._update_armed_signal_from_engine()

        # Bar-loop entry: if engine produced a signal AND tick loop hasn't
        # already fired on this bar, submit here as a fallback.
        if signal and signal.get("type") == "entry":
            armed = self._armed_signal
            if armed and armed.get("bar_fired"):
                log.info("Bar-loop entry SKIPPED (tick loop already fired this signal)")
                return
            async with self._trade_lock:
                ok, reason = self._safety_gates_pass()
                if not ok:
                    log.info(f"Bar-loop entry SKIPPED by safety gate: {reason}")
                    return
                await self._fire_entry_from_signal(session, signal, source="bar")

        # Handle exit signals from engine (informational only when using brackets)
        if signal and signal.get("type") == "exit":
            log.info(f"[engine exit signal — informational under brackets] {signal.get('reason')}")

    def _update_armed_signal_from_engine(self):
        """Read the engine's active FVGs and CHOCH bias. If there's a valid
        midpoint the tick loop should watch, cache it. Otherwise clear it.

        We're not re-implementing the CHOCH detection — we're peeking at the
        engine's internal state to know what price level to watch."""
        eng = self.wrapper.engine
        # The engine tracks _active_fvgs and choch_direction
        active_fvgs = getattr(eng, "active_fvgs", None) or getattr(eng, "_active_fvgs", None)
        choch_dir = getattr(eng, "choch_direction", None)
        if not active_fvgs or not choch_dir:
            self._armed_signal = None
            return

        # Take the freshest FVG in the CHOCH direction
        candidate = None
        for fvg in reversed(active_fvgs):
            fvg_dir = fvg.get("direction") if isinstance(fvg, dict) else getattr(fvg, "direction", None)
            if fvg_dir == choch_dir:
                candidate = fvg
                break
        if not candidate:
            self._armed_signal = None
            return

        mid = candidate.get("midpoint") if isinstance(candidate, dict) else getattr(candidate, "midpoint", None)
        top = candidate.get("top") if isinstance(candidate, dict) else getattr(candidate, "top", None)
        bot = candidate.get("bottom") if isinstance(candidate, dict) else getattr(candidate, "bottom", None)
        if mid is None:
            self._armed_signal = None
            return

        # For a bullish CHOCH we want longs at retrace INTO the FVG midpoint (from above)
        # For a bearish CHOCH we want shorts at retrace INTO the FVG midpoint (from below)
        # Stop is the far side of the FVG, target = midpoint + RR * risk (or symmetric).
        direction = "long" if choch_dir == "bullish" else "short"
        if direction == "long":
            stop = bot if bot is not None else mid - 1.0
            risk = mid - stop
            target = mid + RR_MULTIPLE * risk
        else:
            stop = top if top is not None else mid + 1.0
            risk = stop - mid
            target = mid - RR_MULTIPLE * risk

        # Only arm if the risk points are in the acceptable band
        if risk < MIN_STOP_POINTS or risk > MAX_STOP_POINTS:
            self._armed_signal = None
            return

        # Preserve bar_fired if we already had this signal armed
        existing = self._armed_signal
        bar_fired = existing.get("bar_fired") if existing and existing.get("midpoint") == mid else False

        self._armed_signal = {
            "direction": direction,
            "midpoint": mid,
            "stop": stop,
            "target": target,
            "risk_pts": risk,
            "reward_pts": abs(target - mid),
            "bar_fired": bar_fired,
        }

    def _version_b_filter_passes(self, direction):
        """Version B: use PRIOR-bar close as filter price, PRIOR-bar VWAP/9-SMA.
        Filter blocks:
          - LONG if price < ema9 AND price < vwap
          - SHORT if price > ema9 AND price > vwap
        """
        if not choch_fvg_bot.VWAP_EMA_FILTER:
            return True
        if self._prev_bar_close is None or self._prev_bar_vwap is None or self._prev_bar_ema9 is None:
            # Filter unavailable early in session — allow (matches engine convention)
            return True
        price = self._prev_bar_close
        if direction == "long":
            blocks = (price < self._prev_bar_ema9 and price < self._prev_bar_vwap)
        else:
            blocks = (price > self._prev_bar_ema9 and price > self._prev_bar_vwap)
        return not blocks

    # ═══════════════════════════════════════════════════════════════════════
    # ENTRY FIRING — from either bar loop OR tick loop
    # ═══════════════════════════════════════════════════════════════════════
    async def _fire_entry_from_signal(self, session, signal, source):
        """Called under self._trade_lock. Converts signal → bracket order."""
        if self.in_trade:
            log.info(f"{source}: entry aborted, in_trade=True")
            return

        direction = signal["direction"]
        signal_entry = signal["entry_price"]
        signal_stop  = signal["stop"]
        signal_target = signal["target"]

        if direction == "long":
            risk = signal_entry - signal_stop
            reward = signal_target - signal_entry
        else:
            risk = signal_stop - signal_entry
            reward = signal_entry - signal_target

        # Convert to broker bracket ticks (MES = 0.25 per tick)
        TICK = 0.25
        stop_ticks_mag = max(4, int(round(risk / TICK)))
        target_ticks_mag = max(4, int(round(reward / TICK)))
        if direction == "long":
            action = "buy"
            stop_ticks = -stop_ticks_mag
            target_ticks = +target_ticks_mag
        else:
            action = "sell"
            stop_ticks = +stop_ticks_mag
            target_ticks = -target_ticks_mag

        # Dollar-risk check for MES: risk pts × $5 × 1 contract
        est_risk_dollars = risk * 5.0 * CONTRACTS
        if est_risk_dollars > MAX_TRADE_RISK_DOLLARS:
            log.warning(f"Entry SKIPPED — estimated risk ${est_risk_dollars:.2f} > MAX_TRADE_RISK_DOLLARS ${MAX_TRADE_RISK_DOLLARS}")
            return

        signal_meta = f"{direction} @ signal={signal_entry} stop={signal_stop} target={signal_target} risk={risk:.2f}pt source={source}"
        self.in_trade = True
        try:
            oid, fill, status = await self._place_bracket(session, self.token, action, stop_ticks, target_ticks, signal_meta)
            if status == "filled":
                self.trades_today += 1
                if self._armed_signal is not None:
                    self._armed_signal["bar_fired"] = True
                # Store entry state so _finalize_bracket_trade can compute real P&L
                self._live_entry_fill = fill
                self._live_entry_direction = direction
                self._live_entry_signal = {
                    "direction": direction,
                    "entry_price": signal_entry,
                    "stop": signal_stop,
                    "target": signal_target,
                }
                self._awaiting_bracket_close = True
                log.info(f"Trade #{self.trades_today} OPEN | {direction} MES @ {fill} | source={source}")
                await self.notifier.send(
                    session,
                    f"MES {direction.upper()} FILLED @ {fill} | "
                    f"signal={signal_entry} stop={signal_stop} target={signal_target} | "
                    f"source={source} | trade#{self.trades_today}",
                    prefix="[ENTRY]",
                )
                # We don't wait for exit — broker manages, position monitor will
                # detect the close and call _finalize_bracket_trade with real fills.
            else:
                log.warning(f"Entry did not fill — status={status}")
                await self.notifier.send(
                    session,
                    f"MES {direction.upper()} entry did NOT fill — status={status}",
                    prefix="[REJECT]",
                )
                self.in_trade = False
        except Exception as e:
            log.error(f"Entry submission exception: {e}", exc_info=True)
            await self.notifier.send(
                session,
                f"MES entry EXCEPTION: {type(e).__name__}: {e}",
                prefix="[ERROR]",
            )
            self.in_trade = False

    # ═══════════════════════════════════════════════════════════════════════
    # POSITION MONITOR — detects when the broker's bracket closes our trade
    # ═══════════════════════════════════════════════════════════════════════
    async def _check_position_closed(self, session):
        """Mirrors ES bot's _check_position_closed. Returns (fill_price, reason, order_id).
        If position still open, returns (None, None, None)."""
        try:
            pos_resp = await session.post(f"{BASE_URL}/Position/searchOpen",
                                           headers={"Authorization": f"Bearer {self.token}"},
                                           json={"accountId": self.account_id})
            pos_data = await pos_resp.json()
            still_open = any(p.get("contractId") == self.contract_id
                             for p in pos_data.get("positions", []))
            if still_open:
                return None, None, None
        except Exception as e:
            log.warning(f"Open-position check failed: {e}")
            return None, None, None

        # Position closed — find the closing order in recent Order/search
        try:
            start_ts = (datetime.now(CT) - timedelta(minutes=30)).isoformat()
            resp = await session.post(f"{BASE_URL}/Order/search",
                                       headers={"Authorization": f"Bearer {self.token}"},
                                       json={"accountId": self.account_id,
                                             "startTimestamp": start_ts})
            orders = (await resp.json()).get("orders", [])
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

    async def _finalize_bracket_trade(self, session, exit_fill, close_reason):
        """Reconciles ACTUAL entry fill vs ACTUAL exit fill and calculates REAL P&L.
        Increments consecutive_losses / daily_pnl from real numbers, not the
        stale account-balance snapshot. Matches ES bot pattern."""
        entry_fill = self._live_entry_fill
        direction = self._live_entry_direction or "long"
        signal = self._live_entry_signal or {}

        real_pnl = None
        gross_pnl = None
        fee = None
        if entry_fill is not None and exit_fill not in (None, "unknown"):
            try:
                point_move = ((float(exit_fill) - float(entry_fill)) if direction == "long"
                              else (float(entry_fill) - float(exit_fill)))
                # MES: $5 per point. Force this — do not use POINT_VALUE (which
                # may be $50 if the parent module's import-time dispatch ran
                # before INSTRUMENT was set).
                MES_POINT_VALUE = 5.0
                gross_pnl = round(point_move * MES_POINT_VALUE * CONTRACTS, 2)
                fee = round(FEE_PER_ROUND_TURN * CONTRACTS, 2)
                real_pnl = round(gross_pnl - fee, 2)
            except (TypeError, ValueError):
                real_pnl = None

        # Update in-bot P&L tracking with REAL numbers (not from stale account snapshot)
        if real_pnl is not None:
            self.daily_pnl += real_pnl
            if real_pnl < 0:
                self.consecutive_losses += 1
            else:
                self.consecutive_losses = 0

        log.info(f"Bracket trade CLOSED by broker | {close_reason} | "
                 f"entry_fill={entry_fill} exit_fill={exit_fill} | "
                 f"NET P&L={('$' + format(real_pnl, '+.2f')) if real_pnl is not None else 'unavailable'} | "
                 f"consecutive_losses={self.consecutive_losses} | daily_pnl=${self.daily_pnl:.2f}")

        # Alert with real numbers
        if real_pnl is not None:
            pnl_word = "WIN" if real_pnl > 0 else ("LOSS" if real_pnl < 0 else "SCRATCH")
            await self.notifier.send(
                session,
                f"{close_reason} | {pnl_word} ${real_pnl:+.2f} "
                f"(gross ${gross_pnl:+.2f}, fees ${fee:.2f})\n"
                f"{direction.upper()} {entry_fill} -> {exit_fill}\n"
                f"Daily P&L ${self.daily_pnl:+.2f} | trades {self.trades_today}/{MAX_TRADES_PER_DAY} "
                f"| consec_L={self.consecutive_losses}",
                prefix="[EXIT]",
            )
        else:
            await self.notifier.send(
                session,
                f"{close_reason} — bot couldn't capture real fill prices. "
                f"Check TopstepX for actual result.",
                prefix="[EXIT?]",
            )

        # Safety-gate distinct alerts (only after we've reconciled real P&L)
        if self.consecutive_losses >= MAX_CONSECUTIVE_LOSSES:
            await self.notifier.send(
                session,
                f"MAX_CONSECUTIVE_LOSSES ({MAX_CONSECUTIVE_LOSSES}) reached — no more entries today. "
                f"Daily P&L ${self.daily_pnl:+.2f}",
                prefix="[SAFETY STOP]",
            )
        elif self.daily_pnl <= -abs(DAILY_LOSS_LIMIT):
            await self.notifier.send(
                session,
                f"DAILY_LOSS_LIMIT breached (${self.daily_pnl:+.2f} <= -${abs(DAILY_LOSS_LIMIT)}) — "
                f"no more entries today.",
                prefix="[SAFETY STOP]",
            )
        elif self.trades_today >= MAX_TRADES_PER_DAY:
            await self.notifier.send(
                session,
                f"MAX_TRADES_PER_DAY ({MAX_TRADES_PER_DAY}) reached — done for the day. "
                f"Daily P&L ${self.daily_pnl:+.2f}",
                prefix="[DAILY LIMIT]",
            )

        # Clear trade state
        self.in_trade = False
        self._live_entry_fill = None
        self._live_entry_direction = None
        self._live_entry_signal = None
        self._awaiting_bracket_close = False
        self._armed_signal = None

    async def _position_monitor_loop(self, session):
        """Poll every 3 seconds. When we're awaiting a bracket close and the
        position is no longer open, finalize the trade with REAL fill data."""
        while True:
            try:
                if self._awaiting_bracket_close:
                    exit_fill, reason, _oid = await self._check_position_closed(session)
                    if exit_fill is not None:
                        await self._finalize_bracket_trade(session, exit_fill, reason)
            except Exception as e:
                log.warning(f"Position monitor exception: {e}")
            await asyncio.sleep(3)

    async def _token_refresh_loop(self, session):
        """Refresh the JWT token periodically. Matches ES bot pattern:
        every 20h (72000s), call /Auth/validate to get a fresh token.
        Fail-safe: any exception is logged and swallowed."""
        while True:
            await asyncio.sleep(3600)  # check hourly
            try:
                if self._token_last_refresh is None:
                    continue
                elapsed = (datetime.now(PT) - self._token_last_refresh).total_seconds()
                if elapsed < 72000:
                    continue
                r = await session.post(f"{BASE_URL}/Auth/validate",
                                        headers={"Authorization": f"Bearer {self.token}"})
                vd = await r.json()
                if vd.get("success") and vd.get("newToken"):
                    self.token = vd["newToken"]
                    self._token_last_refresh = datetime.now(PT)
                    log.info("JWT token refreshed.")
            except Exception as e:
                log.warning(f"Token refresh failed: {e}")

    # ═══════════════════════════════════════════════════════════════════════
    # FORCE FLATTEN before Topstep hard flat time
    # ═══════════════════════════════════════════════════════════════════════
    async def _force_flat_loop(self, session):
        """If we're still in a trade approaching the daily hard flatten time,
        close the position ourselves. Topstep flats at 3:10pm CT."""
        while True:
            try:
                now_pt = datetime.now(PT)
                if self._past_force_flat(now_pt) and self.in_trade:
                    log.warning(f"Past FORCE_FLAT_TIME — closing position")
                    try:
                        await session.post(f"{BASE_URL}/Position/closeContract",
                                            headers={"Authorization": f"Bearer {self.token}"},
                                            json={"accountId": self.account_id, "contractId": self.contract_id})
                        log.info("Force-flatten request submitted")
                        await self.notifier.send(
                            session,
                            f"Approaching Topstep 3:10 CT hard flatten — "
                            f"closed position early.",
                            prefix="[FORCE FLAT]",
                        )
                    except Exception as e:
                        log.error(f"Force flatten failed: {e}")
                        await self.notifier.send(
                            session,
                            f"FORCE FLATTEN FAILED — you may need to close manually. "
                            f"Error: {e}",
                            prefix="[ERROR]",
                        )
            except Exception as e:
                log.warning(f"Force flat loop exception: {e}")
            await asyncio.sleep(30)

    # ═══════════════════════════════════════════════════════════════════════
    # SIGNALR TICK LOOP
    # ═══════════════════════════════════════════════════════════════════════
    def _on_trade_tick(self, args):
        """SignalR callback. Runs in the SignalR library's thread — schedules
        the actual decision work onto the main asyncio loop."""
        try:
            self._ticks_received += 1
            # args = [contractId, tradePayload]
            if not isinstance(args, list) or len(args) < 2:
                return
            _cid, payload = args[0], args[1]
            # payload structure: {"price": ..., "volume": ..., "timestamp": ..., ...}
            # Sometimes it's a list of trades; handle both
            trades = payload if isinstance(payload, list) else [payload]
            for tr in trades:
                if not isinstance(tr, dict):
                    continue
                price = tr.get("price")
                if price is None:
                    continue
                # Occasional log
                now = datetime.now(PT)
                if (now - self._last_tick_log).total_seconds() > 60:
                    log.info(f"Tick stream healthy | ticks_received={self._ticks_received} "
                             f"| latest_price={price} | armed={self._armed_signal is not None}")
                    self._last_tick_log = now

                # Only proceed if armed
                armed = self._armed_signal
                if not armed or armed.get("bar_fired"):
                    continue

                if not self._within_session(datetime.now(PT)):
                    continue

                # Midpoint touch check
                if armed["direction"] == "long":
                    # Long entry: FVG below current price, we retrace DOWN to midpoint
                    touched = price <= armed["midpoint"]
                else:
                    # Short entry: FVG above current price, we retrace UP to midpoint
                    touched = price >= armed["midpoint"]
                if not touched:
                    continue

                # Version B filter check
                if not self._version_b_filter_passes(armed["direction"]):
                    log.info(f"Tick touch at {price} — Version B filter BLOCKED "
                             f"({armed['direction']}, prev_close={self._prev_bar_close}, "
                             f"prev_vwap={self._prev_bar_vwap}, prev_ema9={self._prev_bar_ema9})")
                    # Mark bar_fired so we don't keep re-firing on every subsequent tick
                    armed["bar_fired"] = True
                    continue

                # Fire — schedule the async work
                log.info(f"TICK FIRED | price={price} touched midpoint={armed['midpoint']} "
                         f"| dir={armed['direction']}")
                armed["bar_fired"] = True  # prevent duplicate fires
                if self._loop is not None:
                    asyncio.run_coroutine_threadsafe(
                        self._tick_entry_from_armed(price, armed),
                        self._loop,
                    )
        except Exception as e:
            log.error(f"Tick callback exception: {e}", exc_info=True)

    async def _tick_entry_from_armed(self, price, armed):
        """Async entry firing from tick callback."""
        async with self._trade_lock:
            ok, reason = self._safety_gates_pass()
            if not ok:
                log.info(f"Tick entry SKIPPED by safety gate: {reason}")
                return
            # Build a signal-shaped dict for _fire_entry_from_signal
            signal = {
                "type": "entry",
                "direction": armed["direction"],
                "entry_price": armed["midpoint"],
                "stop": armed["stop"],
                "target": armed["target"],
            }
            # Reopen a session — the callback thread doesn't have one.
            # We need to use a session managed by the main loop.
            await self._fire_entry_from_signal(self._shared_session, signal, source="tick")

    async def _connect_signalr(self):
        """Set up SignalR connection to market hub."""
        log.info(f"Connecting SignalR to {MARKET_HUB_URL}")
        conn = (HubConnectionBuilder()
                .with_url(f"{MARKET_HUB_URL}?access_token={self.token}",
                          options={"access_token_factory": lambda: self.token,
                                   "skip_negotiation": True})
                .with_automatic_reconnect({"type": "raw", "keep_alive_interval": 10,
                                            "reconnect_interval": 5, "max_attempts": 999})
                .build())
        conn.on("GatewayTrade", self._on_trade_tick)
        conn.on_open(lambda: log.info("SignalR OPEN"))
        conn.on_close(lambda: log.warning("SignalR CLOSED"))
        conn.on_error(lambda data: log.error(f"SignalR ERROR: {data.error if data else 'unknown'}"))
        conn.start()
        await asyncio.sleep(2)
        try:
            conn.send("SubscribeContractTrades", [self.contract_id])
            log.info(f"Subscribed to GatewayTrade for {self.contract_id}")
        except Exception as e:
            log.error(f"SignalR subscribe failed: {e}")
        self._connection = conn

    # ═══════════════════════════════════════════════════════════════════════
    # MAIN
    # ═══════════════════════════════════════════════════════════════════════
    async def run(self):
        self._loop = asyncio.get_event_loop()
        async with aiohttp.ClientSession() as session:
            self._shared_session = session
            self.token = await self._authenticate(session)
            self.account_id = await self._get_account(session, self.token)
            self.contract_id, self.contract_name = await self._get_contract(session, self.token)

            # Send startup message so you know alerts work BEFORE trading starts
            await self.notifier.send_startup_test(session)

            await self._connect_signalr()

            # Run bar polling, position monitor, force-flat, and token refresh concurrently.
            await asyncio.gather(
                self._bar_polling_loop(session),
                self._position_monitor_loop(session),
                self._force_flat_loop(session),
                self._token_refresh_loop(session),
            )

    def print_startup_banner(self):
        print("=" * 70)
        print("  CHOCH/FVG INTRABAR BOT — MES 1 CONTRACT — LIVE")
        print("=" * 70)
        print(f"  Instrument:     MES (1 contract)")
        print(f"  Session:        {SESSION_OPEN} - {SESSION_CLOSE} PT")
        print(f"  Force flat:     {FORCE_FLAT_TIME} PT (before Topstep 3:10 CT)")
        print(f"  Max trades:     {MAX_TRADES_PER_DAY}/day")
        print(f"  Max consec L:   {MAX_CONSECUTIVE_LOSSES}")
        print(f"  Daily loss lim: ${DAILY_LOSS_LIMIT}")
        print(f"  Filter:         Version B (prev-bar close + prior VWAP/9-SMA)")
        print(f"  Entry:          Tick-level via GatewayTrade + bar-loop fallback")
        print("=" * 70)
        print("  This bot places REAL orders. Ctrl-C kills it immediately.")
        print("  Watch the log — never leave it unattended on first day.")
        print("=" * 70)
        print()


def main():
    bot = IntrabarMESBot()
    bot.print_startup_banner()
    try:
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        log.info("Interrupted by user — shutting down")
        if bot._connection is not None:
            try:
                bot._connection.stop()
            except Exception:
                pass


if __name__ == "__main__":
    main()
