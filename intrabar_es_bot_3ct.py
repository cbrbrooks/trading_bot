"""
CHOCH/FVG INTRABAR Bot — 3-CONTRACT COPY of intrabar_es_bot.py.

This file is a copy of intrabar_es_bot.py with exactly one functional change:
CONTRACTS is set to 3 instead of 1, plus the two fixed-dollar safety
backstops (MAX_TRADE_RISK_DOLLARS, DAILY_LOSS_LIMIT) that don't
auto-scale with contract count are widened to match, so they keep doing
their original job instead of either blocking every trade or firing too
early. See the override block below, right after `import choch_fvg_bot`,
for the exact math. Everything else — strategy logic, filters, brackets,
exits, gates, Telegram — is untouched, byte-for-byte identical to the
original file. The original intrabar_es_bot.py itself is NOT modified.

CHOCH/FVG INTRABAR Bot — ES 3 contracts, tick-level entry with bar-fallback.

Extends `choch_bot_live.ChochLiveBot` with a SignalR trade-tick listener that
fires entries the moment price touches an armed FVG midpoint, rather than
waiting for the 1-min bar to close. The parent bot's bar-polling loop
continues to run as a fallback — if the tick loop misses a signal (SignalR
drop, subscribe not confirmed, etc.), the bar loop still catches it at
bar close.

Design decisions from a full session of investigation:
  * Filter: Version B (previous-bar close as filter price, prior VWAP/9-SMA).
    Validated OOS 2021-2024 + 2025-2026 validation window. Both loops use
    the same filter so tick and bar agree on take/skip.
  * Tick primary, bar fallback (Design C). Tick fires first if it can,
    bar fires only if tick did not.
  * Race lock: entries serialize through `_intrabar_entry_lock` so tick
    and bar loops cannot both submit for the same signal.
  * Bar staleness gate: refuses to fire on bars > 90 seconds old (this
    caught today's 2.5-hour-stale bar correctly on the MES prototype).
  * SignalR: bounded reconnect (10 attempts), rebuild-on-token-refresh,
    clean unsubscribe on shutdown. Reflects research on SignalR + session
    limits.
  * Session policy awareness: TopstepX enforces 1 session per username.
    Recommended: register a separate account for the bot, or close the
    browser while it runs.

USAGE (same flags as intrabar_es_bot.py):
    python intrabar_es_bot_3ct.py                # LIVE — places real orders (3 contracts)
    python intrabar_es_bot_3ct.py --dry-run      # logs signals, no orders
"""
import asyncio
import logging
import os
import sys
from datetime import datetime, time, timedelta

# Belt-and-suspenders, same as choch_bot_live.py does: force ES regardless of
# .env. Set here too because in THIS file choch_fvg_bot (which pulls in
# topstep_bot_v5) is imported before choch_bot_live -- see the 3-CONTRACT
# override block just below for why the order is flipped.
os.environ["INSTRUMENT"] = "ES"

# ── 3-CONTRACT OVERRIDE — the only functional change in this file ─────────
# choch_fvg_bot.py hardcodes CONTRACTS=1 plus two fixed-DOLLAR safety
# backstops that do NOT auto-scale with contract count:
#   MAX_TRADE_RISK_DOLLARS (110.0): checked as `total_risk = FIXED_STOP_POINTS
#     * POINT_VALUE * CONTRACTS; if total_risk > MAX_TRADE_RISK_DOLLARS: skip
#     the trade`. At CONTRACTS=1 that's 2.0pt * $50 * 1 = $100, so 110 sits
#     $10 above it (the original file's own comment explains this margin).
#     At CONTRACTS=3 the real risk is 2.0*50*3 = $300 -- which would blow
#     through 110 and silently cancel EVERY trade if left unchanged.
#   DAILY_LOSS_LIMIT (230.0): calibrated to sit ~$22.44 above the worst
#     reachable day under MAX_CONSECUTIVE_LOSSES=2 (2 losses of $103.78 =
#     $207.56 at 1 contract). At CONTRACTS=3 each loss is $311.34, so the
#     worst 2-loss day is $622.68 -- this must widen too, or the daily-loss
#     backstop would effectively fire after the FIRST loss instead of the
#     second, changing the bot's real behavior, not just its position size.
#
# choch_bot_live.py (imported below) does `from choch_fvg_bot import
# CONTRACTS, ..., MAX_TRADE_RISK_DOLLARS, ..., DAILY_LOSS_LIMIT, ...` at ITS
# OWN module level -- that copies the current values into choch_bot_live's
# own namespace at the moment it's imported. So choch_fvg_bot must be
# imported and patched FIRST, before choch_bot_live (and everything that
# imports it) ever runs, or the override arrives too late and every
# Telegram/log line and safety gate keeps using the original 1-contract
# numbers.
import choch_fvg_bot

NEW_CONTRACTS = 3
choch_fvg_bot.CONTRACTS = NEW_CONTRACTS
choch_fvg_bot.MAX_TRADE_RISK_DOLLARS = (
    choch_fvg_bot.FIXED_STOP_POINTS * choch_fvg_bot.POINT_VALUE * NEW_CONTRACTS + 10.0
)
_loss_per_trade_3ct = (
    choch_fvg_bot.FIXED_STOP_POINTS * choch_fvg_bot.POINT_VALUE * NEW_CONTRACTS
    + choch_fvg_bot.FEE_PER_ROUND_TURN * NEW_CONTRACTS
)
choch_fvg_bot.DAILY_LOSS_LIMIT = choch_fvg_bot.MAX_CONSECUTIVE_LOSSES * _loss_per_trade_3ct + 22.44

# Parent bot imports next — now that choch_fvg_bot's constants are already
# overridden, choch_bot_live's own `from choch_fvg_bot import CONTRACTS, ...`
# picks up 3 contracts and the rescaled backstops, not the original values.
import choch_bot_live
from choch_bot_live import ChochLiveBot, DRY_RUN, log, BASE_URL
from topstep_bot_v5 import PT

from signalrcore.hub_connection_builder import HubConnectionBuilder

from delta_filter import DeltaTracker, DELTA_WINDOW_SECS

# Distinguish this bot's Telegram alerts from the original 1-contract bot's.
# Every entry/exit/bracket alert already mentions contract count dynamically
# (it reads the same CONTRACTS value we just overrode), so those need no
# change -- only the generic startup test message doesn't mention size at all.
async def _send_startup_test_3ct(self, session):
    await self.send(session,
                    "CHOCH bot (3-CONTRACT copy) started. Alerts are working. "
                    "You'll get messages for entries, exits, safety stops, errors, "
                    "and data-feed stalls.",
                    prefix="[STARTUP-3CT]")
choch_bot_live.TelegramNotifier.send_startup_test = _send_startup_test_3ct

MARKET_HUB_URL = "https://rtc.topstepx.com/hubs/market"

MAX_BAR_STALENESS_SECS = 90

# ── Version B is MANDATORY — enforced at import ────────────────────────────
# Version B (the prev-bar close vs prior VWAP/9-SMA filter) shares the engine's
# VWAP_EMA_FILTER flag, which also gates the engine's own close-VWAP check. This
# pair is the out-of-sample-validated quality filter behind the +$4,390 result;
# it only ever removes low-quality setups. Force the flag ON here, unconditionally,
# so NO edit to choch_fvg_bot.py can ever silently disable Version B — this bot
# always trades with it, in every mode. (Read at call-time by both the engine's
# _check_entry and this bot's _version_b_filter_passes, so setting it at import
# guarantees it is True before a single bar is processed.)
choch_fvg_bot.VWAP_EMA_FILTER = True


class IntrabarESBot(ChochLiveBot):
    """Extends ChochLiveBot with tick-level entry via SignalR."""

    def __init__(self):
        super().__init__()

        # Initialize self.token so the SignalR wait loop doesn't AttributeError
        # before _authenticate runs. Set to real token in overridden _authenticate.
        self.token = None

        # TICK-ONLY MODE — when True, disables the bar-loop entry path.
        # If the tick loop can't fire (SignalR not connected, subscribe
        # failed, etc.), NO entry happens. This prevents the July-16-style
        # slippage where a bar-close entry fills several points off signal.
        # Enabled via --tick-only command-line flag.
        self.tick_only = "--tick-only" in sys.argv

        # CHOCH-ONLY MODE — enter EXACTLY like the choch bot: on the engine's
        # bar-close _check_entry decision (plus the Version B filter), and NOTHING
        # from the intrabar tick path. This is the strategy the replay validated at
        # +$4,390; the tick-entry experiment is turned off. Concretely this flag:
        #   * skips SignalR entirely (the tick feed is only used for tick entries),
        #   * disables tick-entry firing (belt-and-suspenders, even if SignalR ran),
        #   * takes the parent's bar-close entries (so tick_only must be False).
        # Everything else — brackets, exits, safety gates, Telegram, force-flat —
        # is the parent's proven machinery, unchanged.
        self.choch_only = "--choch-only" in sys.argv
        if self.choch_only:
            self.tick_only = False   # bar-close entries must NOT be refused

        # ── ORDER-FLOW DELTA FILTER (NEW) ──────────────────────────────────
        # Only fire a tick entry when the trailing 5-min cumulative delta CONFIRMS
        # the trade direction (net buying for a long, net selling for a short).
        # Backtest (3 months of ES trades, Databento 2026-04..07): delta AGREES
        # ~64% win / +$1,969; delta OPPOSES ~24% win / -$2,319 (the false starts).
        # ON by default — it only ever REMOVES trades, so it is strictly more
        # conservative than the current bot. Disable with --no-delta-filter.
        # Needs the SignalR tick stream, so it has no effect in --choch-only mode.
        # NOTE: still UNPROVEN forward — the profitable side is only 14 backtest
        # trades. This runs to COLLECT dry-run data, not as a trusted edge yet.
        self.delta_filter = "--no-delta-filter" not in sys.argv
        self.delta_tracker = DeltaTracker(window_secs=DELTA_WINDOW_SECS, log=log)
        if self.delta_filter:
            log.info(f"ORDER-FLOW DELTA FILTER ON (window={DELTA_WINDOW_SECS}s). Tick "
                     f"entries fire only when 5-min delta confirms direction. "
                     f"Disable with --no-delta-filter.")
        else:
            log.info("Order-flow delta filter OFF (--no-delta-filter).")

        # ── COLLECT-ALL: log EVERY trade for dry-run data collection ────────
        # Disables the daily RISK gates — MAX_TRADES_PER_DAY (the 3/day cap),
        # MAX_CONSECUTIVE_LOSSES (the 2-loss stop), and DAILY_LOSS_LIMIT — so a
        # dry-run records the FULL sample of trades to study, not a truncated
        # 3/day slice that stops after 2 losses. STRICTLY dry-run only: these
        # gates protect a funded account and must never be off with real money,
        # so --collect-all is refused (caps stay ON) unless --dry-run is set too.
        self.collect_all = "--collect-all" in sys.argv
        if self.collect_all and not DRY_RUN:
            log.error("REFUSING --collect-all in LIVE mode: it disables the daily risk "
                      "caps, which must stay ON with real money. Caps remain ENFORCED. "
                      "Use --collect-all only together with --dry-run.")
            self.collect_all = False
        if self.collect_all:
            choch_fvg_bot.MAX_TRADES_PER_DAY   = 100000
            choch_fvg_bot.MAX_CONSECUTIVE_LOSSES = 100000
            choch_fvg_bot.DAILY_LOSS_LIMIT     = 1e12
            log.info("COLLECT-ALL (dry-run only): daily caps DISABLED — no 3/day cap, "
                     "no 2-consecutive-loss stop, no daily-loss stop. Logging EVERY "
                     "delta-confirmed trade for data collection. (One position at a time "
                     "still applies; trades are taken sequentially as each resolves.)")

        # Version B is forced ON at import (see module top). Re-assert here and
        # record it in the LOG FILE (the startup banner only prints to the
        # terminal), so every session's choch_bot.log proves the filter was
        # active for the trades it placed.
        if not choch_fvg_bot.VWAP_EMA_FILTER:
            choch_fvg_bot.VWAP_EMA_FILTER = True
        log.info("Version B ENFORCED ON — mandatory prev-bar close vs prior "
                 "VWAP/9-SMA filter (VWAP_EMA_FILTER=True). Cannot be disabled "
                 "at runtime; applied to every entry.")

        # Set to True if SignalR startup failed hard (no tick entries this session).
        # Combined with tick_only=True, this means the bot will refuse to place
        # ANY entries after we know tick detection isn't working.
        self._signalr_startup_failed = False

        # Race lock: only one entry submission at a time, across all loops.
        # Created lazily in run() since asyncio.Lock needs an event loop.
        self._intrabar_entry_lock = None

        # SignalR connection state
        self._signalr = None
        self._signalr_connected = False
        self._signalr_subscribed = False
        self._signalr_reconnects = 0
        self._loop = None
        self._shared_session = None

        # Tick statistics
        self._ticks_received = 0
        self._last_tick_health_log = datetime.now(PT)

        # Armed FVG midpoint that the tick loop is watching
        self._armed_signal = None

        # ── Confirm-at-close state ────────────────────────────────────────
        # A PROVISIONAL tick entry: fired intrabar to capture the midpoint fill,
        # but NOT yet confirmed by the engine's bar-close _check_entry. At the
        # entry bar's close, if the engine confirms the same setup we keep it (a
        # real choch trade at a better fill); if the bar's own action invalidated
        # it (a CHOCH flip cleared the gap — the reason the naive tick took ~2x
        # the trades), we FLATTEN immediately. Verified offline against Databento:
        # 35 confirmed trades + 108 flattens over 26 days, churn only ~$154, net
        # +$3,281 vs the choch bot's theoretical +$3,435 (which it can't get live
        # because it fills at the bar close, not the midpoint).
        self._provisional = None   # {direction, midpoint, fired_at} or None

        # Version B filter state — captured just before the engine consumes
        # each bar, so at intrabar entry time these reflect "the previous
        # completed bar's" close and indicator values.
        self._prev_bar_close = None
        self._prev_bar_vwap = None
        self._prev_bar_ema9 = None

    # ═══════════════════════════════════════════════════════════════════════
    # Auth overrides — expose the current JWT token as `self.token` for the
    # SignalR loop. The parent bot's run() keeps `token` as a LOCAL variable
    # and doesn't store it on self, so we intercept every method that gets
    # the token as a parameter and stash it. This means:
    #   * Initial auth (via _authenticate) sets self.token
    #   * Token-refresh path (parent calls _check_order_status etc with the
    #     fresh token) also updates self.token
    # Return values are unchanged so parent's flow works identically.
    # ═══════════════════════════════════════════════════════════════════════

    async def _authenticate(self, session):
        token = await super()._authenticate(session)
        self.token = token
        return token

    async def _get_account(self, session, token):
        self.token = token
        return await super()._get_account(session, token)

    async def _get_contract(self, session, token):
        self.token = token
        return await super()._get_contract(session, token)

    async def _check_order_status(self, session, token, order_id):
        self.token = token
        return await super()._check_order_status(session, token, order_id)

    async def _check_position_closed(self, session, token):
        self.token = token
        return await super()._check_position_closed(session, token)

    async def _place_bracket_entry(self, session, token, action, stop_ticks, target_ticks):
        self.token = token
        return await super()._place_bracket_entry(session, token, action, stop_ticks, target_ticks)

    # ═══════════════════════════════════════════════════════════════════════
    # SignalR
    # ═══════════════════════════════════════════════════════════════════════

    def _on_trade_tick(self, args):
        """SignalR callback (library thread). Schedules async entry work
        onto the main loop; does NOT make broker calls directly here."""
        try:
            self._ticks_received += 1

            # CHOCH-ONLY: never fire a tick entry. (SignalR is skipped in this
            # mode, so this callback shouldn't run at all — but guard anyway so
            # a stray subscription can never place an intrabar order.)
            if self.choch_only:
                return

            now = datetime.now(PT)
            if (now - self._last_tick_health_log).total_seconds() > 60:
                log.info(f"Tick stream healthy | received={self._ticks_received} "
                         f"| armed={self._armed_signal is not None} "
                         f"| in_bracket={self._awaiting_bracket_close}")
                self._last_tick_health_log = now

            if not isinstance(args, list) or len(args) < 2:
                return
            _cid, payload = args[0], args[1]
            trades = payload if isinstance(payload, list) else [payload]

            for tr in trades:
                if not isinstance(tr, dict):
                    continue
                price = tr.get("price")
                if price is None:
                    continue

                # Feed the order-flow delta tracker on EVERY trade tick, so the
                # trailing 5-min delta is warm whenever a midpoint touch fires.
                if self.delta_filter:
                    size = tr.get("volume") or tr.get("size") or tr.get("v") or 1
                    self.delta_tracker.add_trade(price, size, tr)

                armed = self._armed_signal
                if armed is None or armed.get("bar_fired"):
                    continue
                if self._awaiting_bracket_close:
                    continue

                now_pt = datetime.now(PT)
                if not (choch_fvg_bot.SESSION_OPEN <= now_pt.time()
                        <= choch_fvg_bot.SESSION_CLOSE):
                    continue

                if armed["direction"] == "long":
                    touched = price <= armed["midpoint"]
                else:
                    touched = price >= armed["midpoint"]
                if not touched:
                    continue

                if not self._version_b_filter_passes(armed["direction"]):
                    log.info(f"Tick touch @ {price} — Version B BLOCKED "
                             f"(dir={armed['direction']}, "
                             f"prev_close={self._prev_bar_close}, "
                             f"prev_vwap={self._prev_bar_vwap}, "
                             f"prev_ema9={self._prev_bar_ema9})")
                    armed["bar_fired"] = True
                    continue

                # ── ORDER-FLOW DELTA CONFIRMATION (NEW) ────────────────────
                # The false-start filter: only fire when the trailing 5-min
                # delta confirms the trade's direction. Backtest: delta agrees
                # ~64% win, opposes ~24% win. delta==0 (cold stream) does NOT
                # confirm, so a warmup/quiet tape never fires an unconfirmed entry.
                if self.delta_filter:
                    agrees, dval = self.delta_tracker.agrees(armed["direction"])
                    if not agrees:
                        log.info(f"[DELTA-REJECT] touch @ {price} "
                                 f"midpoint={armed['midpoint']} dir={armed['direction']} "
                                 f"| 5-min delta={dval:+.0f} does NOT confirm — "
                                 f"skipping (false-start filter).")
                        armed["bar_fired"] = True
                        continue
                    log.info(f"[DELTA-OK] 5-min delta={dval:+.0f} confirms "
                             f"{armed['direction']}.")

                armed["bar_fired"] = True
                log.info(f"TICK FIRED | price={price} "
                         f"touched midpoint={armed['midpoint']} "
                         f"| dir={armed['direction']}")

                if self._loop is not None:
                    asyncio.run_coroutine_threadsafe(
                        self._tick_entry_from_armed(price, armed), self._loop,
                    )
                return
        except Exception as e:
            log.error(f"Tick callback exception: {e}", exc_info=True)

    async def _tick_entry_from_armed(self, price, armed):
        """Submit entry from a tick-level midpoint touch. Uses the parent
        bot's _handle_signal so proven order-placement code runs."""
        if self._intrabar_entry_lock is None:
            log.error("Tick entry: lock not initialized (should never happen)")
            return
        async with self._intrabar_entry_lock:
            if self._awaiting_bracket_close:
                log.info("Tick entry: aborted, already in bracket trade")
                return
            if self._live_entry_fill is not None:
                log.info("Tick entry: aborted, entry already in flight")
                return

            # ── Safety gates ──────────────────────────────────────────────
            # Tick entries bypass the engine's _check_entry, which is where the
            # BAR path enforces the daily caps (choch_fvg_bot.py:555-560). Without
            # replicating them here the tick path ignores every limit — observed
            # 2026-07-21 dry-run, when 5 tick entries fired against a 3/day cap
            # while the engine counter sat at 0/3 the whole session. Enforce the
            # same three gates before committing to a tick entry.
            eng = self.wrapper.engine if self.wrapper is not None else None
            if eng is not None:
                if eng.trades_today >= choch_fvg_bot.MAX_TRADES_PER_DAY:
                    log.info(f"Tick entry BLOCKED — daily trade cap reached "
                             f"({eng.trades_today}/{choch_fvg_bot.MAX_TRADES_PER_DAY}). "
                             f"No more entries today.")
                    armed["bar_fired"] = True
                    return
                if eng.consecutive_losses >= choch_fvg_bot.MAX_CONSECUTIVE_LOSSES:
                    log.info(f"Tick entry BLOCKED — consecutive-loss limit reached "
                             f"({eng.consecutive_losses}/"
                             f"{choch_fvg_bot.MAX_CONSECUTIVE_LOSSES}).")
                    armed["bar_fired"] = True
                    return
                if eng.daily_pnl <= -abs(choch_fvg_bot.DAILY_LOSS_LIMIT):
                    log.info(f"Tick entry BLOCKED — daily loss limit reached "
                             f"(daily_pnl=${eng.daily_pnl:.2f}, "
                             f"limit=${choch_fvg_bot.DAILY_LOSS_LIMIT}).")
                    armed["bar_fired"] = True
                    return

            signal = {
                "type": "entry",
                "direction": armed["direction"],
                "entry_price": armed["midpoint"],
                "stop": armed["stop"],
                "target": armed["target"],
                "reason": (f"TICK ENTRY {armed['direction']} "
                           f"@ midpoint={armed['midpoint']} (touched at {price})"),
            }
            # DRY-RUN alert: the tick path delegates straight to the parent
            # handler (below), which suppresses Telegram in dry-run. Without
            # this, tick entries — the ONLY entries that fire in --tick-only
            # mode — would never notify. Fire it here so the bar override and
            # the tick path alert identically. Fail-safe: never raises.
            await self._send_dry_run_entry_alert(self._shared_session, signal, "tick")

            # Mark this a PROVISIONAL entry pending bar-close confirmation. We do
            # NOT increment trades_today here — the engine's _check_entry does that
            # only when it CONFIRMS at the bar close (see _handle_signal below). A
            # flattened false start must not burn a daily-trade slot.
            self._provisional = {
                "direction": armed["direction"],
                "midpoint": armed["midpoint"],
                "stop": armed["stop"],
                "target": armed["target"],
                "fired_at_price": price,
            }
            log.info(f"PROVISIONAL tick entry placed @ midpoint={armed['midpoint']} "
                     f"dir={armed['direction']} — awaiting bar-close confirmation.")

            # Delegate to parent bot's proven entry handler (places the real
            # bracket and captures the midpoint fill).
            # NOTE: we pass a session; the tick loop uses its own aiohttp
            # session (see _start_signalr_when_ready). Different session,
            # same broker, same token, functionally equivalent.
            await ChochLiveBot._handle_signal(self, self._shared_session,
                                              self.token, signal)

    async def _send_dry_run_entry_alert(self, session, signal, source):
        """In dry-run, push a Telegram alert for an entry the bot WOULD have
        taken. The parent bot suppresses ALL Telegram in dry-run, and the
        tick-entry path delegates straight to the parent (bypassing the bar
        override), so without a shared helper neither path would notify.
        Used by both the tick path and the bar override so they alert with
        identical formatting. Fail-safe: a Telegram error can never interrupt
        evaluation or trading."""
        if not (DRY_RUN and self.notifier is not None):
            return
        try:
            await self.notifier.send(
                session,
                f"ES {signal['direction'].upper()} would fire "
                f"@ signal={signal['entry_price']} "
                f"stop={signal['stop']} target={signal['target']} "
                f"| source={source} | {signal.get('reason', '')}",
                prefix="[DRY-RUN ENTRY]",
            )
        except Exception as e:
            log.warning(f"Dry-run Telegram alert failed: {e}")

    async def _flatten_provisional(self, session, token, prov):
        """Close a provisional tick entry the engine did NOT confirm at bar close.
        Reuses the parent's proven flatten path (Position/closeAll), then resets
        the bracket-tracking state so the bot is clean for the next setup. In
        dry-run the parent's _flatten_all is a no-op log, so this exercises the
        full confirm/flatten DECISION path without placing or closing real orders
        — which is exactly what a dry-run session validates. Fail-safe: a flatten
        error is logged loudly but never left silent."""
        try:
            await self._flatten_all(
                session, token,
                f"Provisional tick entry not confirmed at bar close "
                f"(dir={prov['direction']} @ midpoint={prov['midpoint']})")
        except Exception as e:
            log.error(f"FLATTEN of unconfirmed provisional FAILED — check the "
                      f"account manually for an open position: {e}", exc_info=True)
        # Clear bracket-tracking state (the position, if any, is now closed).
        self._awaiting_bracket_close = False
        self._live_entry_fill = None
        self._bracket_entry_signal = None
        if self.notifier is not None:
            try:
                await self.notifier.send(
                    session,
                    f"Tick entry {prov['direction']} @ {prov['midpoint']} not "
                    f"confirmed at bar close — flattened (setup invalidated).",
                    prefix="[FLATTEN]")
            except Exception:
                pass

    async def _connect_signalr(self):
        """Open (or re-open) SignalR market hub and subscribe to trades."""
        # Tear down any existing connection first (used on token refresh)
        if self._signalr is not None:
            try:
                if self._signalr_subscribed and self.contract_id:
                    self._signalr.send("UnsubscribeContractTrades",
                                       [self.contract_id])
                self._signalr.stop()
            except Exception as e:
                log.warning(f"SignalR teardown (continuing): {e}")
            self._signalr = None
            self._signalr_connected = False
            self._signalr_subscribed = False

        log.info(f"Connecting SignalR to {MARKET_HUB_URL}")
        conn = (HubConnectionBuilder()
                .with_url(f"{MARKET_HUB_URL}?access_token={self.token}",
                          options={
                              "access_token_factory": lambda: self.token,
                              "skip_negotiation": True,
                          })
                .with_automatic_reconnect({
                    "type": "raw",
                    "keep_alive_interval": 10,
                    "reconnect_interval": 5,
                    "max_attempts": 10,
                })
                .build())

        conn.on("GatewayTrade", self._on_trade_tick)
        conn.on_open(self._on_signalr_open)
        conn.on_close(self._on_signalr_close)
        conn.on_error(lambda data: log.error(
            f"SignalR ERROR: {data.error if data else 'unknown'}"))

        conn.start()
        for _ in range(10):
            await asyncio.sleep(0.5)
            if self._signalr_connected:
                break
        if not self._signalr_connected:
            log.warning("SignalR did not confirm OPEN within 5s. "
                        "Bar-loop will still work as fallback.")

        try:
            conn.send("SubscribeContractTrades", [self.contract_id])
            self._signalr_subscribed = True
            log.info(f"Subscribed to GatewayTrade for {self.contract_id}")
        except Exception as e:
            log.error(f"SignalR subscribe failed: {e}. "
                      f"Bar-loop will still work.")

        self._signalr = conn

    def _on_signalr_open(self):
        self._signalr_connected = True
        if self._signalr_reconnects > 0 and self.contract_id:
            try:
                self._signalr.send("SubscribeContractTrades",
                                   [self.contract_id])
                log.info(f"SignalR reconnected — re-subscribed to GatewayTrade")
            except Exception as e:
                log.error(f"SignalR re-subscribe failed: {e}")
        else:
            log.info("SignalR OPEN")

    def _on_signalr_close(self):
        self._signalr_connected = False
        self._signalr_subscribed = False
        self._signalr_reconnects += 1
        log.warning(f"SignalR CLOSED (reconnect attempt "
                    f"#{self._signalr_reconnects})")

    async def _signalr_reconnect_on_token_refresh(self):
        """Called when the parent bot rotates the JWT. WebSocket transport
        only reads the token at connect time, so we rebuild the socket."""
        log.info("Rebuilding SignalR with refreshed token")
        try:
            await self._connect_signalr()
        except Exception as e:
            log.error(f"SignalR rebuild after token refresh failed: {e}")

    # ═══════════════════════════════════════════════════════════════════════
    # Version B filter
    # ═══════════════════════════════════════════════════════════════════════

    def _version_b_filter_passes(self, direction):
        """Version B: prev-bar close vs prior VWAP/9-SMA.
        Blocks LONG if price < BOTH ema9 AND vwap.
        Blocks SHORT if price > BOTH ema9 AND vwap.
        """
        if not choch_fvg_bot.VWAP_EMA_FILTER:
            return True
        if (self._prev_bar_close is None or
            self._prev_bar_vwap is None or
            self._prev_bar_ema9 is None):
            return True  # warmup — allow

        price = self._prev_bar_close
        if direction == "long":
            blocks = (price < self._prev_bar_ema9 and
                      price < self._prev_bar_vwap)
        else:
            blocks = (price > self._prev_bar_ema9 and
                      price > self._prev_bar_vwap)
        return not blocks

    # ═══════════════════════════════════════════════════════════════════════
    # Engine state observation
    # ═══════════════════════════════════════════════════════════════════════

    def _update_armed_signal_from_engine(self):
        """After each bar close, refresh the FVG midpoint the tick loop
        should watch. Reads engine internals: active_fvgs (list of FVG
        objects) and choch_bias (string 'bullish'/'bearish').

        Attribute names verified against choch_fvg_bot.py:
          * engine.active_fvgs: list[FVG]
          * engine.choch_bias: 'bullish' | 'bearish' | None
          * fvg.type: 'bullish' | 'bearish'
          * fvg.midpoint, fvg.top, fvg.bottom
          * fvg.consumed: True if engine already fired on this FVG
        """
        if self.wrapper is None:
            self._armed_signal = None
            return

        eng = self.wrapper.engine
        active_fvgs = getattr(eng, "active_fvgs", None)
        choch_bias = getattr(eng, "choch_bias", None)

        if not active_fvgs or not choch_bias:
            self._armed_signal = None
            return

        # ── ATR volatility gate ───────────────────────────────────────────
        # The bar path (choch_fvg_bot add_candle) refuses ALL entries while
        # 1-min ATR > ATR_MAX_THRESHOLD — the strategy sits out extreme
        # volatility, which clusters in the early session. The tick loop was
        # skipping this gate, so it fired on early high-ATR chop the bar bot
        # avoids, then burned the 3-trade daily cap before the calmer midday
        # setups the bar bot actually takes. Applying it here realigns tick
        # selection with the bar path. Checked at arming (just-closed bar's
        # ATR), matching when the bar path would evaluate the next entry.
        atr = getattr(eng, "atr", None)
        if atr is not None and atr > choch_fvg_bot.ATR_MAX_THRESHOLD:
            self._armed_signal = None
            return

        # Freshest UNCONSUMED FVG matching choch_bias that passes EVERY hard gate
        # the bar loop's _check_entry applies, in the same order. Any gate the
        # tick path skips here means it fires on setups the strategy rejects — the
        # 2026-07-13 quality filter drifted out of sync exactly this way, silently
        # doubling the live tick bot's trade count against a filtered backtest.
        candidate = None
        for fvg in reversed(active_fvgs):
            if getattr(fvg, "consumed", False):
                continue
            fvg_type = getattr(fvg, "type", None)
            if fvg_type != choch_bias:
                continue
            # Fib validity — matches _check_entry line 574.
            try:
                if not eng._fib_valid(fvg):
                    continue
            except Exception:
                # If the engine's fib check errors, be conservative and skip.
                continue
            # Breakaway-only filter — matches _check_entry line 600 (off by default).
            if choch_fvg_bot.BREAKAWAY_ONLY and not getattr(fvg, "breakaway", False):
                continue
            # QUALITY FILTER — matches _check_entry lines 616-623. The bar path
            # runs every setup through evaluate_entry_quality and skips the
            # low-quality ones (wide + stale gaps); the tick path MUST too, or it
            # takes trades the strategy is configured to reject. The filter only
            # actually uses FVG width and age — the other args are accepted for
            # call-site parity and deliberately ignored (see choch_quality_filters).
            if choch_fvg_bot.QUALITY_FILTERS_ENABLED:
                try:
                    from choch_quality_filters import evaluate_entry_quality
                    cai = getattr(fvg, "created_at_index", -1)
                    fvg_age = (len(eng.candles) - 1 - cai) if cai >= 0 else 0
                    allowed, _, _ = evaluate_entry_quality(
                        eng.candles, eng.choch_bias,
                        getattr(eng, "choch_swing_high", None),
                        getattr(eng, "choch_swing_low", None),
                        fvg.top - fvg.bottom, fvg_age, None)
                    if not allowed:
                        continue  # quality-blocked; try an older active FVG
                except Exception:
                    # Scoring error → skip, matching the fib-check fail-safe: never
                    # arm on a setup we couldn't fully validate.
                    continue
            candidate = fvg
            break
        if not candidate:
            self._armed_signal = None
            return

        mid = getattr(candidate, "midpoint", None)
        top = getattr(candidate, "top", None)
        bot = getattr(candidate, "bottom", None)
        if mid is None:
            self._armed_signal = None
            return

        direction = "long" if choch_bias == "bullish" else "short"

        # ── Genuine-retrace gate ──────────────────────────────────────────
        # Only arm while the LAST CLOSED bar sits on the correct side of the
        # midpoint — above it for a long, below it for a short. That is the
        # setup's premise: price is poised to retrace DOWN (long) / UP (short)
        # INTO the frozen gap. Once price has already crossed the midpoint, the
        # retrace has happened (or blown through), and a later tick touch is a
        # continuation entry, NOT the retrace the strategy trades. Without this
        # the tick loop fires on blow-through bars the bar path rejects — the
        # source of its ~2x trade count and lower win rate. The midpoint stays
        # frozen from the prior FVG bars; this only controls WHEN it's live.
        last_close = self._prev_bar_close
        if last_close is not None:
            if direction == "long" and last_close <= mid:
                self._armed_signal = None
                return
            if direction == "short" and last_close >= mid:
                self._armed_signal = None
                return

        # Stop/target geometry — replicate _check_entry (choch_fvg_bot.py 625-669)
        # EXACTLY so tick and bar risk the SAME amount on the SAME setup:
        #   1. natural FVG-width risk drives the risk-BAND quality check
        #      (MIN_STOP_POINTS..MAX_STOP_POINTS), NOT the broker floor;
        #   2. FIXED_STOP_POINTS (if > 0) then overrides the stop distance so
        #      every trade risks a flat 1.0pt (the mode the strategy runs in);
        #   3. broker-minimum padding only applies when NOT in fixed mode;
        #   4. the dollar risk cap is checked on the SUBMITTED stop.
        entry = mid
        if direction == "long":
            stop = bot if bot is not None else entry - choch_fvg_bot.MIN_STOP_POINTS
            risk_points = entry - stop
        else:
            stop = top if top is not None else entry + choch_fvg_bot.MIN_STOP_POINTS
            risk_points = stop - entry

        # Risk-band filter on the NATURAL width (matches _check_entry line 633).
        if (risk_points <= 0
                or risk_points > choch_fvg_bot.MAX_STOP_POINTS
                or risk_points < choch_fvg_bot.MIN_STOP_POINTS):
            self._armed_signal = None
            return

        # FIXED_STOP override (matches _check_entry lines 640-642 + 655-660).
        if choch_fvg_bot.FIXED_STOP_POINTS > 0:
            risk_points = choch_fvg_bot.FIXED_STOP_POINTS
            stop = (entry - risk_points if direction == "long"
                    else entry + risk_points)
            broker_stop = risk_points
        else:
            broker_stop = max(risk_points, choch_fvg_bot.BROKER_MIN_STOP_POINTS)
            if broker_stop != risk_points:
                stop = (entry - broker_stop if direction == "long"
                        else entry + broker_stop)

        # Dollar risk cap on the SUBMITTED stop (matches _check_entry 664-666).
        total_risk = broker_stop * choch_fvg_bot.POINT_VALUE * choch_fvg_bot.CONTRACTS
        if total_risk > choch_fvg_bot.MAX_TRADE_RISK_DOLLARS:
            self._armed_signal = None
            return

        # Target uses risk_points (fixed 1.0pt -> 3.0pt target at RR 3.0).
        target = (entry + choch_fvg_bot.RR_MULTIPLE * risk_points if direction == "long"
                  else entry - choch_fvg_bot.RR_MULTIPLE * risk_points)
        risk = broker_stop  # actual submitted risk, recorded on the armed dict below

        existing = self._armed_signal
        bar_fired = (existing is not None and
                     existing.get("midpoint") == mid and
                     existing.get("bar_fired", False))

        self._armed_signal = {
            "direction": direction,
            "midpoint": mid,
            "stop": stop,
            "target": target,
            "risk_pts": risk,
            "reward_pts": abs(target - mid),
            "bar_fired": bar_fired,
        }

    def _snapshot_prior_bar_state(self):
        """Capture the engine's VWAP/EMA9 BEFORE it consumes the next bar.
        These represent the PRIOR bar's completed indicator values — what
        Version B compares against."""
        if self.wrapper is None:
            return
        eng = self.wrapper.engine
        self._prev_bar_vwap = getattr(eng, "vwap", None)
        self._prev_bar_ema9 = getattr(eng, "ema9", None)

    def _post_bar_hook(self, candle):
        """After the engine has ingested a bar, refresh state for tick loop."""
        # The bar that just closed IS the "previous bar" for the next
        # intrabar entry decision. So its close becomes prev_bar_close.
        self._prev_bar_close = candle.close
        # VWAP and EMA9 stay at the pre-bar values in _prev_bar_vwap /
        # _prev_bar_ema9 — do NOT overwrite. _snapshot_prior_bar_state
        # will refresh them BEFORE the next add_candle call.
        self._update_armed_signal_from_engine()

    # ═══════════════════════════════════════════════════════════════════════
    # Entry override — serialize via lock, apply Version B, dedupe tick vs bar
    # ═══════════════════════════════════════════════════════════════════════

    async def _handle_signal(self, session, token, signal):
        """Override parent's entry handler. Non-entry signals pass through."""
        # ── Confirm-at-close resolution ───────────────────────────────────
        # The parent bar loop calls this once per bar close with the engine's
        # _check_entry decision. If a tick fired a PROVISIONAL during the bar that
        # just closed, resolve it now: the engine is the authority on WHICH trades
        # to take (it decides off the completed bar, including any CHOCH flip the
        # tick couldn't see). Confirm -> keep the tick's better fill; reject ->
        # flatten the false start. This is what makes the tick take the choch
        # bot's EXACT trades instead of ~2x of them.
        if self._provisional is not None:
            prov = self._provisional
            self._provisional = None
            engine_confirms = (signal is not None and signal.get("type") == "entry")
            if engine_confirms:
                # GUARD (added 2026-07-28): if the broker bracket has ALREADY closed this
                # trade — a fast stop/target that resolved WITHIN the same bar the entry
                # fired on — the trade is DONE and its real P&L was already booked and
                # reconciled at the broker close. The engine's _check_entry, run on this
                # bar close, has just re-armed a PHANTOM open position (in_trade=True); on
                # the NEXT bar it would book a DUPLICATE theoretical exit, doubling
                # daily_pnl. (Observed live 2026-07-28: a 15-sec stop-out logged
                # daily_pnl=-$207.56 for a single -$103.78 loss.) Force the engine flat.
                # trades_today / consecutive_losses are already correct and untouched;
                # only the phantom OPEN position is cleared so no second exit is booked.
                if not self._awaiting_bracket_close:
                    eng = self.wrapper.engine if self.wrapper is not None else None
                    if eng is not None:
                        eng.in_trade = False
                        eng.position = None
                        eng.entry_price = None
                        eng.stop_price = None
                        eng.target_price = None
                        eng.breakeven_trigger = None
                        eng.breakeven_done = False
                        eng._last_theoretical_exit_pnl = 0.0
                    log.info(f"Provisional CONFIRMED at bar close, but the broker bracket "
                             f"ALREADY closed this trade within the entry bar "
                             f"(dir={prov['direction']} @ midpoint={prov['midpoint']}). "
                             f"Engine forced FLAT to prevent a duplicate theoretical exit "
                             f"— real P&L was already booked at the broker close.")
                    return
                log.info(f"Provisional CONFIRMED at bar close "
                         f"(dir={prov['direction']} @ midpoint={prov['midpoint']}). "
                         f"Keeping the tick fill; engine now owns the exit. The "
                         f"engine's own entry signal is consumed here so no second "
                         f"bracket is placed.")
                # _check_entry already set eng.in_trade + trades_today; the tick's
                # bracket is live (_awaiting_bracket_close). Swallow the engine's
                # duplicate entry signal — the existing bracket IS the position.
                return
            # NOT re-confirmed by the completed bar. In the OLD (pre-delta)
            # design we flattened here, treating the unconfirmed tick as a
            # "false start." That flatten is what produced the catastrophic
            # bar-close exits (backtested -$206k flatten-at-close vs -$85k
            # let-run). The DELTA filter is now the false-start gate: a tick
            # only fires after trailing 5-min delta AGREES with it, so a
            # delta-confirmed fill has already earned the right to run its
            # bracket. Do NOT flatten. The real broker bracket (placed at the
            # tick via ChochLiveBot._handle_signal) stays live and runs to its
            # 1pt stop / 3pt target exactly like the databento backtest. Sync
            # the engine's internal state so it tracks the live position and
            # will not attempt a duplicate entry while this one is open.
            eng = self.wrapper.engine if self.wrapper is not None else None
            if eng is not None and not eng.in_trade:
                eng.trades_today += 1
                # Same guard as the CONFIRMED path (added 2026-07-28): only hand the OPEN
                # position to the engine if the broker bracket is STILL live. If it already
                # closed within the entry bar (fast stop/target), the real P&L is already
                # booked — leave the engine FLAT so it can't book a duplicate theoretical
                # exit on the next bar.
                if self._awaiting_bracket_close:
                    eng.in_trade = True
                    eng.position = prov["direction"]
                    eng.entry_price = prov["midpoint"]
                    eng.stop_price = prov["stop"]
                    eng.target_price = prov["target"]
                    eng.breakeven_trigger = None
                    eng.breakeven_done = False
                    eng._last_theoretical_exit_pnl = 0.0
            log.info(f"Provisional NOT re-confirmed by the completed bar "
                     f"(dir={prov['direction']} @ midpoint={prov['midpoint']}) — "
                     f"but it was DELTA-CONFIRMED at the tick, so it is KEPT (NOT "
                     f"flattened). Its live 1pt/3pt bracket runs to stop or target, "
                     f"matching the databento backtest. "
                     f"stop={prov['stop']} target={prov['target']}")
            # The live bracket IS the position; nothing else to place.
            return

        if signal is None or signal.get("type") != "entry":
            return await super()._handle_signal(session, token, signal)

        # Detect signal source (tick vs bar) from the reason string
        reason_str = signal.get("reason", "")
        source = "tick" if reason_str.startswith("TICK ENTRY") else "bar"

        # TICK-ONLY MODE ENFORCEMENT
        # If tick_only is enabled and this is a BAR-loop entry, refuse.
        # This prevents late bar-close entries when we specifically wanted
        # the tick-level improvement.
        if self.tick_only and source == "bar":
            log.info(f"TICK-ONLY MODE: bar-loop entry REFUSED "
                     f"(dir={signal['direction']}, "
                     f"signal_price={signal.get('entry_price')}). "
                     f"Only tick-level entries take in this mode.")
            if self.notifier is not None:
                try:
                    await self.notifier.send(
                        session,
                        f"Bar-loop signal REFUSED (tick-only mode). "
                        f"Would have been {signal['direction']} "
                        f"@ {signal.get('entry_price')}",
                        prefix="[TICK-ONLY]",
                    )
                except Exception:
                    pass
            return

        # SignalR startup-fail protection: if we're in tick-only and SignalR
        # never came up, refuse ALL entries (even ones the bar loop tries).
        if self.tick_only and self._signalr_startup_failed:
            log.warning(f"TICK-ONLY MODE + SignalR startup failed: "
                        f"refusing entry. Restart the bot to try again.")
            return

        # If tick loop already fired and got in, don't fire again
        if (self._armed_signal is not None
                and self._armed_signal.get("bar_fired")
                and (self._awaiting_bracket_close or
                     self._live_entry_fill is not None)):
            log.info("Bar-loop entry skipped — tick loop already fired this signal")
            return

        # Version B filter check
        if not self._version_b_filter_passes(signal["direction"]):
            log.info(f"Bar-loop entry BLOCKED by Version B filter "
                     f"(dir={signal['direction']}, "
                     f"prev_close={self._prev_bar_close}, "
                     f"prev_vwap={self._prev_bar_vwap}, "
                     f"prev_ema9={self._prev_bar_ema9})")
            if self._armed_signal is not None:
                self._armed_signal["bar_fired"] = True
            return

        # Serialize with tick loop
        if self._intrabar_entry_lock is None:
            self._intrabar_entry_lock = asyncio.Lock()
        async with self._intrabar_entry_lock:
            if self._awaiting_bracket_close or self._live_entry_fill is not None:
                log.info("Bar-loop entry aborted inside lock — trade in flight")
                return

            # In DRY-RUN, fire a Telegram alert BEFORE delegating to the
            # parent (which logs "[DRY RUN] Would place..." and returns
            # without notifying). Shared with the tick path via the helper
            # so both entry sources alert identically.
            await self._send_dry_run_entry_alert(session, signal, source)

            await super()._handle_signal(session, token, signal)

    # ═══════════════════════════════════════════════════════════════════════
    # Main
    # ═══════════════════════════════════════════════════════════════════════

    async def run(self):
        """Start SignalR listener alongside parent's normal loops.

        Also wraps `wrapper.add_candle` so Version B state tracking hooks
        run automatically without needing to override the parent's polling
        loop code. See _install_add_candle_hooks below.
        """
        self._loop = asyncio.get_event_loop()
        self._intrabar_entry_lock = asyncio.Lock()

        # Watchdog: parent's run() creates self.wrapper lazily on first
        # session start. We install hooks the moment it appears.
        wrapper_hook_task = asyncio.create_task(self._install_add_candle_hooks())

        parent_task = asyncio.create_task(super().run())
        signalr_starter = asyncio.create_task(self._start_signalr_when_ready())

        try:
            await parent_task
        finally:
            wrapper_hook_task.cancel()
            signalr_starter.cancel()
            if self._signalr is not None:
                try:
                    if self._signalr_subscribed and self.contract_id:
                        self._signalr.send("UnsubscribeContractTrades",
                                           [self.contract_id])
                    self._signalr.stop()
                except Exception:
                    pass

    async def _install_add_candle_hooks(self):
        """Watch for self.wrapper to be created by parent's run(), then wrap
        its `add_candle` method so our Version B / armed-signal hooks run
        automatically around every engine advance.

        The parent bot creates a fresh CHOCHFVGReplayWrapper per session day
        (line 888 in choch_bot_live.py: `self.wrapper = CHOCHFVGReplayWrapper()`).
        We need to re-wrap whenever a new wrapper is installed.
        """
        wrapped_wrapper_id = None
        while True:
            try:
                w = self.wrapper
                if w is not None and id(w) != wrapped_wrapper_id:
                    self._wrap_add_candle(w)
                    wrapped_wrapper_id = id(w)
                    log.info("Version B hooks installed on new wrapper")
            except Exception as e:
                log.warning(f"add_candle hook install exception: {e}")
            await asyncio.sleep(1)

    def _wrap_add_candle(self, wrapper):
        """Replace `wrapper.add_candle` with a wrapper that:
          1. Snapshots prior VWAP/EMA9 BEFORE the engine consumes the bar
          2. Calls the original add_candle
          3. Runs the post-bar hook to refresh Version B prev_bar_close and
             the armed FVG midpoint for the tick loop
        """
        original_add_candle = wrapper.add_candle

        def wrapped(candle, pt_time, is_last_candle_of_session, session_vwap=None):
            # 1. Snapshot prior state (before engine ingests the bar)
            self._snapshot_prior_bar_state()

            # 2. Bar staleness check — refuse to advance the engine on stale bars
            bar_ts = candle.ts.astimezone(PT) if hasattr(candle.ts, "astimezone") else candle.ts
            bar_close_time = bar_ts + timedelta(seconds=60)
            now_pt = datetime.now(PT)
            staleness_secs = (now_pt - bar_close_time).total_seconds()
            if staleness_secs > MAX_BAR_STALENESS_SECS:
                log.warning(f"STALE BAR REJECTED | bar_close={bar_close_time.isoformat()} "
                            f"now={now_pt.isoformat()} staleness={staleness_secs:.0f}s "
                            f"(> {MAX_BAR_STALENESS_SECS}s). Not advancing engine.")
                if self.notifier is not None and getattr(self.notifier, "enabled", False):
                    # Fire-and-forget alert; don't block the caller
                    try:
                        if self._loop is not None and self._shared_session is not None:
                            asyncio.run_coroutine_threadsafe(
                                self.notifier.send(
                                    self._shared_session,
                                    f"Bar API returning STALE data ({staleness_secs:.0f}s old). "
                                    f"Not advancing engine. API session may have degraded — "
                                    f"consider restarting bot.",
                                    prefix="[STALE BAR]",
                                ), self._loop)
                    except Exception:
                        pass
                return None  # tell caller: no signal, don't do anything

            # 3. Advance the engine
            signal = original_add_candle(candle, pt_time,
                                         is_last_candle_of_session,
                                         session_vwap=session_vwap)

            # 4. Post-bar hook — refresh prev_bar_close and armed signal
            self._post_bar_hook(candle)

            return signal

        wrapper.add_candle = wrapped

    async def _start_signalr_when_ready(self):
        """Wait for parent auth to complete, then start SignalR.

        CRITICAL: this coroutine runs as an asyncio Task. If an exception
        escapes without being logged, the task fails silently and the tick
        loop never comes online. We catch and LOG all exceptions explicitly.
        """
        if self.choch_only:
            log.info("CHOCH-ONLY mode: skipping SignalR. Entries come only from "
                     "the engine's bar-close _check_entry (+ Version B) — the "
                     "choch strategy the replay validated at +$4,390.")
            return

        try:
            log.info("SignalR starter: waiting for parent bot auth to complete...")
            for _ in range(60):
                await asyncio.sleep(1)
                if self.token and self.contract_id:
                    break
            else:
                log.error("SignalR starter: parent bot did not complete auth "
                          "in 60s. SignalR not starting. Tick-level entries "
                          "will NOT work this session.")
                self._signalr_startup_failed = True
                return
            log.info(f"SignalR starter: parent auth complete "
                     f"(token={'set' if self.token else 'MISSING'}, "
                     f"contract_id={self.contract_id}). "
                     f"Starting SignalR connection...")
        except Exception as e:
            log.error(f"SignalR starter: exception in auth wait: "
                      f"{type(e).__name__}: {e}", exc_info=True)
            self._signalr_startup_failed = True
            return

        try:
            import aiohttp
            self._shared_session = aiohttp.ClientSession()
            log.info("SignalR starter: aiohttp session created")
        except Exception as e:
            log.error(f"SignalR starter: failed to create aiohttp session: "
                      f"{type(e).__name__}: {e}", exc_info=True)
            self._signalr_startup_failed = True
            return

        try:
            await self._connect_signalr()
            log.info(f"SignalR starter: _connect_signalr returned. "
                     f"connected={self._signalr_connected}, "
                     f"subscribed={self._signalr_subscribed}")
        except Exception as e:
            log.error(f"SignalR starter: _connect_signalr exception: "
                      f"{type(e).__name__}: {e}", exc_info=True)
            self._signalr_startup_failed = True
            # Alert the user via Telegram — this is a critical failure
            try:
                if self.notifier is not None:
                    await self.notifier.send(
                        self._shared_session,
                        f"SignalR connection FAILED at startup: "
                        f"{type(e).__name__}: {e}. "
                        f"Tick-level entries will NOT work this session. "
                        f"Bar-loop still runs. Check log for details.",
                        prefix="[SIGNALR FAIL]",
                    )
            except Exception:
                pass
            return

        try:
            # Rebuild SignalR when parent rotates the token.
            last_seen = self.token
            while True:
                await asyncio.sleep(30)
                if self.token != last_seen:
                    log.info("Parent bot rotated token — rebuilding SignalR")
                    last_seen = self.token
                    await self._signalr_reconnect_on_token_refresh()
        except asyncio.CancelledError:
            pass
        except Exception as e:
            log.error(f"SignalR starter: monitor loop exception: "
                      f"{type(e).__name__}: {e}", exc_info=True)
        finally:
            try:
                if self._shared_session is not None:
                    await self._shared_session.close()
            except Exception:
                pass


def _send_shutdown_alert_sync(text: str) -> None:
    """Best-effort, SYNCHRONOUS Telegram send for use in the shutdown/crash
    handlers below. By the time those handlers run, asyncio.run(bot.run())
    has already closed its event loop, so the bot's own async
    TelegramNotifier.send() can't be awaited there. Same stdlib-only
    urllib pattern as watchdog.py's send() — reads the same
    TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID env vars the bot already loaded
    via load_dotenv() earlier in the import chain. Never raises: a failure
    here must not mask (or replace) the real shutdown/crash being handled.

    NOTE — what this can and cannot do: this only helps when the PROCESS
    itself gets a chance to run this except block (Ctrl-C, an in-process
    crash) while the machine and network are still up. It CANNOT alert on
    a power loss, sleep, or hard machine failure — at that point nothing
    in this process runs at all. That case has no local-software fix; it
    would need something external (e.g. a phone-side "did I hear from the
    bot recently" check) to catch it.
    """
    import urllib.request
    import urllib.parse
    try:
        token = os.getenv("TELEGRAM_BOT_TOKEN")
        chat_id = os.getenv("TELEGRAM_CHAT_ID")
        if not (token and chat_id):
            print(f"[shutdown-alert] (no telegram configured) would send: {text}")
            return
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        data = urllib.parse.urlencode({"chat_id": chat_id, "text": text}).encode()
        with urllib.request.urlopen(urllib.request.Request(url, data=data), timeout=15) as r:
            r.read()
    except Exception as e:
        print(f"[shutdown-alert] telegram send failed ({type(e).__name__}: {e})")


def main():
    tick_only = "--tick-only" in sys.argv
    choch_only = "--choch-only" in sys.argv
    print("=" * 70)
    print(f"  CHOCH/FVG INTRABAR BOT — ES {choch_fvg_bot.CONTRACTS} CONTRACT"
          f"{'S' if choch_fvg_bot.CONTRACTS != 1 else ''}")
    if DRY_RUN:
        print("  ==> DRY RUN MODE — no real orders will be placed")
    else:
        print("  ==> LIVE MODE — REAL orders")
    if choch_only:
        print("  ==> CHOCH-ONLY MODE — entries ONLY from the engine's bar-close")
        print("      _check_entry (+ Version B). Tick entry OFF. SignalR OFF.")
        print("      This is the strategy the replay validated at +$4,390.")
    elif tick_only:
        print("  ==> TICK-ONLY MODE — bar-loop entries will be REFUSED")
    print("=" * 70)
    print(f"  Instrument:     ES ({choch_fvg_bot.CONTRACTS} contract"
          f"{'s' if choch_fvg_bot.CONTRACTS != 1 else ''}) | "
          f"stop {choch_fvg_bot.FIXED_STOP_POINTS}pt / target "
          f"{choch_fvg_bot.FIXED_STOP_POINTS * choch_fvg_bot.RR_MULTIPLE:g}pt")
    print(f"  Session:        {choch_fvg_bot.SESSION_OPEN} - "
          f"{choch_fvg_bot.SESSION_CLOSE} PT")
    print(f"  Force flat:     {choch_fvg_bot.FORCE_FLAT_TIME} PT")
    print(f"  Max trades:     {choch_fvg_bot.MAX_TRADES_PER_DAY}/day")
    print(f"  Max consec L:   {choch_fvg_bot.MAX_CONSECUTIVE_LOSSES}")
    print(f"  Daily loss lim: ${choch_fvg_bot.DAILY_LOSS_LIMIT}")
    print(f"  Filter:         Version B — ENFORCED ON (mandatory, "
          f"VWAP_EMA_FILTER={choch_fvg_bot.VWAP_EMA_FILTER})")
    print(f"  Delta filter:   {'ON — 5-min order-flow must confirm entry (NEW; collecting dry-run data)' if '--no-delta-filter' not in sys.argv else 'OFF (--no-delta-filter)'}")
    if "--collect-all" in sys.argv and DRY_RUN:
        print(f"  Daily caps:     DISABLED (--collect-all) — logging EVERY trade; no 3/day, 2-loss, or daily-loss stop")
    if choch_only:
        print(f"  Entry:          CHOCH-ONLY — engine bar-close _check_entry "
              f"(+ Version B)")
    elif tick_only:
        print(f"  Entry:          TICK-LEVEL ONLY via GatewayTrade "
              f"(bar-loop REFUSED)")
    else:
        print(f"  Entry:          Tick-level via GatewayTrade "
              f"+ bar-loop fallback")
    print("=" * 70)
    if not DRY_RUN:
        print("  This bot places REAL orders. Ctrl-C kills it immediately.")
        print("  Watch the log — never leave it unattended on first day.")
        print("=" * 70)
    print()

    bot = IntrabarESBot()
    try:
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        log.info("Interrupted by user — shutting down")
        _send_shutdown_alert_sync(
            "[STARTUP-3CT] CHOCH bot (3-CONTRACT copy) stopped: Ctrl-C / interrupted "
            "by user. No more trading until it's restarted."
        )
    except Exception as e:
        log.exception("Bot crashed — shutting down")
        _send_shutdown_alert_sync(
            f"[STARTUP-3CT] CHOCH bot (3-CONTRACT copy) CRASHED and stopped: "
            f"{type(e).__name__}: {e}. No more trading until it's restarted."
        )
        raise


if __name__ == "__main__":
    main()
