"""
CHOCH/FVG INTRABAR Bot — ES 2 contracts, tick-level entry with bar-fallback.

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

USAGE:
    python intrabar_es_bot.py                # LIVE — places real orders
    python intrabar_es_bot.py --dry-run      # logs signals, no orders
"""
import asyncio
import logging
import os
import sys
from datetime import datetime, time, timedelta

# Parent bot must import first — it sets INSTRUMENT=ES and honors --dry-run
import choch_bot_live
from choch_bot_live import ChochLiveBot, DRY_RUN, log, BASE_URL
from topstep_bot_v5 import PT
import choch_fvg_bot

from signalrcore.hub_connection_builder import HubConnectionBuilder

MARKET_HUB_URL = "https://rtc.topstepx.com/hubs/market"

MAX_BAR_STALENESS_SECS = 90


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

            signal = {
                "type": "entry",
                "direction": armed["direction"],
                "entry_price": armed["midpoint"],
                "stop": armed["stop"],
                "target": armed["target"],
                "reason": (f"TICK ENTRY {armed['direction']} "
                           f"@ midpoint={armed['midpoint']} (touched at {price})"),
            }
            # Delegate to parent bot's proven entry handler.
            # NOTE: we pass a session; the tick loop uses its own aiohttp
            # session (see _start_signalr_when_ready). Different session,
            # same broker, same token, functionally equivalent.
            await ChochLiveBot._handle_signal(self, self._shared_session,
                                              self.token, signal)

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

        # Freshest UNCONSUMED FVG matching choch_bias that ALSO passes fib
        # validity — the bar loop uses fib_valid as a hard gate before firing,
        # so we must too, or we'd fire tick-entries the bar loop would reject.
        candidate = None
        for fvg in reversed(active_fvgs):
            if getattr(fvg, "consumed", False):
                continue
            fvg_type = getattr(fvg, "type", None)
            if fvg_type != choch_bias:
                continue
            # Fib validity check — matches engine's _check_entry gating
            try:
                if not eng._fib_valid(fvg):
                    continue
            except Exception:
                # If the engine's fib check errors, be conservative and skip
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
        # Use BROKER_MIN_STOP_POINTS (1.0pt = 4 ticks) as the geometry floor
        # so armed signals are consistent with what the broker will accept.
        # The 4-tick minimum is enforced again at order-submission time in the
        # parent bot's _handle_signal via max(4, ...), so this is belt-and-suspenders.
        min_stop_floor = getattr(choch_fvg_bot, "BROKER_MIN_STOP_POINTS",
                                 choch_fvg_bot.MIN_STOP_POINTS)
        if direction == "long":
            stop = bot if bot is not None else mid - min_stop_floor
            risk = mid - stop
            if risk < min_stop_floor:
                # FVG geometry tighter than broker minimum — widen the stop
                stop = mid - min_stop_floor
                risk = min_stop_floor
            target = mid + choch_fvg_bot.RR_MULTIPLE * risk
        else:
            stop = top if top is not None else mid + min_stop_floor
            risk = stop - mid
            if risk < min_stop_floor:
                stop = mid + min_stop_floor
                risk = min_stop_floor
            target = mid - choch_fvg_bot.RR_MULTIPLE * risk

        # Reject setups outside the acceptable risk band (matches engine)
        if risk < min_stop_floor or risk > choch_fvg_bot.MAX_STOP_POINTS:
            self._armed_signal = None
            return

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
            # parent (which will log "[DRY RUN] Would place..." and return
            # without notifying). The parent bot suppresses Telegram in
            # dry-run — we override that here so evaluation from the phone
            # is possible.
            if DRY_RUN and self.notifier is not None:
                try:
                    await self.notifier.send(
                        session,
                        f"ES {signal['direction'].upper()} would fire "
                        f"@ signal={signal['entry_price']} "
                        f"stop={signal['stop']} target={signal['target']} "
                        f"| source={source} | {reason_str}",
                        prefix="[DRY-RUN ENTRY]",
                    )
                except Exception as e:
                    log.warning(f"Dry-run Telegram alert failed: {e}")

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


def main():
    tick_only = "--tick-only" in sys.argv
    print("=" * 70)
    print("  CHOCH/FVG INTRABAR BOT — ES 2 CONTRACTS")
    if DRY_RUN:
        print("  ==> DRY RUN MODE — no real orders will be placed")
    else:
        print("  ==> LIVE MODE — REAL orders")
    if tick_only:
        print("  ==> TICK-ONLY MODE — bar-loop entries will be REFUSED")
    print("=" * 70)
    print(f"  Instrument:     ES (2 contracts)")
    print(f"  Session:        {choch_fvg_bot.SESSION_OPEN} - "
          f"{choch_fvg_bot.SESSION_CLOSE} PT")
    print(f"  Force flat:     {choch_fvg_bot.FORCE_FLAT_TIME} PT")
    print(f"  Max trades:     {choch_fvg_bot.MAX_TRADES_PER_DAY}/day")
    print(f"  Max consec L:   {choch_fvg_bot.MAX_CONSECUTIVE_LOSSES}")
    print(f"  Daily loss lim: ${choch_fvg_bot.DAILY_LOSS_LIMIT}")
    print(f"  Filter:         Version B (prev-bar close + prior VWAP/9-SMA)")
    if tick_only:
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


if __name__ == "__main__":
    main()
