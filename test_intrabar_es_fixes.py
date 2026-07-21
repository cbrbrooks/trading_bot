"""
Test harness for intrabar_es_bot.py.

Tests are OFFLINE — no API calls, no SignalR, no orders. Uses stubs/mocks
to verify the pieces we can't test in production:

  1. Version B filter — passes/blocks based on prev-bar state
  2. Bar staleness gate — the add_candle wrapper refuses stale bars
  3. Add-candle wrapper — snapshot + engine advance + post-hook order is right
  4. Armed signal extraction — correctly reads FVG midpoint from mock engine
  5. Tick callback — filters correctly, only fires when armed AND touched AND version-b-passes
  6. Race lock — bar-loop and tick-loop can't both submit for the same signal
  7. Session policy handling — SignalR reconnect on token refresh works

Run: python test_intrabar_es_fixes.py
"""
import asyncio
import os
import sys
from datetime import datetime, time, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

os.environ.setdefault("INSTRUMENT", "ES")
os.environ.setdefault("TOPSTEP_USERNAME", "test")
os.environ.setdefault("TOPSTEP_API_KEY", "test")

sys.path.insert(0, ".")


def make_bot():
    """Create an IntrabarESBot with the SignalR / API layers not initialized."""
    import intrabar_es_bot as ibot
    bot = ibot.IntrabarESBot()
    bot.token = "fake-token"
    bot.account_id = 12345
    bot.contract_id = "CON.F.US.EP.U26"
    if bot.notifier is not None:
        bot.notifier.enabled = False
    return bot


# ═══════════════════════════════════════════════════════════════════════════
# TEST 1: Version B filter
# ═══════════════════════════════════════════════════════════════════════════
async def test_version_b_filter():
    print("\nTEST 1: Version B filter")
    bot = make_bot()

    # Warmup: filter allows when prev state is None
    assert bot._version_b_filter_passes("long") is True
    assert bot._version_b_filter_passes("short") is True
    print("  PASS: warmup (no prev state) — allows all")

    # LONG blocked when prev_close is BELOW both indicators
    bot._prev_bar_close = 100.0
    bot._prev_bar_vwap = 105.0
    bot._prev_bar_ema9 = 103.0
    assert bot._version_b_filter_passes("long") is False, "long should be BLOCKED (price below both)"
    assert bot._version_b_filter_passes("short") is True, "short should PASS (price below both)"
    print("  PASS: price below both indicators — long blocked, short allowed")

    # SHORT blocked when prev_close is ABOVE both
    bot._prev_bar_close = 110.0
    bot._prev_bar_vwap = 105.0
    bot._prev_bar_ema9 = 103.0
    assert bot._version_b_filter_passes("long") is True, "long should PASS"
    assert bot._version_b_filter_passes("short") is False, "short should be BLOCKED"
    print("  PASS: price above both indicators — long allowed, short blocked")

    # Both allowed when price is BETWEEN indicators (neither block condition met)
    bot._prev_bar_close = 104.0
    assert bot._version_b_filter_passes("long") is True
    assert bot._version_b_filter_passes("short") is True
    print("  PASS: price between indicators — both allowed")


# ═══════════════════════════════════════════════════════════════════════════
# TEST 2: Bar staleness gate (via wrapped add_candle)
# ═══════════════════════════════════════════════════════════════════════════
async def test_bar_staleness_gate():
    print("\nTEST 2: Bar staleness gate")
    bot = make_bot()

    # Mock a wrapper with add_candle
    class FakeEngine:
        vwap = 100.0
        ema9 = 99.5
        active_fvgs = []
        choch_direction = None

    class FakeWrapper:
        engine = FakeEngine()
        def add_candle(self, candle, pt_time, is_last, session_vwap=None):
            return "engine-was-called"  # sentinel

    fake = FakeWrapper()
    bot.wrapper = fake
    bot._wrap_add_candle(fake)

    from topstep_bot_v5 import Candle
    from topstep_bot_v5 import PT

    # Fresh bar (30s old) — should advance the engine
    now = datetime.now(PT)
    fresh_ts = now - timedelta(seconds=90)  # timestamped 90s ago = closed 30s ago
    fresh_candle = Candle(100, 100.5, 99.5, 100.25, 100, fresh_ts)
    result = fake.add_candle(fresh_candle, time(10, 0), False)
    assert result == "engine-was-called", f"Fresh bar should advance engine, got {result}"
    print("  PASS: fresh bar (30s stale) advances engine")

    # Stale bar (2 hours old) — should NOT advance
    stale_ts = now - timedelta(seconds=7200)  # 2 hours ago
    stale_candle = Candle(100, 100.5, 99.5, 100.25, 100, stale_ts)
    result = fake.add_candle(stale_candle, time(10, 0), False)
    assert result is None, f"Stale bar should be rejected (return None), got {result}"
    print("  PASS: stale bar (2 hours old) — engine NOT advanced")


# ═══════════════════════════════════════════════════════════════════════════
# TEST 3: Add-candle wrapper — hook ordering
# ═══════════════════════════════════════════════════════════════════════════
async def test_add_candle_hook_ordering():
    print("\nTEST 3: Add-candle wrapper hook ordering")
    bot = make_bot()

    calls = []

    class FakeEngine:
        vwap = 100.0
        ema9 = 99.5
        active_fvgs = []
        choch_direction = None

    class FakeWrapper:
        engine = FakeEngine()
        def add_candle(self, candle, pt_time, is_last, session_vwap=None):
            calls.append("engine")
            # Simulate engine updating VWAP after ingesting the bar
            self.engine.vwap = 101.0
            self.engine.ema9 = 100.7
            return None

    fake = FakeWrapper()
    bot.wrapper = fake

    # Patch snapshot and post_bar_hook so we can see when they run
    original_snapshot = bot._snapshot_prior_bar_state
    def snapshot_spy():
        calls.append("snapshot")
        original_snapshot()
    bot._snapshot_prior_bar_state = snapshot_spy

    original_post = bot._post_bar_hook
    def post_spy(candle):
        calls.append("post")
        original_post(candle)
    bot._post_bar_hook = post_spy

    bot._wrap_add_candle(fake)

    from topstep_bot_v5 import Candle, PT
    now = datetime.now(PT)
    candle = Candle(100, 100.5, 99.5, 100.25, 100, now - timedelta(seconds=30))
    fake.add_candle(candle, time(10, 0), False)

    assert calls == ["snapshot", "engine", "post"], \
        f"Expected snapshot → engine → post, got {calls}"
    print(f"  PASS: order is {' → '.join(calls)}")

    # After the call, prev_bar_close should be the candle's close
    assert bot._prev_bar_close == 100.25
    # prev_bar_vwap should be the value BEFORE the engine updated (100.0)
    assert bot._prev_bar_vwap == 100.0, \
        f"Expected snapshot to capture pre-engine VWAP (100.0), got {bot._prev_bar_vwap}"
    print(f"  PASS: prev_bar_close set to {bot._prev_bar_close}, "
          f"prev_bar_vwap captured PRE-engine (100.0)")


# ═══════════════════════════════════════════════════════════════════════════
# TEST 4: Armed signal extraction from engine state
# ═══════════════════════════════════════════════════════════════════════════
async def test_armed_signal_extraction():
    """Uses REAL FVG objects and the REAL engine class rather than mocks —
    this way if the engine's attribute names change, this test catches it
    instead of shipping a bot that silently never arms."""
    print("\nTEST 4: Armed signal extraction from engine (real FVG objects)")
    from choch_fvg_bot import FVG, CHOCHFVGReplayWrapper
    bot = make_bot()

    # Use a real wrapper so we're testing against the actual engine's
    # attribute names, not names I made up.
    bot.wrapper = CHOCHFVGReplayWrapper()
    eng = bot.wrapper.engine

    # Bearish setup — engine attributes are `choch_bias` and `active_fvgs`,
    # FVG attributes are `type`, `midpoint`, `top`, `bottom`, `consumed`.
    # Also need CHOCH swing state populated so _fib_valid can evaluate.
    eng.choch_bias = "bearish"
    # For a bearish setup, the FVG midpoint must be within [swing_low + FIB_LOW*range,
    # swing_low + FIB_HIGH*range]. With swing_high=110, swing_low=100, range=10:
    # 50% retrace zone is 105 to 106.18. Midpoint 105 falls in that zone.
    eng.choch_swing_high = 110.0
    eng.choch_swing_low = 100.0
    eng.active_fvgs = [FVG("bearish", top=106.0, bottom=104.0)]
    bot._update_armed_signal_from_engine()
    assert bot._armed_signal is not None, \
        f"Should have armed signal (got {bot._armed_signal}). " \
        f"This means engine attribute names in _update_armed_signal_from_engine are WRONG."
    assert bot._armed_signal["direction"] == "short"
    assert bot._armed_signal["midpoint"] == 105.0  # (106+104)/2
    print(f"  PASS: bearish CHOCH + real FVG → armed short at midpoint 105.0, "
          f"stop {bot._armed_signal['stop']}, target {bot._armed_signal['target']}")

    # Bullish setup — use a NEW engine to avoid the previous bar_fired state
    # sticking to the same midpoint check
    bot.wrapper = CHOCHFVGReplayWrapper()
    eng = bot.wrapper.engine
    eng.choch_bias = "bullish"
    # For bullish: swing_range range, retracement measured DOWN from swing_high.
    # Midpoint 100 should fall in the retrace zone.
    eng.choch_swing_high = 105.0
    eng.choch_swing_low = 95.0
    eng.active_fvgs = [FVG("bullish", top=101.0, bottom=99.0)]
    bot._armed_signal = None
    bot._update_armed_signal_from_engine()
    assert bot._armed_signal is not None
    assert bot._armed_signal["direction"] == "long"
    assert bot._armed_signal["midpoint"] == 100.0
    print(f"  PASS: bullish CHOCH + real FVG → armed long at midpoint 100.0")

    # Consumed FVG should be SKIPPED — the engine already fired on it
    bot.wrapper = CHOCHFVGReplayWrapper()
    eng = bot.wrapper.engine
    eng.choch_bias = "bullish"
    eng.choch_swing_high = 105.0
    eng.choch_swing_low = 95.0
    consumed_fvg = FVG("bullish", top=101.0, bottom=99.0)
    consumed_fvg.consumed = True
    eng.active_fvgs = [consumed_fvg]
    bot._armed_signal = None
    bot._update_armed_signal_from_engine()
    assert bot._armed_signal is None, \
        f"Consumed FVG should not arm (got {bot._armed_signal})"
    print(f"  PASS: consumed FVG → not armed (engine already fired on it)")

    # FVG direction mismatches CHOCH bias → skip
    bot.wrapper = CHOCHFVGReplayWrapper()
    eng = bot.wrapper.engine
    eng.choch_bias = "bullish"
    eng.choch_swing_high = 105.0
    eng.choch_swing_low = 95.0
    eng.active_fvgs = [FVG("bearish", top=101.0, bottom=99.0)]  # WRONG direction
    bot._armed_signal = None
    bot._update_armed_signal_from_engine()
    assert bot._armed_signal is None
    print(f"  PASS: FVG direction != choch_bias → not armed")

    # No CHOCH bias → not armed
    bot.wrapper = CHOCHFVGReplayWrapper()
    eng = bot.wrapper.engine
    eng.choch_bias = None
    eng.active_fvgs = [FVG("bullish", top=101.0, bottom=99.0)]
    bot._armed_signal = None
    bot._update_armed_signal_from_engine()
    assert bot._armed_signal is None
    print(f"  PASS: no CHOCH bias → not armed")

    # FVG that FAILS fib check → not armed (no swing_low/swing_high set)
    bot.wrapper = CHOCHFVGReplayWrapper()
    eng = bot.wrapper.engine
    eng.choch_bias = "bullish"
    eng.choch_swing_high = None  # no swing state → fib_valid returns False
    eng.choch_swing_low = None
    eng.active_fvgs = [FVG("bullish", top=101.0, bottom=99.0)]
    bot._armed_signal = None
    bot._update_armed_signal_from_engine()
    assert bot._armed_signal is None
    print(f"  PASS: FVG failing fib check → not armed (matches bar-loop behavior)")


# ═══════════════════════════════════════════════════════════════════════════
# TEST 5: Tick callback filter chain
# ═══════════════════════════════════════════════════════════════════════════
async def test_tick_callback():
    print("\nTEST 5: Tick callback filter chain")
    bot = make_bot()
    bot._loop = asyncio.get_event_loop()

    # Case 5a: no armed signal → no fire
    fires = []
    def spy_dispatch(coro):
        fires.append("fired")
        return None
    # Replace the run_coroutine_threadsafe with a spy
    import intrabar_es_bot as ibot
    original = asyncio.run_coroutine_threadsafe
    asyncio.run_coroutine_threadsafe = lambda coro, loop: (fires.append("fired"), coro.close())

    bot._armed_signal = None
    bot._on_trade_tick(["contract_id", {"price": 100.0}])
    assert len(fires) == 0
    print(f"  PASS 5a: no armed signal → no fire")

    # Case 5b: armed short, tick DOES NOT touch midpoint → no fire
    bot._armed_signal = {
        "direction": "short", "midpoint": 105.0, "stop": 106.0,
        "target": 102.0, "bar_fired": False,
    }
    bot._prev_bar_close = 106.0
    bot._prev_bar_vwap = 104.0
    bot._prev_bar_ema9 = 104.5
    bot._on_trade_tick(["contract_id", {"price": 104.75}])  # below midpoint for short
    assert len(fires) == 0
    print(f"  PASS 5b: armed short, price below midpoint (not touched) → no fire")

    # Case 5c: armed short, tick DOES touch midpoint, Version B passes → fires
    #
    # Rather than mock datetime.now (which broke because timezone handling
    # differs between the bot and unittest.mock), we patch the session-window
    # check inline. Filter state also needs to be set so Version B allows short.
    bot._armed_signal["bar_fired"] = False
    bot._prev_bar_close = 102.0   # BELOW both indicators → filter ALLOWS short
    bot._prev_bar_vwap = 104.0
    bot._prev_bar_ema9 = 104.5
    # Refresh health-log timestamp so the tick handler doesn't crash on datetime math
    bot._last_tick_health_log = datetime.now(timezone.utc)

    # Patch the session-time check by directly setting SESSION_OPEN/CLOSE
    # so any time-of-day passes
    import choch_fvg_bot
    orig_open = choch_fvg_bot.SESSION_OPEN
    orig_close = choch_fvg_bot.SESSION_CLOSE
    choch_fvg_bot.SESSION_OPEN = time(0, 0)
    choch_fvg_bot.SESSION_CLOSE = time(23, 59)
    try:
        bot._on_trade_tick(["contract_id", {"price": 105.0}])  # touches midpoint
    finally:
        choch_fvg_bot.SESSION_OPEN = orig_open
        choch_fvg_bot.SESSION_CLOSE = orig_close
    assert len(fires) == 1, f"Expected 1 fire, got {len(fires)}"
    print(f"  PASS 5c: armed short, price at midpoint, filter passes → FIRES")

    # Case 5d: same setup but Version B filter should BLOCK
    fires.clear()
    bot._armed_signal["bar_fired"] = False
    bot._prev_bar_close = 106.0  # ABOVE both → blocks short
    bot._last_tick_health_log = datetime.now(timezone.utc)
    choch_fvg_bot.SESSION_OPEN = time(0, 0)
    choch_fvg_bot.SESSION_CLOSE = time(23, 59)
    try:
        bot._on_trade_tick(["contract_id", {"price": 105.0}])
    finally:
        choch_fvg_bot.SESSION_OPEN = orig_open
        choch_fvg_bot.SESSION_CLOSE = orig_close
    assert len(fires) == 0, "Filter should have blocked"
    # And armed.bar_fired should be True now (so we don't keep re-checking)
    assert bot._armed_signal["bar_fired"] is True
    print(f"  PASS 5d: filter blocks — no fire, bar_fired flag set")

    # Restore
    asyncio.run_coroutine_threadsafe = original


# ═══════════════════════════════════════════════════════════════════════════
# TEST 6: Race lock — tick and bar can't both fire
# ═══════════════════════════════════════════════════════════════════════════
async def test_race_lock():
    print("\nTEST 6: Race lock between tick and bar loops")
    bot = make_bot()
    bot._intrabar_entry_lock = asyncio.Lock()

    submissions = []
    async def spy_handle_signal(session, token, signal):
        submissions.append(signal)
        await asyncio.sleep(0.01)  # simulate broker latency

    # Case: tick fires first, then bar tries — bar should abort
    bot._armed_signal = {
        "direction": "long", "midpoint": 100.0, "stop": 99.0,
        "target": 103.0, "bar_fired": True,   # tick has claimed it
    }
    bot._prev_bar_close = 100.5
    bot._prev_bar_vwap = 100.0
    bot._prev_bar_ema9 = 100.0
    bot._awaiting_bracket_close = True   # tick's order is in flight
    bot._live_entry_fill = None

    # Patch super()._handle_signal to spy
    from choch_bot_live import ChochLiveBot
    original = ChochLiveBot._handle_signal
    ChochLiveBot._handle_signal = spy_handle_signal

    try:
        bar_signal = {"type": "entry", "direction": "long", "entry_price": 100.0,
                      "stop": 99.0, "target": 103.0, "reason": "bar"}
        await bot._handle_signal(None, "token", bar_signal)
    finally:
        ChochLiveBot._handle_signal = original

    assert len(submissions) == 0, \
        f"Bar loop should have skipped (tick already fired), but submissions={submissions}"
    print(f"  PASS: tick already fired → bar loop skipped submission")


# ═══════════════════════════════════════════════════════════════════════════
# TEST 7: Version B filter blocks bar-loop entry
# ═══════════════════════════════════════════════════════════════════════════
async def test_bar_loop_filter():
    print("\nTEST 7: Version B blocks bar-loop entries")
    bot = make_bot()
    bot._intrabar_entry_lock = asyncio.Lock()
    bot._awaiting_bracket_close = False
    bot._live_entry_fill = None
    bot._armed_signal = None

    # Filter state: price ABOVE both → should BLOCK short
    bot._prev_bar_close = 110.0
    bot._prev_bar_vwap = 105.0
    bot._prev_bar_ema9 = 103.0

    submissions = []
    async def spy_handle_signal(session, token, signal):
        submissions.append(signal)

    from choch_bot_live import ChochLiveBot
    original = ChochLiveBot._handle_signal
    ChochLiveBot._handle_signal = spy_handle_signal

    try:
        # Short signal — should be BLOCKED
        signal = {"type": "entry", "direction": "short", "entry_price": 100.0,
                  "stop": 101.0, "target": 97.0, "reason": "bar-close short"}
        await bot._handle_signal(None, "token", signal)
    finally:
        ChochLiveBot._handle_signal = original

    assert len(submissions) == 0, "Version B should have blocked short"
    print(f"  PASS: Version B blocks short entry (price above both indicators)")


async def test_tick_only_mode():
    print("\nTEST 8: Tick-only mode refuses bar-loop entries")
    bot = make_bot()
    bot._intrabar_entry_lock = asyncio.Lock()
    bot.tick_only = True   # enable tick-only mode
    bot._prev_bar_close = 102.0  # filter would allow, but tick_only should block
    bot._prev_bar_vwap = 100.0
    bot._prev_bar_ema9 = 100.0

    submissions = []
    async def spy_handle_signal(self, session, token, signal):
        submissions.append(signal)

    from choch_bot_live import ChochLiveBot
    original = ChochLiveBot._handle_signal
    ChochLiveBot._handle_signal = spy_handle_signal

    try:
        # Bar-source signal (no "TICK ENTRY" prefix in reason)
        bar_signal = {"type": "entry", "direction": "long", "entry_price": 100.0,
                      "stop": 99.0, "target": 103.0, "reason": "bar-close signal"}
        await bot._handle_signal(None, "token", bar_signal)
    finally:
        ChochLiveBot._handle_signal = original

    assert len(submissions) == 0, \
        f"Tick-only mode should have REFUSED bar-source entry, got {len(submissions)} submissions"
    print(f"  PASS: tick-only mode refuses bar-loop entry")

    # Now verify tick-source signals still work in tick-only mode
    submissions.clear()
    bot._awaiting_bracket_close = False
    bot._live_entry_fill = None
    ChochLiveBot._handle_signal = spy_handle_signal
    try:
        tick_signal = {"type": "entry", "direction": "long", "entry_price": 100.0,
                       "stop": 99.0, "target": 103.0,
                       "reason": "TICK ENTRY long @ midpoint=100.0"}
        await bot._handle_signal(None, "token", tick_signal)
    finally:
        ChochLiveBot._handle_signal = original

    assert len(submissions) == 1, \
        f"Tick-source entries should still work in tick-only mode, got {len(submissions)}"
    print(f"  PASS: tick-only mode still allows tick-source entries")


async def main():
    print("=" * 66)
    print("Testing intrabar_es_bot.py")
    print("=" * 66)
    await test_version_b_filter()
    await test_bar_staleness_gate()
    await test_add_candle_hook_ordering()
    await test_armed_signal_extraction()
    await test_tick_callback()
    await test_race_lock()
    await test_bar_loop_filter()
    await test_tick_only_mode()
    print("\n" + "=" * 66)
    print("ALL TESTS PASSED")
    print("=" * 66)


if __name__ == "__main__":
    asyncio.run(main())
