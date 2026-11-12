"""
Test harness for the intrabar_mes_bot.py fixes.

Simulates broker responses without hitting a real API. Verifies:
  1. Order status parsing handles integer codes (the .lower() bug)
  2. Bar staleness gate rejects bars > 90s old
  3. _finalize_bracket_trade computes REAL P&L from actual fills, not stale account snapshot
  4. Real P&L drives consecutive_losses correctly (win resets, loss increments)

Runs OFFLINE — no auth, no orders, no network calls. All broker responses are
mocked. This is exactly the test I should have run before shipping v1.
"""
# --- path bootstrap (repo reorganized into folders) ---
import sys as _sys, pathlib as _pathlib
_ROOT = _pathlib.Path(__file__).resolve().parents[1]
_sys.path[:0] = [str(_ROOT / _d) for _d in ("bot", "backtests", "research")]
# ---------------------------------------------------------
import asyncio
import os
import sys
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

os.environ.setdefault("INSTRUMENT", "MES")
os.environ.setdefault("TOPSTEP_USERNAME", "test")
os.environ.setdefault("TOPSTEP_API_KEY", "test")

# Import after env vars set
sys.path.insert(0, ".")
import intrabar_mes_bot as ibot


class MockResponse:
    def __init__(self, json_data, status=200):
        self._json = json_data
        self.status = status
    async def __aenter__(self): return self
    async def __aexit__(self, *args): pass
    async def json(self): return self._json
    async def text(self): return str(self._json)


class MockSession:
    def __init__(self):
        self.calls = []
        self.responses_by_url = {}
    def set_response(self, url_substring, response_json, status=200):
        self.responses_by_url[url_substring] = (response_json, status)
    async def post(self, url, **kwargs):
        self.calls.append((url, kwargs))
        for substring, (rj, st) in self.responses_by_url.items():
            if substring in url:
                return MockResponse(rj, st)
        return MockResponse({"success": True}, 200)


def make_bot():
    bot = ibot.IntrabarMESBot()
    bot.token = "fake-token"
    bot.account_id = 12345
    bot.contract_id = "CON.F.US.MES.U26"
    bot.contract_name = "MESU26"
    # Force notifier disabled so we don't need to mock Telegram
    bot.notifier.enabled = False
    return bot


# ═══════════════════════════════════════════════════════════════════════════
# TEST 1: Order status parsing (the .lower() bug fix)
# ═══════════════════════════════════════════════════════════════════════════
async def test_order_status_integer():
    print("\nTEST 1: Order status parsing with integer codes")
    bot = make_bot()
    session = MockSession()

    # Case A: status=2 → filled
    session.set_response("/Order/search", {
        "orders": [{"id": 111, "status": 2, "filledPrice": 7500.25, "limitPrice": None}]
    })
    status, fill = await bot._check_order_status(session, "fake-token", 111)
    assert status == "filled", f"Expected 'filled', got '{status}'"
    assert fill == 7500.25, f"Expected 7500.25, got {fill}"
    print("  PASS: integer status=2 parsed as 'filled', fill price captured")

    # Case B: status=4 → rejected
    session.set_response("/Order/search", {
        "orders": [{"id": 222, "status": 4, "filledPrice": None}]
    })
    status, fill = await bot._check_order_status(session, "fake-token", 222)
    assert status == "rejected", f"Expected 'rejected', got '{status}'"
    print("  PASS: integer status=4 parsed as 'rejected'")

    # Case C: status=0 → pending
    session.set_response("/Order/search", {
        "orders": [{"id": 333, "status": 0, "filledPrice": None}]
    })
    status, fill = await bot._check_order_status(session, "fake-token", 333)
    assert status == "pending", f"Expected 'pending', got '{status}'"
    print("  PASS: integer status=0 parsed as 'pending'")

    # Case D: order missing → pending
    session.set_response("/Order/search", {"orders": []})
    status, fill = await bot._check_order_status(session, "fake-token", 999)
    assert status == "pending", f"Expected 'pending' for missing order, got '{status}'"
    print("  PASS: missing order parsed as 'pending'")


# ═══════════════════════════════════════════════════════════════════════════
# TEST 2: Bar staleness gate
# ═══════════════════════════════════════════════════════════════════════════
async def test_bar_staleness():
    print("\nTEST 2: Bar staleness rejection")

    # Fresh bar (close 30s ago) should be accepted
    bot = make_bot()
    now = datetime.now(ibot.PT)
    fresh_ts = now - timedelta(seconds=90)  # bar timestamp = 90s ago → close at 30s ago
    session = MockSession()
    session.set_response("/History/retrieveBars", {
        "success": True,
        "bars": [{
            "t": fresh_ts.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
            "o": 7500.0, "h": 7500.5, "l": 7499.5, "c": 7500.25, "v": 100
        }]
    })
    # Patch the within-session check so the test time doesn't matter
    bot._within_session = lambda t: True
    bot._refresh_daily_pnl = AsyncMock(return_value=0.0)

    # Fresh bar should not trigger stale-bar rejection
    await bot._process_new_bars(session)
    # If it went through, _last_bar_ts should be set to the bar's timestamp
    assert bot._last_bar_ts is not None
    print(f"  PASS: fresh bar (30s stale) processed, _last_bar_ts set")

    # Now feed a stale bar
    bot2 = make_bot()
    stale_ts = now - timedelta(seconds=600)  # bar closed 540s ago (way past 90s limit)
    session2 = MockSession()
    session2.set_response("/History/retrieveBars", {
        "success": True,
        "bars": [{
            "t": stale_ts.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
            "o": 7400.0, "h": 7400.5, "l": 7399.5, "c": 7400.0, "v": 50
        }]
    })
    bot2._within_session = lambda t: True
    bot2._refresh_daily_pnl = AsyncMock(return_value=0.0)

    # Track wrapper add_candle to confirm it's NOT called
    add_candle_called = [False]
    original_add = bot2.wrapper.add_candle
    def spy_add(*args, **kwargs):
        add_candle_called[0] = True
        return original_add(*args, **kwargs)
    bot2.wrapper.add_candle = spy_add

    await bot2._process_new_bars(session2)
    assert not add_candle_called[0], "STALE BAR was NOT rejected — bug!"
    print(f"  PASS: stale bar (540s old, >90s limit) rejected — no engine advance")


# ═══════════════════════════════════════════════════════════════════════════
# TEST 3: _finalize_bracket_trade computes REAL P&L
# ═══════════════════════════════════════════════════════════════════════════
async def test_finalize_pnl_win():
    print("\nTEST 3a: _finalize_bracket_trade — winning long")
    bot = make_bot()
    session = MockSession()

    # Simulate a long entry filled at 7520.00 with a target hit at 7523.00
    bot._live_entry_fill = 7520.00
    bot._live_entry_direction = "long"
    bot._live_entry_signal = {"direction": "long", "entry_price": 7519.75, "stop": 7519.0, "target": 7523.0}
    bot._awaiting_bracket_close = True
    bot.trades_today = 1

    prev_pnl = bot.daily_pnl
    prev_losses = bot.consecutive_losses

    await bot._finalize_bracket_trade(session, exit_fill=7523.00, close_reason="Target bracket hit")

    # Point move: 7523 - 7520 = 3 pts × $5/pt × 1 contract = $15 gross
    # Fee: FEE_PER_ROUND_TURN × 1 = whatever's configured (matches ES bot)
    expected_gross = 3.0 * 5.0 * 1
    fee = ibot.FEE_PER_ROUND_TURN * 1
    expected_net = round(expected_gross - fee, 2)
    delta = bot.daily_pnl - prev_pnl
    assert abs(delta - expected_net) < 0.01, f"Expected P&L delta ${expected_net}, got ${delta}"
    assert bot.consecutive_losses == 0, f"Winning trade should reset consec_losses to 0, got {bot.consecutive_losses}"
    assert not bot.in_trade
    assert not bot._awaiting_bracket_close
    assert bot._live_entry_fill is None
    print(f"  PASS: net ${delta:+.2f} (gross ${expected_gross:+.2f} - ${fee:.2f} fee), consec_losses=0")


async def test_finalize_pnl_loss():
    print("\nTEST 3b: _finalize_bracket_trade — losing long (the July 17 scenario)")
    bot = make_bot()
    session = MockSession()

    # Reproduce the actual July 17 morning trade:
    # long entry filled at 7520.00, stopped out at 7519.00
    bot._live_entry_fill = 7520.00
    bot._live_entry_direction = "long"
    bot._live_entry_signal = {"direction": "long", "entry_price": 7503.25, "stop": 7502.25, "target": 7506.25}
    bot._awaiting_bracket_close = True
    bot.trades_today = 1

    prev_losses = bot.consecutive_losses

    await bot._finalize_bracket_trade(session, exit_fill=7519.00, close_reason="Stop bracket hit")

    # Point move: 7519 - 7520 = -1 pt × $5/pt × 1 contract = -$5 gross
    # Fee: FEE_PER_ROUND_TURN
    expected_gross = -1.0 * 5.0 * 1
    fee = ibot.FEE_PER_ROUND_TURN * 1
    expected_net = round(expected_gross - fee, 2)
    assert abs(bot.daily_pnl - expected_net) < 0.01, f"Expected P&L ${expected_net}, got ${bot.daily_pnl}"
    assert bot.consecutive_losses == prev_losses + 1, f"Losing trade should increment consec_losses"
    print(f"  PASS: net ${bot.daily_pnl:+.2f} correctly booked, consec_losses -> {bot.consecutive_losses}")


async def test_finalize_pnl_short():
    print("\nTEST 3c: _finalize_bracket_trade — winning short")
    bot = make_bot()
    session = MockSession()

    # Short entry filled at 7595.00, target hit at 7592.00
    bot._live_entry_fill = 7595.00
    bot._live_entry_direction = "short"
    bot._live_entry_signal = {"direction": "short", "entry_price": 7595.25, "stop": 7596.0, "target": 7592.0}
    bot._awaiting_bracket_close = True
    bot.trades_today = 1

    prev_pnl = bot.daily_pnl
    await bot._finalize_bracket_trade(session, exit_fill=7592.00, close_reason="Target bracket hit")

    # For short: point_move = entry - exit = 7595 - 7592 = 3 pts winner
    expected_gross = 3.0 * 5.0 * 1
    fee = ibot.FEE_PER_ROUND_TURN * 1
    expected_net = round(expected_gross - fee, 2)
    assert abs(bot.daily_pnl - prev_pnl - expected_net) < 0.01
    assert bot.consecutive_losses == 0
    print(f"  PASS: short winner net ${bot.daily_pnl - prev_pnl:+.2f}, consec_losses=0")


async def test_finalize_pnl_missing_fill():
    print("\nTEST 3d: _finalize_bracket_trade — missing exit fill")
    bot = make_bot()
    session = MockSession()

    bot._live_entry_fill = 7520.0
    bot._live_entry_direction = "long"
    bot._live_entry_signal = {"direction": "long", "entry_price": 7520.0, "stop": 7519.0, "target": 7523.0}
    bot._awaiting_bracket_close = True

    prev_pnl = bot.daily_pnl
    prev_losses = bot.consecutive_losses
    await bot._finalize_bracket_trade(session, exit_fill="unknown", close_reason="Position closed unknown")

    # With unknown exit, we don't book P&L or update counters (no data to trust)
    assert bot.daily_pnl == prev_pnl, f"P&L should NOT change with unknown fill; changed by ${bot.daily_pnl - prev_pnl}"
    assert bot.consecutive_losses == prev_losses, "Consec losses should not change with unknown fill"
    assert not bot.in_trade  # But we do clear the trade state
    print(f"  PASS: unknown fill → no P&L booked, trade state cleared, no false counter increment")


# ═══════════════════════════════════════════════════════════════════════════
# TEST 4: Consecutive losses accumulate correctly across trades
# ═══════════════════════════════════════════════════════════════════════════
async def test_consec_losses_accumulation():
    print("\nTEST 4: Consecutive losses stop trading after 2 losses (safety gate)")
    bot = make_bot()
    session = MockSession()

    # Trade 1 — loss
    bot._live_entry_fill = 7520.0
    bot._live_entry_direction = "long"
    bot._live_entry_signal = {"direction": "long", "entry_price": 7520.0, "stop": 7519.0, "target": 7523.0}
    bot._awaiting_bracket_close = True
    bot.trades_today = 1
    await bot._finalize_bracket_trade(session, exit_fill=7519.0, close_reason="Stop bracket hit")
    assert bot.consecutive_losses == 1

    # Trade 2 — loss
    bot._live_entry_fill = 7520.0
    bot._live_entry_direction = "long"
    bot._live_entry_signal = {"direction": "long", "entry_price": 7520.0, "stop": 7519.0, "target": 7523.0}
    bot._awaiting_bracket_close = True
    bot.trades_today = 2
    await bot._finalize_bracket_trade(session, exit_fill=7519.0, close_reason="Stop bracket hit")
    assert bot.consecutive_losses == 2

    # Safety gate must now block entries
    ok, reason = bot._safety_gates_pass()
    assert not ok, "Safety gate should block after 2 consec losses"
    assert "MAX_CONSECUTIVE_LOSSES" in reason
    print(f"  PASS: after 2 losses, gate blocks with reason: {reason}")


async def main():
    print("=" * 66)
    print("Testing intrabar_mes_bot.py fixes")
    print("=" * 66)
    await test_order_status_integer()
    await test_bar_staleness()
    await test_finalize_pnl_win()
    await test_finalize_pnl_loss()
    await test_finalize_pnl_short()
    await test_finalize_pnl_missing_fill()
    await test_consec_losses_accumulation()
    print("\n" + "=" * 66)
    print("ALL TESTS PASSED")
    print("=" * 66)


if __name__ == "__main__":
    asyncio.run(main())
