"""
Mock-based test of the limit-entry state machine in choch_bot_live.py.
Real aiohttp calls are stubbed out so this runs with no network and no
TopstepX credentials, but exercises the actual bot code paths: _place_order,
_check_order_status, _cancel_order, _handle_signal, _abandon_pending_entry.
"""
import asyncio
import sys

sys.argv = ["choch_bot_live.py"]  # LIVE mode code paths, not --dry-run
import choch_bot_live as m


class FakeResp:
    def __init__(self, data, status=200):
        self._data = data
        self.status = status
    async def json(self):
        return self._data


class FakeSession:
    """Routes POSTs by URL suffix. `order_status_sequence` lets a test script
    a specific order through pending -> filled (or pending -> rejected)."""
    def __init__(self):
        self.calls = []
        self.next_order_id = 1000
        self.next_order_id_seen = []
        self.order_status_by_id = {}   # order_id -> list of statuses to return in sequence
        self.cancelled_orders = set()

    async def post(self, url, headers=None, json=None):
        self.calls.append((url, json))
        if url.endswith("/Order/place"):
            oid = self.next_order_id
            self.next_order_id += 1
            self.next_order_id_seen.append(oid)
            return FakeResp({"success": True, "orderId": oid})
        if url.endswith("/Order/search"):
            orders = []
            for oid, seq in self.order_status_by_id.items():
                status = seq.pop(0) if len(seq) > 1 else seq[0]
                orders.append({"id": oid, "status": status, "filledPrice": 7519.25, "limitPrice": 7516.75})
            # Any order NOT explicitly configured auto-fills immediately — matches realistic
            # fast market-order behavior, while tests keep explicit control over the entry
            # (limit) order's fill timing via order_status_by_id.
            for oid in list(self.next_order_id_seen):
                if oid not in self.order_status_by_id:
                    orders.append({"id": oid, "status": 2, "filledPrice": 7518.75, "limitPrice": None})
            return FakeResp({"orders": orders})
        if url.endswith("/Order/cancel"):
            self.cancelled_orders.add(json["orderId"])
            return FakeResp({"success": True})
        if url.endswith("/Position/searchOpen"):
            return FakeResp({"positions": []})
        raise AssertionError(f"Unexpected URL in test: {url}")


async def test_limit_fills_then_normal_exit():
    print("=== Test 1: limit entry fills, then a normal market exit follows ===")
    bot = m.ChochLiveBot()
    bot.account_id, bot.contract_id = 1, "CON.TEST"
    session = FakeSession()
    token = "faketoken"

    entry_signal = {"type": "entry", "direction": "long", "entry_price": 7516.75,
                     "stop": 7516.25, "target": 7518.75, "reason": "test entry"}

    class FakeEngine:
        trades_today = 1
    bot.wrapper = type("W", (), {"engine": FakeEngine()})()

    await bot._handle_signal(session, token, entry_signal)
    assert bot._pending_entry is not None, "expected a pending entry after limit order submitted"
    oid = bot._pending_entry["order_id"]
    session.order_status_by_id[oid] = [1, 1, 2]  # pending, pending, then filled (status 2)

    # Simulate the main loop's per-iteration poll, a few times, until it fills
    for _ in range(3):
        if bot._pending_entry is None:
            break
        status, fill = await bot._check_order_status(session, token, bot._pending_entry["order_id"])
        if status == "filled":
            bot._live_entry_fill = fill
            bot._pending_entry = None

    assert bot._pending_entry is None, "pending entry should have cleared on fill"
    assert bot._live_entry_fill == 7519.25, f"expected actual fill captured, got {bot._live_entry_fill}"
    print("  PASS: limit entry filled, pending state cleared, actual fill price captured")

    # Now a real exit should behave exactly as before (market order, real P&L calc)
    exit_signal = {"type": "exit", "direction": "long", "entry_price": 7516.75,
                    "exit_price": 7518.75, "reason": "Target hit", "pnl": 200.0}
    await bot._handle_signal(session, token, exit_signal)
    last_trade = bot.analytics.trades[-1] if bot.analytics.trades else None
    assert last_trade is not None, "expected a trade to be logged for a real exit"
    print(f"  PASS: exit handled normally, logged trade actual_pnl={last_trade.get('actual_pnl')}")
    print()


async def test_limit_never_fills_gets_abandoned():
    print("=== Test 2: limit entry never fills, theoretical trade resolves first ===")
    bot = m.ChochLiveBot()
    bot.account_id, bot.contract_id = 1, "CON.TEST"
    session = FakeSession()
    token = "faketoken"

    entry_signal = {"type": "entry", "direction": "long", "entry_price": 7581.875,
                     "stop": 7581.75, "target": 7582.75, "reason": "test tight entry"}

    class FakeEngine:
        trades_today = 1
        in_trade = True
    fake_engine = FakeEngine()
    bot.wrapper = type("W", (), {"engine": fake_engine})()

    await bot._handle_signal(session, token, entry_signal)
    assert bot._pending_entry is not None
    oid = bot._pending_entry["order_id"]
    session.order_status_by_id[oid] = [1, 1, 1]  # stays pending — never fills

    # Poll a couple times — still pending
    for _ in range(2):
        status, _ = await bot._check_order_status(session, token, bot._pending_entry["order_id"])
        assert status == "pending"
    assert bot._pending_entry is not None, "should still be pending, not yet abandoned"
    print("  PASS: order correctly stays pending across multiple polls")

    # Now the underlying theoretical trade resolves (stop hit) before the limit ever filled —
    # the engine would emit an exit signal; _handle_signal should abandon, not treat as a real exit.
    exit_signal = {"type": "exit", "direction": "long", "entry_price": 7581.875,
                    "exit_price": 7581.75, "reason": "Stop loss hit", "pnl": -12.5}
    trades_before = len(bot.analytics.trades)
    await bot._handle_signal(session, token, exit_signal)

    assert bot._pending_entry is None, "pending entry should be cleared after abandonment"
    assert oid in session.cancelled_orders, "the resting limit order should have been cancelled"
    assert len(bot.analytics.trades) == trades_before, "no trade should be logged for an abandoned entry"
    print("  PASS: pending entry abandoned, order cancelled, NOTHING logged to analytics")
    print()


async def main():
    await test_limit_fills_then_normal_exit()
    await test_limit_never_fills_gets_abandoned()
    print("ALL TESTS PASSED")


asyncio.run(main())
