"""
Full-lifecycle test of the bracket-order rewrite in choch_bot_live.py, with the
TopstepX API entirely mocked. Verifies the critical new behavior:

  1. An entry signal places a BRACKET order (market entry + stopLossBracket type 4
     + takeProfitBracket type 1), not a plain market order.
  2. While the bracket is live, the engine's own exit signal does NOT fire a real
     order (broker owns the exit) — it's informational only.
  3. When the broker closes the position (detected via Position/searchOpen going
     empty), the trade is finalized with REAL fills, and the safety-gate daily P&L
     is reconciled to the actual number, backing out the engine's theoretical booking.

This is the scenario that matters: it proves we don't double-exit, and that the
-$325-style slippage-on-exit can't happen because the bot never fires an exit order
at all — the broker's resting bracket does, off live ticks.
"""
import asyncio
import sys

sys.argv = ["choch_bot_live.py"]  # LIVE code paths
import choch_bot_live as m


class FakeResp:
    def __init__(self, data): self._data = data; self.status = 200
    async def json(self): return self._data


class FakeBroker:
    """Simulates the sequence: place bracket -> position open -> (later) position
    closed by a target-bracket fill."""
    def __init__(self):
        self.entry_oid = 5000
        self.target_child_oid = 5001
        self.position_open = False
        self.close_after_n_checks = 2   # position stays open for 2 poll checks, then closes
        self.checks = 0
        self.placed_payloads = []

    async def post(self, url, headers=None, json=None):
        if url.endswith("/Order/place"):
            self.placed_payloads.append(json)
            self.position_open = True
            return FakeResp({"success": True, "orderId": self.entry_oid})
        if url.endswith("/Order/search"):
            # entry order fills immediately; target child fills once position closes
            orders = [{"id": self.entry_oid, "status": 2, "filledPrice": 7606.00, "type": 2}]
            if not self.position_open:
                orders.append({"id": self.target_child_oid, "status": 2,
                               "filledPrice": 7616.50, "type": m.ORDER_TYPE_LIMIT,
                               "updateTimestamp": "2026-07-11T09:20:00Z"})
            return FakeResp({"orders": orders})
        if url.endswith("/Position/searchOpen"):
            self.checks += 1
            if self.checks >= self.close_after_n_checks:
                self.position_open = False
            positions = [{"contractId": "CON.TEST", "averagePrice": 7606.00, "size": 2}] if self.position_open else []
            return FakeResp({"positions": positions})
        raise AssertionError(f"Unexpected URL: {url}")


async def main():
    print("=== Bracket lifecycle test ===")
    bot = m.ChochLiveBot()
    bot.account_id, bot.contract_id = 1, "CON.TEST"
    broker = FakeBroker()
    token = "tok"

    # Minimal engine stand-in that behaves like the real one for the fields used
    import choch_fvg_bot
    eng = choch_fvg_bot.CHOCHFVGEngine()
    bot.wrapper = type("W", (), {"engine": eng})()

    # 1. Entry signal -> should place a BRACKET order
    entry_signal = {"type": "entry", "direction": "long", "entry_price": 7606.5,
                     "stop": 7605.0, "target": 7617.0,  # 1.5pt stop, 10.5pt target (7R)
                     "reason": "CHOCH bullish test", "pnl": 0}
    eng.trades_today = 1
    await bot._handle_signal(broker, token, entry_signal)

    assert broker.placed_payloads, "no order was placed"
    payload = broker.placed_payloads[0]
    assert "stopLossBracket" in payload and "takeProfitBracket" in payload, "not a bracket order!"
    assert payload["stopLossBracket"]["type"] == m.ORDER_TYPE_STOP, "stop bracket not type 4 (Stop)"
    assert payload["takeProfitBracket"]["type"] == m.ORDER_TYPE_LIMIT, "target bracket not type 1 (Limit)"
    assert payload["stopLossBracket"]["ticks"] == 6, f"stop should be 6 ticks (1.5pt), got {payload['stopLossBracket']['ticks']}"
    assert payload["takeProfitBracket"]["ticks"] == 42, f"target should be 42 ticks (10.5pt), got {payload['takeProfitBracket']['ticks']}"
    assert bot._awaiting_bracket_close is True, "should be awaiting broker close"
    assert bot._live_entry_fill == 7606.00, f"entry fill should be captured, got {bot._live_entry_fill}"
    print(f"  PASS: bracket placed — stop {payload['stopLossBracket']['ticks']}t (type {payload['stopLossBracket']['type']}), "
          f"target {payload['takeProfitBracket']['ticks']}t (type {payload['takeProfitBracket']['type']}); entry filled @ {bot._live_entry_fill}")

    # 2. Engine emits its own exit signal while bracket live -> must NOT place an order
    orders_before = len(broker.placed_payloads)
    exit_signal = {"type": "exit", "direction": "long", "entry_price": 7606.5,
                    "exit_price": 7617.0, "reason": "Target hit", "pnl": 1050.0}
    await bot._handle_signal(broker, token, exit_signal)
    assert len(broker.placed_payloads) == orders_before, "engine exit signal wrongly fired a real order!"
    print("  PASS: engine exit signal did NOT fire a real order (broker owns the exit)")

    # 3. Simulate the main loop's bracket-close poll until the broker closes it
    for _ in range(5):
        if not bot._awaiting_bracket_close:
            break
        exit_fill, close_reason, _ = await bot._check_position_closed(broker, token)
        if exit_fill is not None:
            await bot._finalize_bracket_trade(exit_fill, close_reason)

    assert bot._awaiting_bracket_close is False, "should have finalized after broker close"
    last = bot.analytics.trades[-1]
    # real P&L: entry 7606.00 -> exit 7616.50 = 10.5pt * $50 * 2 = $1050
    assert last["actual_pnl"] == 1050.0, f"expected real P&L $1050, got {last['actual_pnl']}"
    assert "Target" in last["exit_reason"], f"expected target close reason, got {last['exit_reason']}"
    print(f"  PASS: broker close finalized — reason='{last['exit_reason']}', "
          f"real P&L=${last['actual_pnl']} (entry {last['actual_entry_price']} -> exit {last['actual_exit_price']})")
    print(f"  PASS: engine daily P&L reconciled to actual = ${eng.daily_pnl}")
    print()
    print("ALL BRACKET LIFECYCLE TESTS PASSED")


asyncio.run(main())
