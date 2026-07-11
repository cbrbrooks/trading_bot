"""
Batch Replay — runs topstep_bot_v5's actual LevelEngine/ExecutionEngine
across a range of trading days and aggregates results.

Imports the live bot's classes directly (not a reimplementation) so
whatever you test here is exactly what would have traded live.

Usage:
    python batch_replay.py 2026-06-10 2026-07-02
    python batch_replay.py 2026-06-10              # end date defaults to today

Requires the same .env as topstep_bot_v5.py (TOPSTEP_USERNAME, TOPSTEP_API_KEY),
and topstep_bot_v5.py must be in the same directory (or on PYTHONPATH).
"""

import asyncio
import json
import sys
from datetime import date, datetime, time, timedelta

import aiohttp

import topstep_bot_v5 as bot
from topstep_bot_v5 import (
    Candle, LevelEngine, ExecutionEngine,
    BASE_URL, USERNAME, API_KEY, MES_SYMBOL, INSTRUMENT, MAX_CONSECUTIVE_LOSSES,
    POINT_VALUE, CONTRACTS, MAX_ENTRIES,
    DAILY_LOSS_LIMIT, DAILY_PROFIT_TARGET, PER_TRADE_LOSS_LIMIT,
    SESSION_1_START, SESSION_1_END, SESSION_2_START, SESSION_2_END,
    PT, CT,
)


# ── Make cooldown logic (which calls datetime.now(PT) inside ExecutionEngine)
#    respect simulated bar time instead of real wall-clock time during replay.
class SimDateTime(datetime):
    _sim_now = None

    @classmethod
    def now(cls, tz=None):
        if cls._sim_now is not None:
            return cls._sim_now.astimezone(tz) if tz else cls._sim_now
        return datetime.now(tz)


bot.datetime = SimDateTime


async def fetch_bars(session, token, contract_id, unit_n, start, end, limit):
    resp = await session.post(
        f"{BASE_URL}/History/retrieveBars",
        headers={"Authorization": f"Bearer {token}"},
        json={"contractId": contract_id, "live": False,
              "unit": 2, "unitNumber": unit_n, "limit": limit,
              "includePartialBar": False,
              "startTime": start.isoformat(), "endTime": end.isoformat()},
    )
    d = await resp.json()
    bars = d.get("bars") or d.get("d") or d.get("data") or []
    bars.sort(key=lambda b: b.get("t") or b.get("time") or "")
    return bars


def daterange(start: date, end: date):
    d = start
    while d <= end:
        if d.weekday() < 5:  # skip Sat/Sun
            yield d
        d += timedelta(days=1)


async def replay_one_day(session, token, contract_id, day: date) -> dict:
    result = {
        "date": day.isoformat(), "bars": 0, "trades": [], "skipped": 0,
        "signals": 0, "pnl": 0.0, "wins": 0, "losses": 0, "note": None,
    }

    # 15-min bars for level detection (14-day lookback ending at 6:30 PT — matches live bot's _refresh_levels)
    end_15 = datetime.combine(day, time(6, 30), tzinfo=PT).astimezone(CT)
    start_15 = end_15 - timedelta(days=14)
    bars_15 = await fetch_bars(session, token, contract_id, 15, start_15, end_15, limit=384)
    if len(bars_15) < 5:
        result["note"] = f"only {len(bars_15)} 15m bars — likely holiday/no data"
        return result
    candles_15 = [Candle.from_bar(b) for b in bars_15]

    # 1-min bars for both sessions
    start_1m = datetime.combine(day, SESSION_1_START, tzinfo=PT).astimezone(CT)
    end_1m = datetime.combine(day, SESSION_2_END, tzinfo=PT).astimezone(CT)
    bars_1m = await fetch_bars(session, token, contract_id, 1, start_1m, end_1m, limit=500)
    if len(bars_1m) < 2:
        result["note"] = f"only {len(bars_1m)} 1m bars — likely holiday/no data"
        return result
    result["bars"] = len(bars_1m)

    candles_1m = [Candle.from_bar(b) for b in bars_1m]
    session_open = candles_1m[0].open

    level_engine = LevelEngine()
    level_engine.build(candles_15, session_open)
    exec_engine = ExecutionEngine(level_engine)

    entries_today = 0
    daily_pnl = 0.0
    consecutive_losses = 0

    for candle in candles_1m:
        SimDateTime._sim_now = candle.ts if candle.ts.tzinfo else candle.ts.replace(tzinfo=PT)

        pt_time = candle.ts.astimezone(PT).time()
        in_session = (SESSION_1_START <= pt_time < SESSION_1_END or
                      SESSION_2_START <= pt_time < SESSION_2_END)
        if not in_session:
            continue

        if daily_pnl <= -DAILY_LOSS_LIMIT or daily_pnl >= DAILY_PROFIT_TARGET:
            break
        if consecutive_losses >= MAX_CONSECUTIVE_LOSSES:
            break

        # Per-trade hard loss check (mirrors bot's proactive intrabar flatten).
        # Checks against exec_engine.stop_price directly — the same value
        # check_exit() maintains (including breakeven/trailing moves) — rather
        # than recomputing an independent threshold from entry price. Two
        # independent thresholds using the bar's raw extreme as exit price
        # was letting losses overshoot the cap even when "intrabar-aware."
        if exec_engine.in_trade and exec_engine.stop_price is not None:
            hit_price = None
            if exec_engine.position == "long" and candle.low <= exec_engine.stop_price:
                hit_price = exec_engine.stop_price
            elif exec_engine.position == "short" and candle.high >= exec_engine.stop_price:
                hit_price = exec_engine.stop_price
            if hit_price is not None:
                pnl = ((hit_price - exec_engine.entry_price) if exec_engine.position == "long"
                       else (exec_engine.entry_price - hit_price)) * POINT_VALUE * CONTRACTS
                daily_pnl += pnl
                if pnl < 0:
                    result["losses"] += 1
                    consecutive_losses += 1
                else:
                    result["wins"] += 1
                    consecutive_losses = 0
                result["trades"].append({
                    "entry_time": exec_engine.candles[-2].ts.astimezone(PT).strftime("%H:%M") if len(exec_engine.candles) >= 2 else "?",
                    "exit_time": candle.ts.astimezone(PT).strftime("%H:%M"),
                    "direction": exec_engine.position,
                    "entry_price": exec_engine.entry_price,
                    "exit_price": hit_price,
                    "level": exec_engine.entry_level.price if exec_engine.entry_level else None,
                    "reason": "Breakeven stop hit (scratch)" if abs(hit_price - exec_engine.entry_price) < 0.01
                              else "Per-trade loss limit",
                    "pnl": round(pnl, 2),
                })
                exec_engine.last_stop_out = SimDateTime._sim_now
                if exec_engine.entry_level:
                    level_engine.record_loss(exec_engine.entry_level.price)
                exec_engine.in_trade = False
                exec_engine.position = None
                exec_engine.entry_price = None
                exec_engine.entry_level = None
                exec_engine.target_level = None
                exec_engine.last_passed_level = None
                continue

        signal = exec_engine.add_candle(candle)

        if signal and signal.get("type") == "entry":
            if entries_today < MAX_ENTRIES:
                result["signals"] += 1
                entries_today += 1
                exec_engine.mark_entry_filled(
                    signal["position"], signal["entry_price"],
                    signal["stop"], signal.get("level")
                )
                signal["_entry_time"] = candle.ts.astimezone(PT).strftime("%H:%M")
                result["_pending"] = signal
        elif signal and signal.get("type") == "exit":
            fill_price = signal.get("exit_price")
            if fill_price is None:
                fill_price = candle.close
            entry_price = exec_engine.entry_price
            position = exec_engine.position
            pnl = ((fill_price - entry_price) if position == "long"
                   else (entry_price - fill_price)) * POINT_VALUE * CONTRACTS
            daily_pnl += pnl
            if pnl < 0:
                result["losses"] += 1
                consecutive_losses += 1
            else:
                result["wins"] += 1
                consecutive_losses = 0
            pending = result.pop("_pending", {})
            result["trades"].append({
                "entry_time": pending.get("_entry_time", "?"),
                "exit_time": candle.ts.astimezone(PT).strftime("%H:%M"),
                "direction": position,
                "entry_price": entry_price,
                "exit_price": round(fill_price, 2),
                "level": exec_engine.entry_level.price if exec_engine.entry_level else None,
                "reason": signal["reason"],
                "pnl": round(pnl, 2),
            })
            exec_engine.mark_flat()

    result["pnl"] = round(daily_pnl, 2)
    result.pop("_pending", None)
    return result


async def main():
    if len(sys.argv) < 2:
        print("Usage: python batch_replay.py START_DATE [END_DATE]")
        print("       dates in YYYY-MM-DD format")
        return

    start_date = date.fromisoformat(sys.argv[1])
    end_date = date.fromisoformat(sys.argv[2]) if len(sys.argv) > 2 else date.today()

    if not USERNAME or not API_KEY:
        raise SystemExit("ERROR: Set TOPSTEP_USERNAME and TOPSTEP_API_KEY in your .env file.")

    async with aiohttp.ClientSession() as session:
        resp = await session.post(f"{BASE_URL}/Auth/loginKey",
                                   json={"userName": USERNAME, "apiKey": API_KEY})
        data = await resp.json()
        if not data.get("success"):
            print(f"Auth failed: {data}")
            return
        token = data["token"]
        print("Authenticated OK")

        resp = await session.post(f"{BASE_URL}/Contract/search",
                                   headers={"Authorization": f"Bearer {token}"},
                                   json={"searchText": INSTRUMENT, "live": False})
        data = await resp.json()
        contracts = data.get("contracts", [])
        exact = [c for c in contracts if c.get("name") == MES_SYMBOL]
        contract = exact[0] if exact else sorted(
            [c for c in contracts if c.get("name", "").startswith(INSTRUMENT)],
            key=lambda c: c.get("name", ""))[0]
        contract_id = contract["id"]
        print(f"Contract: {contract['name']} ({contract_id})\n")

        all_results = []
        for day in daterange(start_date, end_date):
            print(f"Replaying {day} ...", end=" ")
            try:
                r = await replay_one_day(session, token, contract_id, day)
            except Exception as e:
                print(f"ERROR: {e}")
                continue
            if r["note"]:
                print(r["note"])
                continue
            print(f"{len(r['trades'])} trades, P&L ${r['pnl']:.2f}")
            all_results.append(r)
            await asyncio.sleep(0.5)  # be polite to the API

        # ── Aggregate ──────────────────────────────────────────────────
        traded_days = [r for r in all_results if r["trades"]]
        all_trades = [t for r in all_results for t in r["trades"]]
        total_pnl = sum(r["pnl"] for r in all_results)
        wins = sum(r["wins"] for r in all_results)
        losses = sum(r["losses"] for r in all_results)
        total_trades = wins + losses
        win_rate = (wins / total_trades * 100) if total_trades else 0.0
        avg_win = (sum(t["pnl"] for t in all_trades if t["pnl"] > 0) / wins) if wins else 0.0
        avg_loss = (sum(t["pnl"] for t in all_trades if t["pnl"] < 0) / losses) if losses else 0.0
        expectancy = (win_rate / 100 * avg_win) + ((1 - win_rate / 100) * avg_loss)

        print("\n" + "=" * 60)
        print(f"  BATCH REPLAY SUMMARY — {start_date} to {end_date}")
        print("=" * 60)
        print(f"  Trading days replayed : {len(all_results)}")
        print(f"  Days with trades       : {len(traded_days)}")
        print(f"  Total trades            : {total_trades}")
        print(f"  Wins / Losses           : {wins} / {losses}")
        print(f"  Win rate                : {win_rate:.1f}%")
        print(f"  Avg win / Avg loss      : ${avg_win:.2f} / ${avg_loss:.2f}")
        print(f"  Expectancy per trade    : ${expectancy:.2f}")
        print(f"  Total P&L               : ${total_pnl:.2f}")
        print("=" * 60)

        print("\n  PER-DAY BREAKDOWN:")
        for r in all_results:
            marker = "  " if r["trades"] else "  (no trades)"
            print(f"    {r['date']}: {len(r['trades'])} trades, P&L ${r['pnl']:.2f}{marker if not r['trades'] else ''}")

        print("\n  ALL TRADES:")
        for r in all_results:
            for t in r["trades"]:
                print(f"    {r['date']} {t['entry_time']}->{t['exit_time']} "
                      f"{t['direction'].upper()} @ {t['entry_price']} -> {t['exit_price']} "
                      f"level={t['level']} | {t['reason']} | P&L=${t['pnl']:.2f}")

        # ── JSON output ────────────────────────────────────────────────
        out = {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "summary": {
                "trading_days_replayed": len(all_results),
                "days_with_trades": len(traded_days),
                "total_trades": total_trades,
                "wins": wins,
                "losses": losses,
                "win_rate_pct": round(win_rate, 2),
                "avg_win": round(avg_win, 2),
                "avg_loss": round(avg_loss, 2),
                "expectancy_per_trade": round(expectancy, 2),
                "total_pnl": round(total_pnl, 2),
            },
            "days": all_results,
        }
        out_path = f"batch_replay_{start_date}_{end_date}.json"
        with open(out_path, "w") as f:
            json.dump(out, f, indent=2, default=str)
        print(f"\n  Saved full results to {out_path}")


if __name__ == "__main__":
    asyncio.run(main())
