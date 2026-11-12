"""
Databento Batch Replay — runs the actual Dual Trail strategy (imported
directly from topstep_bot_v5.py, same as batch_replay.py does) against
locally-downloaded Databento historical data instead of live TopstepX API
calls. This exists purely to test further back than TopstepX's own
historical data retention allows.

Usage:
    python databento_replay.py glbx-mdp3-20100606-20260705.ohlcv-1m.csv.zst 2024-01-01 2024-06-01

First run will decompress the .zst once (~1-2 min) and cache the result;
subsequent runs reuse the cached .csv.
"""
# --- path bootstrap (repo reorganized into folders) ---
import sys as _sys, pathlib as _pathlib
_ROOT = _pathlib.Path(__file__).resolve().parents[1]
_sys.path[:0] = [str(_ROOT / _d) for _d in ("bot", "backtests", "research")]
# ---------------------------------------------------------

import json
import sys
from datetime import date, datetime, timedelta

import pandas as pd

from databento_loader import ensure_decompressed, load_range, resample
import topstep_bot_v5 as bot
from topstep_bot_v5 import (
    Candle, LevelEngine, ExecutionEngine,
    POINT_VALUE, CONTRACTS, MAX_ENTRIES,
    DAILY_LOSS_LIMIT, DAILY_PROFIT_TARGET, PER_TRADE_LOSS_LIMIT, MAX_CONSECUTIVE_LOSSES,
    STOP_AFTER_FIRST_WIN,
    SESSION_1_START, SESSION_1_END, SESSION_2_START, SESSION_2_END,
    PT, CT,
)


# Cooldown logic inside ExecutionEngine calls datetime.now(PT) internally —
# during a fast backtest that's real wall-clock time, not simulated candle
# time, which makes cooldown behavior essentially broken (confirmed: it let
# a trade re-enter 3 minutes after a loss when it should have been in a
# 5-minute cooldown). Same fix as batch_replay.py: make datetime.now()
# inside the strategy module return the current simulated candle time.
class SimDateTime(datetime):
    _sim_now = None

    @classmethod
    def now(cls, tz=None):
        if cls._sim_now is not None:
            return cls._sim_now.astimezone(tz) if tz else cls._sim_now
        return datetime.now(tz)


bot.datetime = SimDateTime


def df_to_candles(df: pd.DataFrame) -> list[Candle]:
    candles = []
    for ts, row in df.iterrows():
        candles.append(Candle(row["open"], row["high"], row["low"], row["close"], row["volume"], ts.to_pydatetime()))
    return candles


def daterange(start: date, end: date):
    d = start
    while d <= end:
        if d.weekday() < 5:
            yield d
        d += timedelta(days=1)


def replay_one_day(full_1m_df: pd.DataFrame, day: date) -> dict:
    """Mirrors batch_replay.py's replay_one_day() logic exactly — same
    strategy simulation, just sourcing bars from a local DataFrame slice
    instead of an API call. Keep this in sync with batch_replay.py if that
    file's simulation logic changes."""
    result = {"date": day.isoformat(), "bars": 0, "trades": [], "skipped": 0,
              "signals": 0, "pnl": 0.0, "wins": 0, "losses": 0, "note": None}

    # 15-min bars for level detection: 14-day date window, but the live bot's
    # actual fetch caps at limit=700 bars (~7.3 days of real trading time) —
    # that bar cap, not the date range, is what determines the TRUE effective
    # lookback. Matching it here so replay behaves identically to live.
    end_15_pt = datetime.combine(day, SESSION_1_START, tzinfo=PT)
    start_15_pt = end_15_pt - timedelta(days=14)
    window_15m = full_1m_df.loc[start_15_pt:end_15_pt]
    if len(window_15m) < 20:
        result["note"] = f"only {len(window_15m)} 1m bars in lookback — likely no data for this period"
        return result
    candles_15 = df_to_candles(resample(window_15m, "15min"))
    candles_15 = candles_15[-384:]  # 4-day lookback, confirmed best across properly-isolated comparison
                                      # also matches the live bot's actual limit=700 behavior
    if len(candles_15) < 5:
        result["note"] = f"only {len(candles_15)} 15m bars after resample"
        return result

    # 1-min bars for both sessions
    start_1m_pt = datetime.combine(day, SESSION_1_START, tzinfo=PT)
    end_1m_pt = datetime.combine(day, SESSION_2_END, tzinfo=PT)
    window_1m = full_1m_df.loc[start_1m_pt:end_1m_pt]
    if len(window_1m) < 10:
        result["note"] = f"only {len(window_1m)} 1m bars — likely holiday/no data"
        return result
    candles_1m = df_to_candles(window_1m)
    result["bars"] = len(candles_1m)

    session_open = candles_1m[0].open
    level_engine = LevelEngine()
    level_engine.build(candles_15, session_open)
    exec_engine = ExecutionEngine(level_engine)

    entries_today = 0
    daily_pnl = 0.0
    consecutive_losses = 0
    pending = None

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

        # Per-trade hard loss check — checks exec_engine.stop_price directly
        # (same value check_exit() maintains), matching the confirmed-correct
        # logic in batch_replay.py.
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
                    if STOP_AFTER_FIRST_WIN and entries_today == 1:
                        consecutive_losses = MAX_CONSECUTIVE_LOSSES  # reuses the existing stop-for-day gate
                result["trades"].append({
                    "entry_time": pending.get("_entry_time", "?") if pending else "?",
                    "exit_time": pt_time.strftime("%H:%M"),
                    "direction": exec_engine.position,
                    "entry_price": exec_engine.entry_price,
                    "exit_price": hit_price,
                    "level": exec_engine.entry_level.price if exec_engine.entry_level else None,
                    "reason": "Breakeven stop hit (scratch)" if abs(hit_price - exec_engine.entry_price) < 0.01
                              else "Per-trade loss limit",
                    "pnl": round(pnl, 2),
                })
                if exec_engine.entry_level:
                    level_engine.record_loss(exec_engine.entry_level.price)
                exec_engine.last_stop_out = SimDateTime._sim_now
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
                signal["_entry_time"] = pt_time.strftime("%H:%M")
                pending = signal
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
                if STOP_AFTER_FIRST_WIN and entries_today == 1:
                    consecutive_losses = MAX_CONSECUTIVE_LOSSES  # reuses the existing stop-for-day gate
            result["trades"].append({
                "entry_time": (pending or {}).get("_entry_time", "?"),
                "exit_time": pt_time.strftime("%H:%M"),
                "direction": position,
                "entry_price": entry_price,
                "exit_price": round(fill_price, 2),
                "level": exec_engine.entry_level.price if exec_engine.entry_level else None,
                "reason": signal["reason"],
                "pnl": round(pnl, 2),
            })
            exec_engine.mark_flat()

    result["pnl"] = round(daily_pnl, 2)
    return result


def main():
    if len(sys.argv) < 4:
        print("Usage: python databento_replay.py DATA_FILE START_DATE END_DATE")
        print("  DATA_FILE can be the .csv.zst or an already-decompressed .csv")
        return

    data_file = sys.argv[1]
    start_date = date.fromisoformat(sys.argv[2])
    end_date = date.fromisoformat(sys.argv[3])

    csv_path = ensure_decompressed(data_file)

    # Pad the load window: need 4 extra days before start_date for the
    # 15-min level lookback on the very first day.
    load_start = pd.Timestamp(datetime.combine(start_date, SESSION_1_START, tzinfo=PT)) - pd.Timedelta(days=5)
    load_end = pd.Timestamp(datetime.combine(end_date, SESSION_2_END, tzinfo=PT)) + pd.Timedelta(days=1)
    load_start_utc = load_start.tz_convert("UTC")
    load_end_utc = load_end.tz_convert("UTC")

    print(f"Loading data from {load_start_utc} to {load_end_utc} (UTC)...")
    full_df = load_range(csv_path, load_start_utc, load_end_utc)
    if full_df.empty:
        print("No data found in that range. Check your dates are within the downloaded file's coverage.")
        return
    print(f"Loaded {len(full_df)} front-month-selected 1-minute bars.")

    # Index needs to support PT-based slicing (.loc[start_pt:end_pt]) —
    # convert index to PT so datetime.combine(..., tzinfo=PT) bounds compare correctly.
    full_df.index = full_df.index.tz_convert(PT)

    all_results = []
    for day in daterange(start_date, end_date):
        print(f"Replaying {day} ...", end=" ")
        r = replay_one_day(full_df, day)
        if r["note"]:
            print(r["note"])
            continue
        print(f"{len(r['trades'])} trades, P&L ${r['pnl']:.2f}")
        all_results.append(r)

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
    print(f"  DATABENTO BATCH REPLAY SUMMARY — {start_date} to {end_date}")
    print("=" * 60)
    print(f"  Trading days replayed : {len(all_results)}")
    print(f"  Total trades           : {total_trades}")
    print(f"  Wins / Losses          : {wins} / {losses}")
    print(f"  Win rate               : {win_rate:.1f}%")
    print(f"  Avg win / Avg loss     : ${avg_win:.2f} / ${avg_loss:.2f}")
    print(f"  Expectancy per trade   : ${expectancy:.2f}")
    print(f"  Total P&L              : ${total_pnl:.2f}")
    print("=" * 60)

    out = {
        "data_source": "databento",
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "summary": {
            "trading_days_replayed": len(all_results),
            "total_trades": total_trades,
            "wins": wins, "losses": losses,
            "win_rate_pct": round(win_rate, 2),
            "avg_win": round(avg_win, 2), "avg_loss": round(avg_loss, 2),
            "expectancy_per_trade": round(expectancy, 2),
            "total_pnl": round(total_pnl, 2),
        },
        "days": all_results,
    }
    out_path = f"databento_replay_{start_date}_{end_date}.json"
    with open(out_path, "w") as f:
        json.dump(out, f, indent=2, default=str)
    print(f"\nSaved full results to {out_path}")


if __name__ == "__main__":
    main()
