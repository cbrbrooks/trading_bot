"""
CHOCH/FVG Databento Batch Replay — runs choch_fvg_bot.py's CHOCH/FVG
strategy against locally-downloaded Databento historical data.

The CHOCH engine is ES-specific by design (CONTRACTS=1, MAX_STOP_POINTS,
MAX_TRADE_RISK_DOLLARS are all calibrated to ES's $50/point). topstep_bot_v5
picks POINT_VALUE based on the INSTRUMENT env var, defaulting to MES
($5/point) if unset — so INSTRUMENT is forced to "ES" here BEFORE
topstep_bot_v5 (and therefore choch_fvg_bot's imported POINT_VALUE) gets
imported. Skipping this would silently produce PNL/risk numbers 10x too
small without erroring.

Unlike Dual Trail, the CHOCH engine manages its own entries, exits, and
daily risk gating internally (CHOCHFVGReplayWrapper.add_candle returns
entry/exit dicts directly) — so this replay driver is a thinner wrapper
than databento_replay.py: no LevelEngine/ExecutionEngine split, no 15-min
lookback resample, just a fresh engine per day fed 1-min candles across the
6:30 AM PT session open through the 1:10 PM PT forced-flat.

Usage:
    python choch_databento_replay.py glbx-mdp3-20100606-20260705_ohlcv-1m_csv.zst 2024-01-01 2024-06-01

First run will decompress the .zst once (~1-2 min) and cache the result;
subsequent runs reuse the cached .csv.
"""
# --- path bootstrap (repo reorganized into folders) ---
import sys as _sys, pathlib as _pathlib
_ROOT = _pathlib.Path(__file__).resolve().parents[1]
_sys.path[:0] = [str(_ROOT / _d) for _d in ("bot", "backtests", "research")]
# ---------------------------------------------------------

import os
os.environ.setdefault("INSTRUMENT", "ES")

import json
import sys
from datetime import date, datetime, time, timedelta

import pandas as pd

from databento_loader import ensure_decompressed, load_range
from topstep_bot_v5 import Candle, PT, POINT_VALUE
import choch_fvg_bot
from choch_fvg_bot import CHOCHFVGReplayWrapper, SESSION_OPEN, FORCE_FLAT_TIME, CONTRACTS

# True PER-CONTRACT baselines, derived (not just captured) before any override below runs —
# used to scale MAX_TRADE_RISK_DOLLARS/DAILY_LOSS_LIMIT correctly under CHOCH_CONTRACTS no
# matter what CONTRACTS currently defaults to in choch_fvg_bot.py. Capturing the raw values
# directly (rather than dividing by the current CONTRACTS) was a real bug: once the source
# default moved from 1->2 contracts, MAX_TRADE_RISK_DOLLARS/DAILY_LOSS_LIMIT at import time
# were already 2x-scaled, so multiplying by a new override contract count double-counted the
# scaling. Dividing by the current CONTRACTS first keeps this correct even if the source
# default changes again.
_PER_CONTRACT_MAX_TRADE_RISK_DOLLARS = choch_fvg_bot.MAX_TRADE_RISK_DOLLARS / choch_fvg_bot.CONTRACTS
_PER_CONTRACT_DAILY_LOSS_LIMIT = choch_fvg_bot.DAILY_LOSS_LIMIT / choch_fvg_bot.CONTRACTS

REPLAY_WINDOW_END = time(13, 15)  # small buffer past the 1:10 PM PT forced-flat,
                                    # so the wrapper always sees a candle at/after FORCE_FLAT_TIME

# Optional parameter sweep override, e.g. CHOCH_RR_MULTIPLE=4 python choch_databento_replay.py ...
# RR_MULTIPLE is read as a module global at call-time inside choch_fvg_bot's _check_entry,
# so patching the attribute post-import is sufficient — no need to touch the source file.
_rr_override = os.environ.get("CHOCH_RR_MULTIPLE")
if _rr_override:
    choch_fvg_bot.RR_MULTIPLE = float(_rr_override)

# Optional entry-window override, e.g. CHOCH_SESSION_CLOSE=11:00 — tests how much the narrow
# 6:30-8:00 AM PT entry window itself is constraining trade frequency, independent of any
# other filter (stop-distance cap, fib zone, CHOCH definition, etc.)
_session_close_override = os.environ.get("CHOCH_SESSION_CLOSE")
if _session_close_override:
    hh, mm = _session_close_override.split(":")
    choch_fvg_bot.SESSION_CLOSE = choch_fvg_bot.time(int(hh), int(mm))

SESSION_CLOSE = choch_fvg_bot.SESSION_CLOSE

# Optional stop-width cap override — the video's spec describes fixed-R position sizing
# (contracts scale to keep dollar risk constant regardless of stop width), not a hard
# point-distance filter that skips wide-stop setups outright. MAX_STOP_POINTS/
# MAX_TRADE_RISK_DOLLARS in choch_fvg_bot.py are our own addition to keep this workable at
# a fixed 1 contract. This override tests how many setups that cap is rejecting.
_max_stop_override = os.environ.get("CHOCH_MAX_STOP_POINTS")
if _max_stop_override:
    choch_fvg_bot.MAX_STOP_POINTS = float(_max_stop_override)
_min_stop_override = os.environ.get("CHOCH_MIN_STOP_POINTS")
if _min_stop_override:
    choch_fvg_bot.MIN_STOP_POINTS = float(_min_stop_override)
_broker_min_stop_override = os.environ.get("CHOCH_BROKER_MIN_STOP_POINTS")
if _broker_min_stop_override:
    choch_fvg_bot.BROKER_MIN_STOP_POINTS = float(_broker_min_stop_override)
_breakeven_override = os.environ.get("CHOCH_USE_BREAKEVEN")
if _breakeven_override is not None:
    choch_fvg_bot.USE_BREAKEVEN = _breakeven_override.lower() in ("1", "true", "yes")
_max_risk_override = os.environ.get("CHOCH_MAX_TRADE_RISK_DOLLARS")
if _max_risk_override:
    choch_fvg_bot.MAX_TRADE_RISK_DOLLARS = float(_max_risk_override)

# Optional contract-count override, e.g. CHOCH_CONTRACTS=2. MAX_STOP_POINTS is already
# contract-count-independent (a pure point-distance filter, per its own comment in
# choch_fvg_bot.py), so it's left alone. But MAX_TRADE_RISK_DOLLARS and DAILY_LOSS_LIMIT
# are dollar thresholds calibrated to CONTRACTS=1 ($75 = 1R, $225 = 3x1R) — left unscaled,
# doubling CONTRACTS would double every trade's dollar risk and make MAX_TRADE_RISK_DOLLARS
# a NEW, tighter, unintended filter (only stops <=0.75pts would pass instead of <=1.5pts),
# silently changing which trades qualify rather than just sizing the same trades bigger.
# Scaling both proportionally keeps trade SELECTION identical to the 1-contract baseline —
# only position size and dollar P&L change, which is what "test with N contracts" should mean.
_contracts_override = os.environ.get("CHOCH_CONTRACTS")
if _contracts_override:
    _new_contracts = int(_contracts_override)
    choch_fvg_bot.CONTRACTS = _new_contracts
    choch_fvg_bot.MAX_TRADE_RISK_DOLLARS = _PER_CONTRACT_MAX_TRADE_RISK_DOLLARS * _new_contracts
    choch_fvg_bot.DAILY_LOSS_LIMIT = _PER_CONTRACT_DAILY_LOSS_LIMIT * _new_contracts

CONTRACTS = choch_fvg_bot.CONTRACTS

# Optional diagnostic mode: track max-favorable-excursion per trade against the
# breakeven trigger and target, to see how often (and by how much) price actually
# reaches the breakeven trigger relative to the target distance.
TRACK_DIAGNOSTICS = os.environ.get("CHOCH_DIAGNOSTICS", "0") == "1"


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
    result = {"date": day.isoformat(), "bars": 0, "trades": [], "pnl": 0.0,
              "wins": 0, "losses": 0, "scratches": 0, "note": None}

    start_pt = pd.Timestamp(datetime.combine(day, SESSION_OPEN, tzinfo=PT))
    end_pt = pd.Timestamp(datetime.combine(day, REPLAY_WINDOW_END, tzinfo=PT))
    window = full_1m_df.loc[start_pt:end_pt]
    if len(window) < 10:
        result["note"] = f"only {len(window)} 1m bars — likely holiday/no data"
        return result

    candles = df_to_candles(window)
    result["bars"] = len(candles)

    wrapper = CHOCHFVGReplayWrapper()
    pending_entry_time = None
    pending_diag = None  # set on entry, consumed on exit, when TRACK_DIAGNOSTICS is on

    for i, candle in enumerate(candles):
        pt_time = candle.ts.astimezone(PT).time()
        is_last = (i == len(candles) - 1)

        was_in_trade = wrapper.engine.in_trade
        if TRACK_DIAGNOSTICS and was_in_trade and pending_diag is not None:
            if pending_diag["direction"] == "long":
                pending_diag["mfe"] = max(pending_diag["mfe"], candle.high)
            else:
                pending_diag["mfe"] = min(pending_diag["mfe"], candle.low)

        signal = wrapper.add_candle(candle, pt_time, is_last)
        if signal is None:
            continue

        if signal["type"] == "entry":
            pending_entry_time = pt_time.strftime("%H:%M")
            if TRACK_DIAGNOSTICS:
                bet = wrapper.engine.breakeven_trigger
                entry = signal["entry_price"]
                target = signal["target"]
                pending_diag = {
                    "direction": signal["direction"],
                    "entry": entry,
                    "target_dist": round(abs(target - entry), 4),
                    "be_dist": round(abs(bet - entry), 4) if bet is not None else None,
                    "mfe": entry,
                }
        elif signal["type"] == "exit":
            pnl = signal["pnl"]
            trade = {
                "entry_time": pending_entry_time or "?",
                "exit_time": pt_time.strftime("%H:%M"),
                "direction": signal["direction"],
                "entry_price": signal["entry_price"],
                "exit_price": round(signal["exit_price"], 2),
                "reason": signal["reason"],
                "pnl": pnl,
            }
            if TRACK_DIAGNOSTICS and pending_diag is not None:
                trade["target_dist"] = pending_diag["target_dist"]
                trade["be_dist"] = pending_diag["be_dist"]
                mfe_dist = abs(pending_diag["mfe"] - pending_diag["entry"])
                trade["mfe_dist"] = round(mfe_dist, 4)
                if pending_diag["be_dist"]:
                    trade["reached_be_trigger"] = mfe_dist >= pending_diag["be_dist"]
                    trade["be_dist_vs_target_ratio"] = round(pending_diag["be_dist"] / pending_diag["target_dist"], 2)
                else:
                    trade["reached_be_trigger"] = None
            result["trades"].append(trade)
            if pnl < 0:
                result["losses"] += 1
            elif pnl == 0:
                result["scratches"] += 1
            else:
                result["wins"] += 1
            pending_entry_time = None
            pending_diag = None

    result["pnl"] = round(sum(t["pnl"] for t in result["trades"]), 2)
    return result


def main():
    if len(sys.argv) < 4:
        print("Usage: python choch_databento_replay.py DATA_FILE START_DATE END_DATE")
        print("  DATA_FILE can be the .csv.zst or an already-decompressed .csv")
        return

    if POINT_VALUE != 50.0:
        raise SystemExit(f"ERROR: POINT_VALUE={POINT_VALUE}, expected 50.0 (ES). "
                          f"INSTRUMENT env var must not be overriding to MES.")

    data_file = sys.argv[1]
    start_date = date.fromisoformat(sys.argv[2])
    end_date = date.fromisoformat(sys.argv[3])

    csv_path = ensure_decompressed(data_file)

    load_start = pd.Timestamp(datetime.combine(start_date, SESSION_OPEN, tzinfo=PT)) - pd.Timedelta(days=1)
    load_end = pd.Timestamp(datetime.combine(end_date, REPLAY_WINDOW_END, tzinfo=PT)) + pd.Timedelta(days=1)
    load_start_utc = load_start.tz_convert("UTC")
    load_end_utc = load_end.tz_convert("UTC")

    print(f"Loading data from {load_start_utc} to {load_end_utc} (UTC)...")
    full_df = load_range(csv_path, load_start_utc, load_end_utc)
    if full_df.empty:
        print("No data found in that range. Check your dates are within the downloaded file's coverage.")
        return
    print(f"Loaded {len(full_df)} front-month-selected 1-minute bars.")

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
    scratches = sum(r["scratches"] for r in all_results)
    total_trades = wins + losses + scratches
    decisive_trades = wins + losses
    win_rate = (wins / decisive_trades * 100) if decisive_trades else 0.0
    avg_win = (sum(t["pnl"] for t in all_trades if t["pnl"] > 0) / wins) if wins else 0.0
    avg_loss = (sum(t["pnl"] for t in all_trades if t["pnl"] < 0) / losses) if losses else 0.0
    expectancy = (win_rate / 100 * avg_win) + ((1 - win_rate / 100) * avg_loss)

    print("\n" + "=" * 60)
    print(f"  CHOCH/FVG DATABENTO REPLAY SUMMARY — {start_date} to {end_date}")
    print("=" * 60)
    print(f"  RR_MULTIPLE            : {choch_fvg_bot.RR_MULTIPLE}")
    print(f"  Trading days replayed : {len(all_results)}")
    print(f"  Total trades           : {total_trades}")
    print(f"  Wins / Losses / Scratch: {wins} / {losses} / {scratches}")
    print(f"  Win rate (of decisive) : {win_rate:.1f}%")
    print(f"  Avg win / Avg loss     : ${avg_win:.2f} / ${avg_loss:.2f}")
    print(f"  Expectancy per trade   : ${expectancy:.2f}")
    print(f"  Total P&L              : ${total_pnl:.2f}")
    print(f"  Contracts              : {CONTRACTS}")
    print("=" * 60)

    reasons = {}
    for t in all_trades:
        reasons[t["reason"]] = reasons.get(t["reason"], 0) + 1
    if reasons:
        print("\n  EXIT REASON BREAKDOWN:")
        for reason, count in sorted(reasons.items(), key=lambda x: -x[1]):
            print(f"    {reason}: {count}")

    out = {
        "data_source": "databento",
        "strategy": "choch_fvg",
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "summary": {
            "trading_days_replayed": len(all_results),
            "total_trades": total_trades,
            "wins": wins, "losses": losses, "scratches": scratches,
            "win_rate_pct": round(win_rate, 2),
            "avg_win": round(avg_win, 2), "avg_loss": round(avg_loss, 2),
            "expectancy_per_trade": round(expectancy, 2),
            "total_pnl": round(total_pnl, 2),
        },
        "days": all_results,
    }
    rr_tag = f"_rr{choch_fvg_bot.RR_MULTIPLE:g}" if _rr_override else ""
    out_path = f"choch_replay_{start_date}_{end_date}{rr_tag}.json"
    with open(out_path, "w") as f:
        json.dump(out, f, indent=2, default=str)
    print(f"\nSaved full results to {out_path}")


if __name__ == "__main__":
    main()
