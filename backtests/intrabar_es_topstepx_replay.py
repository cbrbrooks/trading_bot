"""
Intrabar ES Bot Batch Replay — TopstepX API version
====================================================
Runs intrabar_es_bot.py's Version B filter + safety-gate logic across a range
of trading days using LIVE TopstepX historical bar data (History/retrieveBars),
instead of the local Databento file. This is the intrabar-bot counterpart to
choch_batch_replay.py.

DIFFERENCES FROM choch_batch_replay.py:
  * Applies Version B filter (prev-bar close + prior VWAP/9-SMA)
  * Enforces intrabar bot's safety gates: MAX_TRADES_PER_DAY=3,
    MAX_CONSECUTIVE_LOSSES=2, DAILY_LOSS_LIMIT=$230
  * Enforces BROKER_MIN_STOP_POINTS = 1.0 (broker's 4-tick minimum)
  * Reports what intrabar_es_bot would have decided over the same window,
    with the same code paths (minus the SignalR tick loop — this is 1-min OHLC)

Usage:
    python intrabar_es_topstepx_replay.py 2026-01-01 2026-07-14
    python intrabar_es_topstepx_replay.py 2026-07-01                # end defaults to today
    python intrabar_es_topstepx_replay.py 2026-01-01 2026-07-14 --slippage_pts 0.25

Requires the same .env as intrabar_es_bot.py (TOPSTEP_USERNAME, TOPSTEP_API_KEY).

IMPORTANT: This consumes your TopstepX API session. Close the browser and
stop any running bots first, or you'll get force-logged out mid-replay.
"""
# --- path bootstrap (repo reorganized into folders) ---
import sys as _sys, pathlib as _pathlib
_ROOT = _pathlib.Path(__file__).resolve().parents[1]
_sys.path[:0] = [str(_ROOT / _d) for _d in ("bot", "backtests", "research")]
# ---------------------------------------------------------

import argparse
import asyncio
import json
import os
import sys
from datetime import date, datetime, time, timedelta

# Force ES regardless of .env — this strategy's risk constants are ES-specific.
# Must happen before topstep_bot_v5 is imported.
os.environ["INSTRUMENT"] = "ES"

import aiohttp
from dotenv import load_dotenv
load_dotenv()

from topstep_bot_v5 import Candle, PT, CT, BASE_URL, USERNAME, API_KEY, INSTRUMENT, POINT_VALUE
import choch_fvg_bot
from choch_fvg_bot import (
    CHOCHFVGReplayWrapper, SESSION_OPEN, FORCE_FLAT_TIME, CONTRACTS, RR_MULTIPLE,
    MAX_TRADES_PER_DAY, MAX_CONSECUTIVE_LOSSES, DAILY_LOSS_LIMIT,
    BROKER_MIN_STOP_POINTS, FEE_PER_ROUND_TURN,
)

if POINT_VALUE != 50.0 or INSTRUMENT != "ES":
    raise SystemExit(f"ERROR: INSTRUMENT={INSTRUMENT!r}, POINT_VALUE={POINT_VALUE} — expected "
                      f"ES/50.0. This should be impossible since INSTRUMENT is force-set above.")

REPLAY_WINDOW_END = time(13, 15)   # small buffer past the 1:10 PM PT forced-flat


async def fetch_bars(session, token, contract_id, start, end):
    resp = await session.post(
        f"{BASE_URL}/History/retrieveBars",
        headers={"Authorization": f"Bearer {token}"},
        json={"contractId": contract_id, "live": False,
              "unit": 2, "unitNumber": 1, "limit": 500,
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


def version_b_filter_passes(direction, prev_bar_close, prev_bar_vwap, prev_bar_ema9):
    """Version B: prev-bar close vs prior VWAP/9-SMA.
    Blocks LONG if price < BOTH ema9 AND vwap.
    Blocks SHORT if price > BOTH ema9 AND vwap.
    """
    if not choch_fvg_bot.VWAP_EMA_FILTER:
        return True
    if prev_bar_close is None or prev_bar_vwap is None or prev_bar_ema9 is None:
        return True  # warmup — allow
    if direction == "long":
        blocks = (prev_bar_close < prev_bar_ema9 and prev_bar_close < prev_bar_vwap)
    else:
        blocks = (prev_bar_close > prev_bar_ema9 and prev_bar_close > prev_bar_vwap)
    return not blocks


async def replay_one_day(session, token, contract_id, day: date, slippage_pts: float) -> dict:
    result = {"date": day.isoformat(), "bars": 0, "trades": [], "pnl": 0.0,
              "wins": 0, "losses": 0, "scratches": 0,
              "signals_filtered": 0, "signals_safety_gated": 0, "note": None}

    start_pt = datetime.combine(day, SESSION_OPEN, tzinfo=PT).astimezone(CT)
    end_pt = datetime.combine(day, REPLAY_WINDOW_END, tzinfo=PT).astimezone(CT)
    bars = await fetch_bars(session, token, contract_id, start_pt, end_pt)
    if len(bars) < 10:
        result["note"] = f"only {len(bars)} 1m bars — likely holiday/no data"
        return result

    candles = [Candle.from_bar(b) for b in bars]
    result["bars"] = len(candles)

    wrapper = CHOCHFVGReplayWrapper()
    pending_entry_time = None

    # Version B filter state (updates each bar)
    prev_bar_close = None
    prev_bar_vwap = None
    prev_bar_ema9 = None

    # Safety-gate daily state
    trades_today = 0
    consecutive_losses = 0
    daily_pnl = 0.0

    for i, candle in enumerate(candles):
        pt_time = candle.ts.astimezone(PT).time()
        is_last = (i == len(candles) - 1)

        # Snapshot PRIOR-bar values BEFORE engine consumes this bar
        prior_vwap = wrapper.engine.vwap
        prior_ema9 = wrapper.engine.ema9

        signal = wrapper.add_candle(candle, pt_time, is_last)

        # After engine consumed this bar, update prev-bar state for next iteration
        prev_bar_close = candle.close
        prev_bar_vwap = prior_vwap
        prev_bar_ema9 = prior_ema9

        if signal is None:
            continue

        if signal["type"] == "entry":
            # Safety gates first — same as intrabar_es_bot._safety_gates_pass
            if trades_today >= MAX_TRADES_PER_DAY:
                result["signals_safety_gated"] += 1
                continue
            if consecutive_losses >= MAX_CONSECUTIVE_LOSSES:
                result["signals_safety_gated"] += 1
                continue
            if daily_pnl <= -abs(DAILY_LOSS_LIMIT):
                result["signals_safety_gated"] += 1
                continue

            # Version B filter check
            if not version_b_filter_passes(signal["direction"], prev_bar_close,
                                            prev_bar_vwap, prev_bar_ema9):
                result["signals_filtered"] += 1
                continue

            pending_entry_time = pt_time.strftime("%H:%M")

        elif signal["type"] == "exit":
            # The engine's exit calc uses signal_entry as the reference. If we
            # applied slippage on entry, PnL needs to be recomputed at the
            # actual fill price.
            direction = signal["direction"]
            signal_entry = signal["entry_price"]
            exit_price = signal["exit_price"]

            # Apply slippage: worse fill on entry (bracket levels are
            # fill-relative on TopstepX brackets, so bracket adjusts, but for
            # this bar-based OHLC replay we just approximate with fixed slippage)
            fill_price = signal_entry + (slippage_pts if direction == "long"
                                          else -slippage_pts)

            if direction == "long":
                pt_move = exit_price - fill_price
            else:
                pt_move = fill_price - exit_price
            gross = round(pt_move * POINT_VALUE * CONTRACTS, 2)
            fee = round(FEE_PER_ROUND_TURN * CONTRACTS, 2)
            pnl = round(gross - fee, 2)

            result["trades"].append({
                "entry_time": pending_entry_time or "?",
                "exit_time": pt_time.strftime("%H:%M"),
                "direction": direction,
                "signal_entry": signal_entry,
                "fill_price": round(fill_price, 2),
                "exit_price": round(exit_price, 2),
                "reason": signal["reason"],
                "gross_pnl": gross,
                "fee": fee,
                "pnl": pnl,
            })
            if pnl < 0:
                result["losses"] += 1
                consecutive_losses += 1
            elif pnl == 0:
                result["scratches"] += 1
            else:
                result["wins"] += 1
                consecutive_losses = 0
            trades_today += 1
            daily_pnl += pnl
            pending_entry_time = None

    result["pnl"] = round(sum(t["pnl"] for t in result["trades"]), 2)
    return result


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("start", help="Start date YYYY-MM-DD")
    parser.add_argument("end", nargs="?", help="End date YYYY-MM-DD (default: today)")
    parser.add_argument("--slippage_pts", type=float, default=0.0,
                        help="Adverse execution slippage in points (default 0)")
    args = parser.parse_args()

    start_date = date.fromisoformat(args.start)
    end_date = date.fromisoformat(args.end) if args.end else date.today()

    if not USERNAME or not API_KEY:
        raise SystemExit("ERROR: Set TOPSTEP_USERNAME and TOPSTEP_API_KEY in your .env file.")

    print("=" * 60)
    print("  INTRABAR ES BOT REPLAY — TOPSTEPX HISTORICAL DATA")
    print("=" * 60)
    print(f"  IMPORTANT: uses your TopstepX API session.")
    print(f"  Close browser + stop live bot first, or you'll be logged out.")
    print(f"  Range: {start_date} to {end_date}")
    print(f"  Slippage: {args.slippage_pts} pts")
    print("=" * 60)
    print()

    timeout = aiohttp.ClientTimeout(total=20)
    async with aiohttp.ClientSession(timeout=timeout) as session:
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
        # Prefer ES contracts (not MES). Take the alphabetically first ES that matches.
        es_matches = [c for c in contracts
                      if c.get("name", "").startswith("ES") and "MES" not in c.get("name", "")]
        if not es_matches:
            print(f"No ES contract found. Available: {[c.get('name') for c in contracts]}")
            return
        contract = sorted(es_matches, key=lambda c: c.get("name", ""))[0]
        contract_id = contract["id"]
        print(f"Contract: {contract['name']} ({contract_id})\n")

        all_results = []
        for day in daterange(start_date, end_date):
            print(f"Replaying {day} ...", end=" ")
            try:
                r = await replay_one_day(session, token, contract_id, day, args.slippage_pts)
            except Exception as e:
                print(f"ERROR: {e}")
                continue
            if r["note"]:
                print(r["note"])
                continue
            print(f"{len(r['trades'])} trades, P&L ${r['pnl']:.2f}")
            all_results.append(r)
            await asyncio.sleep(0.7)  # be polite to the API (50 req / 30 sec limit)

        all_trades = [t for r in all_results for t in r["trades"]]
        total_pnl = sum(r["pnl"] for r in all_results)
        wins = sum(r["wins"] for r in all_results)
        losses = sum(r["losses"] for r in all_results)
        scratches = sum(r["scratches"] for r in all_results)
        filtered = sum(r["signals_filtered"] for r in all_results)
        safety_gated = sum(r["signals_safety_gated"] for r in all_results)
        total_trades = wins + losses + scratches
        decisive = wins + losses
        win_rate = (wins / decisive * 100) if decisive else 0.0
        avg_win = (sum(t["pnl"] for t in all_trades if t["pnl"] > 0) / wins) if wins else 0.0
        avg_loss = (sum(t["pnl"] for t in all_trades if t["pnl"] < 0) / losses) if losses else 0.0

        print("\n" + "=" * 60)
        print(f"  INTRABAR ES BOT TOPSTEPX BATCH REPLAY — {start_date} to {end_date}")
        print("=" * 60)
        print(f"  RR_MULTIPLE               : {RR_MULTIPLE}")
        print(f"  Filter                    : Version B (prev-bar close + prior VWAP/9-SMA)")
        print(f"  Safety gates              : MAX_TRADES={MAX_TRADES_PER_DAY}/day, "
              f"MAX_CONSEC_L={MAX_CONSECUTIVE_LOSSES}, DAILY_LOSS=${DAILY_LOSS_LIMIT}")
        print(f"  Slippage assumption       : {args.slippage_pts} pts")
        print(f"  Trading days replayed     : {len(all_results)}")
        print(f"  Total trades              : {total_trades}")
        print(f"  Wins / Losses / Scratch   : {wins} / {losses} / {scratches}")
        print(f"  Signals blocked by filter : {filtered}")
        print(f"  Signals safety-gated      : {safety_gated}")
        print(f"  Win rate (of decisive)    : {win_rate:.1f}%")
        print(f"  Avg win / Avg loss        : ${avg_win:.2f} / ${avg_loss:.2f}")
        print(f"  Total P&L                 : ${total_pnl:.2f}")
        print(f"  Contracts                 : {CONTRACTS}")
        print("=" * 60)

        if all_trades:
            print("\n  ALL TRADES:")
            for r in all_results:
                for t in r["trades"]:
                    print(f"    {r['date']} {t['entry_time']}->{t['exit_time']} "
                          f"{t['direction'].upper()} @ {t['fill_price']} -> {t['exit_price']} "
                          f"| {t['reason']} | P&L=${t['pnl']:.2f}")

        out = {
            "data_source": "topstepx_api",
            "bot_variant": "intrabar_es_bot",
            "filter": "version_b",
            "slippage_pts": args.slippage_pts,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "safety_gates": {
                "max_trades_per_day": MAX_TRADES_PER_DAY,
                "max_consecutive_losses": MAX_CONSECUTIVE_LOSSES,
                "daily_loss_limit": DAILY_LOSS_LIMIT,
            },
            "summary": {
                "trading_days_replayed": len(all_results),
                "total_trades": total_trades,
                "wins": wins, "losses": losses, "scratches": scratches,
                "signals_filtered": filtered,
                "signals_safety_gated": safety_gated,
                "win_rate_pct": round(win_rate, 2),
                "avg_win": round(avg_win, 2), "avg_loss": round(avg_loss, 2),
                "total_pnl": round(total_pnl, 2),
            },
            "days": all_results,
        }
        out_path = f"intrabar_es_topstepx_replay_{start_date}_{end_date}.json"
        with open(out_path, "w") as f:
            json.dump(out, f, indent=2, default=str)
        print(f"\nSaved full results to {out_path}")


if __name__ == "__main__":
    asyncio.run(main())
