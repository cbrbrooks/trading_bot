"""
CHOCH/FVG Batch Replay — TopstepX API version
==============================================
Runs choch_fvg_bot.py's CHOCH/FVG strategy across a range of trading days
using LIVE TopstepX historical bar data (History/retrieveBars), instead of
the local Databento file. This is the TopstepX-API counterpart to
choch_databento_replay.py — useful for recent dates not yet in your
Databento export (that file only goes up to 2026-07-05), or to sanity-check
that TopstepX's own bar data produces the same signals as the Databento
backtest.

Imports CHOCHFVGReplayWrapper directly — the exact same class
choch_bot_live.py drives — so what this replays is exactly what the live
bot would have done on these days.

Usage:
    python choch_batch_replay.py 2026-07-01 2026-07-08
    python choch_batch_replay.py 2026-07-01              # end date defaults to today

Requires the same .env as choch_bot_live.py (TOPSTEP_USERNAME, TOPSTEP_API_KEY).
"""

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

from topstep_bot_v5 import Candle, PT, CT, BASE_URL, USERNAME, API_KEY, MES_SYMBOL, INSTRUMENT, POINT_VALUE
from choch_fvg_bot import CHOCHFVGReplayWrapper, SESSION_OPEN, FORCE_FLAT_TIME, CONTRACTS, RR_MULTIPLE

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


async def replay_one_day(session, token, contract_id, day: date) -> dict:
    result = {"date": day.isoformat(), "bars": 0, "trades": [], "pnl": 0.0,
              "wins": 0, "losses": 0, "scratches": 0, "note": None}

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

    for i, candle in enumerate(candles):
        pt_time = candle.ts.astimezone(PT).time()
        is_last = (i == len(candles) - 1)
        signal = wrapper.add_candle(candle, pt_time, is_last)
        if signal is None:
            continue

        if signal["type"] == "entry":
            pending_entry_time = pt_time.strftime("%H:%M")
        elif signal["type"] == "exit":
            pnl = signal["pnl"]
            result["trades"].append({
                "entry_time": pending_entry_time or "?",
                "exit_time": pt_time.strftime("%H:%M"),
                "direction": signal["direction"],
                "entry_price": signal["entry_price"],
                "exit_price": round(signal["exit_price"], 2),
                "reason": signal["reason"],
                "pnl": pnl,
            })
            if pnl < 0:
                result["losses"] += 1
            elif pnl == 0:
                result["scratches"] += 1
            else:
                result["wins"] += 1
            pending_entry_time = None

    result["pnl"] = round(sum(t["pnl"] for t in result["trades"]), 2)
    return result


async def main():
    if len(sys.argv) < 2:
        print("Usage: python choch_batch_replay.py START_DATE [END_DATE]")
        print("       dates in YYYY-MM-DD format")
        return

    start_date = date.fromisoformat(sys.argv[1])
    end_date = date.fromisoformat(sys.argv[2]) if len(sys.argv) > 2 else date.today()

    if not USERNAME or not API_KEY:
        raise SystemExit("ERROR: Set TOPSTEP_USERNAME and TOPSTEP_API_KEY in your .env file.")

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

        all_trades = [t for r in all_results for t in r["trades"]]
        total_pnl = sum(r["pnl"] for r in all_results)
        wins = sum(r["wins"] for r in all_results)
        losses = sum(r["losses"] for r in all_results)
        scratches = sum(r["scratches"] for r in all_results)
        total_trades = wins + losses + scratches
        decisive = wins + losses
        win_rate = (wins / decisive * 100) if decisive else 0.0
        avg_win = (sum(t["pnl"] for t in all_trades if t["pnl"] > 0) / wins) if wins else 0.0
        avg_loss = (sum(t["pnl"] for t in all_trades if t["pnl"] < 0) / losses) if losses else 0.0

        print("\n" + "=" * 60)
        print(f"  CHOCH/FVG TOPSTEPX BATCH REPLAY — {start_date} to {end_date}")
        print("=" * 60)
        print(f"  RR_MULTIPLE             : {RR_MULTIPLE}")
        print(f"  Trading days replayed    : {len(all_results)}")
        print(f"  Total trades             : {total_trades}")
        print(f"  Wins / Losses / Scratch  : {wins} / {losses} / {scratches}")
        print(f"  Win rate (of decisive)   : {win_rate:.1f}%")
        print(f"  Avg win / Avg loss       : ${avg_win:.2f} / ${avg_loss:.2f}")
        print(f"  Total P&L                : ${total_pnl:.2f}")
        print(f"  Contracts                : {CONTRACTS}")
        print("=" * 60)

        if all_trades:
            print("\n  ALL TRADES:")
            for r in all_results:
                for t in r["trades"]:
                    print(f"    {r['date']} {t['entry_time']}->{t['exit_time']} "
                          f"{t['direction'].upper()} @ {t['entry_price']} -> {t['exit_price']} "
                          f"| {t['reason']} | P&L=${t['pnl']:.2f}")

        out = {
            "data_source": "topstepx_api",
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "summary": {
                "trading_days_replayed": len(all_results),
                "total_trades": total_trades,
                "wins": wins, "losses": losses, "scratches": scratches,
                "win_rate_pct": round(win_rate, 2),
                "avg_win": round(avg_win, 2), "avg_loss": round(avg_loss, 2),
                "total_pnl": round(total_pnl, 2),
            },
            "days": all_results,
        }
        out_path = f"choch_batch_replay_{start_date}_{end_date}.json"
        with open(out_path, "w") as f:
            json.dump(out, f, indent=2, default=str)
        print(f"\nSaved full results to {out_path}")


if __name__ == "__main__":
    asyncio.run(main())
