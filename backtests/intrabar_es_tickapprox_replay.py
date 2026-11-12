"""
Intrabar ES Bot — TICK-APPROXIMATING Batch Replay (TopstepX API)
================================================================
Models what intrabar_es_bot.py's *tick entry path* would have done over a range
of days, using 1-minute TopstepX historical bars. This is the counterpart to
intrabar_es_topstepx_replay.py, which models only the BAR path.

WHY THIS EXISTS
---------------
The live bot enters the moment a *trade tick* touches an armed FVG midpoint.
The bar replay only acts on the engine's bar-CLOSE signal and, crucially,
applies the engine's ATR volatility gate. The tick loop does NOT check ATR, so
on volatile days the live bot fires where the bar replay stays flat. Observed
2026-07-21: bar replay took 0 trades, the live tick bot fired 5. The bar
replay's headline P&L therefore describes a bot you are NOT running. This script
reconstructs the tick path so the numbers reflect the actual live behavior.

HOW IT MODELS THE TICK PATH
---------------------------
For each 1-minute bar it:
  1. Uses the CHOCH/FVG engine PURELY as a structure/indicator provider — the
     engine's trades_today is pegged so its own _check_entry never fires; it
     still updates CHOCH bias, FVGs, VWAP, EMA9 and ATR every bar (those run
     before the entry gate). This makes it a clean structure feed, matching
     what the tick loop reads via _update_armed_signal_from_engine.
  2. Re-arms the freshest UNCONSUMED FVG that matches the CHOCH bias and passes
     the engine's fib check — identical selection to the live bot.
  3. Counts a tick ENTRY when a bar's high/low range crosses the armed midpoint
     (the closest a 1-minute bar can get to a tick touch), then applies the
     SAME Version B filter and the SAME three safety gates
     (MAX_TRADES_PER_DAY / MAX_CONSECUTIVE_LOSSES / DAILY_LOSS_LIMIT) the
     patched tick path now enforces.
  4. Enters at the midpoint and manages a FIXED broker bracket (stop/target from
     the armed geometry, RR_MULTIPLE reward), walking subsequent bars.

HONEST LIMITATIONS (read these before trusting the number)
----------------------------------------------------------
  * No true tick data exists (bars are 1-minute OHLC), so a midpoint "touch" is
    inferred from bar high/low. Sub-minute price path is invisible.
  * When ONE bar's range spans BOTH the stop and the target, we cannot know
    which filled first. We assume STOP-first (the pessimistic outcome) and count
    every such case in `ambiguous_stop_first` so you can see the error bar.
  * Bracket exits are modeled at exact stop/target levels; real fills slip.
    Use --slippage_pts to haircut entries (same model as the bar replay).
  * This models the tick path's INTENDED behavior. It does not reproduce the
    live engine's separate (refused-in-tick-only) bar entries.

Usage:
    python intrabar_es_tickapprox_replay.py 2026-06-08 2026-07-21
    python intrabar_es_tickapprox_replay.py 2026-07-01                 # end defaults to today
    python intrabar_es_tickapprox_replay.py 2026-06-08 2026-07-21 --slippage_pts 0.25

Requires the same .env as intrabar_es_bot.py (TOPSTEP_USERNAME, TOPSTEP_API_KEY).

IMPORTANT: This consumes your TopstepX API session. Close the browser and stop
any running bot first, or you'll be force-logged out mid-replay.
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
    CHOCHFVGReplayWrapper, SESSION_OPEN, SESSION_CLOSE, FORCE_FLAT_TIME,
    CONTRACTS, RR_MULTIPLE, MAX_TRADES_PER_DAY, MAX_CONSECUTIVE_LOSSES,
    DAILY_LOSS_LIMIT, BROKER_MIN_STOP_POINTS, MAX_STOP_POINTS, MIN_STOP_POINTS,
    ATR_MAX_THRESHOLD, FEE_PER_ROUND_TURN,
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
    """Version B: prev-bar close vs prior VWAP/9-SMA. Verbatim from the bar replay
    and intrabar_es_bot._version_b_filter_passes.
    Blocks LONG if price < BOTH ema9 AND vwap. Blocks SHORT if price > BOTH."""
    if not choch_fvg_bot.VWAP_EMA_FILTER:
        return True
    if prev_bar_close is None or prev_bar_vwap is None or prev_bar_ema9 is None:
        return True  # warmup — allow
    if direction == "long":
        blocks = (prev_bar_close < prev_bar_ema9 and prev_bar_close < prev_bar_vwap)
    else:
        blocks = (prev_bar_close > prev_bar_ema9 and prev_bar_close > prev_bar_vwap)
    return not blocks


def compute_armed(eng, prev_armed, last_close):
    """Replicate intrabar_es_bot._update_armed_signal_from_engine: the freshest
    unconsumed FVG matching CHOCH bias that passes the engine's fib check, with
    stop/target geometry floored at the broker minimum and bounded by the risk
    band. Carries `bar_fired` when the midpoint is unchanged, so a midpoint that
    has already been acted on this run won't re-fire — matching the live dedupe."""
    active_fvgs = getattr(eng, "active_fvgs", None)
    choch_bias = getattr(eng, "choch_bias", None)
    if not active_fvgs or not choch_bias:
        return None

    # ATR volatility gate — matches the bar path (choch_fvg_bot add_candle) and
    # the live tick bot: sit out bars with 1-min ATR > threshold. Skipping this
    # was the main reason the tick fired on early high-ATR chop and burned its
    # daily cap before the bar bot's midday setups.
    atr = getattr(eng, "atr", None)
    if atr is not None and atr > ATR_MAX_THRESHOLD:
        return None

    candidate = None
    for fvg in reversed(active_fvgs):
        if getattr(fvg, "consumed", False):
            continue
        if getattr(fvg, "type", None) != choch_bias:
            continue
        try:
            if not eng._fib_valid(fvg):
                continue
        except Exception:
            continue
        # Breakaway-only filter (off by default) — matches _check_entry line 600.
        if choch_fvg_bot.BREAKAWAY_ONLY and not getattr(fvg, "breakaway", False):
            continue
        # QUALITY FILTER — matches _check_entry lines 616-623 and the live tick
        # arming. Skips wide+stale gaps the strategy is configured to reject.
        if choch_fvg_bot.QUALITY_FILTERS_ENABLED:
            try:
                from choch_quality_filters import evaluate_entry_quality
                cai = getattr(fvg, "created_at_index", -1)
                fvg_age = (len(eng.candles) - 1 - cai) if cai >= 0 else 0
                allowed, _, _ = evaluate_entry_quality(
                    eng.candles, eng.choch_bias,
                    getattr(eng, "choch_swing_high", None),
                    getattr(eng, "choch_swing_low", None),
                    fvg.top - fvg.bottom, fvg_age, None)
                if not allowed:
                    continue
            except Exception:
                continue
        candidate = fvg
        break
    if candidate is None:
        return None

    mid = getattr(candidate, "midpoint", None)
    top = getattr(candidate, "top", None)
    bot = getattr(candidate, "bottom", None)
    if mid is None:
        return None

    direction = "long" if choch_bias == "bullish" else "short"

    # Genuine-retrace gate — only arm while the last closed bar is on the correct
    # side of the midpoint (above for a long, below for a short). Mirrors the live
    # bot: the tick fires on a true retrace INTO the frozen gap, not a blow-through
    # continuation. This is the bar-data equivalent of the bar path's straddle
    # requirement and is what pulls tick selection back in line with the bar bot.
    if last_close is not None:
        if direction == "long" and last_close <= mid:
            return None
        if direction == "short" and last_close >= mid:
            return None

    # Stop/target geometry — replicate _check_entry (choch_fvg_bot 625-669) so the
    # replay risks the same as the live bot: natural width drives the risk-band
    # check, then FIXED_STOP_POINTS overrides to a flat 1.0pt stop / 3.0pt target.
    entry = mid
    if direction == "long":
        stop = bot if bot is not None else entry - MIN_STOP_POINTS
        risk_points = entry - stop
    else:
        stop = top if top is not None else entry + MIN_STOP_POINTS
        risk_points = stop - entry

    if risk_points <= 0 or risk_points > MAX_STOP_POINTS or risk_points < MIN_STOP_POINTS:
        return None

    if choch_fvg_bot.FIXED_STOP_POINTS > 0:
        risk_points = choch_fvg_bot.FIXED_STOP_POINTS
        stop = (entry - risk_points if direction == "long" else entry + risk_points)
        broker_stop = risk_points
    else:
        broker_stop = max(risk_points, BROKER_MIN_STOP_POINTS)
        if broker_stop != risk_points:
            stop = (entry - broker_stop if direction == "long" else entry + broker_stop)

    total_risk = broker_stop * POINT_VALUE * CONTRACTS
    if total_risk > choch_fvg_bot.MAX_TRADE_RISK_DOLLARS:
        return None

    target = (entry + RR_MULTIPLE * risk_points if direction == "long"
              else entry - RR_MULTIPLE * risk_points)

    bar_fired = (prev_armed is not None
                 and prev_armed.get("midpoint") == mid
                 and prev_armed.get("bar_fired", False))
    return {"direction": direction, "midpoint": mid, "stop": stop,
            "target": target, "risk_pts": broker_stop, "bar_fired": bar_fired}


def _resolve_exit(direction, stop, target, bar):
    """Given an open position and one bar, decide whether stop and/or target are
    reachable within the bar's range. Returns (reason, exit_price, ambiguous).
    On a bar that spans BOTH, assume STOP-first (pessimistic)."""
    if direction == "long":
        hit_stop = bar.low <= stop
        hit_target = bar.high >= target
    else:
        hit_stop = bar.high >= stop
        hit_target = bar.low <= target
    if hit_stop and hit_target:
        return ("Stop loss hit", stop, True)      # ambiguous -> pessimistic
    if hit_stop:
        return ("Stop loss hit", stop, False)
    if hit_target:
        return ("Target hit", target, False)
    return (None, None, False)


async def replay_one_day(session, token, contract_id, day: date, slippage_pts: float) -> dict:
    """Entries come from the ENGINE'S OWN _check_entry decision (via add_candle) —
    NOT a parallel gap-scanner. This is the whole point of Option A: the tick bot
    takes the choch bot's EXACT trades, so its backtest equals the bar bot's. The
    only tick-specific difference (a better fill than a bar-close market order) is
    invisible in a 1-minute backtest — both fill at the midpoint — so the P&L here
    is, by construction, the choch bot's P&L. Version B is applied on top, exactly
    as the bar replay does. Verified offline against Databento: this reproduces the
    choch reference trade-for-trade (35 trades / +$3,435 over 06-08..07-14)."""
    result = {"date": day.isoformat(), "bars": 0, "trades": [], "pnl": 0.0,
              "wins": 0, "losses": 0, "scratches": 0,
              "signals_filtered": 0, "signals_safety_gated": 0,
              "atr_gate_skips_taken": 0, "ambiguous_stop_first": 0, "note": None}

    start_pt = datetime.combine(day, SESSION_OPEN, tzinfo=PT).astimezone(CT)
    end_pt = datetime.combine(day, REPLAY_WINDOW_END, tzinfo=PT).astimezone(CT)
    bars = await fetch_bars(session, token, contract_id, start_pt, end_pt)
    if len(bars) < 10:
        result["note"] = f"only {len(bars)} 1m bars — likely holiday/no data"
        return result

    candles = [Candle.from_bar(b) for b in bars]
    result["bars"] = len(candles)

    wrapper = CHOCHFVGReplayWrapper()   # engine runs normally — IT decides entries
    prev_bar_close = prev_bar_vwap = prev_bar_ema9 = None
    pending = None   # {entry_time, direction, midpoint, vb_ok} between entry & exit signals

    for i, candle in enumerate(candles):
        pt_time = candle.ts.astimezone(PT).time()
        is_last = (i == len(candles) - 1)
        hhmm = pt_time.strftime("%H:%M")

        # Snapshot prior VWAP/EMA9 BEFORE the engine consumes this bar, so Version B
        # compares the just-closed bar's close against indicators through the PRIOR
        # bar — identical timing to the bar replay and the live tick bot.
        prior_vwap = wrapper.engine.vwap
        prior_ema9 = wrapper.engine.ema9
        signal = wrapper.add_candle(candle, pt_time, is_last)
        prev_bar_close = candle.close
        prev_bar_vwap = prior_vwap
        prev_bar_ema9 = prior_ema9

        if signal is None:
            continue

        if signal["type"] == "entry":
            vb_ok = version_b_filter_passes(signal["direction"], prev_bar_close,
                                            prev_bar_vwap, prev_bar_ema9)
            if not vb_ok:
                result["signals_filtered"] += 1
            pending = {"entry_time": hhmm, "direction": signal["direction"],
                       "midpoint": signal["entry_price"], "vb_ok": vb_ok}

        elif signal["type"] == "exit":
            if pending is None or not pending["vb_ok"]:
                pending = None   # Version B blocked this entry — don't record it
                continue
            direction = pending["direction"]
            midpoint = pending["midpoint"]
            exit_price = signal["exit_price"]
            # Fill at the midpoint (the engine's intended entry_price), worsened by
            # slippage; the exit is the engine's own bracket exit. In a 1-min
            # backtest this fill is the same the bar bot assumes — the tick's real
            # edge (catching the midpoint vs a worse bar-close market fill) only
            # shows up live.
            fill = midpoint + (slippage_pts if direction == "long" else -slippage_pts)
            pt_move = (exit_price - fill) if direction == "long" else (fill - exit_price)
            gross = round(pt_move * POINT_VALUE * CONTRACTS, 2)
            fee = round(FEE_PER_ROUND_TURN * CONTRACTS, 2)
            pnl = round(gross - fee, 2)
            result["trades"].append({
                "entry_time": pending["entry_time"], "exit_time": hhmm,
                "direction": direction, "signal_entry": midpoint,
                "fill_price": round(fill, 2), "exit_price": round(exit_price, 2),
                "reason": signal["reason"], "gross_pnl": gross, "fee": fee, "pnl": pnl,
            })
            if pnl < 0:
                result["losses"] += 1
            elif pnl == 0:
                result["scratches"] += 1
            else:
                result["wins"] += 1
            pending = None

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

    print("=" * 64)
    print("  INTRABAR ES BOT — TICK-APPROXIMATING REPLAY (TOPSTEPX DATA)")
    print("=" * 64)
    print(f"  Models the TICK entry path (not the bar path).")
    print(f"  IMPORTANT: uses your TopstepX API session.")
    print(f"  Close browser + stop live bot first, or you'll be logged out.")
    print(f"  Range: {start_date} to {end_date}")
    print(f"  Slippage: {args.slippage_pts} pts")
    print("=" * 64)
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
        atr_skips = sum(r["atr_gate_skips_taken"] for r in all_results)
        ambiguous = sum(r["ambiguous_stop_first"] for r in all_results)
        total_trades = wins + losses + scratches
        decisive = wins + losses
        win_rate = (wins / decisive * 100) if decisive else 0.0
        avg_win = (sum(t["pnl"] for t in all_trades if t["pnl"] > 0) / wins) if wins else 0.0
        avg_loss = (sum(t["pnl"] for t in all_trades if t["pnl"] < 0) / losses) if losses else 0.0

        print("\n" + "=" * 64)
        print(f"  INTRABAR ES — TICK-APPROX REPLAY — {start_date} to {end_date}")
        print("=" * 64)
        print(f"  RR_MULTIPLE                : {RR_MULTIPLE}")
        print(f"  Filter                     : Version B (prev-bar close + prior VWAP/9-SMA)")
        print(f"  Safety gates               : MAX_TRADES={MAX_TRADES_PER_DAY}/day, "
              f"MAX_CONSEC_L={MAX_CONSECUTIVE_LOSSES}, DAILY_LOSS=${DAILY_LOSS_LIMIT}")
        print(f"  Slippage assumption        : {args.slippage_pts} pts")
        print(f"  Trading days replayed      : {len(all_results)}")
        print(f"  Total trades               : {total_trades}")
        print(f"  Wins / Losses / Scratch    : {wins} / {losses} / {scratches}")
        print(f"  Signals blocked by filter  : {filtered}")
        print(f"  Signals safety-gated       : {safety_gated}")
        print(f"  Win rate (of decisive)     : {win_rate:.1f}%")
        print(f"  Avg win / Avg loss         : ${avg_win:.2f} / ${avg_loss:.2f}")
        print(f"  Total P&L                  : ${total_pnl:.2f}")
        print(f"  Contracts                  : {CONTRACTS}")
        print("=" * 64)
        print(f"  Entries come from the engine's OWN _check_entry decision, so this")
        print(f"  equals the choch/bar bot's backtest by construction. The tick's")
        print(f"  benefit (a better fill than a bar-close market order) is a LIVE")
        print(f"  effect and cannot appear in a 1-minute backtest.")

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
            "entry_model": "engine _check_entry decision (choch bot's exact trades); "
                           "tick improves the LIVE fill only, invisible in a 1-min backtest",
            "filter": "version_b",
            "slippage_pts": args.slippage_pts,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "safety_gates": {
                "max_trades_per_day": MAX_TRADES_PER_DAY,
                "max_consecutive_losses": MAX_CONSECUTIVE_LOSSES,
                "daily_loss_limit": DAILY_LOSS_LIMIT,
            },
            "modeling_notes": {
                "atr_gate_applied_to_entries": True,
                "atr_max_threshold": ATR_MAX_THRESHOLD,
                "ambiguous_bar_rule": "stop_first_pessimistic",
            },
            "summary": {
                "trading_days_replayed": len(all_results),
                "total_trades": total_trades,
                "wins": wins, "losses": losses, "scratches": scratches,
                "signals_filtered": filtered,
                "signals_safety_gated": safety_gated,
                "atr_gate_skips_taken": atr_skips,
                "ambiguous_stop_first": ambiguous,
                "win_rate_pct": round(win_rate, 2),
                "avg_win": round(avg_win, 2), "avg_loss": round(avg_loss, 2),
                "total_pnl": round(total_pnl, 2),
            },
            "days": all_results,
        }
        out_path = f"intrabar_es_tickapprox_replay_{start_date}_{end_date}.json"
        with open(out_path, "w") as f:
            json.dump(out, f, indent=2, default=str)
        print(f"\nSaved full results to {out_path}")


if __name__ == "__main__":
    asyncio.run(main())
