"""
Resting-limit-at-midpoint / confirm-at-close BACKTEST (offline, databento).

Models the PROPOSED live design over 1-minute bars:
  * The CHOCH engine runs normally and produces the CONFIRMED trades (its own
    _check_entry decision, filled at the FVG midpoint) — these are identical to
    the choch+VersionB baseline (choch_databento_replay.py).
  * ADDITIONALLY, each bar we rest a LIMIT at the eligible FVG midpoint computed
    from PRIOR-bar structure (compute_armed, same as the live bot). If a bar's
    range crosses that midpoint but the engine does NOT confirm an entry on that
    bar's close, that is a FALSE START: the limit filled intrabar on a setup the
    entry bar then invalidated (CHOCH flip / gap consumed / no straddle). The
    live design flattens it at the close.

False starts are the real, added cost of intrabar fills. They CANNOT be measured
precisely on 1-minute bars (sub-minute path invisible), so we report two bounds:
  * PESSIMISTIC: if the entry bar's range also reached the 1pt stop, assume the
    limit stopped out first (-1pt).
  * FLATTEN-AT-CLOSE: always exit the false start at the bar close.

Confirmed trades do NOT change between the two bounds (they are the baseline).
Only the false-start churn does.
"""
import os
os.environ.setdefault("INSTRUMENT", "ES")
import sys
from datetime import date, datetime, time, timedelta
import pandas as pd

sys.path.insert(0, "/mnt/user-data/uploads/topstep_bot")
from databento_loader import ensure_decompressed, load_range
from topstep_bot_v5 import Candle, PT, POINT_VALUE
import choch_fvg_bot
from choch_fvg_bot import (
    CHOCHFVGReplayWrapper, SESSION_OPEN, CONTRACTS, RR_MULTIPLE,
    MAX_TRADES_PER_DAY, MAX_CONSECUTIVE_LOSSES, DAILY_LOSS_LIMIT,
    BROKER_MIN_STOP_POINTS, MAX_STOP_POINTS, MIN_STOP_POINTS,
    ATR_MAX_THRESHOLD, FEE_PER_ROUND_TURN,
)

REPLAY_WINDOW_END = time(13, 15)
FEE = FEE_PER_ROUND_TURN * CONTRACTS


def version_b_passes(direction, close, vwap, ema9):
    if not choch_fvg_bot.VWAP_EMA_FILTER:
        return True
    if close is None or vwap is None or ema9 is None:
        return True
    if direction == "long":
        return not (close < ema9 and close < vwap)
    return not (close > ema9 and close > vwap)


def compute_armed(eng, last_close):
    """Freshest unconsumed FVG matching CHOCH bias, fib-valid, quality-passing,
    within the risk band — the exact selection the live bot arms a limit on."""
    active_fvgs = getattr(eng, "active_fvgs", None)
    choch_bias = getattr(eng, "choch_bias", None)
    if not active_fvgs or not choch_bias:
        return None
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
        if choch_fvg_bot.BREAKAWAY_ONLY and not getattr(fvg, "breakaway", False):
            continue
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
    mid = candidate.midpoint
    top, bot = candidate.top, candidate.bottom
    direction = "long" if choch_bias == "bullish" else "short"
    # retrace gate: last closed bar on correct side of midpoint
    if last_close is not None:
        if direction == "long" and last_close <= mid:
            return None
        if direction == "short" and last_close >= mid:
            return None
    entry = mid
    if direction == "long":
        stop = bot if bot is not None else entry - MIN_STOP_POINTS
        risk = entry - stop
    else:
        stop = top if top is not None else entry + MIN_STOP_POINTS
        risk = stop - entry
    if risk <= 0 or risk > MAX_STOP_POINTS or risk < MIN_STOP_POINTS:
        return None
    if choch_fvg_bot.FIXED_STOP_POINTS > 0:
        risk = choch_fvg_bot.FIXED_STOP_POINTS
        stop = entry - risk if direction == "long" else entry + risk
        broker_stop = risk
    else:
        broker_stop = max(risk, BROKER_MIN_STOP_POINTS)
        if broker_stop != risk:
            stop = entry - broker_stop if direction == "long" else entry + broker_stop
    if broker_stop * POINT_VALUE * CONTRACTS > choch_fvg_bot.MAX_TRADE_RISK_DOLLARS:
        return None
    target = entry + RR_MULTIPLE * risk if direction == "long" else entry - RR_MULTIPLE * risk
    return {"direction": direction, "midpoint": mid, "stop": stop, "target": target}


def df_to_candles(df):
    return [Candle(r["open"], r["high"], r["low"], r["close"], r["volume"], ts.to_pydatetime())
            for ts, r in df.iterrows()]


def daterange(s, e):
    d = s
    while d <= e:
        if d.weekday() < 5:
            yield d
        d += timedelta(days=1)


def replay_one_day(full_df, day):
    res = {"date": day.isoformat(), "confirmed": [], "false_starts": [],
           "conf_pnl": 0.0, "fs_pnl_pess": 0.0, "fs_pnl_flat": 0.0, "note": None}
    start = pd.Timestamp(datetime.combine(day, SESSION_OPEN, tzinfo=PT))
    end = pd.Timestamp(datetime.combine(day, REPLAY_WINDOW_END, tzinfo=PT))
    window = full_df.loc[start:end]
    if len(window) < 10:
        res["note"] = "no data"
        return res
    candles = df_to_candles(window)
    wrapper = CHOCHFVGReplayWrapper()
    eng = wrapper.engine
    trades_today = 0
    consec_losses = 0
    daily_pnl = 0.0
    pending = None  # confirmed trade between entry & exit
    pend_entry_time = None
    armed_fired_mid = None  # midpoint already filled this approach (dedupe)

    for i, candle in enumerate(candles):
        pt_time = candle.ts.astimezone(PT).time()
        is_last = (i == len(candles) - 1)
        last_close = candles[i - 1].close if i > 0 else None
        prior_vwap, prior_ema9 = eng.vwap, eng.ema9
        in_trade_at_open = eng.in_trade

        # limit resting at the armed midpoint going INTO this bar (prior structure)
        armed = compute_armed(eng, last_close) if not in_trade_at_open else None
        # dedupe: a resting limit fills once per approach. Clear the fired-flag
        # only once price has LEFT the midpoint (a bar whose range no longer
        # contains it), so it can re-fill when price genuinely returns.
        if armed is None or armed_fired_mid is None:
            pass
        elif not (candle.low <= armed_fired_mid <= candle.high):
            armed_fired_mid = None
        provisional = None
        if (armed is not None and candle.low <= armed["midpoint"] <= candle.high
                and armed["midpoint"] != armed_fired_mid):
            gates_ok = (trades_today < MAX_TRADES_PER_DAY
                        and consec_losses < MAX_CONSECUTIVE_LOSSES
                        and daily_pnl > -DAILY_LOSS_LIMIT)
            vb = version_b_passes(armed["direction"], last_close, prior_vwap, prior_ema9)
            if gates_ok and vb:
                provisional = armed

        signal = wrapper.add_candle(candle, pt_time, is_last)

        if signal and signal["type"] == "entry":
            vb_ok = version_b_passes(signal["direction"], candle.close, prior_vwap, prior_ema9)
            pending = {"direction": signal["direction"], "midpoint": signal["entry_price"], "vb_ok": vb_ok}
            pend_entry_time = pt_time.strftime("%H:%M")
            provisional = None  # this bar's fill IS the confirmed trade
            armed_fired_mid = signal["entry_price"]
        elif signal and signal["type"] == "exit":
            if pending and pending["vb_ok"]:
                direction = pending["direction"]
                mid = pending["midpoint"]
                exit_price = signal["exit_price"]
                move = (exit_price - mid) if direction == "long" else (mid - exit_price)
                pnl = round(move * POINT_VALUE * CONTRACTS - FEE, 2)
                res["confirmed"].append({"entry": pend_entry_time, "exit": pt_time.strftime("%H:%M"),
                                         "direction": direction, "pnl": pnl, "reason": signal["reason"]})
                trades_today += 1
                daily_pnl += pnl
                consec_losses = consec_losses + 1 if pnl < 0 else 0
            pending = None

        # false start: provisional filled but engine did NOT confirm on this bar
        if provisional is not None:
            direction = provisional["direction"]
            mid = provisional["midpoint"]
            stop = provisional["stop"]
            target = provisional["target"]
            # The limit fills with a LIVE 1pt/3pt bracket. Resolve it intrabar
            # through that bracket; if neither leg is touched, flatten at close
            # because the engine did not confirm the setup at this bar's close.
            if direction == "long":
                hit_stop = candle.low <= stop
                hit_target = candle.high >= target
            else:
                hit_stop = candle.high >= stop
                hit_target = candle.low <= target
            if hit_target and not hit_stop:
                exit_px, outcome = target, "target"
            elif hit_stop and not hit_target:
                exit_px, outcome = stop, "stop"
            elif hit_stop and hit_target:
                exit_px, outcome = stop, "ambiguous_stop_first"  # pessimistic
            else:
                exit_px, outcome = candle.close, "flatten_close"
            move = (exit_px - mid) if direction == "long" else (mid - exit_px)
            pnl = round(move * POINT_VALUE * CONTRACTS - FEE, 2)
            res["false_starts"].append({"time": pt_time.strftime("%H:%M"), "direction": direction,
                                        "outcome": outcome, "pnl": pnl})
            daily_pnl += pnl
            armed_fired_mid = mid  # don't re-fill until price leaves & returns

    res["conf_pnl"] = round(sum(t["pnl"] for t in res["confirmed"]), 2)
    res["fs_pnl"] = round(sum(f["pnl"] for f in res["false_starts"]), 2)
    return res


def main():
    data_file = "/mnt/user-data/uploads/topstep_bot/glbx-mdp3-20210715-20260714.ohlcv-1m.zip"
    start_date = date.fromisoformat(sys.argv[1]) if len(sys.argv) > 1 else date(2026, 1, 1)
    end_date = date.fromisoformat(sys.argv[2]) if len(sys.argv) > 2 else date(2026, 7, 14)
    csv_path = ensure_decompressed(data_file)
    ls = pd.Timestamp(datetime.combine(start_date, SESSION_OPEN, tzinfo=PT)) - pd.Timedelta(days=1)
    le = pd.Timestamp(datetime.combine(end_date, REPLAY_WINDOW_END, tzinfo=PT)) + pd.Timedelta(days=1)
    full_df = load_range(csv_path, ls.tz_convert("UTC"), le.tz_convert("UTC"))
    full_df.index = full_df.index.tz_convert(PT)

    results = []
    for day in daterange(start_date, end_date):
        r = replay_one_day(full_df, day)
        if r["note"]:
            continue
        results.append(r)

    conf = [t for r in results for t in r["confirmed"]]
    fs = [f for r in results for f in r["false_starts"]]
    conf_pnl = round(sum(t["pnl"] for t in conf), 2)
    conf_w = sum(1 for t in conf if t["pnl"] > 0)
    conf_l = sum(1 for t in conf if t["pnl"] < 0)
    fs_pnl = round(sum(f["pnl"] for f in fs), 2)
    from collections import Counter
    oc = Counter(f["outcome"] for f in fs)
    fs_fees = round(len(fs) * FEE, 2)

    print("=" * 64)
    print(f"  RESTING-LIMIT / CONFIRM-AT-CLOSE BACKTEST  {start_date} -> {end_date}")
    print(f"  CHOCH + Version B | {CONTRACTS} contracts | RR {RR_MULTIPLE}")
    print("=" * 64)
    print(f"  Trading days                : {len(results)}")
    print("  --- CONFIRMED trades (= choch+VersionB baseline) ---")
    print(f"  Confirmed trades            : {len(conf)}  ({conf_w}W / {conf_l}L, "
          f"{100*conf_w/max(1,conf_w+conf_l):.1f}% WR)")
    print(f"  Confirmed P&L               : ${conf_pnl:,.2f}")
    print("  --- FALSE STARTS (added intrabar-fill churn, real bracket) ---")
    print(f"  False starts                : {len(fs)}   (~{len(fs)/max(1,len(results)):.1f}/day)")
    print(f"    stop  / target / flat/ amb: {oc['stop']} / {oc['target']} / "
          f"{oc['flatten_close']} / {oc['ambiguous_stop_first']}")
    print(f"    of which commissions      : ${fs_fees:,.2f}")
    print(f"  False-start churn (net)     : ${fs_pnl:,.2f}")
    print("  --- NET ---")
    print(f"  NET (confirmed + churn)     : ${conf_pnl + fs_pnl:,.2f}")
    print("=" * 64)


if __name__ == "__main__":
    main()
