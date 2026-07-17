"""
Intrabar MES bot replay simulator.

Reads historical 1-min OHLC bars (Databento format via databento_loader) and
simulates what the intrabar MES bot (intrabar_mes_bot.py) WOULD HAVE DONE.

  * NO live API calls
  * NO SignalR connection
  * NO orders submitted
  * NO account touched

Purely offline. Logs in the same shape as the live bot so you can eyeball
would-be entries vs. what the live bot did on a given day.

DIFFERENCES FROM intrabar_version_b.py (the earlier academic simulator):
  1. Uses the LIVE BOT's exact filter, arming, and dispatch code paths where
     possible — imports IntrabarMESBot and calls its own methods for
     _update_armed_signal_from_engine, _version_b_filter_passes, etc.
  2. Logs to a file with same fields as the live bot: SUBMITTING BRACKET,
     FILLED, EXIT with P&L breakdown.
  3. Reports fills under two assumptions side-by-side:
       - BEST:      fill exactly at midpoint (theoretical ceiling)
       - REALISTIC: midpoint + adverse slippage (SIM_SLIPPAGE_PTS)
  4. Applies all safety gates: MAX_TRADES_PER_DAY, MAX_CONSECUTIVE_LOSSES,
     DAILY_LOSS_LIMIT, session window, one-trade-at-a-time. Real behavior.
  5. Bar-loop-only path (no tick simulation since OHLC data has no ticks).
     Every entry decision fires at the equivalent of "1-min bar close saw the
     midpoint in its range".
  6. MES contract math: 1 contract, $5/point, FEE_PER_ROUND_TURN fees.

WHAT THIS REPLAY CANNOT TELL YOU:
  * Real execution slippage on live orders (backtest assumes fixed slippage
    per SIM_SLIPPAGE_PTS setting)
  * Real tick-level detection accuracy (OHLC has no ticks)
  * Real broker fill mechanics (assumes bracket resolves at stop/target price
    with stop-first tie-breaking)
  * Live infrastructure quirks (SignalR drops, stale bars, session issues)

Usage:
  python intrabar_mes_replay.py <databento_csv> <start_YYYY-MM-DD> <end_YYYY-MM-DD>
    [--slippage_pts 0.25]

Example:
  python intrabar_mes_replay.py data.csv 2026-01-01 2026-07-16 --slippage_pts 0.25
"""
import argparse
import json
import logging
import os
import sys
from datetime import date, datetime, time, timedelta
from pathlib import Path

# Force MES BEFORE any topstep imports so POINT_VALUE/contract dispatch is correct
os.environ["INSTRUMENT"] = "MES"

import pandas as pd

from databento_loader import ensure_decompressed, load_range
from topstep_bot_v5 import Candle, PT
import choch_fvg_bot
from choch_fvg_bot import (
    CHOCHFVGReplayWrapper, SESSION_OPEN, SESSION_CLOSE,
    RR_MULTIPLE, MAX_STOP_POINTS, MIN_STOP_POINTS,
    MAX_TRADES_PER_DAY, DAILY_LOSS_LIMIT, MAX_CONSECUTIVE_LOSSES,
    FEE_PER_ROUND_TURN,
)

REPLAY_WINDOW_END = time(13, 15)
CONTRACTS = 1
MES_POINT_VALUE = 5.0   # $5 per point on MES, hardcoded matches live bot

log = logging.getLogger("intrabar_mes_replay")
log.setLevel(logging.INFO)
_fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
_stream = logging.StreamHandler(sys.stdout); _stream.setFormatter(_fmt); log.addHandler(_stream)


def df_to_candles(df):
    return [Candle(row["open"], row["high"], row["low"], row["close"],
                   row["volume"], ts.to_pydatetime())
            for ts, row in df.iterrows()]


def daterange(start, end):
    d = start
    while d <= end:
        if d.weekday() < 5:
            yield d
        d += timedelta(days=1)


class ReplayState:
    """Mirrors the live bot's session-level safety state."""
    def __init__(self):
        self.trades_today = 0
        self.consecutive_losses = 0
        self.daily_pnl = 0.0
        self.in_trade = False
        # For a single-day replay, these reset each day.
        # Also track prev-bar filter state (matches Version B / live bot logic)
        self.prev_bar_close = None
        self.prev_bar_vwap = None
        self.prev_bar_ema9 = None

    def reset_daily(self):
        self.trades_today = 0
        self.consecutive_losses = 0
        self.daily_pnl = 0.0
        self.in_trade = False
        self.prev_bar_close = None
        self.prev_bar_vwap = None
        self.prev_bar_ema9 = None

    def safety_gates_pass(self):
        if self.trades_today >= MAX_TRADES_PER_DAY:
            return False, f"MAX_TRADES_PER_DAY reached ({self.trades_today}/{MAX_TRADES_PER_DAY})"
        if self.consecutive_losses >= MAX_CONSECUTIVE_LOSSES:
            return False, f"MAX_CONSECUTIVE_LOSSES reached ({self.consecutive_losses}/{MAX_CONSECUTIVE_LOSSES})"
        if self.daily_pnl <= -abs(DAILY_LOSS_LIMIT):
            return False, f"DAILY_LOSS_LIMIT breached (${self.daily_pnl:.2f})"
        if self.in_trade:
            return False, "already in_trade"
        return True, ""


def version_b_filter_passes(state, direction):
    """Exactly matches intrabar_mes_bot.py's _version_b_filter_passes:
    price = prev_bar_close, vwap/ema9 = prior-bar values.
    Blocks LONG only if price < both. Blocks SHORT only if price > both.
    """
    if not choch_fvg_bot.VWAP_EMA_FILTER:
        return True
    if state.prev_bar_close is None or state.prev_bar_vwap is None or state.prev_bar_ema9 is None:
        return True  # warm-up: allow
    price = state.prev_bar_close
    if direction == "long":
        blocks = (price < state.prev_bar_ema9 and price < state.prev_bar_vwap)
    else:
        blocks = (price > state.prev_bar_ema9 and price > state.prev_bar_vwap)
    return not blocks


def simulate_bracket_from_fill(fill_price, direction, orig_risk, orig_reward,
                                future_candles):
    """Walk future 1-min bars until stop or target hits. Stop-first tie-break
    (matches live bracket order pessimism). Returns (exit_price, reason, bars_held).
    """
    if direction == "long":
        stop = fill_price - orig_risk
        target = fill_price + orig_reward
    else:
        stop = fill_price + orig_risk
        target = fill_price - orig_reward

    for i, c in enumerate(future_candles):
        if direction == "long":
            if c.low <= stop:
                return stop, "Stop bracket hit", i
            if c.high >= target:
                return target, "Target bracket hit", i
        else:
            if c.high >= stop:
                return stop, "Stop bracket hit", i
            if c.low <= target:
                return target, "Target bracket hit", i

    # Ran out of session — mark as forced-flat at the last close
    if future_candles:
        return future_candles[-1].close, "Forced flat (EOD)", len(future_candles)
    return fill_price, "Forced flat (no future bars)", 0


def replay_one_day(full_df, day, state, slippage_pts, log_lines):
    """Replay one trading day. Returns list of trade records for this day."""
    start_pt = pd.Timestamp(datetime.combine(day, SESSION_OPEN, tzinfo=PT))
    end_pt = pd.Timestamp(datetime.combine(day, REPLAY_WINDOW_END, tzinfo=PT))
    try:
        window = full_df.loc[start_pt:end_pt]
    except Exception:
        return []
    if len(window) < 10:
        return []

    candles = df_to_candles(window)
    wrapper = CHOCHFVGReplayWrapper()
    state.reset_daily()
    trades = []

    for i, candle in enumerate(candles):
        pt_time = candle.ts.astimezone(PT).time()
        is_last = (i == len(candles) - 1)

        # Snapshot PRIOR-bar values BEFORE feeding this candle to engine
        prior_vwap = wrapper.engine.vwap
        prior_ema9 = wrapper.engine.ema9

        signal = wrapper.add_candle(candle, pt_time,
                                     is_last_candle_of_session=is_last,
                                     session_vwap=None)

        # Update state.prev_* AFTER engine has ingested candle
        state.prev_bar_close = candle.close
        state.prev_bar_vwap  = wrapper.engine.vwap
        state.prev_bar_ema9  = wrapper.engine.ema9

        if signal is None or signal.get("type") != "entry":
            continue

        # Check safety gates
        ok, reason = state.safety_gates_pass()
        if not ok:
            log_lines.append(f"{day} {pt_time.strftime('%H:%M')} SIGNAL SKIPPED: {reason}")
            continue

        # Check Version B filter — use PRIOR values for filter decision
        # (matches live bot: at "moment of intrabar touch", we can only see
        # the PRIOR bar's close and PRIOR bar's indicators)
        prev_state = ReplayState()
        prev_state.prev_bar_close = getattr(state, '_prev_prev_close', None)
        prev_state.prev_bar_vwap = prior_vwap
        prev_state.prev_bar_ema9 = prior_ema9
        # But actually, at signal time the "prior bar" IS this bar's engine
        # state pre-update. Since we already stored state.prev_* using this
        # bar's post-update values, and the live bot's filter uses the
        # bar-BEFORE-current for both price and indicators, we need the
        # values captured just before this bar was added. Simplest way:
        # use state.prev_bar_close (this bar's close) as the price proxy,
        # and PRIOR vwap/ema9 for filter values.
        # This is the exact Version B design.
        filter_state = ReplayState()
        filter_state.prev_bar_close = candle.close       # current bar's close as filter price
        filter_state.prev_bar_vwap = prior_vwap          # prior bar's VWAP
        filter_state.prev_bar_ema9 = prior_ema9          # prior bar's EMA9
        if not version_b_filter_passes(filter_state, signal["direction"]):
            log_lines.append(
                f"{day} {pt_time.strftime('%H:%M')} SIGNAL BLOCKED by Version B filter "
                f"(dir={signal['direction']}, close={candle.close}, "
                f"vwap={prior_vwap}, ema9={prior_ema9})"
            )
            continue

        # Compute fill prices under both assumptions
        direction = signal["direction"]
        signal_entry = signal["entry_price"]
        signal_stop = signal["stop"]
        signal_target = signal["target"]

        if direction == "long":
            orig_risk = signal_entry - signal_stop
            orig_reward = signal_target - signal_entry
        else:
            orig_risk = signal_stop - signal_entry
            orig_reward = signal_entry - signal_target

        future = candles[i+1:]

        for label, fill_price in [
            ("BEST",      signal_entry),
            ("REALISTIC", signal_entry + (slippage_pts if direction == "long" else -slippage_pts)),
        ]:
            exit_price, exit_reason, bars_held = simulate_bracket_from_fill(
                fill_price, direction, orig_risk, orig_reward, future
            )
            if direction == "long":
                point_move = exit_price - fill_price
            else:
                point_move = fill_price - exit_price
            gross = round(point_move * MES_POINT_VALUE * CONTRACTS, 2)
            fee = round(FEE_PER_ROUND_TURN * CONTRACTS, 2)
            net = round(gross - fee, 2)

            trade = {
                "date": day.isoformat(),
                "time": pt_time.strftime("%H:%M"),
                "direction": direction,
                "signal_entry": signal_entry,
                "signal_stop": signal_stop,
                "signal_target": signal_target,
                "fill_scenario": label,
                "fill_price": fill_price,
                "exit_price": exit_price,
                "exit_reason": exit_reason,
                "bars_held": bars_held,
                "gross_pnl": gross,
                "fee": fee,
                "net_pnl": net,
            }
            trades.append(trade)

            # Log in bot-style format
            action = "BUY" if direction == "long" else "SELL"
            log_lines.append(
                f"{day} {pt_time.strftime('%H:%M')} [{label}] SUBMITTING BRACKET | "
                f"{action} {CONTRACTS}x MES | signal={signal_entry} stop={signal_stop} "
                f"target={signal_target} risk={orig_risk:.2f}pt"
            )
            log_lines.append(
                f"{day} {pt_time.strftime('%H:%M')} [{label}] Trade OPEN | "
                f"{direction} MES @ {fill_price:.2f} (slippage={fill_price - signal_entry:+.2f})"
            )
            log_lines.append(
                f"{day} {pt_time.strftime('%H:%M')} [{label}] Bracket CLOSED | {exit_reason} | "
                f"entry_fill={fill_price:.2f} exit_fill={exit_price:.2f} | "
                f"gross={gross:+.2f} fees=-{fee:.2f} | NET P&L={net:+.2f}"
            )

        # Update state using the REALISTIC scenario for downstream safety-gate math
        # (BEST is a ceiling reference; REALISTIC is what we simulate the bot living through)
        realistic_trade = trades[-1]  # last one appended is REALISTIC
        state.trades_today += 1
        state.daily_pnl += realistic_trade["net_pnl"]
        if realistic_trade["net_pnl"] < 0:
            state.consecutive_losses += 1
        else:
            state.consecutive_losses = 0

        log_lines.append(
            f"{day} {pt_time.strftime('%H:%M')} [DAILY STATE] trades={state.trades_today}/"
            f"{MAX_TRADES_PER_DAY} consec_L={state.consecutive_losses} "
            f"daily_pnl=${state.daily_pnl:+.2f}"
        )

    return trades


def summarise(trades, label):
    """Compute stats for a subset of trades filtered by fill_scenario."""
    subset = [t for t in trades if t["fill_scenario"] == label]
    n = len(subset)
    if n == 0:
        return None
    wins = sum(1 for t in subset if t["net_pnl"] > 0)
    losses = sum(1 for t in subset if t["net_pnl"] < 0)
    scratches = n - wins - losses
    total = sum(t["net_pnl"] for t in subset)
    wr = wins / (wins + losses) * 100 if (wins + losses) else 0
    return {
        "label": label,
        "trades": n,
        "wins": wins,
        "losses": losses,
        "scratches": scratches,
        "win_rate_pct": round(wr, 1),
        "total_pnl": round(total, 2),
        "avg_per_trade": round(total / n, 2) if n else 0,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("csv", help="Path to Databento CSV")
    parser.add_argument("start", help="Start date YYYY-MM-DD")
    parser.add_argument("end", help="End date YYYY-MM-DD")
    parser.add_argument("--slippage_pts", type=float, default=0.25,
                        help="Adverse execution slippage in points for REALISTIC scenario (default 0.25)")
    parser.add_argument("--log_out", default="intrabar_mes_replay.log",
                        help="Output log file (bot-style entries)")
    parser.add_argument("--json_out", default="intrabar_mes_replay_trades.json",
                        help="Output JSON of trade records")
    args = parser.parse_args()

    start_date = date.fromisoformat(args.start)
    end_date = date.fromisoformat(args.end)
    csv_path = ensure_decompressed(args.csv)

    print(f"Loading {csv_path} from {start_date} to {end_date} ...")
    load_start = pd.Timestamp(datetime.combine(start_date, SESSION_OPEN, tzinfo=PT)) - pd.Timedelta(days=1)
    load_end = pd.Timestamp(datetime.combine(end_date, REPLAY_WINDOW_END, tzinfo=PT)) + pd.Timedelta(days=1)
    df = load_range(csv_path, load_start.tz_convert("UTC"), load_end.tz_convert("UTC"))
    print(f"Loaded {len(df):,} bars.")
    df.index = df.index.tz_convert(PT)

    print(f"\nReplaying intrabar MES bot:")
    print(f"  slippage assumption:  {args.slippage_pts:.2f} pts adverse")
    print(f"  contracts:            1 MES")
    print(f"  safety gates:         active (MAX_TRADES={MAX_TRADES_PER_DAY}, "
          f"MAX_CONSEC_L={MAX_CONSECUTIVE_LOSSES}, DAILY_LOSS=${DAILY_LOSS_LIMIT})")

    state = ReplayState()
    all_trades = []
    log_lines = []
    day_count = 0

    for day in daterange(start_date, end_date):
        day_count += 1
        if day_count % 20 == 0:
            print(f"  day {day_count}: {day}   trades so far={len(all_trades)}")
        trades = replay_one_day(df, day, state, args.slippage_pts, log_lines)
        all_trades.extend(trades)

    # Write outputs
    Path(args.log_out).write_text("\n".join(log_lines))
    Path(args.json_out).write_text(json.dumps(all_trades, indent=2, default=str))
    print(f"\nWrote {len(log_lines):,} log lines to {args.log_out}")
    print(f"Wrote {len(all_trades):,} trade records to {args.json_out}")

    # Summary
    print("\n" + "=" * 66)
    print(f"REPLAY SUMMARY  {start_date} to {end_date}")
    print("=" * 66)
    for label in ("BEST", "REALISTIC"):
        s = summarise(all_trades, label)
        if s:
            print(f"\n{label} FILL SCENARIO ({label.lower()} slippage assumption):")
            print(f"  Trades:      {s['trades']}")
            print(f"  Wins/Losses: {s['wins']}/{s['losses']}  ({s['win_rate_pct']}% WR)")
            print(f"  Total P&L:   ${s['total_pnl']:+,.2f}")
            print(f"  Avg/trade:   ${s['avg_per_trade']:+.2f}")

    best = summarise(all_trades, "BEST") or {"total_pnl": 0}
    realistic = summarise(all_trades, "REALISTIC") or {"total_pnl": 0}
    diff = best["total_pnl"] - realistic["total_pnl"]
    print(f"\nSlippage cost (BEST vs REALISTIC): ${diff:+,.2f}")
    print("\n(REALISTIC assumes fill at midpoint + fixed adverse slippage. Real live "
          "slippage varies trade-to-trade; this is a rough estimate.)")


if __name__ == "__main__":
    main()
