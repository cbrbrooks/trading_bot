#!/usr/bin/env python3
"""
Daily trade summary — SL 3.0pt / TP 5.0pt, 2 contracts, delta OFF, Jan 2025 -> data end.
Runs the REAL engine over 1-minute bars (no tick data pre-2026-04, so this is the bar model;
at 3pt/5pt distances the intrabar ambiguity is negligible). Uncapped (all version-B setups,
matching collect-all) so the daily behavior is the strategy's, not the risk gates'.
"""
# --- path bootstrap (repo reorganized into folders) ---
import sys as _sys, pathlib as _pathlib
_ROOT = _pathlib.Path(__file__).resolve().parents[1]
_sys.path[:0] = [str(_ROOT / _d) for _d in ("bot", "backtests", "research")]
# ---------------------------------------------------------
import os
os.environ["INSTRUMENT"] = "ES"
import pandas as pd
from datetime import date, datetime
import choch_fvg_bot
from databento_loader import load_range
import choch_databento_replay as RP
from topstep_bot_v5 import PT
from choch_fvg_bot import SESSION_OPEN

# --- config under test ---
choch_fvg_bot.FIXED_STOP_POINTS = 3.0
choch_fvg_bot.RR_MULTIPLE = 5.0 / 3.0        # target = (5/3)*3.0 = 5.0pt
choch_fvg_bot.CONTRACTS = 2
choch_fvg_bot.MAX_TRADE_RISK_DOLLARS = 1e9   # lift $110 backstop (does NOT change setup selection)
choch_fvg_bot.DAILY_LOSS_LIMIT = 1e12        # uncapped daily view
choch_fvg_bot.MAX_TRADES_PER_DAY = 100000
choch_fvg_bot.MAX_CONSECUTIVE_LOSSES = 100000
if hasattr(choch_fvg_bot, "USE_BREAKEVEN"):
    choch_fvg_bot.USE_BREAKEVEN = False      # pure fixed brackets, matching live

CSV = "ohlcv1m.csv"
START, END = date(2025, 1, 1), date(2026, 7, 14)

ls = (pd.Timestamp(datetime.combine(START, SESSION_OPEN, tzinfo=PT)) - pd.Timedelta(days=1)).tz_convert("UTC")
le = (pd.Timestamp(datetime.combine(END, RP.REPLAY_WINDOW_END, tzinfo=PT)) + pd.Timedelta(days=1)).tz_convert("UTC")
print("loading 1-min bars ...")
full = load_range(CSV, ls, le)
full.index = full.index.tz_convert(PT)
print(f"loaded {len(full)} front-month bars")

days = []
alltrades = []
for day in RP.daterange(START, END):
    r = RP.replay_one_day(full, day)
    if r["note"]:
        continue
    days.append(r)
    for t in r["trades"]:
        t["date"] = r["date"]
        alltrades.append(t)

# ---- daily CSV ----
import csv as _csv
cum = 0.0
with open("/home/claude/sl3tp5_daily.csv", "w", newline="") as f:
    w = _csv.writer(f)
    w.writerow(["date", "trades", "wins", "losses", "day_pnl_$", "cum_pnl_$"])
    for r in days:
        if not r["trades"]:
            continue
        cum += r["pnl"]
        w.writerow([r["date"], len(r["trades"]), r["wins"], r["losses"],
                    f"{r['pnl']:.2f}", f"{cum:.2f}"])

# ---- per-trade CSV ----
with open("/home/claude/sl3tp5_trades.csv", "w", newline="") as f:
    w = _csv.writer(f)
    w.writerow(["date", "entry_time", "exit_time", "direction", "entry", "exit", "reason", "pnl_$"])
    for t in alltrades:
        w.writerow([t["date"], t["entry_time"], t["exit_time"], t["direction"],
                    t["entry_price"], t["exit_price"], t["reason"], f"{t['pnl']:.2f}"])

# ---- monthly rollup ----
from collections import defaultdict
mo = defaultdict(lambda: [0, 0, 0, 0.0])   # trades, wins, losses, pnl
for t in alltrades:
    m = t["date"][:7]
    mo[m][0] += 1
    mo[m][1] += t["pnl"] > 0
    mo[m][2] += t["pnl"] <= 0
    mo[m][3] += t["pnl"]

wins = sum(1 for t in alltrades if t["pnl"] > 0)
losses = len(alltrades) - wins
gp = sum(t["pnl"] for t in alltrades if t["pnl"] > 0)
gl = sum(t["pnl"] for t in alltrades if t["pnl"] <= 0)
net = gp + gl
avg_win = gp / wins if wins else 0
avg_loss = gl / losses if losses else 0

# max drawdown on cumulative equity (per-trade)
eq = 0.0; peak = 0.0; mdd = 0.0
for t in alltrades:
    eq += t["pnl"]; peak = max(peak, eq); mdd = min(mdd, eq - peak)

trading_days = sum(1 for r in days if r["trades"])
n_months = len(mo)

def slip_net(sl):
    return net - sl * 50 * 2 * len(alltrades)

print("\n" + "=" * 78)
print(f"SL 3.0 / TP 5.0 | 2 contracts | delta OFF | uncapped | {START} -> {END}  (bar model)")
print("=" * 78)
print(f"  Trades: {len(alltrades)}  over {trading_days} trading days ({n_months} months)")
print(f"  Wins/Losses: {wins}/{losses}   Win rate: {wins/len(alltrades)*100:.1f}%")
print(f"  Avg win / loss: ${avg_win:.2f} / ${avg_loss:.2f}   (per-trade risk ${3.0*50*2+7.56:.2f})")
print(f"  Net P&L (0 slip):     ${net:,.2f}   (~${net/n_months:,.0f}/mo, ~{len(alltrades)/trading_days:.2f} trades/day)")
print(f"  Net P&L (0.25 slip):  ${slip_net(0.25):,.2f}   (~${slip_net(0.25)/n_months:,.0f}/mo)")
print(f"  Net P&L (0.50 slip):  ${slip_net(0.50):,.2f}   (~${slip_net(0.50)/n_months:,.0f}/mo)")
print(f"  Max equity drawdown (0 slip, per-trade): ${mdd:,.2f}")
print("\n  MONTHLY (0 slip):")
print(f"    {'month':>8}  {'trds':>4}  {'W':>3} {'L':>3}  {'WR':>5}  {'net$':>10}  {'@0.25slip':>10}")
for m in sorted(mo):
    tr, wi, lo, pnl = mo[m]
    print(f"    {m:>8}  {tr:>4}  {wi:>3} {lo:>3}  {wi/tr*100:>4.0f}%  {pnl:>10,.0f}  {pnl-0.25*50*2*tr:>10,.0f}")

print("\n  wrote sl3tp5_daily.csv and sl3tp5_trades.csv")
