import os
os.environ["INSTRUMENT"]="ES"
import pandas as pd, csv as _csv
from datetime import date, datetime
from collections import defaultdict
import choch_fvg_bot
from databento_loader import load_range
import choch_databento_replay as RP
from topstep_bot_v5 import PT
from choch_fvg_bot import SESSION_OPEN

# --- config: 2pt stop / 3pt target, 1 contract, caps 3/day + stop@2 ---
choch_fvg_bot.FIXED_STOP_POINTS=2.0
choch_fvg_bot.RR_MULTIPLE=1.5           # 2.0*1.5 = 3.0pt target
choch_fvg_bot.CONTRACTS=1
choch_fvg_bot.MAX_TRADE_RISK_DOLLARS=1e9
choch_fvg_bot.DAILY_LOSS_LIMIT=1e12
choch_fvg_bot.MAX_TRADES_PER_DAY=3
choch_fvg_bot.MAX_CONSECUTIVE_LOSSES=2
if hasattr(choch_fvg_bot,"USE_BREAKEVEN"): choch_fvg_bot.USE_BREAKEVEN=False
CSV="ohlcv1m.csv"; START,END=date(2026,1,1),date(2026,7,14); PV,CT=50.0,1
ls=(pd.Timestamp(datetime.combine(START,SESSION_OPEN,tzinfo=PT))-pd.Timedelta(days=1)).tz_convert("UTC")
le=(pd.Timestamp(datetime.combine(END,RP.REPLAY_WINDOW_END,tzinfo=PT))+pd.Timedelta(days=1)).tz_convert("UTC")
full=load_range(CSV,ls,le); full.index=full.index.tz_convert(PT)
days=[]; alltr=[]
for day in RP.daterange(START,END):
    r=RP.replay_one_day(full,day)
    if r["note"]: continue
    days.append(r)
    for t in r["trades"]: t["date"]=r["date"]; alltr.append(t)

cum=0.0
with open("/home/claude/sl200tp300_1ct_daily.csv","w",newline="") as f:
    w=_csv.writer(f); w.writerow(["date","trades","wins","losses","day_pnl_$","cum_pnl_$"])
    for r in days:
        if not r["trades"]: continue
        cum+=r["pnl"]; w.writerow([r["date"],len(r["trades"]),r["wins"],r["losses"],f"{r['pnl']:.2f}",f"{cum:.2f}"])
with open("/home/claude/sl200tp300_1ct_trades.csv","w",newline="") as f:
    w=_csv.writer(f); w.writerow(["date","entry_time","exit_time","direction","entry","exit","reason","pnl_$"])
    for t in alltr: w.writerow([t["date"],t["entry_time"],t["exit_time"],t["direction"],t["entry_price"],t["exit_price"],t["reason"],f"{t['pnl']:.2f}"])

wins=sum(1 for t in alltr if t["pnl"]>0); losses=len(alltr)-wins
gw=sum(t["pnl"] for t in alltr if t["pnl"]>0); gl=sum(t["pnl"] for t in alltr if t["pnl"]<=0); net=gw+gl
mo=defaultdict(lambda:[0,0,0,0.0])
for t in alltr:
    m=t["date"][:7]; mo[m][0]+=1; mo[m][1]+= t["pnl"]>0; mo[m][2]+= t["pnl"]<=0; mo[m][3]+=t["pnl"]
def dd(slip):
    eq=peak=m=0.0
    for t in alltr:
        eq+=t["pnl"]-slip*PV*CT; peak=max(peak,eq); m=min(m,eq-peak)
    return m
tdays=sum(1 for r in days if r["trades"]); nm=len(mo)
print("="*74)
print(f"SL 2pt/$100  TP 3pt/$150 | 1 CONTRACT | 3/day + stop@2 | {START}->{END}")
print("="*74)
print(f"  Trades {len(alltr)} over {tdays} trading days ({nm} months) | {len(alltr)/tdays:.2f}/day")
print(f"  Win rate {wins/len(alltr)*100:.1f}%  (W{wins}/L{losses})")
print(f"  Avg win/loss ${gw/wins:.2f} / ${gl/losses:.2f}  | risk/trade ${2.0*PV*CT+3.78:.2f}")
print(f"  Net 0 slip     ${net:,.2f}  (~${net/nm:,.0f}/mo)")
print(f"  Net 0.25 slip  ${net-0.25*PV*CT*len(alltr):,.2f}  (~${(net-0.25*PV*CT*len(alltr))/nm:,.0f}/mo)")
print(f"  Max drawdown   0slip ${dd(0):,.0f}   0.25slip ${dd(0.25):,.0f}")
print(f"\n  MONTHLY:")
print(f"    {'month':>8} {'trds':>4} {'W':>3} {'L':>3} {'WR':>5} {'net$':>9} {'@.25slip':>9}")
for m in sorted(mo):
    tr,wi,lo,pnl=mo[m]
    print(f"    {m:>8} {tr:>4} {wi:>3} {lo:>3} {wi/tr*100:>4.0f}% {pnl:>9,.0f} {pnl-0.25*PV*CT*tr:>9,.0f}")
print("\n  DAILY (recent 15 trading days):")
recent=[r for r in days if r["trades"]][-15:]
c2=sum(r["pnl"] for r in days if r["trades"] and r["date"]< recent[0]["date"])
for r in recent:
    c2+=r["pnl"]
    print(f"    {r['date']}  {len(r['trades'])}t  {r['wins']}W {r['losses']}L  day ${r['pnl']:>7,.2f}  cum ${c2:>8,.2f}")
