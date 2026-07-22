#!/usr/bin/env python3
"""
Delta ON vs OFF, tick-honest — same 79 version-B entries (2026-04-22..07-14),
scored on the TRUE tick path (first-passage stop/target), split by whether the
live 300s DeltaTracker agreed. Answers: does keeping the ~27% of trades the delta
gate removes help or hurt the monthly number?
"""
import json, sys
from datetime import date as _date
sys.path.insert(0, "/home/claude")
from mae_study import load_day_ticks, analyze_trade, outcome, HORIZON_SEC, DELTA_WIN
from collections import defaultdict

PV, CT, FEE = 50.0, 2, 7.56

# span for monthly scaling
SPAN_MONTHS = ( _date(2026,7,14) - _date(2026,4,22) ).days / 30.4   # ~2.73 months


def load():
    rep = json.load(open("choch_replay_2026-04-22_2026-07-14.json"))
    entries = []
    for day in rep["days"]:
        for t in day["trades"]:
            entries.append({"date": day["date"], "entry_time": t["entry_time"],
                            "direction": t["direction"], "signal_entry": t["entry_price"],
                            "reason": t["reason"]})
    dr = json.load(open("/home/claude/delta_results.json"))
    agree = {(r["date"], r["entry_time"], r["direction"]): r["agree_300"] for r in dr}
    for e in entries:
        e["agree"] = agree.get((e["date"], e["entry_time"], e["direction"]))
    return entries


def main():
    entries = load()
    by_date = defaultdict(list)
    for e in entries:
        by_date[e["date"]].append(e)
    res = []
    for date in sorted(by_date):
        wins = []
        for t in by_date[date]:
            s = (int(t["entry_time"][:2]) + 7) * 3600 + int(t["entry_time"][3:5]) * 60
            wins.append((s - DELTA_WIN - 30, s + HORIZON_SEC + 60))
        try:
            dticks = load_day_ticks(date, wins)
        except FileNotFoundError:
            continue
        for t in by_date[date]:
            r = analyze_trade(t, dticks)
            if r is not None:
                r["agree"] = t["agree"]
                res.append(r)

    def stats(rows, s, tg):
        n = len(rows)
        if n == 0: return None
        pnls = [outcome(r, s, tg)[1] for r in rows]
        w = sum(1 for r in rows if outcome(r, s, tg)[0] == "win")
        net = sum(pnls)
        return {"n": n, "w": w, "wr": w/n*100, "net": net, "exp": net/n, "mo": net/SPAN_MONTHS}

    def line(lbl, st):
        if not st: return f"  {lbl:<22} (none)"
        return (f"  {lbl:<22} n={st['n']:>3}  WR={st['wr']:>5.1f}%  net=${st['net']:>8,.0f}  "
                f"exp=${st['exp']:>7,.1f}  ~${st['mo']:>7,.0f}/mo")

    allr = res
    agree = [r for r in res if r["agree"] is True]
    oppose = [r for r in res if r["agree"] is False]

    print("="*94)
    print(f"DELTA ON vs OFF — tick-honest, {len(res)} version-B entries, ~{SPAN_MONTHS:.1f} months, 2 contracts")
    print("="*94)
    print(f"(delta OFF = take all {len(allr)}; delta ON = take only the {len(agree)} the 300s tracker agreed with)")

    for s, tg, tag in [(1.0,3.0,"current 1pt/3pt"), (1.5,3.0,"1.5pt/3pt"), (2.0,3.0,"2pt/3pt")]:
        print(f"\n--- stop/target = {tag} (tick-honest) ---")
        print(line("delta OFF (all)", stats(allr, s, tg)))
        print(line("delta ON (agree)", stats(agree, s, tg)))
        print(line("   [oppose only]", stats(oppose, s, tg)))
        off = stats(allr,s,tg); on = stats(agree,s,tg)
        d = off["mo"] - on["mo"]
        print(f"  >> dropping delta {'ADDS' if d>0 else 'REMOVES'} ~${abs(d):,.0f}/mo "
              f"({off['n']-on['n']} more trades); per-trade exp {on['exp']:.0f}->{off['exp']:.0f}")

    # half-split on current config, delta OFF
    print("\n--- half-split, delta OFF, current 1pt/3pt (stability) ---")
    R2 = sorted(allr, key=lambda r:(r["date"], r["entry_time"]))
    mid=len(R2)//2
    print(line("H1", stats(R2[:mid],1.0,3.0)))
    print(line("H2", stats(R2[mid:],1.0,3.0)))

    json.dump([{"date":r["date"],"entry_time":r["entry_time"],"direction":r["direction"],
                "agree":r["agree"],"o13":outcome(r,1.0,3.0),"o23":outcome(r,2.0,3.0)} for r in res],
              open("/home/claude/onoff_results.json","w"), indent=2, default=str)


if __name__ == "__main__":
    main()
