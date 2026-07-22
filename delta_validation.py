#!/usr/bin/env python3
"""
Delta-edge validation — feeds the ACTUAL live DeltaTracker (delta_filter.py) the
Databento front-month ticks and snapshots agrees()/delta() at each of the engine's
79 version-B entries (choch_replay_2026-04-22_2026-07-14.json). Tests whether
delta-agreement actually predicts winners: overall, by window, half-split, and with
a Fisher exact significance test. Also answers the real question — does GATING on
delta-agree beat taking every trade?
"""
import json, io, zstandard, sys, math
from datetime import datetime, timezone
from collections import defaultdict

sys.path.insert(0, "/mnt/user-data/uploads/topstep_bot")
from delta_filter import DeltaTracker
from databento_loader import contract_symbol_for_date

PV, CT, FEE = 50.0, 2, 7.56
WINDOWS = [60, 120, 300, 600]     # trailing-delta windows to test (300 = live default)
BASE = "/mnt/user-data/uploads/topstep_bot/GLBX-20260722-PKNS633WLS/glbx-mdp3-{d}.trades.csv.zst"


def day_epoch(date):
    return datetime(int(date[:4]), int(date[5:7]), int(date[8:10]), tzinfo=timezone.utc).timestamp()


def load_entries():
    d = json.load(open("/mnt/user-data/uploads/topstep_bot/choch_replay_2026-04-22_2026-07-14.json"))
    out = []
    for day in d["days"]:
        for t in day["trades"]:
            out.append({"date": day["date"], "entry_time": t["entry_time"],
                        "direction": t["direction"], "entry": t["entry_price"],
                        "pnl": t["pnl"], "win": t["reason"] == "Target hit"})
    return out


def process_day(date, entries):
    """Feed the real DeltaTracker(s) the day's front-month ticks; snapshot at each entry."""
    front = contract_symbol_for_date(datetime.strptime(date, "%Y-%m-%d").date())
    base_ep = day_epoch(date)
    # entry fill windows (UTC seconds-of-day): entry minute .. +3min
    for e in entries:
        e["sod"] = (int(e["entry_time"][:2]) + 7) * 3600 + int(e["entry_time"][3:5]) * 60
        e["filled"] = False
    last_sod = max(e["sod"] for e in entries) + 240
    # one tracker per window, fed continuously (mirrors a live session)
    trackers = {w: DeltaTracker(window_secs=w) for w in WINDOWS}
    path = BASE.format(d=date.replace("-", ""))
    with open(path, "rb") as f:
        with zstandard.ZstdDecompressor().stream_reader(f) as r:
            text = io.TextIOWrapper(io.BufferedReader(r), encoding="utf-8")
            next(text)
            for line in text:
                hh = line[11:13]
                if hh < "13" or hh > "20":
                    continue
                c = line.split(",")
                if c[13].strip() != front:
                    continue
                sod = int(c[1][11:13]) * 3600 + int(c[1][14:16]) * 60 + int(c[1][17:19]) + float("0." + c[1][20:26])
                if sod > last_sod:
                    break
                price = float(c[8]); size = float(c[9]); side = c[6]
                now = base_ep + sod
                for w in WINDOWS:
                    trackers[w].add_trade(price, size, {"side": side}, now=now)
                # snapshot any entry whose fill tick this is
                for e in entries:
                    if e["filled"]:
                        continue
                    if sod < e["sod"] or sod > e["sod"] + 240:
                        continue
                    touch = price <= e["entry"] if e["direction"] == "long" else price >= e["entry"]
                    if touch:
                        e["filled"] = True
                        e["front"] = front
                        for w in WINDOWS:
                            dv = trackers[w].delta(now=now)
                            ag, _ = trackers[w].agrees(e["direction"], now=now)
                            e[f"delta_{w}"] = dv
                            e[f"agree_{w}"] = ag
    return entries


def agg(rows):
    n = len(rows)
    if n == 0:
        return None
    w = sum(1 for r in rows if r["win"])
    net = sum(r["pnl"] for r in rows)
    return {"n": n, "w": w, "l": n - w, "wr": w / n * 100, "net": net, "exp": net / n}


def fisher_exact(a, b, c, d):
    """Two-sided Fisher exact p for table [[a,b],[c,d]] (small n, exact via hypergeometric)."""
    n = a + b + c + d
    row1, col1 = a + b, a + c
    def hyp(k):
        return (math.comb(col1, k) * math.comb(n - col1, row1 - k)) / math.comb(n, row1)
    p_obs = hyp(a)
    lo = max(0, row1 - (n - col1)); hi = min(row1, col1)
    return sum(hyp(k) for k in range(lo, hi + 1) if hyp(k) <= p_obs * 1.0000001)


def fmt(a):
    return f"n={a['n']:>3}  WR={a['wr']:>5.1f}%  net=${a['net']:>9,.0f}  exp=${a['exp']:>7,.1f}  (W{a['w']}/L{a['l']})" if a else "(none)"


def main():
    entries = load_entries()
    by_date = defaultdict(list)
    for e in entries:
        by_date[e["date"]].append(e)
    resolved = []
    for date in sorted(by_date):
        try:
            process_day(date, by_date[date])
        except FileNotFoundError:
            print(f"  [skip {date}]"); continue
        for e in by_date[date]:
            if e.get("filled") and "agree_300" in e:
                resolved.append(e)
    print(f"\nResolved delta at fill for {len(resolved)}/{len(entries)} entries.\n")
    json.dump(resolved, open("/home/claude/delta_results.json", "w"),
              indent=2, default=lambda o: bool(o) if isinstance(o, bool) else o)

    R = resolved
    print("=" * 84)
    print("DELTA-EDGE VALIDATION — live DeltaTracker on Databento front-month, 79 version-B entries")
    print("=" * 84)
    base = agg(R)
    print(f"\nALL entries (no delta gate):        {fmt(base)}")

    print("\nDoes delta AGREEMENT predict winners? (by trailing-delta window)")
    print(f"  {'window':>7}  {'AGREE (bot takes)':>34}   {'OPPOSE (bot skips)':>34}   Fisher p")
    for w in WINDOWS:
        ag = [r for r in R if r[f"agree_{w}"]]
        op = [r for r in R if not r[f"agree_{w}"]]
        a, b = sum(r["win"] for r in ag), sum(1 for r in ag if not r["win"])
        c, d = sum(r["win"] for r in op), sum(1 for r in op if not r["win"])
        p = fisher_exact(a, b, c, d) if (ag and op) else float("nan")
        star = " <-- default" if w == 300 else ""
        print(f"  {w:>5}s  {fmt(agg(ag)):>34}")
        print(f"         {fmt(agg(op)):>34}   p={p:.3f}{star}")

    # The decision-relevant comparison at the live window (300s):
    w = 300
    ag = [r for r in R if r[f"agree_{w}"]]
    print("\n" + "-" * 84)
    print(f"THE FILTER'S VALUE (300s window): gate to delta-agree vs take everything")
    print(f"  take ALL 79:          {fmt(base)}")
    print(f"  take only AGREE:      {fmt(agg(ag))}")
    dn = base["exp"]; an = agg(ag)["exp"] if ag else 0
    print(f"  --> gating changes expectancy from ${dn:,.1f} to ${an:,.1f}/trade "
          f"and trade count {base['n']}->{len(ag)}")

    print("\n" + "-" * 84)
    print("HALF-SPLIT (chronological) at 300s window")
    R2 = sorted(R, key=lambda r: (r["date"], r["entry_time"]))
    mid = len(R2) // 2
    for lbl, half in [("H1", R2[:mid]), ("H2", R2[mid:])]:
        ag = [r for r in half if r["agree_300"]]; op = [r for r in half if not r["agree_300"]]
        print(f"  {lbl} AGREE : {fmt(agg(ag))}")
        print(f"  {lbl} OPPOSE: {fmt(agg(op))}")

    # polarity sanity: how often did each day's tracker calibrate & to what sign
    print("\n" + "-" * 84)
    print(f"Contracts used: {sorted({r['front'] for r in R})}")


if __name__ == "__main__":
    main()
