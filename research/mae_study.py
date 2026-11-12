#!/usr/bin/env python3
"""
Adverse-excursion / stop-vs-target study for the CHOCH+FVG ES bot.
Uses Databento tick trades (GLBX ESU6) to walk the TRUE intrabar path after each
of the replay's 41 version-B entries, at tick granularity (no 1-min ambiguity).

For every entry it measures:
  * MAE (max adverse excursion) and MFE (max favorable) in points.
  * First-passage time to adverse levels {1.0,1.25,1.5,2.0}pt and favorable {3.0,3.75,4.5,6.0}pt.
  * Baseline outcome (1pt stop vs 3pt target) at true tick order.
  * "False stop" rate: of baseline losers, would the 3pt target have filled within 5/15/30 min after the stop?
  * Stop-size sweep: fixed 3pt target with wider stop; and matched-RR (target=3xstop).
  * Trailing 5-min order-flow delta at the fill (to flag delta-agree vs oppose).
Aggregates overall, by delta-agreement, and first-half vs second-half.
"""
# --- path bootstrap (repo reorganized into folders) ---
import sys as _sys, pathlib as _pathlib
_ROOT = _pathlib.Path(__file__).resolve().parents[1]
_sys.path[:0] = [str(_ROOT / _d) for _d in ("bot", "backtests", "research")]
# ---------------------------------------------------------
import json, io, zstandard, statistics, sys
from collections import defaultdict

POINT_VALUE = 50.0
CONTRACTS   = 2
FEE_RT      = 3.78 * CONTRACTS          # 7.56 total per round turn
UTC_OFFSET  = 7 * 3600                  # PT(PDT) -> UTC
FORCE_FLAT_UTC_SOD = (13 + 7) * 3600 + 10 * 60   # 13:10 PT -> 20:10 UTC
HORIZON_SEC = 60 * 60
DELTA_WIN   = 300

ADV = [1.0, 1.25, 1.5, 2.0]
FAV = [3.0, 3.75, 4.5, 6.0]

BASE = "data/GLBX-20260722-PKNS633WLS/glbx-mdp3-{d}.trades.csv.zst"


def sod_of(ts):  # ts_event "YYYY-MM-DDTHH:MM:SS.fffZ" -> seconds of day (UTC)
    return int(ts[11:13]) * 3600 + int(ts[14:16]) * 60 + int(ts[17:19]) + float("0." + ts[20:26])


def load_day_ticks(date, windows):
    """Return {symbol: [(sod, price, size, sign)]} for ticks whose sod is in any window."""
    path = BASE.format(d=date.replace("-", ""))
    out = defaultdict(list)
    lo = min(w[0] for w in windows) - 5
    hi = max(w[1] for w in windows) + 5
    with open(path, "rb") as f:
        with zstandard.ZstdDecompressor().stream_reader(f) as r:
            text = io.TextIOWrapper(io.BufferedReader(r), encoding="utf-8")
            next(text)
            for line in text:
                # cheap prefilter on HH before full split
                hh = line[11:13]
                if hh < "13" or hh > "20":
                    continue
                c = line.split(",")
                sod = sod_of(c[1])
                if sod < lo or sod > hi:
                    continue
                sym = c[13].strip()
                price = float(c[8])
                size = float(c[9])
                s = c[6]
                sign = 1 if s == "B" else (-1 if s == "A" else 0)
                out[sym].append((sod, price, size, sign))
    for sym in out:
        out[sym].sort()
    return out


def analyze_trade(t, day_ticks):
    direction = t["direction"]
    mid = t["signal_entry"]
    sod_entry = (int(t["entry_time"][:2]) + 7) * 3600 + int(t["entry_time"][3:5]) * 60

    # choose the contract whose price is nearest the midpoint around entry
    best_sym, best_err = None, 1e9
    for sym, ticks in day_ticks.items():
        if "-" in sym:            # skip spread instruments like ESM6-ESU6
            continue
        for sod, price, *_ in ticks:
            if sod_entry - 60 <= sod <= sod_entry + 180:
                e = abs(price - mid)
                if e < best_err:
                    best_err, best_sym = e, sym
    if best_sym is None or best_err > 3.0:
        return None
    ticks = day_ticks[best_sym]

    # polarity calibration (mirrors the live DeltaTracker): aggressive buying
    # should push price UP. cov(sign, dprice) < 0 => this feed's side field is
    # inverted relative to our +1=buy assumption, so flip.
    cov = 0.0; last = None
    for (sod, price, sz, sg) in ticks:
        if last is not None and price != last and sg != 0:
            cov += sg * (price - last)
        last = price
    polarity = -1.0 if cov < 0 else 1.0

    # fill: first tick at/after entry minute that touches the midpoint
    fill_i = None
    for i, (sod, price, *_ ) in enumerate(ticks):
        if sod < sod_entry:
            continue
        if sod > sod_entry + 180:
            break
        touch = price <= mid if direction == "long" else price >= mid
        if touch:
            fill_i = i
            break
    if fill_i is None:
        return None
    t_fill = ticks[fill_i][0]

    # trailing 5-min delta at fill (polarity-calibrated)
    delta5 = polarity * sum(sz * sg for (sod, pr, sz, sg) in ticks
                            if t_fill - DELTA_WIN <= sod <= t_fill)
    agree = (delta5 > 0) if direction == "long" else (delta5 < 0)

    # forward walk
    cap = min(t_fill + HORIZON_SEC, FORCE_FLAT_UTC_SOD)
    t_adv = {a: None for a in ADV}
    t_fav = {f: None for f in FAV}
    mae = 0.0; mfe = 0.0; mae_t = 0.0; mfe_t = 0.0
    mae_to_tgt = 0.0; reached_tgt = False
    terminal = mid
    for (sod, price, *_ ) in ticks[fill_i:]:
        if sod > cap:
            break
        terminal = price
        fav = (price - mid) if direction == "long" else (mid - price)
        adv = -fav
        if fav > mfe: mfe, mfe_t = fav, sod - t_fill
        if adv > mae: mae, mae_t = adv, sod - t_fill
        if not reached_tgt:                     # MAE dug BEFORE the 3pt target prints
            if adv > mae_to_tgt: mae_to_tgt = adv
            if fav >= 3.0: reached_tgt = True
        for a in ADV:
            if t_adv[a] is None and adv >= a:
                t_adv[a] = sod - t_fill
        for fv in FAV:
            if t_fav[fv] is None and fav >= fv:
                t_fav[fv] = sod - t_fill

    return {
        "date": t["date"], "entry_time": t["entry_time"], "direction": direction,
        "entry": mid, "symbol": best_sym, "delta5": delta5, "agree": agree,
        "t_adv": t_adv, "t_fav": t_fav, "mae": mae, "mfe": mfe,
        "mae_t": mae_t, "mfe_t": mfe_t, "terminal": terminal,
        "mae_to_tgt": mae_to_tgt, "reached_tgt": reached_tgt,
        "replay_reason": t["reason"],
    }


def outcome(res, stop_pts, tgt_pts):
    """Resolve (win/loss/flat) for a given stop & target using first-passage times."""
    ts = res["t_adv"].get(stop_pts)
    tt = res["t_fav"].get(tgt_pts)
    if tt is not None and (ts is None or tt <= ts):
        return "win", tgt_pts * POINT_VALUE * CONTRACTS - FEE_RT
    if ts is not None and (tt is None or ts < tt):
        return "loss", -(stop_pts * POINT_VALUE * CONTRACTS) - FEE_RT
    # neither hit -> flat at terminal price
    fav_term = (res["terminal"] - res["entry"]) if res["direction"] == "long" else (res["entry"] - res["terminal"])
    return "flat", fav_term * POINT_VALUE * CONTRACTS - FEE_RT


def agg(results, stop_pts, tgt_pts):
    pnls, wins, losses, flats = [], 0, 0, 0
    for r in results:
        o, pnl = outcome(r, stop_pts, tgt_pts)
        pnls.append(pnl)
        wins += o == "win"; losses += o == "loss"; flats += o == "flat"
    n = len(pnls)
    net = sum(pnls)
    return {"n": n, "wins": wins, "losses": losses, "flats": flats,
            "wr": wins / n * 100 if n else 0, "net": net,
            "exp": net / n if n else 0}


def main():
    trades = []
    d = json.load(open("intrabar_es_tickapprox_replay_2026-06-08_2026-07-21.json"))
    for day in d["days"]:
        for tr in day["trades"]:
            tr = dict(tr); tr["date"] = day["date"]; trades.append(tr)

    by_date = defaultdict(list)
    for t in trades:
        by_date[t["date"]].append(t)

    results = []
    for date in sorted(by_date):
        wins = []
        for t in by_date[date]:
            s = (int(t["entry_time"][:2]) + 7) * 3600 + int(t["entry_time"][3:5]) * 60
            wins.append((s - DELTA_WIN - 30, s + HORIZON_SEC + 60))
        try:
            day_ticks = load_day_ticks(date, wins)
        except FileNotFoundError:
            print(f"  [skip {date}: tick file not staged]"); continue
        for t in by_date[date]:
            r = analyze_trade(t, day_ticks)
            if r is not None:
                results.append(r)
        print(f"  {date}: {sum(1 for r in results if r['date']==date)}/{len(by_date[date])} trades resolved")

    print(f"\nResolved {len(results)}/{len(trades)} entries on tick data.\n")
    json.dump([{**r, "t_adv": {str(k): v for k, v in r["t_adv"].items()},
                "t_fav": {str(k): v for k, v in r["t_fav"].items()}} for r in results],
              open("/home/claude/mae_results.json", "w"), indent=2)
    print_report(results)


def _fmt(a):
    return (f"n={a['n']:>2}  WR={a['wr']:>5.1f}%  net=${a['net']:>9,.2f}  "
            f"exp=${a['exp']:>8,.2f}/trade  (W{a['wins']}/L{a['losses']}/flat{a['flats']})")


def print_report(results):
    R = results
    print("=" * 90)
    print("ADVERSE-EXCURSION / STOP-vs-TARGET STUDY  —  tick-level, ESU6, 41 version-B entries")
    print("=" * 90)

    # sanity vs replay
    base = agg(R, 1.0, 3.0)
    rep_wins = sum(1 for r in R if r["replay_reason"] == "Target hit")
    print(f"\nSanity: tick baseline (1pt/3pt) {_fmt(base)}")
    print(f"        replay said {rep_wins}/{len(R)} targets ({rep_wins/len(R)*100:.1f}% WR) on 1-min bars")

    # false-stop analysis
    print("\n" + "-" * 90)
    print("FALSE-STOP TEST — of trades the 1pt stop loses, would the 3pt target fill later?")
    losers = [r for r in R if outcome(r, 1.0, 3.0)[0] == "loss"]
    print(f"  baseline losers (stopped at 1pt before 3pt target): {len(losers)}")
    for N in (5, 15, 30):
        c = 0
        for r in losers:
            ts = r["t_adv"][1.0]; tt = r["t_fav"][3.0]
            if tt is not None and ts is not None and tt > ts and (tt - ts) <= N * 60:
                c += 1
        print(f"    ...of which target would fill within {N:>2} min after the stop: "
              f"{c}  ({c/len(losers)*100:.0f}% of losers)" if losers else "")

    # MAE-before-target: among trades that EVER reach +3pt, how far did they dig first?
    print("\n" + "-" * 90)
    print("MAE-BEFORE-TARGET — for trades that DO reach +3pt, how far against first (points)?")
    reachers = [r for r in R if r["reached_tgt"]]
    if reachers:
        maes = sorted(r["mae_to_tgt"] for r in reachers)
        med = statistics.median(maes)
        p75 = maes[int(len(maes)*0.75)-1] if len(maes) >= 4 else maes[-1]
        print(f"  {len(reachers)}/{len(R)} entries eventually print +3pt.  "
              f"median dig-first={med:.2f}pt  75th={p75:.2f}pt  max={maes[-1]:.2f}pt")
        for thr in (1.0, 1.25, 1.5, 2.0):
            k = sum(1 for r in reachers if r["mae_to_tgt"] >= thr)
            print(f"    of those, {k:>2}/{len(reachers)} ({k/len(reachers)*100:>3.0f}%) first dug >= {thr}pt "
                  f"-> a {thr}pt stop would have missed this eventual +3pt winner")

    # stop sweeps
    print("\n" + "-" * 90)
    print("STOP-SIZE SWEEP A — keep the 3.0pt target, widen the stop (RR degrades):")
    for s in ADV:
        print(f"    stop={s:>4}pt / tgt=3.0pt (RR={3.0/s:.1f}):  {_fmt(agg(R, s, 3.0))}")
    print("\nSTOP-SIZE SWEEP B — matched RR=3.0 (target = 3x stop):")
    for s in ADV:
        print(f"    stop={s:>4}pt / tgt={3*s:>4}pt (RR=3.0):  {_fmt(agg(R, s, 3*s))}")

    # delta split
    print("\n" + "-" * 90)
    print("DELTA AGREEMENT at fill (trailing 5-min cumulative delta, this ESU6 tape):")
    ag = [r for r in R if r["agree"]]; op = [r for r in R if not r["agree"]]
    print(f"  delta AGREE : {_fmt(agg(ag,1.0,3.0))}" if ag else "  (none)")
    print(f"  delta OPPOSE: {_fmt(agg(op,1.0,3.0))}" if op else "  (none)")
    print(f"  (agree={len(ag)}, oppose={len(op)}; note ESU6 pre-roll tape is thin in early June)")

    # half-split on best-looking configs
    print("\n" + "-" * 90)
    print("HALF-SPLIT robustness (chronological): first half vs second half")
    R2 = sorted(R, key=lambda r: (r["date"], r["entry_time"]))
    mid = len(R2)//2
    for label, s, tg in [("1.0/3.0 baseline",1.0,3.0), ("1.5/3.0 fixedT",1.5,3.0),
                         ("2.0/3.0 fixedT",2.0,3.0), ("1.5/4.5 matched",1.5,4.5)]:
        h1, h2 = agg(R2[:mid], s, tg), agg(R2[mid:], s, tg)
        print(f"  {label:>18}:  H1 {_fmt(h1)}")
        print(f"  {'':>18}   H2 {_fmt(h2)}")


if __name__ == "__main__":
    main()
