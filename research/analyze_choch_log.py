#!/usr/bin/env python3
"""
analyze_choch_log.py  —  End-of-week / daily analyzer for the CHOCH+FVG+Delta ES bot.
=====================================================================================
Parses choch_bot.log and reports the performance of DELTA-CONFIRMED trades taken by
the intrabar ES bot, exactly the metrics called for in the project handoff §8:

  * Overall win rate and net P&L (fees already included in the dry-run P&L figure).
  * Win rate BY HOUR OF DAY (does the pre-7am-PT weakness show up live?).
  * FIRST-HALF vs SECOND-HALF of the sample (edge stability / decay check).
  * Delta selectivity (DELTA-OK vs DELTA-REJECT).
  * A FLATTEN-REGRESSION GUARD: any "FLATTENING" that occurs after the fix is a bug.

It reads the log grammar produced by intrabar_es_bot.py:

  Entry (delta-confirmed, one line each):
     [DELTA-OK] 5-min delta=<v> confirms <dir>.
     TICK FIRED | price=<p> touched midpoint=<m> | dir=<dir>
     PROVISIONAL tick entry placed @ midpoint=<m> dir=<dir> ...
  Disposition at bar close:
     Provisional CONFIRMED at bar close ...
     ... KEPT (NOT flattened) ...            (post-fix "not re-confirmed" keep)
     ... FLATTENING the false start.         (OLD pre-fix behavior — should be gone)
  Exit:
     [engine exit signal — informational only under brackets] <reason> | theoretical P&L=$<x>.   (DRY RUN)
     Bracket trade CLOSED by broker | <reason> | ... gross P&L=$<g> fees=<f>                       (LIVE)

Design notes
------------
* Timestamps in the log are the machine's local wall-clock (America/Los_Angeles for
  Ruth). Hour-of-day buckets are taken straight from the log timestamp, so "pre-7am
  PT weakness" reads directly off the PT hour.
* A trade is counted once it is TAKEN (goes in_trade=True via CONFIRMED or KEPT). Its
  result is the next exit-signal line for that position.
* The dry-run "theoretical P&L" already nets fees (verified: 1pt stop on 2 ES = -$100
  gross, logged as -$107.56 = -$100 - 2*$3.78). We therefore treat the logged dry-run
  P&L as NET. For live "Bracket trade CLOSED" lines we net gross+fees ourselves.
* Every DELTA-OK precedes exactly one TICK FIRED, so counting TICK FIRED == taken
  delta-confirmed entries. DELTA-REJECT lines are counted separately for selectivity.

Usage
-----
    python analyze_choch_log.py [path/to/choch_bot.log] [--all] [--json out.json] [--since YYYY-MM-DD]

    --all     also include historical LIVE trades (different RR config) in a separate section
    --since   only analyze trades on/after this date (e.g. the day collect-all started)
    --json    write the machine-readable summary to a file
"""
# --- path bootstrap (repo reorganized into folders) ---
import sys as _sys, pathlib as _pathlib
_ROOT = _pathlib.Path(__file__).resolve().parents[1]
_sys.path[:0] = [str(_ROOT / _d) for _d in ("bot", "backtests", "research")]
# ---------------------------------------------------------
import sys, re, json, argparse, statistics
from datetime import datetime

TS = r"(\d{4}-\d{2}-\d{2}) (\d{2}):(\d{2}):(\d{2}),\d+"
RE_TS_LINE      = re.compile(r"^" + TS)
RE_DELTA_OK     = re.compile(TS + r".*\[DELTA-OK\] 5-min delta=([+-]?\d+(?:\.\d+)?) confirms (\w+)")
RE_DELTA_REJECT = re.compile(TS + r".*\[DELTA-REJECT\]")
RE_TICK_FIRED   = re.compile(TS + r".*TICK FIRED \| price=(-?\d+(?:\.\d+)?) touched midpoint=(-?\d+(?:\.\d+)?) \| dir=(\w+)")
RE_CONFIRMED    = re.compile(TS + r".*Provisional CONFIRMED at bar close")
RE_KEPT         = re.compile(TS + r".*KEPT \(NOT flattened\)")
RE_FLATTEN      = re.compile(TS + r".*FLATTENING the false start")
RE_EXIT_DRY     = re.compile(TS + r".*\[engine exit signal[^\]]*\] (.+?) \| theoretical P&L=\$(-?\d+(?:\.\d+)?)")
RE_EXIT_LIVE    = re.compile(TS + r".*Bracket trade CLOSED by broker \| (.+?) \| .*gross P&L=\$(-?\d+(?:\.\d+)?) fees=(-?\d+(?:\.\d+)?)")
RE_TRADE_LOGGED = re.compile(TS + r".*Trade logged: (\{.*\})\s*$")
RE_DRYRUN_BANNER= re.compile(TS + r".*\[DRY RUN — no real orders\]")
RE_COLLECT_ALL  = re.compile(TS + r".*COLLECT-ALL \(dry-run only\)")

FIX_SAVED = "2026-07-22 09:30:47"   # intrabar_es_bot.py mtime when the flatten was removed


def _dt(m, base=0):
    """Build a datetime from a regex match whose groups 1..4 are Y-M-D H M S."""
    d, hh, mm, ss = m.group(base + 1), m.group(base + 2), m.group(base + 3), m.group(base + 4)
    return datetime.strptime(f"{d} {hh}:{mm}:{ss}", "%Y-%m-%d %H:%M:%S")


def parse(path, since=None):
    trades = []          # delta-confirmed trades taken in the current (dry-run) regime
    live_logged = []     # historical "Trade logged" dicts (may be live, different RR)
    delta_ok = delta_reject = 0
    flatten_events = []  # (datetime, is_regression)
    kept_events = []
    n_sessions_collect = 0

    pending = None       # entry captured, awaiting exit
    last_delta = None    # (value, dir) from the most recent DELTA-OK

    with open(path, encoding="utf-8", errors="replace") as f:
        for line in f:
            m = RE_COLLECT_ALL.search(line)
            if m:
                n_sessions_collect += 1

            m = RE_DELTA_OK.search(line)
            if m:
                delta_ok += 1
                last_delta = (float(m.group(5)), m.group(6))
                continue
            if RE_DELTA_REJECT.search(line):
                delta_reject += 1
                continue

            m = RE_TICK_FIRED.search(line)
            if m:
                dt = _dt(m)
                if since and dt.date().isoformat() < since:
                    pending = None
                    continue
                pending = {
                    "dt": dt,
                    "entry_price": float(m.group(6)),
                    "midpoint": float(m.group(6)),
                    "direction": m.group(7),
                    "delta": last_delta[0] if last_delta else None,
                    "disposition": None,
                    "exit_reason": None,
                    "pnl": None,
                }
                continue

            if pending is not None:
                if RE_CONFIRMED.search(line):
                    pending["disposition"] = "CONFIRMED"; continue
                if RE_KEPT.search(line):
                    pending["disposition"] = "KEPT"; continue

            m = RE_FLATTEN.search(line)
            if m:
                dt = _dt(m)
                regression = (line[:19] > FIX_SAVED)  # ts string compare is chronological
                flatten_events.append((dt, regression))
                if pending is not None:
                    pending["disposition"] = "FLATTENED"
                    # flattened => no bracket run; treat as closed with no result to record
                    pending = None
                continue
            if RE_KEPT.search(line):
                kept_events.append(_dt(m) if m else None)

            m = RE_EXIT_DRY.search(line)
            if m and pending is not None:
                pending["exit_reason"] = m.group(5).strip()
                pending["pnl"] = float(m.group(6))     # already net of fees
                pending["mode"] = "dry"
                trades.append(pending); pending = None
                continue

            m = RE_EXIT_LIVE.search(line)
            if m and pending is not None:
                gross = float(m.group(6)); fees = float(m.group(7))
                pending["exit_reason"] = m.group(5).strip()
                pending["pnl"] = gross - abs(fees)
                pending["mode"] = "live"
                trades.append(pending); pending = None
                continue

            m = RE_TRADE_LOGGED.search(line)
            if m:
                try:
                    live_logged.append(eval(m.group(5), {"__builtins__": {}}, {}))
                except Exception:
                    pass

    return {
        "trades": trades,
        "live_logged": live_logged,
        "delta_ok": delta_ok,
        "delta_reject": delta_reject,
        "flatten_events": flatten_events,
        "kept_events": kept_events,
        "n_collect_sessions": n_sessions_collect,
    }


def _stats(trades):
    n = len(trades)
    if n == 0:
        return None
    wins = [t for t in trades if t["pnl"] > 0]
    losses = [t for t in trades if t["pnl"] <= 0]
    net = sum(t["pnl"] for t in trades)
    gp = sum(t["pnl"] for t in wins)
    gl = sum(t["pnl"] for t in losses)
    return {
        "n": n,
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": len(wins) / n,
        "net_pnl": net,
        "avg_win": (gp / len(wins)) if wins else 0.0,
        "avg_loss": (gl / len(losses)) if losses else 0.0,
        "expectancy": net / n,
        "profit_factor": (gp / abs(gl)) if gl < 0 else float("inf"),
    }


def _fmt_stats(s, indent="  "):
    if not s:
        return indent + "(no trades)"
    lines = [
        f"trades={s['n']}  wins={s['wins']}  losses={s['losses']}  win_rate={s['win_rate']*100:.1f}%",
        f"net P&L=${s['net_pnl']:,.2f}  expectancy=${s['expectancy']:,.2f}/trade  profit_factor={s['profit_factor']:.2f}",
        f"avg win=${s['avg_win']:,.2f}  avg loss=${s['avg_loss']:,.2f}",
    ]
    return "\n".join(indent + l for l in lines)


def report(data, since=None):
    trades = data["trades"]
    out = []
    out.append("=" * 74)
    out.append("CHOCH+FVG+Delta ES bot — dry-run performance report")
    out.append("=" * 74)

    # Flatten-regression guard (the single most important integrity check).
    flat = data["flatten_events"]
    regressions = [dt for dt, reg in flat if reg]
    out.append("")
    out.append("INTEGRITY: flatten-removal fix")
    if not flat:
        out.append("  No FLATTENING events anywhere in the log. ✓")
    else:
        out.append(f"  FLATTENING lines seen: {len(flat)} "
                   f"(last at {max(dt for dt,_ in flat):%Y-%m-%d %H:%M:%S})")
        if regressions:
            out.append(f"  *** REGRESSION: {len(regressions)} FLATTENING event(s) AFTER the fix "
                       f"({FIX_SAVED}). Investigate — the running bot may be on stale code. ***")
        else:
            out.append(f"  All are BEFORE the fix was saved ({FIX_SAVED}) — stale/expected, not a bug. ✓")
    out.append(f"  KEPT (NOT flattened) events (new post-fix keep path exercised): {len(data['kept_events'])}")

    # Delta selectivity.
    ok, rj = data["delta_ok"], data["delta_reject"]
    tot = ok + rj
    out.append("")
    out.append("DELTA GATE selectivity")
    if tot:
        out.append(f"  DELTA-OK={ok}  DELTA-REJECT={rj}  "
                   f"(fired on {ok/tot*100:.0f}% of evaluated touches)")
    else:
        out.append("  no delta evaluations logged")

    # Overall performance (delta-confirmed dry-run trades).
    scope = f" since {since}" if since else ""
    out.append("")
    out.append(f"OVERALL — delta-confirmed trades{scope}")
    s = _stats(trades)
    out.append(_fmt_stats(s))
    if s:
        dates = sorted({t["dt"].date().isoformat() for t in trades})
        out.append(f"  span: {dates[0]} … {dates[-1]}  ({len(dates)} trading day(s))")
        # target check from handoff: ~1-3 trades/day, ~50% WR, >= $1000/mo
        out.append(f"  vs handoff target (~1-3 trades/day, ~50% WR, ≥$1,000/mo): "
                   f"{s['n']/max(len(dates),1):.1f} trades/day, {s['win_rate']*100:.0f}% WR")

    # By hour of day.
    if trades:
        out.append("")
        out.append("BY HOUR OF DAY (PT) — is the pre-7am open really weakest?")
        out.append(f"  {'hour':>5}  {'n':>3}  {'wins':>4}  {'win%':>6}  {'net$':>10}")
        by_hour = {}
        for t in trades:
            by_hour.setdefault(t["dt"].hour, []).append(t)
        for hr in sorted(by_hour):
            g = by_hour[hr]
            w = sum(1 for t in g if t["pnl"] > 0)
            net = sum(t["pnl"] for t in g)
            out.append(f"  {hr:02d}:00  {len(g):>3}  {w:>4}  {w/len(g)*100:>5.0f}%  {net:>10,.2f}")

    # First half vs second half (decay check).
    if len(trades) >= 2:
        out.append("")
        out.append("FIRST-HALF vs SECOND-HALF (edge stability / decay)")
        ordered = sorted(trades, key=lambda t: t["dt"])
        mid = len(ordered) // 2
        out.append("  first half:")
        out.append(_fmt_stats(_stats(ordered[:mid]), indent="    "))
        out.append("  second half:")
        out.append(_fmt_stats(_stats(ordered[mid:]), indent="    "))

    # Trade blotter.
    if trades:
        out.append("")
        out.append("TRADE BLOTTER (delta-confirmed)")
        out.append(f"  {'when':>19}  {'dir':>5}  {'entry':>9}  {'delta':>7}  {'disp':>9}  {'result':>18}  {'pnl$':>9}")
        for t in sorted(trades, key=lambda x: x["dt"]):
            out.append(f"  {t['dt']:%Y-%m-%d %H:%M:%S}  {t['direction']:>5}  {t['entry_price']:>9.3f}  "
                       f"{(t['delta'] if t['delta'] is not None else 0):>7.0f}  "
                       f"{str(t['disposition']):>9}  {str(t['exit_reason'])[:18]:>18}  {t['pnl']:>9,.2f}")

    # Historical live-logged trades (different config — informational only).
    if data["live_logged"]:
        out.append("")
        out.append(f"NOTE: {len(data['live_logged'])} historical 'Trade logged' record(s) exist in this log "
                   f"(earlier live/dry runs, some with different RR). Not included above — different regime.")

    return "\n".join(out)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("logfile", nargs="?", default="choch_bot.log")
    ap.add_argument("--all", action="store_true")
    ap.add_argument("--since", default=None)
    ap.add_argument("--json", default=None)
    args = ap.parse_args()

    data = parse(args.logfile, since=args.since)
    text = report(data, since=args.since)
    print(text)

    if args.json:
        s = _stats(data["trades"])
        summary = {
            "overall": s,
            "delta_ok": data["delta_ok"],
            "delta_reject": data["delta_reject"],
            "kept_events": len(data["kept_events"]),
            "flatten_events": len(data["flatten_events"]),
            "flatten_regressions": sum(1 for _, reg in data["flatten_events"] if reg),
            "trades": [
                {**{k: (v.isoformat() if isinstance(v, datetime) else v)
                    for k, v in t.items()}}
                for t in data["trades"]
            ],
        }
        with open(args.json, "w") as f:
            json.dump(summary, f, indent=2, default=str)
        print(f"\n[wrote JSON summary -> {args.json}]")


if __name__ == "__main__":
    main()
