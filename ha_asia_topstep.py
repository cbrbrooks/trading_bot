"""
ha_asia_topstep.py — OUT-OF-SAMPLE validator for the Asia bot (TopstepX data)
=============================================================================
Runs the volatility-gated Asia strategy against your Topstep account's own
1-minute bars, so you can check it on data the tick backtest never saw (e.g.
after Jul 21) BEFORE risking the account. Same strategy brain (ha_strategy.py),
1-minute-bar approximation for the sub-minute parts (Topstep serves no ticks).

RUN THIS FIRST, on the out-of-sample stretch, and confirm the edge holds:
    python ha_asia_topstep.py 2026-07-22 2026-08-11
    python ha_asia_topstep.py 2026-07-22 2026-08-11 --instrument ES --contracts 1
    python ha_asia_topstep.py 2026-07-22 2026-08-11 --no_filter   # see the gate's effect

⚠️  Uses your live TopstepX session (single-session-per-user) — CLOSE the platform
and STOP any running bot first. And it's a 1-minute approximation: entry at the
last minute's extreme, stop-first on ambiguous bars — deliberately a touch
pessimistic vs the tick replay, which is the honest way to read it.

Needs the .env: TOPSTEP_USERNAME, TOPSTEP_API_KEY.
"""
import argparse, asyncio, os
from collections import defaultdict
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo
import aiohttp, pandas as pd
from dotenv import load_dotenv
import ha_strategy as S
load_dotenv()

BASE_URL = "https://api.topstepx.com/api"
USERNAME = os.getenv("TOPSTEP_USERNAME"); API_KEY = os.getenv("TOPSTEP_API_KEY")
PT = ZoneInfo("America/Los_Angeles"); CT = ZoneInfo("America/Chicago")
INSTRUMENT_PV = {"ES": 50.0, "MES": 5.0}; DEFAULT_FEE = {"ES": 3.78, "MES": 1.34}
NY_OPEN, NY_CLOSE = time(6, 30), time(13, 0)


async def _auth(s):
    d = await (await s.post(f"{BASE_URL}/Auth/loginKey", json={"userName": USERNAME, "apiKey": API_KEY})).json()
    if not d.get("success"): raise SystemExit(f"Auth failed: {d}")
    return d["token"]

async def _contract(s, tok, inst, sym):
    d = await (await s.post(f"{BASE_URL}/Contract/search", headers={"Authorization": f"Bearer {tok}"},
                            json={"searchText": inst, "live": False})).json()
    cs = [c for c in d.get("contracts", []) if "-" not in c.get("name", "")]
    if sym:
        e = [c for c in cs if c.get("name") == sym]
        if e: return e[0]["id"], e[0]["name"]
    m = [c for c in cs if c.get("name", "").startswith(inst)]
    if not m: raise SystemExit(f"{inst} contract not found; pass --symbol")
    c = sorted(m, key=lambda x: x.get("name", ""))[0]; return c["id"], c["name"]

async def _bars(s, tok, cid, a, b):
    d = await (await s.post(f"{BASE_URL}/History/retrieveBars", headers={"Authorization": f"Bearer {tok}"},
                            json={"contractId": cid, "live": False, "unit": 2, "unitNumber": 1,
                                  "limit": 5000, "includePartialBar": False,
                                  "startTime": a.isoformat(), "endTime": b.isoformat()})).json()
    out = []
    for x in d.get("bars") or d.get("data") or []:
        ts = x.get("t") or x.get("timestamp")
        o = x.get("o", x.get("open"))
        if ts is None or o is None: continue
        out.append({"ts": pd.to_datetime(ts, utc=True).tz_convert(PT), "o": float(o),
                    "h": float(x.get("h", x.get("high"))), "l": float(x.get("l", x.get("low"))),
                    "c": float(x.get("c", x.get("close"))), "v": float(x.get("v", x.get("volume", 0)) or 0)})
    out.sort(key=lambda z: z["ts"]); return out

def daterange(a, b):
    d = a
    while d <= b:
        if d.weekday() < 5: yield d
        d += timedelta(days=1)

def wick_tips(mins):
    if len(mins) < 20: return [], []
    up = [m["h"] for m in mins if (m["h"] - max(m["o"], m["c"])) >= S.MIN_WICK]
    lo = [m["l"] for m in mins if (min(m["o"], m["c"]) - m["l"]) >= S.MIN_WICK]
    return up, lo

def agg5(mins):
    buckets = {}; order = []
    for m in mins:
        t = m["ts"]; key = (t.year, t.month, t.day, t.hour, (t.minute // 5) * 5)
        if key not in buckets:
            buckets[key] = {"ts": t.replace(minute=(t.minute // 5) * 5, second=0, microsecond=0),
                            "o": m["o"], "h": m["h"], "l": m["l"], "c": m["c"], "v": m["v"], "sub": [m]}
            order.append(key)
        else:
            bk = buckets[key]; bk["h"] = max(bk["h"], m["h"]); bk["l"] = min(bk["l"], m["l"])
            bk["c"] = m["c"]; bk["v"] += m["v"]; bk["sub"].append(m)
    return [buckets[k] for k in order]

def replay_day(sess_mins, ny_up, ny_lo, D, N, pv, fee, use_filter, atr_min, max_tp, use_be):
    two = agg5([m for m in sess_mins if m["ts"].time() >= S.ASIA_VWAP_ANCHOR])
    if len(two) < 5: return []
    prev_ha = None; vn = vd = 0.0
    H = [x["h"] for x in two]; L = [x["l"] for x in two]
    for bk in two:
        ho, hh, hl, hc = S.heikin(bk["o"], bk["h"], bk["l"], bk["c"],
                                  prev_ha[0] if prev_ha else None, prev_ha[1] if prev_ha else None)
        prev_ha = (ho, hc); tp = (bk["h"] + bk["l"] + bk["c"]) / 3.0
        vn += tp * bk["v"]; vd += bk["v"]
        bk.update(ha_open=ho, ha_high=hh, ha_low=hl, ha_close=hc, vwap=(vn / vd if vd else bk["c"]))
    trades = []; busy_idx = -1
    fn = {"sig": 0, "atr_skip": 0, "no_target": 0, "cap_skip": 0, "atrvals": []}
    for i in range(2, len(two)):
        if i <= busy_idx: continue
        cur, prev = two[i], two[i-1]; tt = cur["ts"].time()
        if tt < S.ASIA_TRADE_OPEN or tt > S.ASIA_ENTRY_CUTOFF: continue
        pd_ = {"ha_open": prev["ha_open"], "ha_high": prev["ha_high"], "ha_low": prev["ha_low"],
               "ha_close": prev["ha_close"], "raw_high": prev["h"], "raw_low": prev["l"]}
        cd = {"ha_open": cur["ha_open"], "ha_high": cur["ha_high"], "ha_low": cur["ha_low"],
              "ha_close": cur["ha_close"], "raw_high": cur["h"], "raw_low": cur["l"]}
        sig = S.entry_signal(pd_, cd, cur["vwap"])
        if sig is None: continue
        fn["sig"] += 1
        _atr = S.atr_points(H, L, i)
        if use_filter and _atr < atr_min:
            fn["atr_skip"] += 1; fn["atrvals"].append(round(_atr, 1)); continue
        is_long = sig == "long"
        entry = cur["sub"][-1]["l"] if is_long else cur["sub"][-1]["h"]
        stop = S.initial_stop(prev["l"], prev["h"], is_long)
        sess_tips = []
        for bk in two[:i]:
            for m in bk["sub"]:
                if is_long and (m["h"] - max(m["o"], m["c"])) >= S.MIN_WICK: sess_tips.append(m["h"])
                if (not is_long) and (min(m["o"], m["c"]) - m["l"]) >= S.MIN_WICK: sess_tips.append(m["l"])
        tgt = S.cluster_target((ny_up if is_long else ny_lo) + sess_tips, entry, is_long)
        if tgt is None: fn["no_target"] += 1; continue
        level, target = tgt
        if use_filter and abs(target - entry) > max_tp: fn["cap_skip"] += 1; continue
        trg, be_stop = S.breakeven_stop(entry, target, is_long)
        cur_stop = stop; armed = False; rx = xp = None
        for bk in [m for bb in two[i+1:] for m in bb["sub"]] + [m for m in cur["sub"] if m["ts"] > cur["sub"][-1]["ts"]]:
            Hb, Lb = bk["h"], bk["l"]
            hs = (Lb <= cur_stop) if is_long else (Hb >= cur_stop)
            ht = (Hb >= target) if is_long else (Lb <= target)
            if hs and ht: rx, xp = ("be" if armed else "stop"), cur_stop; break
            if hs: rx, xp = ("be" if armed else "stop"), cur_stop; break
            if ht: rx, xp = "target", target; break
            if use_be and not armed and ((Hb if is_long else Lb) >= trg if is_long else (Lb <= trg)):
                armed = True; cur_stop = be_stop
        if rx is None: rx, xp = "flat", cur["sub"][-1]["c"]
        busy_idx = i
        pts = (xp - entry) if is_long else (entry - xp)
        trades.append({"date": str(D), "time": cur["ts"].strftime("%H:%M"), "dir": sig, "reason": rx,
                       "points": round(pts, 2), "net": round(pts * pv * N - fee * N, 2)})
    return trades, fn

async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("start"); ap.add_argument("end", nargs="?", default=None)
    ap.add_argument("--instrument", default="MES", choices=["MES", "ES"]); ap.add_argument("--contracts", type=int, default=3)
    ap.add_argument("--symbol", default=""); ap.add_argument("--fee", type=float, default=None)
    ap.add_argument("--atr_min", type=float, default=S.ASIA_ATR_MIN); ap.add_argument("--max_tp", type=float, default=S.ASIA_MAX_TP_POINTS)
    ap.add_argument("--no_filter", action="store_true"); ap.add_argument("--no_be", action="store_true")
    ap.add_argument("--csv", default="ha_asia_topstep_trades.csv")
    a = ap.parse_args()
    if not USERNAME or not API_KEY: raise SystemExit("Set TOPSTEP_USERNAME / TOPSTEP_API_KEY in .env")
    pv = INSTRUMENT_PV[a.instrument]; fee = a.fee if a.fee is not None else DEFAULT_FEE[a.instrument]
    N = a.contracts; use_filter = not a.no_filter; use_be = not a.no_be
    sd = date.fromisoformat(a.start); ed = date.fromisoformat(a.end) if a.end else date.today()
    print("="*66); print(f"  ASIA OOS VALIDATOR (TopstepX 1-min) — {a.instrument} x{N} | filter {'OFF' if not use_filter else f'ATR>={a.atr_min},tgt<{a.max_tp}'}")
    print(f"  {sd}..{ed} | uses your live session — close platform/bot first"); print("="*66)
    all_t = []; days = 0
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=25)) as s:
        tok = await _auth(s); cid, cname = await _contract(s, tok, a.instrument, a.symbol)
        print(f"  contract {cname}\n")
        n_data = n_nodata = n_err = 0
        for D in daterange(sd, ed):
            # NY levels: this day's 6:30-1pm.  Session: 3pm(D) -> 8pm(D).
            ny_a = datetime.combine(D, NY_OPEN, tzinfo=PT).astimezone(CT); ny_b = datetime.combine(D, NY_CLOSE, tzinfo=PT).astimezone(CT)
            se_a = datetime.combine(D, S.ASIA_VWAP_ANCHOR, tzinfo=PT).astimezone(CT); se_b = datetime.combine(D, S.ASIA_CLOSE, tzinfo=PT).astimezone(CT)
            for attempt in (1, 2):        # re-auth once if a fetch fails (session may have dropped)
                try:
                    ny = await _bars(s, tok, cid, ny_a, ny_b); sess = await _bars(s, tok, cid, se_a, se_b)
                    break
                except Exception as e:
                    if attempt == 1:
                        print(f"  {D}: fetch failed ({e}) — re-authenticating…")
                        try: tok = await _auth(s)
                        except Exception as e2: print(f"  {D}: RE-AUTH FAILED ({e2}) — is the platform open?"); ny = sess = None
                    else:
                        print(f"  {D}: FETCH ERROR after re-auth ({e})"); ny = sess = None
            if ny is None:
                n_err += 1; continue
            asia_ct = [m for m in sess if m["ts"].time() >= S.ASIA_TRADE_OPEN]
            if len(ny) < 30 or len(asia_ct) < 20:
                n_nodata += 1
                print(f"  {D}: NO DATA (NY bars={len(ny)}, session bars={len(asia_ct)}) — "
                      f"{'weekend/holiday' if D.weekday()>=5 else 'contract rolled or session dropped?'}")
                await asyncio.sleep(0.7); continue
            n_data += 1; days += 1; ny_up, ny_lo = wick_tips(ny)
            t, fn = replay_day(sess, ny_up, ny_lo, D, N, pv, fee, use_filter, a.atr_min, a.max_tp, use_be)
            all_t.extend(t)
            # signal funnel: raw HA signals -> why each was (not) taken
            funnel = (f"[{fn['sig']} HA signals: {fn['atr_skip']} cut by ATR"
                      + (f" (were {'/'.join(str(x) for x in fn['atrvals'])})" if fn['atrvals'] else "")
                      + f", {fn['no_target']} no NY target, {fn['cap_skip']} target>{a.max_tp}pt, {len(t)} traded]")
            print(f"  {D}: {len(t)} tr, ${sum(x['net'] for x in t):+.0f}  {funnel}")
            await asyncio.sleep(0.7)
        print(f"\n  coverage: {n_data} days with data, {n_nodata} no-data, {n_err} fetch errors")
    net = sum(t["net"] for t in all_t); w = [t for t in all_t if t["net"] > 0.01]; los = [t for t in all_t if t["net"] < -0.01]
    pf = sum(t["net"] for t in w) / (-sum(t["net"] for t in los)) if los else 0
    eq = pk = dd = 0
    for t in sorted(all_t, key=lambda x: (x["date"], x["time"])): eq += t["net"]; pk = max(pk, eq); dd = min(dd, eq - pk)
    print("\n" + "="*66)
    print(f"  ASIA OOS — {a.instrument} x{N} | {days} sessions | {len(all_t)} trades")
    print(f"  win/scratch/loss {len(w)}/{len(all_t)-len(w)-len(los)}/{len(los)} | PF {pf:.2f} | net ${net:,.0f} | maxDD ${dd:,.0f}")
    print("="*66)
    if all_t: pd.DataFrame(all_t).to_csv(a.csv, index=False); print(f"  -> {a.csv}")

if __name__ == "__main__":
    asyncio.run(main())
