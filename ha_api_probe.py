"""
ha_api_probe.py — diagnose why the Asia validator stops at a date wall
======================================================================
Does NOT run the strategy. It just asks TopstepX exactly what bar data it returns
for a few specific dates — daytime (6:30am-1pm PT) vs evening (3-8pm PT) — and
which contract it's using, so we can see WHERE the data actually stops and WHY.

    python ha_api_probe.py
    python ha_api_probe.py --instrument MES --dates 2026-07-22,2026-07-29,2026-07-30,2026-08-03,2026-08-07,2026-08-10

Close the TopstepX platform / any bot first (single session per user).
"""
import argparse, asyncio, os, json
from datetime import date, datetime, time
from zoneinfo import ZoneInfo
import aiohttp
from dotenv import load_dotenv
load_dotenv()

BASE_URL = "https://api.topstepx.com/api"
USERNAME = os.getenv("TOPSTEP_USERNAME"); API_KEY = os.getenv("TOPSTEP_API_KEY")
PT = ZoneInfo("America/Los_Angeles"); CT = ZoneInfo("America/Chicago")


async def auth(s):
    d = await (await s.post(f"{BASE_URL}/Auth/loginKey", json={"userName": USERNAME, "apiKey": API_KEY})).json()
    if not d.get("success"): raise SystemExit(f"Auth failed: {d}")
    print("Auth OK"); return d["token"]


async def bars(s, tok, cid, a_pt, b_pt, live):
    a = a_pt.astimezone(CT); b = b_pt.astimezone(CT)
    payload = {"contractId": cid, "live": live, "unit": 2, "unitNumber": 1,
               "limit": 5000, "includePartialBar": False,
               "startTime": a.isoformat(), "endTime": b.isoformat()}
    r = await s.post(f"{BASE_URL}/History/retrieveBars", headers={"Authorization": f"Bearer {tok}"}, json=payload)
    d = await r.json()
    bs = d.get("bars") or d.get("data") or []
    return bs, d, payload


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--instrument", default="MES")
    ap.add_argument("--dates", default="2026-07-22,2026-07-27,2026-07-29,2026-07-30,2026-07-31,2026-08-03,2026-08-07,2026-08-10")
    a = ap.parse_args()
    if not USERNAME or not API_KEY: raise SystemExit("Set TOPSTEP_USERNAME / TOPSTEP_API_KEY in .env")

    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=25)) as s:
        tok = await auth(s)
        # 1) show EVERY contract the search returns + which one the validator would pick
        r = await s.post(f"{BASE_URL}/Contract/search", headers={"Authorization": f"Bearer {tok}"},
                         json={"searchText": a.instrument, "live": False})
        contracts = (await r.json()).get("contracts", [])
        print(f"\nContract search '{a.instrument}' returned {len(contracts)}:")
        for c in contracts:
            print(f"   name={c.get('name')!r}  id={c.get('id')}  "
                  f"activeContract={c.get('activeContract')}  "
                  f"{'<-- expires ' + str(c.get('expirationDate')) if c.get('expirationDate') else ''}")
        nonspread = [c for c in contracts if "-" not in c.get("name", "")]
        matches = [c for c in nonspread if c.get("name", "").startswith(a.instrument)]
        picked = sorted(matches, key=lambda x: x.get("name", ""))[0]
        print(f"\n   >>> validator picks (alphabetical): {picked.get('name')} ({picked.get('id')})")
        # also identify the highest-volume / active one for comparison
        active = [c for c in matches if c.get("activeContract")]
        if active: print(f"   >>> API marks activeContract=True: {[c.get('name') for c in active]}")

        cid = picked["id"]
        print("\n" + "=" * 78)
        print(f"{'date':12}{'DAYTIME 6:30-1pm':>26}{'EVENING 3-8pm':>26}")
        print("=" * 78)
        for ds in a.dates.split(","):
            D = date.fromisoformat(ds.strip())
            day_a = datetime.combine(D, time(6, 30), tzinfo=PT); day_b = datetime.combine(D, time(13, 0), tzinfo=PT)
            eve_a = datetime.combine(D, time(15, 0), tzinfo=PT); eve_b = datetime.combine(D, time(20, 0), tzinfo=PT)
            dbs, _, _ = await bars(s, tok, cid, day_a, day_b, False)
            ebs, ed, epay = await bars(s, tok, cid, eve_a, eve_b, False)
            def desc(bs):
                if not bs: return "0 bars"
                t0 = bs[0].get("t"); t1 = bs[-1].get("t")
                return f"{len(bs)}b {str(t0)[11:16]}-{str(t1)[11:16]}Z"
            print(f"{ds:12}{desc(dbs):>26}{desc(ebs):>26}")
            if not ebs:   # show the raw response when evening is empty — reveals errors/limits
                print(f"    evening raw response keys={list(ed.keys())}  "
                      f"errorMessage={ed.get('errorMessage')}  success={ed.get('success')}")
            await asyncio.sleep(0.5)
        # 2) retry the first empty evening with live:True, in case historical lags recent days
        print("\nRe-testing the LAST date's evening with live:True (recent-data lag check)…")
        D = date.fromisoformat(a.dates.split(",")[-1].strip())
        eve_a = datetime.combine(D, time(15, 0), tzinfo=PT); eve_b = datetime.combine(D, time(20, 0), tzinfo=PT)
        ebs2, ed2, _ = await bars(s, tok, cid, eve_a, eve_b, True)
        print(f"   live:True evening {D}: {len(ebs2)} bars  (success={ed2.get('success')})")


if __name__ == "__main__":
    asyncio.run(main())
