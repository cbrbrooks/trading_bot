"""
Standalone LIVE test: does the TopstepX API accept a 4-tick bracket stop?

This settles one question and nothing else: when we submit a bracket order through
the API with stopLossBracket ticks = 4, does the broker ACCEPT or REJECT it?

  - If ACCEPTED -> the 1.0pt stop is viable via the API, and we can revert to it.
  - If REJECTED with a tick-minimum error -> the ">5 ticks" API rule is real and
    we stay at 1.5pt (6 ticks).

IMPORTANT — READ BEFORE RUNNING:
  * This places a REAL MARKET ORDER on your funded account for 1 contract. It WILL
    open a position and (if accepted) attach a real stop and target. You should run
    it during market hours and be ready to see a 1-lot position appear.
  * It uses size = 1 (not 2) to minimise risk while testing.
  * It tests the SHORT side (side 1) with SIGNED ticks: stop = +N (above entry),
    target = -M (below entry), matching the convention learned from live rejections.
  * After submitting, it prints the broker's exact JSON response, then attempts to
    FLATTEN the position immediately so you are not left holding a trade. Confirm on
    the platform afterwards regardless.
  * It reads credentials from your .env exactly like the live bot does.

Run:  python test_bracket_ticks_live.py
"""
# --- path bootstrap (repo reorganized into folders) ---
import sys as _sys, pathlib as _pathlib
_ROOT = _pathlib.Path(__file__).resolve().parents[1]
_sys.path[:0] = [str(_ROOT / _d) for _d in ("bot", "backtests", "research")]
# ---------------------------------------------------------
import asyncio
import aiohttp
from dotenv import load_dotenv

load_dotenv()

from topstep_bot_v5 import BASE_URL, USERNAME, API_KEY, INSTRUMENT, MES_SYMBOL

ORDER_TYPE_MARKET = 2
ORDER_TYPE_LIMIT = 1
ORDER_TYPE_STOP = 4


async def authenticate(session):
    resp = await session.post(f"{BASE_URL}/Auth/loginKey",
                              json={"userName": USERNAME, "apiKey": API_KEY})
    data = await resp.json()
    if not data.get("success"):
        raise RuntimeError(f"Auth failed: {data}")
    print("[ok] authenticated")
    return data["token"]


async def get_account(session, token):
    resp = await session.post(f"{BASE_URL}/Account/search",
                              headers={"Authorization": f"Bearer {token}"},
                              json={"onlyActiveAccounts": True})
    data = await resp.json()
    acct = data.get("accounts", [])[0]
    print(f"[ok] account: {acct['name']} (id={acct['id']})")
    return acct["id"]


async def get_contract(session, token):
    resp = await session.post(f"{BASE_URL}/Contract/search",
                              headers={"Authorization": f"Bearer {token}"},
                              json={"searchText": INSTRUMENT, "live": False})
    data = await resp.json()
    contracts = data.get("contracts", [])
    exact = [c for c in contracts if c.get("name") == MES_SYMBOL]
    contract = exact[0] if exact else sorted(
        [c for c in contracts if c.get("name", "").startswith(INSTRUMENT)],
        key=lambda c: c.get("name", ""))[0]
    print(f"[ok] contract: {contract['name']} (id={contract['id']})")
    return contract["id"]


async def try_bracket(session, token, account_id, contract_id, stop_ticks, target_ticks, label):
    """Submit ONE short market order with the given SIGNED bracket ticks. Print the raw response."""
    payload = {
        "accountId": account_id,
        "contractId": contract_id,
        "type": ORDER_TYPE_MARKET,
        "side": 1,                       # SHORT (matches the convention we validated)
        "size": 1,                       # 1 lot only, to minimise test risk
        "limitPrice": None,
        "stopPrice": None,
        "trailPrice": None,
        "customTag": None,
        "linkedOrderId": None,
        "stopLossBracket":   {"ticks": stop_ticks,   "type": ORDER_TYPE_STOP},
        "takeProfitBracket": {"ticks": target_ticks, "type": ORDER_TYPE_LIMIT},
    }
    print(f"\n=== TEST: {label} ===")
    print(f"    stopLossBracket ticks = {stop_ticks}   takeProfitBracket ticks = {target_ticks}")
    resp = await session.post(f"{BASE_URL}/Order/place",
                              headers={"Authorization": f"Bearer {token}"},
                              json=payload)
    data = await resp.json()
    print(f"    RAW RESPONSE: {data}")
    if data.get("success"):
        print(f"    >>> ACCEPTED. orderId={data.get('orderId')}")
        return True, data.get("orderId")
    else:
        print(f"    >>> REJECTED. errorCode={data.get('errorCode')} "
              f"message={data.get('errorMessage')!r}")
        return False, None


async def flatten(session, token, account_id, contract_id):
    """Proper cleanup: FIRST cancel any working orders on this contract (the orphaned
    bracket legs), THEN close any open position. Doing it in this order prevents a
    surviving stop/target leg from re-opening a position after we flatten — the exact
    bug that left a leftover opposite contract in earlier tests."""
    # Step 1: cancel ALL working orders on this contract.
    try:
        from datetime import datetime, timedelta, timezone
        start_ts = (datetime.now(timezone.utc) - timedelta(minutes=30)).isoformat()
        r = await session.post(f"{BASE_URL}/Order/search",
                               headers={"Authorization": f"Bearer {token}"},
                               json={"accountId": account_id, "startTimestamp": start_ts})
        orders = (await r.json()).get("orders", [])
        # status 1 = working/open (not yet filled or cancelled)
        working = [o for o in orders
                   if o.get("contractId") == contract_id and o.get("status") == 1]
        for o in working:
            c = await session.post(f"{BASE_URL}/Order/cancel",
                                   headers={"Authorization": f"Bearer {token}"},
                                   json={"accountId": account_id, "orderId": o.get("id")})
            print(f"[flatten] cancelled working order {o.get('id')}: {await c.json()}")
        if not working:
            print("[flatten] no working orders to cancel.")
    except Exception as e:
        print(f"[flatten] order-cancel step failed: {e}")

    # Step 2: now close any open position (safe — no orphan legs left to re-open one).
    try:
        resp = await session.post(f"{BASE_URL}/Position/searchOpen",
                                  headers={"Authorization": f"Bearer {token}"},
                                  json={"accountId": account_id})
        data = await resp.json()
        pos = next((p for p in data.get("positions", [])
                    if p.get("contractId") == contract_id), None)
        if not pos:
            print("[flatten] no open position to close. You are flat.")
            return
        size = pos.get("size", 0)
        ptype = pos.get("type")
        close_side = 1 if ptype == 1 else 0
        close_payload = {
            "accountId": account_id, "contractId": contract_id,
            "type": ORDER_TYPE_MARKET, "side": close_side, "size": abs(size),
            "limitPrice": None, "stopPrice": None, "trailPrice": None,
            "customTag": None, "linkedOrderId": None,
        }
        r = await session.post(f"{BASE_URL}/Order/place",
                               headers={"Authorization": f"Bearer {token}"},
                               json=close_payload)
        print(f"[flatten] sent closing order: {await r.json()}")
    except Exception as e:
        print(f"[flatten] position-close step failed: {e}")
    print("[flatten] CONFIRM ON THE PLATFORM that you are flat with NO working orders.")


async def main():
    async with aiohttp.ClientSession() as session:
        token = await authenticate(session)
        account_id = await get_account(session, token)
        contract_id = await get_contract(session, token)

        # TEST 1: the disputed 4-tick stop (short -> stop +4 above, target -12 below)
        ok4, oid4 = await try_bracket(session, token, account_id, contract_id,
                                      stop_ticks=4, target_ticks=-12,
                                      label="4-tick stop (the 1.0pt question)")
        await asyncio.sleep(2)
        await flatten(session, token, account_id, contract_id)

        # Only bother testing 6 if 4 failed — confirms the boundary.
        if not ok4:
            await asyncio.sleep(2)
            ok6, oid6 = await try_bracket(session, token, account_id, contract_id,
                                          stop_ticks=6, target_ticks=-18,
                                          label="6-tick stop (the 1.5pt fallback)")
            await asyncio.sleep(2)
            await flatten(session, token, account_id, contract_id)

        print("\n" + "=" * 60)
        print("VERDICT")
        print("=" * 60)
        if ok4:
            print("  4-tick stop ACCEPTED. The API allows a 1.0pt stop.")
            print("  -> Tell Claude to revert the bot to FIXED_STOP_POINTS = 1.0.")
        else:
            print("  4-tick stop REJECTED by the API.")
            print("  -> The >5-tick rule is real on the API. Stay at 1.5pt (6 ticks).")
        print("\n  Paste this whole output back to Claude.")
        print("  CONFIRM ON THE PLATFORM that you have no leftover open position.")


if __name__ == "__main__":
    asyncio.run(main())
