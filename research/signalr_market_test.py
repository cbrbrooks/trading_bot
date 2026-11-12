"""
SignalR market-hub sniff test.

Purpose: measure — not trade — the real-time market data stream from TopstepX.

What it does:
  1. Authenticates using the same .env credentials the CHOCH bot uses.
  2. Resolves the ES contract ID the same way the bot does.
  3. Connects to rtc.topstepx.com/hubs/market via SignalR.
  4. Subscribes to GatewayQuote AND GatewayTrade for that contract.
  5. Logs every event received to a CSV file with:
       - Local wall-clock time (ms precision)
       - Event type (Quote / Trade)
       - Payload (price, size, etc.)
  6. Prints running stats every 30s: messages/sec, inter-arrival gap
     distribution, whether trade stream is producing data.
  7. Runs until you Ctrl-C it, or DURATION_MINUTES elapses.

What it tells you afterwards:
  * Whether GatewayTrade is available on YOUR API tier (if 0 messages
    arrive on the trade stream during active RTH trading, it's probably
    gated behind the L2 subscription).
  * Update rate — how many trade prints per second at peak, and what the
    typical inter-message gap looks like.
  * Whether GatewayQuote runs independently and at what rate.

What it DOES NOT do:
  * Place any orders.
  * Change any bot state.
  * Cost anything you're not already paying for the API subscription.

Requires: `pip install signalrcore python-dotenv aiohttp`

Run it during ACTIVE trading hours (RTH open, ~6:30am PT onward) — otherwise
there's just no data to observe.
"""
# --- path bootstrap (repo reorganized into folders) ---
import sys as _sys, pathlib as _pathlib
_ROOT = _pathlib.Path(__file__).resolve().parents[1]
_sys.path[:0] = [str(_ROOT / _d) for _d in ("bot", "backtests", "research")]
# ---------------------------------------------------------
import asyncio
import csv
import os
import statistics
import sys
import time as _time
from collections import deque
from datetime import datetime, timezone

import aiohttp
from dotenv import load_dotenv
from signalrcore.hub_connection_builder import HubConnectionBuilder

load_dotenv()

USERNAME = os.getenv("TOPSTEP_USERNAME")
API_KEY = os.getenv("TOPSTEP_API_KEY")
INSTRUMENT = os.getenv("INSTRUMENT", "ES")
BASE_URL = "https://api.topstepx.com/api"
MARKET_HUB_URL = "https://rtc.topstepx.com/hubs/market"

# How long to run before auto-exiting. Set to a large number if you want to
# leave it running and Ctrl-C manually.
DURATION_MINUTES = 30

# Output CSV — one row per message received.
OUTPUT_CSV = f"signalr_sniff_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"


def now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


async def authenticate(session):
    resp = await session.post(
        f"{BASE_URL}/Auth/loginKey",
        json={"userName": USERNAME, "apiKey": API_KEY},
    )
    data = await resp.json()
    token = data.get("token")
    if not token:
        raise RuntimeError(f"Auth failed: {data}")
    print(f"[{now_iso()}] Authenticated OK.")
    return token


async def resolve_contract(session, token):
    resp = await session.post(
        f"{BASE_URL}/Contract/search",
        headers={"Authorization": f"Bearer {token}"},
        json={"searchText": INSTRUMENT, "live": False},
    )
    data = await resp.json()
    contracts = data.get("contracts", [])
    # Prefer front-month; matching the bot's own logic loosely.
    matches = [c for c in contracts if c.get("name", "").startswith(INSTRUMENT)]
    if not matches:
        raise RuntimeError(f"No {INSTRUMENT} contract found.")
    contract = sorted(matches, key=lambda c: c.get("name", ""))[0]
    print(f"[{now_iso()}] Using contract: {contract['name']} (id={contract['id']})")
    return contract["id"]


# Shared state for the callbacks + reporter
counters = {"Quote": 0, "Trade": 0, "Depth": 0}
last_ts = {"Quote": None, "Trade": None, "Depth": None}
inter_gaps = {"Quote": deque(maxlen=1000), "Trade": deque(maxlen=1000), "Depth": deque(maxlen=1000)}
csv_writer = None
csv_lock = asyncio.Lock()
csv_file = None


def log_event(event_type, payload):
    """Called from SignalR callback thread — thread-safe via a queue-less approach:
    we just write directly since the CSV writer + counter increments are cheap.
    The reporter task reads counters snapshotted, which is fine even without a lock
    for our purposes (we don't need exact counts, just approximate rates)."""
    global csv_writer
    wall = _time.time()
    prev = last_ts[event_type]
    if prev is not None:
        inter_gaps[event_type].append(wall - prev)
    last_ts[event_type] = wall
    counters[event_type] += 1

    if csv_writer is not None:
        try:
            csv_writer.writerow([now_iso(), event_type, str(payload)[:500]])
            csv_file.flush()
        except Exception as e:
            print(f"CSV write error: {e}")


def on_quote(args):
    # SignalR passes args as a list — [contractId, quoteData] per docs
    log_event("Quote", args)


def on_trade(args):
    log_event("Trade", args)


def on_depth(args):
    log_event("Depth", args)


async def stats_reporter():
    """Prints running stats every 30 seconds so you can see whether data is
    flowing without having to open the CSV."""
    while True:
        await asyncio.sleep(30)
        print(f"\n[{now_iso()}] --- 30s snapshot ---")
        for kind in ("Quote", "Trade", "Depth"):
            n = counters[kind]
            gaps = list(inter_gaps[kind])
            if gaps:
                mean_gap = statistics.mean(gaps) * 1000  # ms
                p50 = statistics.median(gaps) * 1000
                p95 = sorted(gaps)[int(0.95 * len(gaps))] * 1000 if len(gaps) > 20 else float("nan")
                print(f"  {kind:6s}: {n:>7,} total msgs | mean gap {mean_gap:>7.1f}ms | "
                      f"p50 {p50:>7.1f}ms | p95 {p95:>7.1f}ms")
            else:
                print(f"  {kind:6s}: {n:>7,} total msgs | no gap data yet")


async def main():
    if not USERNAME or not API_KEY:
        print("ERROR: TOPSTEP_USERNAME and TOPSTEP_API_KEY must be set in .env")
        sys.exit(1)

    global csv_writer, csv_file
    csv_file = open(OUTPUT_CSV, "w", newline="")
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow(["wall_time_iso", "event_type", "payload"])

    token, http_session = None, None
    async with aiohttp.ClientSession() as http_session:
        token = await authenticate(http_session)
        contract_id = await resolve_contract(http_session, token)

    print(f"[{now_iso()}] Connecting to {MARKET_HUB_URL} ...")

    # Build SignalR connection. Skip negotiation + force WebSocket transport, per docs.
    connection = (
        HubConnectionBuilder()
        .with_url(
            f"{MARKET_HUB_URL}?access_token={token}",
            options={
                "access_token_factory": lambda: token,
                "skip_negotiation": True,
                # signalrcore reads this to choose transport
            },
        )
        .with_automatic_reconnect(
            {"type": "raw", "keep_alive_interval": 10, "reconnect_interval": 5, "max_attempts": 5}
        )
        .build()
    )

    # Register handlers BEFORE start().
    connection.on("GatewayQuote", on_quote)
    connection.on("GatewayTrade", on_trade)
    connection.on("GatewayDepth", on_depth)

    connection.on_open(lambda: print(f"[{now_iso()}] SignalR OPEN — subscribing..."))
    connection.on_close(lambda: print(f"[{now_iso()}] SignalR CLOSED"))
    connection.on_error(lambda data: print(f"[{now_iso()}] SignalR ERROR: {data.error}"))

    connection.start()

    # Give the connection a moment to open before invoking subscribes.
    await asyncio.sleep(2)

    # Subscribe to the three market streams for our contract.
    try:
        connection.send("SubscribeContractQuotes", [contract_id])
        connection.send("SubscribeContractTrades", [contract_id])
        connection.send("SubscribeContractMarketDepth", [contract_id])
        print(f"[{now_iso()}] Subscribed to Quote/Trade/Depth for {contract_id}")
    except Exception as e:
        print(f"Subscribe error: {e}")
        print("Note: exact subscribe method names may vary — check docs if this fails.")

    # Start the stats reporter alongside.
    reporter = asyncio.create_task(stats_reporter())

    # Run for DURATION_MINUTES then clean up.
    try:
        await asyncio.sleep(DURATION_MINUTES * 60)
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass

    print(f"\n[{now_iso()}] Duration reached — shutting down.")
    reporter.cancel()
    try:
        connection.stop()
    except Exception:
        pass
    csv_file.close()

    # Final summary
    print(f"\n=== FINAL SUMMARY ===")
    for kind in ("Quote", "Trade", "Depth"):
        n = counters[kind]
        gaps = list(inter_gaps[kind])
        print(f"\n{kind}:")
        print(f"  Total messages: {n:,}")
        if gaps:
            mean_gap = statistics.mean(gaps) * 1000
            p50 = statistics.median(gaps) * 1000
            print(f"  Mean inter-arrival gap: {mean_gap:.1f} ms")
            print(f"  Median gap: {p50:.1f} ms")
            if len(gaps) > 20:
                sg = sorted(gaps)
                print(f"  p95 gap: {sg[int(0.95 * len(sg))] * 1000:.1f} ms")
                print(f"  p99 gap: {sg[int(0.99 * len(sg))] * 1000:.1f} ms")
        else:
            print(f"  No messages — stream may not be entitled on your tier.")

    print(f"\nRaw log saved to: {OUTPUT_CSV}")
    print("If Trade count is 0 but Quote count is >0, GatewayTrade is likely gated.")
    print("If ALL counts are 0, subscription didn't take — check method names in docs.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nInterrupted by user.")
