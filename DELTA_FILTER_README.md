# Order-Flow Delta Filter — dry-run build

## What this is
A new false-start filter for the CHOCH/FVG intrabar bot. It only lets a tick
entry fire when the trailing **5-minute cumulative delta** (net aggressive
buying vs selling) **confirms** the trade direction:

- LONG  fires only if 5-min delta > 0 (buyers stepping in at the level)
- SHORT fires only if 5-min delta < 0 (sellers stepping in)

**Backtest basis (3 months of ES trades, Databento, Apr–Jul 2026):**
delta AGREES = ~64% win, +$1,969 (14 trades) · delta OPPOSES = ~24% win,
−$2,319 (72 trades). The split held in every third of the sample.

## Files
- `delta_filter.py`  — NEW. The `DeltaTracker` (rolling delta from the tick feed).
- `intrabar_es_bot.py` — MODIFIED. Feeds every GatewayTrade tick to the tracker
  and gates tick entries on delta confirmation. Back up your old copy first.

## Run it (dry-run — NO orders placed)
    python intrabar_es_bot.py --dry-run
The delta filter is ON by default. Add `--no-delta-filter` to turn it off.

## FIRST thing to check (important)
On startup the log prints up to 20 lines tagged `[DELTA-TICK-SAMPLE ...]` showing
the RAW GatewayTrade payload for the first ticks. **Copy those lines to me.**
They tell us:
  1. what fields the live feed actually provides (price / size / side), and
  2. whether delta is being signed by a real aggressor-side field or by the
     schema-free tick rule (uptick=buy).
Delta should move WITH price. If the samples show a clean side field we'll lock
it in; if they look inverted, tell me and I'll flip the mapping.

## What you'll see while it runs
- `[DELTA-OK] 5-min delta=+123 confirms long`  → a confirmed entry (taken)
- `[DELTA-REJECT] ... delta=-45 does NOT confirm` → a false start (skipped)
Expect roughly 5–6 rejects for every 1 take (the 84%/16% split from the backtest).

## The honest status — read this
- The profitable side rests on **14 backtest trades over one quarter**. It is
  PROMISING, not proven. This build exists to **collect forward dry-run data**,
  not because the edge is confirmed.
- Live signing uses the **tick rule** unless the feed gives a clean side field —
  a proxy for the exact aggressor side the backtest used. The dry-run tells us
  whether that proxy behaves the same.
- Plan: run in dry-run ~2–4 weeks, then compare the `[DELTA-OK]` trades' outcomes
  to the backtest's 64%. Only after that holds up should real size be considered,
  and then 1 contract first.
