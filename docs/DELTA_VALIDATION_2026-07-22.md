# Delta-Filter Validation — is the "breakthrough" edge really there?
_Run 2026-07-22. Your **actual** `DeltaTracker` (delta_filter.py) fed Databento front-month
ticks; snapshot at each of **79** engine version-B entries, 2026-04-22 → 2026-07-14 (60 days)._

## How this was done (why it's trustworthy)
- Entries: I ran your **real engine** (`choch_databento_replay.py` → `CHOCHFVGReplayWrapper`)
  over 3 months of 1-min data → 79 version-B trades (45W/34L, 57.0% WR, +$9,503 on the bar model).
- Delta: I fed those days' Databento trade ticks into your **live `DeltaTracker` class itself**
  — same trailing window, same polarity auto-calibration — one tracker per day, run continuously
  through the session, and read `agrees(direction)` at each entry's fill. This is literally the
  live code, not a hand-rolled copy.
- Polarity verified: the tracker locks `sign_mult=+1` (cov +70 on 7/08) — buy aggressor
  correlates with upticks, exactly right. The result is **not** a sign-flip artifact.

## Result: at the live 300s window, delta agreement does NOT predict winners

| trailing window | AGREE (bot would take) | OPPOSE (bot would skip) | Fisher p |
|---|---|---|---|
| 60s  | 59.3% WR (n=27) | 55.8% WR (n=52) | 0.81 |
| 120s | 58.0% WR (n=50) | 55.2% WR (n=29) | 0.82 |
| **300s (live default)** | **56.9% WR (n=58)** | **57.1% WR (n=21)** | **1.00** |
| 600s | 60.0% WR (n=50) | 51.7% WR (n=29) | 0.49 |

At the window the bot actually uses (300s), delta-agree and delta-oppose win at the **same
rate** (56.9% vs 57.1%). No other window reaches significance either (all p ≥ 0.49).

**What the gate actually does (300s):**
- Take all 79: 57.0% WR, **$120.3/trade**.
- Take only delta-agree (58): 56.9% WR, **$120.0/trade**.
- So gating **removes 27% of trades (79→58) and improves expectancy by −$0.3/trade** — i.e.
  it throws away roughly a quarter of your trade frequency for no win-rate or expectancy gain.
  Given the ~1–3 trades/day and $1,000/month goals, fewer trades for no edge works against you.

## The edge, if any, is not stable — it flips by period
Half-split at 300s:
- **H1 (Apr–May):** agree 64.5% (n=31) vs oppose 50.0% (n=8) — agree looks good.
- **H2 (Jun–Jul):** agree 48.1% (n=27) vs oppose 61.5% (n=13) — agree looks *bad*.

An effect that is strongly positive in one half and negative in the other, netting to zero,
is the signature of **noise, not edge**.

## Correction to my earlier note
In the stop study I flagged that delta-agree *under-performed* (36% vs 62%) and said it needed
reconciling. Reconciled: that was the **June–July window only** — the faithful tracker
reproduces it there (45.8% vs 63.6% on 6/08–7/14) — but it **reverses in April–May**, so the
honest read is not "delta is backwards," it's "**delta has no stable edge either way**" on this
data. My earlier alarm over-weighted one window; this larger, faithful test supersedes it.

## Honest caveats (what could still rescue delta)
1. **Feed difference.** This uses the Databento tape; the live bot gates on the **TopStepX**
   feed. Aggressor tagging could differ — though Databento is the cleaner source and polarity
   checks out, so I wouldn't bet the effect reappears on the live feed.
2. **Selector vs live gate.** I tested delta as a filter over the engine's *bar* entries. The
   live gate fires at the *tick* touch and may take somewhat different trades. So this refutes
   the **core claim** — "delta agreement predicts CHOCH-entry success" — rather than the exact
   tick-gated bot. It's possible (untested) that delta helps specifically at the tick level by
   filtering the fast stop-outs the earlier study found; that needs live tick data to check.
3. **n=79.** Decent, not huge — but the 300s null (p=1.00) is a clean null, not low power.
4. Absolute WR here (57%) is the **optimistic 1-min bar model**; the tick study showed the 1pt
   stop is more fragile (~44% at true tick granularity). The agree-vs-oppose comparison is
   apples-to-apples within the bar model, so the null still holds.

## Bottom line & recommendation
On 3 months of data, using your own tracker, **the delta filter is not adding edge at the live
setting** — agree ≈ oppose ≈ take-everything, and it costs ~27% of your trades. It isn't
*hurting* P&L per trade, but it isn't the separator it was believed to be, and the trade-count
cut fights your monthly target.

Do **not** treat delta as load-bearing on the strength of the backtest. Two concrete paths:
1. **Validate on the live feed, directly.** In the collect-all dry run, log delta at every
   entry and the outcome, and re-run this same agree-vs-oppose test on the TopStepX feed. If it's
   flat there too (likely), retire or de-emphasize the filter.
2. **Re-focus on what did show signal:** the stop mechanics. The 1pt stop's noise-stops
   (prior study) are a bigger, more robust lever than delta. That's where to spend effort.

## Reproduce
`python delta_validation.py` (needs the staged tick files + `choch_replay_2026-04-22_2026-07-14.json`
from `choch_databento_replay.py`). Per-entry output: `delta_results.json`.
