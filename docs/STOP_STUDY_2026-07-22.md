# Stop-vs-Target / Adverse-Excursion Study — ES CHOCH+FVG bot
_Run 2026-07-22. Tick-level (Databento GLBX **ESU6** trades), 41 version-B entries, 2026-06-08 → 2026-07-21._

## What this answers
You asked whether the 1-point stop is getting tagged by noise *before* the move that
would have hit the 3-point target — and whether a different stop would help. I took the
**41 entries your 1-minute replay already produced** (the engine's exact version-B trades)
and walked the **true tick path** after each one, using Databento ESU6 trade ticks. Tick
granularity removes the 1-minute replay's "which hit first" guess (its pessimistic
stop-first rule), so this is a sharper read than the bar backtest.

Money model matches the bot: $50/pt × 2 contracts, $7.56 round-turn fees. Fixed 1.0pt
stop / 3.0pt target unless stated.

## Headline findings

**1. The false-stop effect is real and large.**
Of the trades a 1pt stop turns into losers, **~60% would have reached the 3pt target within
15 minutes** of being stopped (14/23 on the full set; 11/20 post-roll; 7/12 in July). The
1pt stop is the dominant loss mechanism — it's cashing in noise, not protecting you from
real adverse moves.

**2. A 1pt stop knocks out about half of its own eventual winners.**
Of the 38/41 entries that *ever* print +3pt, **53% first dig ≥1.0pt against the entry**
before getting there (median dig-against-first = 1.19pt). So roughly half the real winners
have to survive a >1pt wiggle the current stop doesn't give them.

**3. Widening the stop raises win rate monotonically — and it holds out-of-sample.**
Keeping the 3pt target and widening the stop (this lowers RR):

| stop | target | RR | WR (all 41) | WR (post-roll 37) | WR (July 23) | exp/trade (all) |
|------|--------|----|-------------|-------------------|--------------|-----------------|
| 1.0  | 3.0 | 3.0 | 43.9% | 45.9% | 47.8% | $68 |
| 1.25 | 3.0 | 2.4 | 46.3% | 48.6% | 52.2% | $64 |
| 1.5  | 3.0 | 2.0 | 48.8% | 51.4% | 52.2% | $62 |
| 2.0  | 3.0 | 1.5 | **56.1%** | **59.5%** | **65.2%** | $73 |

The win-rate lift is consistent across the full set, the liquid post-roll set, and clean
July — it is **not** curve-fit to one window.

## The important caveats (why I'm not telling you to just widen the stop)

- **Win rate improves; expectancy barely does.** The baseline 1.0/3.0 is already positive
  and is the most *half-stable* config (H1 exp $52 / H2 exp $83). Widening mostly converts
  noise-stops into wins but gives back RR, so net P&L moves only a little. The variants that
  boosted *net* the most (e.g. matched-RR 2.0/6.0) **failed the half-split** — great in one
  half, collapsed in the other. Those are curve-fit; discard them.

- **A 2pt stop doubles your per-trade risk.** Max loss goes from ~$107 to ~$207 (2 contracts).
  That **breaks your current risk gates**: `DAILY_LOSS_LIMIT=230` is now ~one losing trade,
  and two 2pt losses = ~$414. It also raises Topstep trailing-drawdown exposure. You cannot
  drop in a 2pt stop without re-scoping the daily-loss limit / consecutive-loss logic and
  probably cutting size. It is not a free lunch.

- **Sample is 41 trades over ~6 weeks**, on the version-B population (not the delta-gated
  subset), reconstructed on **databento ESU6**, not the live TopStepX feed.

## Flag worth more of your attention than the stop: the delta signal did NOT reproduce

Splitting the same 41 entries by whether the trailing 5-min **delta agreed** with the trade
(polarity auto-calibrated from price action, same idea as the live tracker):

| subset | delta-AGREE WR | delta-OPPOSE WR |
|--------|----------------|-----------------|
| all 41 | 36% (n=28) | 62% (n=13) |
| post-roll | 38% (n=26) | 64% (n=11) |
| **July only** | **44% (n=18)** | **60% (n=5)** |

On this reconstruction, delta-*agreement* under-performed delta-*opposition* in **every**
subset, including clean July tape — the **opposite** of the delta thesis. I do **not** claim
this refutes your delta breakthrough: this is reconstructed from databento ESU6, not the
live TopStepX feed the bot actually gates on; the windows/fills are modeled; and the oppose
group is tiny (5–13). But it persists off the thin early-June tape, so I can't dismiss it as
noise either. **Recommendation: re-validate the delta filter on the exact live feed data
before relying on it.** If the delta edge is as fragile as this suggests, that matters far
more than any stop tweak.

## What I'd actually do

1. **Don't overhaul the stop live off 41 trades.** The honest read is "1pt is too tight and
   costs you winners," not "here's the magic number."
2. **Test a modest widening (1.25–1.5pt), forward, in the collect-all dry run** — it pushes
   WR toward your ~50% target with roughly neutral expectancy and only a small per-trade risk
   bump you can absorb within a re-scoped daily limit. Treat **2.0pt as a separate, higher-risk
   regime** (higher WR, ~$207 risk/trade, gates must be re-scoped) — interesting, not casual.
3. **Reconcile the delta signal on the live feed.** This is the higher-priority open question.
4. Re-run this study on the **delta-gated live sample** once the dry-run week fills in.

## Reproduce
`python mae_study.py` (needs the staged GLBX ESU6 tick files + the replay JSON). Per-trade
output is in `mae_results.json`.
