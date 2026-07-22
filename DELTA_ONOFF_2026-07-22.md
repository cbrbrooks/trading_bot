# Delta ON vs OFF, tick-honest — does dropping the filter help the monthly number?
_Run 2026-07-22. 79 version-B entries, 2026-04-22 → 07-14 (~2.7 months), 2 contracts, ES.
Outcomes are TRUE tick-path first-passage (not optimistic 1-min fills). Delta = your live
300s `DeltaTracker`. "OFF" = take all 79; "ON" = take only the 58 the tracker agreed with._

## Direct answer: dropping delta ADDS money, at every stop setting

| config (tick-honest) | delta OFF (all 79) | delta ON (agree 58) | dropping delta |
|---|---|---|---|
| **1pt / 3pt (current)** | 49.4% WR · **~$2,601/mo** | 48.3% WR · ~$1,817/mo | **+$784/mo** (21 more trades) |
| 1.5pt / 3pt | 57.0% WR · ~$2,858/mo | 58.6% WR · ~$2,257/mo | +$601/mo |
| 2pt / 3pt | 63.3% WR · ~$3,151/mo | 65.5% WR · ~$2,550/mo | +$601/mo |

The 21 trades the delta gate removes are **as good or better** than the ones it keeps
(delta-*oppose* trades actually had higher per-trade expectancy: $102 vs $86 at 1/3). So the
filter is pure subtraction — fewer trades, same quality, less money. It confirms the
validation study: delta isn't earning its keep.

## But two honest reality checks pull the numbers down hard

**1. The edge is decaying — the recent half is much weaker.**
Half-split, delta OFF, current 1pt/3pt:
- H1 (Apr–May): 56.4% WR, **~$1,687/mo**
- H2 (Jun–Jul): 42.5% WR, **~$915/mo** ← below the $1,000 target already

The flattering ~$2,600/mo full-period figure is carried by the spring. The **recent six weeks
run ~$915/mo** at the current stop — and that's *before* slippage.

**2. Slippage matters a lot for a stop this tight** (many trades, each slips on the stop):

| | full period 1/3 | recent half (H2) 1/3 |
|---|---|---|
| 0.00pt slip | ~$2,601/mo | ~$915/mo |
| 0.25pt slip | ~$1,878/mo | **~$549/mo** |
| 0.50pt slip | ~$1,155/mo | ~$182/mo |

At a realistic 0.25pt round-turn slippage, the **recent half runs ~$550/mo — well under target.**
Delta ON is worse still: in H2 with 0.25pt slippage it's ~$154/mo, and goes **negative** at
0.5pt. Dropping delta helps even here (+~$400/mo in the recent half), but neither version clears
$1,000/mo recently once fills are honest.

Wider stops hold up better against slippage (fewer, bigger winners): 2pt/3pt full-period is
~$2,428/mo even at 0.25pt slip, and the recent half ~$622/mo.

## What this means

- **Drop or deprioritize delta.** It costs ~27% of your trades for no win-rate gain, subtracts
  ~$500–780/mo, and in the recent tape it's near breakeven once slippage is honest. Keep the
  trades.
- **The stop is the real lever, not delta.** Widening toward 1.5–2pt lifts win rate and monthly
  P&L across every cut — but 2pt **doubles per-trade risk to ~$207**, which breaks your
  `DAILY_LOSS_LIMIT=230` and raises Topstep trailing-drawdown exposure. It's a P&L win that
  requires re-scoping the risk gates (and maybe dropping to 1 contract), not a free toggle.
- **Temper the forward number.** Honestly modeled (tick stops + 0.25pt slippage), plan around
  **~$1,000/mo at best in current conditions, not $2–3k.** The strategy has a real but modest and
  softening edge. The full-period backtest overstates what the last month actually did.

## Caveats (so you can weight this correctly)
- Slippage is applied as a flat symmetric per-trade haircut. Your entry is a **resting limit** at
  the FVG midpoint, so entry slippage is likely small/favorable, while the **stop is a market
  exit** that slips more — so real results sit somewhere in the 0–0.5pt band, probably closer to
  the 0.25pt column.
- The daily loss-based caps (2-loss stop, daily-loss limit) aren't re-simulated on tick outcomes;
  they'd trim some losing days, nudging real results up slightly.
- Backtest on version-B bar entries with idealized fills; the **live collect-all dry run is the
  real test.** You now know exactly what to log there: delta-at-entry + outcome, stop width, and
  actual fill slippage vs the midpoint/stop.

## Reproduce
`python compare_delta_onoff.py` (needs the staged tick files, `mae_study.py`,
`choch_replay_2026-04-22_2026-07-14.json`, and `delta_results.json`).
