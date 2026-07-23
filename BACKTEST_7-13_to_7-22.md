# New-config backtest: 7/13 → 7/22/2026
_Generated 2026-07-22. Config: **1 contract, 2.0pt stop / 3.0pt target, 3 trades/day + stop@2 losses, delta OFF, Version B ON.**_
_Purpose: a baseline to compare the live dry-run against over the same kind of window._

## How this was produced (read before trusting the numbers)
- **7/13–7/21:** your actual engine (`choch_databento_replay` → `CHOCHFVGReplayWrapper`) run on
  1-minute bars I built from Databento **tick** data (my 1-min bar file ends 7/14, so 7/15–7/21
  were resampled from ticks). These are the engine's **bar-confirmed** entries — the same basis
  as every backtest in this project. The engine replay is inherently delta-off / Version-B.
- **7/22:** today's **actual live trades** (tick entries) repriced to the new geometry. Both of
  today's setups stop out even at a 2pt stop (verified), so −$207.56 is correct for today.
- Mixing note: 7/13–7/21 are bar-confirmed entries; 7/22 is live tick entries. The live tick bot
  fires a little more often than the bar engine, so the last day isn't perfectly apples-to-apples.

## Daily results

| date | trades | W | L | day P&L | cumulative |
|------|-------:|--:|--:|--------:|-----------:|
| 2026-07-13 | 2 | 2 | 0 | +$292.44 | $292.44 |
| 2026-07-14 | 1 | 1 | 0 | +$146.22 | $438.66 |
| 2026-07-15 | 2 | 0 | 2 | −$207.56 | $231.10 |
| 2026-07-16 | 3 | 3 | 0 | +$438.66 | $669.76 |
| 2026-07-17 | 1 | 1 | 0 | +$146.22 | $815.98 |
| 2026-07-20 | 0 | — | — | $0.00 | $815.98 |
| 2026-07-21 | 0 | — | — | $0.00 | $815.98 |
| 2026-07-22 | 2 | 0 | 2 | −$207.56 | $608.42 |

## Totals
- **Trades:** 11 (7 wins / 4 losses) — **63.6% win rate**
- **Net P&L (0 slippage):** +$608.42
- **Net P&L (0.25pt slippage, realistic):** +$470.92
- **Max drawdown:** −$207.56 (a single two-loss day)
- Pace: ~$471 over 8 trading days ≈ roughly on a ~$1,000/month track.

## Trade log
| date | time (PT) | dir | result | P&L |
|------|-----------|-----|--------|----:|
| 7/13 | 08:04 | short | Target hit | +$146.22 |
| 7/13 | 10:52 | short | Target hit | +$146.22 |
| 7/14 | 12:03 | short | Target hit | +$146.22 |
| 7/15 | 07:35 | long | Stop loss | −$103.78 |
| 7/15 | 08:10 | long | Stop loss | −$103.78 |
| 7/16 | 10:13 | short | Target hit | +$146.22 |
| 7/16 | 11:07 | short | Target hit | +$146.22 |
| 7/16 | 11:25 | short | Target hit | +$146.22 |
| 7/17 | 06:55 | long | Target hit | +$146.22 |
| 7/22 | 08:29 | long | Stop loss | −$103.78 |
| 7/22 | 09:49 | short | Stop loss | −$103.78 |

## Caveats (don't over-read this)
1. **8 trading days / 11 trades is a tiny sample — this is noise, not proof.** The 63.6% win
   rate is above the ~57% this config does over the full Jan-2025→present history, so this was a
   *favorable* window. Do not annualize it.
2. **7/20 and 7/21 had zero qualifying setups.** The strategy sits out some days; that's normal.
3. Still a backtest with idealized fills (the 0.25pt-slip column is the honest one). The live
   dry-run is what confirms whether real fills and live setups match this.
4. The caps behaved correctly: 7/16 capped at 3 trades (all wins), 7/15 and 7/22 stopped at 2
   losses (−$207.56, the worst-case day).

## What to compare against
Run the live dry-run (`python intrabar_es_bot.py --dry-run --no-delta-filter`) and, at the end of
a comparable stretch, run `analyze_choch_log.py choch_bot.log --since <start>` — then check the
live win rate, $/day, and worst day against this table. Watch especially whether live fills slip
worse than 0.25pt, which is the main thing that would pull live below this.
