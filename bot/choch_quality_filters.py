"""
FVG quality filter — the ONE filter that survived honest testing.

WHAT IT DOES
------------
Blocks entries whose FVG quality score is below FVG_MIN_QUALITY_SCORE (45).
In practice this rejects exactly one class of setup: a WIDE gap (>= ~1.5pt) that
has gone STALE (price took >= 2 candles to return to it). Nothing else is scored,
weighted, or blocked. Every other setup the CHOCH/FVG pattern produces is taken.

Roughly 4% of otherwise-valid setups are blocked.

WHY THIS ONE AND NOT THE OTHERS
-------------------------------
Four earlier filter designs were built and rejected on the data:
  - v1 four-filter scoring system: blocked 58 collectively-PROFITABLE trades
    worth +$4,961 over Jan-Jul 2026. Blocked-group win rate 48.3% vs 49.5%
    baseline — i.e. it was discarding a random slice of a profitable pool.
  - v2 (market filter removed, time/trend softened): score floor landed at 61.5,
    above the 50 threshold, so it could never reject anything. No-op.
  - v3 (fib removed from scoring): floor dropped to 49.5, still blocked zero real
    trades. No-op.
  - Limit-order entries, and raising MIN_STOP_POINTS to 1.0: both cost money.

A separate train-set feature analysis (388 trades, chronological 80/20 split)
tested hour-of-day, day-of-week, FVG width, swing size, ATR, FVG age, direction,
and position-in-swing. NOT ONE reached statistical significance (best |z| = 1.41
across ~25 buckets, which is what pure chance produces). The strategy's edge is
real but is NOT conditional on anything measurable at entry — which is exactly
why every filter that "sounded right" lost money.

This filter is the single exception, and it was validated properly:

  TRAIN (Jan 2025 - Feb 2026, 388 trades):
      baseline        388 trades, 36.3% WR, $14,541.72
      with filter     373 trades, 37.3% WR, $15,355.12   (+$813)
      blocked group   18 trades, 16.7% WR, -$736.08      (z = -1.74)

  SEALED TEST (Mar - Jun 2026, 121 trades — held out, touched exactly once):
      baseline        117 trades, 55.6% WR, $13,415.48
      with filter     115 trades, 56.5% WR, $13,630.60   (+$215)
      blocked group    3 trades,  0.0% WR, -$322.68

  Direction HELD on unseen data. Every blocked trade in both periods was a
  wide + stale gap.

HONEST LIMITATIONS — READ BEFORE TRUSTING THIS
----------------------------------------------
  * The blocked sample is TINY: 18 train + 3 test = 21 trades, 3 wins. Combined
    z is approximately -1.9, still just under the 1.96 significance bar. This is
    NOT statistically proven. It is weak evidence pointing the same direction
    twice, plus a coherent mechanical story (a 2-3pt gap that price ignored for
    several candles is a genuinely different animal than a tight fresh gap).
  * Expected impact is small: roughly +1-2% on total P&L. Well inside noise.
  * It was deployed as a judgment call on bounded downside (blocks ~4% of trades,
    so if it is noise almost nothing is lost), NOT because the statistics are strong.

THE TEST SET IS SPENT. Do not re-tune this threshold against the 2025-2026
backtest — 45 was already chosen with knowledge of where trades clustered, and
any further tuning on this data is pure curve-fitting. The next honest evaluation
of this filter comes from LIVE trades, not more backtesting. If it is going to be
revisited, it needs new out-of-sample data.

NO MOMENTUM FILTER. NO TIME FILTER. NO TREND FILTER. NO MARKET-STRUCTURE FILTER.
Those were all tested and all cost money. This module scores FVG quality and
nothing else, on purpose.
"""

# The one tunable. Setups scoring below this are skipped.
# DO NOT re-tune against the 2025-2026 backtest — see the docstring.
FVG_MIN_QUALITY_SCORE = 45.0

# Scoring parameters (fixed — these define the score, they are not knobs to sweep)
FVG_OPTIMAL_WIDTH = 0.5      # points — at or below this, full width marks
GAP_STRENGTH_PTS = 0.3       # points — gaps wider than this get a strength bonus
CLEAN_BREAKOUT_PTS = 0.15    # points — gaps wider than this get a clean-breakout bonus


def fvg_quality_score(fvg_width_pts: float, fvg_age_candles: int) -> float:
    """0-100 quality score for an FVG.

    Narrow is better (optimal <= 0.5pt), with bonuses for gap strength and a clean
    breakout, and a penalty for staleness (how many candles elapsed before price
    returned to the gap).

    The practical effect at threshold 45: only WIDE (>= ~1.5pt) AND STALE (age >= 2)
    gaps score low enough to be blocked. A fresh gap of any width passes; a narrow
    gap of any age passes. It takes both problems together to fail.
    """
    if fvg_width_pts <= FVG_OPTIMAL_WIDTH:
        width_score = 100.0
    else:
        width_score = max(0.0, 100.0 - (fvg_width_pts - FVG_OPTIMAL_WIDTH) * 100.0)

    score = width_score * 0.5 + 50.0 * 0.5   # blend width quality against a neutral base

    if fvg_width_pts > GAP_STRENGTH_PTS:
        score += 10.0
    if fvg_width_pts > CLEAN_BREAKOUT_PTS:
        score += 10.0

    # Staleness penalty — price taking longer to return to the gap is a negative signal
    if fvg_age_candles >= 4:
        score -= 20.0
    elif fvg_age_candles == 3:
        score -= 10.0
    elif fvg_age_candles == 2:
        score -= 5.0

    return max(0.0, min(100.0, score))


def evaluate_entry_quality(candles, choch_bias, swing_high, swing_low,
                            fvg_width_pts, fvg_age_candles, pt_time):
    """Single gate: allow the entry iff FVG quality score >= FVG_MIN_QUALITY_SCORE.

    Returns (allowed: bool, score: float, detail: str).

    The `candles`, `choch_bias`, `swing_high`, `swing_low`, and `pt_time` arguments
    are accepted only to keep the call-site signature stable in choch_fvg_bot.py.
    They are DELIBERATELY UNUSED — trend, market-structure, and time-of-day filters
    were all tested and all reduced P&L. Do not quietly reintroduce them here.
    """
    score = fvg_quality_score(fvg_width_pts, fvg_age_candles)
    allowed = score >= FVG_MIN_QUALITY_SCORE

    if allowed:
        detail = f"fvg_quality={score:.0f}"
    else:
        detail = (f"blocked: fvg_quality={score:.0f} < {FVG_MIN_QUALITY_SCORE:.0f} "
                  f"(wide {fvg_width_pts:.2f}pt gap, {fvg_age_candles} candles stale)")
    return allowed, score, detail
