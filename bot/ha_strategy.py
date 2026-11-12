"""
ha_strategy.py — Heikin-Ashi 2-minute strategy CORE (shared logic)
==================================================================
This module holds the STRATEGY DECISIONS ONLY — no broker code, no data feed.
Both the offline backtester (ha_replay.py) and the live bot (ha_mes_bot.py)
import from here, so the two can never silently disagree about what a trade is.

THE STRATEGY (validated on 62 tick-days of ES, Apr 22 – Jul 21 2026)
-------------------------------------------------------------------
Timeframe/session : 2-minute Heikin-Ashi candles, RTH 6:30 AM–1:00 PM PT,
                    no NEW entries after 12:30 PM PT.
Long setup        : two consecutive BULLISH HA candles, BOTH flat-bottomed
                    (no lower wick, l >= ha_open), the second making a higher
                    HA high by >= 2 ticks, AND the HA body midpoint >= session VWAP.
Short setup       : mirror — two BEARISH flat-TOPPED HA candles, lower HA low
                    by >= 2 ticks, body midpoint <= VWAP.
Entry (the fill)  : in the ~30 s before the 2-min candle closes, read the 1-min
                    tick consolidation. Buy at the BOTTOM of that consolidation,
                    sell at the TOP. (Best-case fill in the backtest — see caveats.)
Initial stop      : one tick beyond the prior 2-min candle's extreme
                    (low-1 tick for longs, high+1 tick for shorts).
Target            : nearest 1-minute wick-cluster KEY LEVEL — 2+ candle wicks
                    stacked within CLUST points; TP placed 2 ticks in FRONT of
                    the shorter wick of the cluster. Skip the trade if no such
                    level exists at least MIN_TP_POINTS away.
Exit management   : BE@30 — once price travels BE_TRIGGER_FRAC (30%) of the way
                    to target, move the stop to breakeven (entry). This is the
                    single change that cut backtest max drawdown ~4x while
                    slightly RAISING net. It is the whole reason the live bot
                    actively manages the stop instead of using a static bracket.

INSTRUMENT NOTE: the price STRUCTURE (points) is identical on ES and MES — they
track the same index. Only the dollar-per-point differs ($50 ES / $5 MES). So
every distance here is in POINTS, and MIN_TP_POINTS stays 2.0 regardless of
instrument. The "$100 minimum target" figure from the original spec was just the
ES dollar value of that 2.0-point floor; do NOT re-scale it to a dollar figure on
MES or you change the strategy.
"""

from datetime import time

# ── Tunable strategy parameters (shared) ─────────────────────────────────────
TICK              = 0.25     # ES/MES tick size (points)
SESSION_OPEN      = time(6, 30)    # PT — first bar considered
ENTRY_CUTOFF      = time(12, 30)   # PT — no NEW entries after this
SESSION_CLOSE     = time(13, 0)    # PT — force flat / stop building bars

BREAK_TICKS       = 2        # 2nd HA candle must extend the HA high/low by >= this many ticks
CONSOL_WINDOW_SEC = 30       # seconds before the 2-min close used to read the entry consolidation
CLUST             = 0.75     # wick tips within this many points count as one "key level" cluster
# A 1-min candle's wick must be >= this to count toward a key-level cluster.
#
# TRIED AND REVERTED 2026-08-12. Raising this to 1.0 was an attempt to stabilise the
# TARGET across data sources. The problem it was aimed at is real: comparing the
# Databento-tick replay with the TopstepX 1-min replay on 125 exactly-matched trades,
# ENTRIES agreed 57% and STOPS 63% (median difference 0.00pt) but TARGETS agreed only
# 36%, with differences up to 18 points; those same trades swung +$1,455 vs -$322.
#
# The fix FAILED. At 1.0: tick went +$3,455 -> +$3,259, but TopstepX went
# -$304 -> -$909, and target agreement moved only 36% -> 39% (max diff 18 -> 23pt).
# The reasoning was backwards — a HIGHER threshold means FEWER qualifying wicks, so
# each cluster rests on fewer points and the level becomes MORE sensitive, not less.
#
# CONCLUSION: cluster_target is not stabilisable by tuning this constant. Two
# independent datasets of the same market disagree on where the key levels are, which
# is why the tick backtest (+$3,259) and the TopstepX backtest (-$909) cannot both be
# predictive of live behaviour. Do not tune this again without a way to measure
# level agreement across feeds directly.
MIN_WICK          = 0.25
MIN_TP_POINTS     = 2.0      # target must be at least this far from entry (points), else skip
TP_FRONT_TICKS    = 2        # place TP this many ticks in FRONT of the cluster level

BE_TRIGGER_FRAC   = 0.30     # move stop to breakeven once price travels this fraction toward target
BE_LOCK_FRAC      = 0.00     # 0.0 = pure breakeven. Set e.g. 0.25 to lock in 25% of target instead.

# Minimum distance the BE trigger may sit from entry, in POINTS. Added 2026-08-12
# after the live DRY session: two trades with 3.5-4.0pt targets put the 30% trigger
# only 1.05-1.20pt away — inside normal noise — so breakeven armed within 6-11
# seconds and both scratched at -$4. A floor stops BE arming on a wiggle while
# keeping the drawdown protection that makes this survivable.
#   Tick backtest 6/16-7/21, limit entry, 1-tick-through fills:
#     floor 0.0 : WR 39.4%  PF 2.60  +$2,836  DD -$185
#     floor 1.5 : WR 41.9%  PF 2.56  +$2,926  DD -$185   <-- same DD, better net
#     floor 2.0 : WR 45.5%  PF 2.38  +$2,957  DD -$231
#     BE OFF    : WR 60.1%  PF 1.62  +$2,599  DD -$499   <-- do NOT do this
# Removing BE entirely raises the win rate but nearly triples drawdown for no
# extra profit. The scratches are the cost of the drawdown protection.
BE_MIN_TRIGGER_PTS = 1.5

# ── VWAP anchor ──────────────────────────────────────────────────────────────
# WHERE the session VWAP starts accumulating. This is a REAL strategy parameter,
# not a display setting: the entry filter is "long only above VWAP / short only
# below", so moving the anchor moves the line and changes which signals pass.
#   "RTH" — anchor at SESSION_OPEN 6:30 PT. Every validated backtest number
#           (ES x1 +$36,204 / MES x3 +$9,776, 6/16-8/11) was produced this way.
#   "CME" — anchor at the 3:00 PM PT CME session open of the PRIOR day, which is
#           what the TopstepX platform chart displays. VWAP therefore already has
#           a value at 6:30 instead of building from scratch.
# Only VWAP accumulation is affected. The Heikin-Ashi chain, the wick-cluster
# target levels, and the trading window itself stay RTH-only under both settings.
VWAP_ANCHOR       = "RTH"          # "RTH" | "CME"
CME_OPEN          = time(15, 0)    # 3:00 PM PT — CME/TopstepX session open

# ── ASIA-SESSION VARIANT (5-minute, volatility-gated) ────────────────────────
# The RTH edge does NOT transfer to the overnight session as-is — Asia has ~1/3
# the range and ~1/10 the liquidity, so the strategy only works on the ACTIVE
# nights. These parameters implement that: trade 5–7:30pm PT off 5-minute HA
# candles, anchor VWAP at the 3pm PT CME open (matches TopstepX), target the
# nearest key level pooled from the prior NY session + the overnight, and — the
# key filter — only take a trade when 5-minute ATR is at least ASIA_ATR_MIN.
# Below that threshold every backtested night lost; above it is where all the
# profit lives. MAX_TP caps how far a target can be (Asia can't travel to a
# distant level, so those setups are skipped).
ASIA_VWAP_ANCHOR  = time(15, 0)    # 3pm PT — CME/TopstepX session open (VWAP resets here)
ASIA_TRADE_OPEN   = time(17, 0)    # 5pm PT — first entry
ASIA_ENTRY_CUTOFF = time(19, 30)   # 7:30pm PT — no new entries after this
ASIA_CLOSE        = time(20, 0)    # 8pm PT — force flat
ASIA_TIMEFRAME    = "5min"
ASIA_ATR_MIN      = 4.0            # skip the trade unless 5-min ATR >= this (the activity gate)
ASIA_ATR_LOOKBACK = 4             # bars used for the ATR estimate
ASIA_MAX_TP_POINTS = 20.0         # skip if the nearest key level is farther than this


def atr_points(highs, lows, i, lookback=ASIA_ATR_LOOKBACK):
    """Simple 5-min ATR proxy: mean bar range (high-low) over the last `lookback`
    bars up to and including bar i. Used as the Asia activity gate."""
    import numpy as _np
    a = max(0, i - lookback + 1)
    return float(_np.mean(_np.asarray(highs[a:i+1]) - _np.asarray(lows[a:i+1])))


# ── Heikin-Ashi ──────────────────────────────────────────────────────────────
def heikin(o, h, l, c, prev_ha_open=None, prev_ha_close=None):
    """Single-candle Heikin-Ashi transform. Pass the PRIOR candle's ha_open/ha_close
    to chain them live; for the first candle of a session pass None (seeds ha_open
    at (o+c)/2). Returns (ha_open, ha_high, ha_low, ha_close)."""
    ha_close = (o + h + l + c) / 4.0
    if prev_ha_open is None or prev_ha_close is None:
        ha_open = (o + c) / 2.0
    else:
        ha_open = (prev_ha_open + prev_ha_close) / 2.0
    ha_high = max(h, ha_open, ha_close)
    ha_low  = min(l, ha_open, ha_close)
    return ha_open, ha_high, ha_low, ha_close


def ha_flags(ha_open, ha_high, ha_low, ha_close, raw_high, raw_low):
    """Return (is_bull, is_bear, flat_bottom, flat_top) for one HA candle."""
    is_bull = ha_close > ha_open
    is_bear = ha_close < ha_open
    flat_bottom = raw_low  >= ha_open      # no lower wick — strong up candle
    flat_top    = raw_high <= ha_open      # no upper wick — strong down candle
    return is_bull, is_bear, flat_bottom, flat_top


# ── Entry pattern test (evaluated at a 2-min candle close) ───────────────────
def entry_signal(prev, cur, vwap):
    """Given the two most recent CLOSED 2-min HA candles and current session VWAP,
    return 'long', 'short', or None.

    Each candle arg is a dict with keys:
        ha_open, ha_high, ha_low, ha_close, raw_high, raw_low
    `cur` is the just-closed candle, `prev` the one before it.
    """
    pb_bull, pb_bear, pb_fb, pb_ft = ha_flags(prev["ha_open"], prev["ha_high"], prev["ha_low"],
                                              prev["ha_close"], prev["raw_high"], prev["raw_low"])
    cb_bull, cb_bear, cb_fb, cb_ft = ha_flags(cur["ha_open"], cur["ha_high"], cur["ha_low"],
                                              cur["ha_close"], cur["raw_high"], cur["raw_low"])
    body_mid = (cur["ha_open"] + cur["ha_close"]) / 2.0

    long_ok = (cb_bull and pb_bull and cb_fb and pb_fb
               and cur["ha_high"] >= prev["ha_high"] + BREAK_TICKS * TICK
               and body_mid >= vwap)
    short_ok = (cb_bear and pb_bear and cb_ft and pb_ft
                and cur["ha_low"] <= prev["ha_low"] - BREAK_TICKS * TICK
                and body_mid <= vwap)
    if long_ok:
        return "long"
    if short_ok:
        return "short"
    return None


# ── Entry price from the last-30-seconds consolidation ───────────────────────
def consolidation_entry(consol_prices, is_long):
    """consol_prices: iterable of trade prices in the ~30 s before the 2-min close.
    Buy at the bottom of that range, sell at the top. Returns the entry price, or
    None if there were no ticks (nothing to enter from — skip the trade)."""
    if not len(consol_prices):
        return None
    return min(consol_prices) if is_long else max(consol_prices)


# ── Wick-cluster key-level target ────────────────────────────────────────────
def cluster_target(wick_tips, entry, is_long):
    """wick_tips: 1-minute candle wick extremes on the target side (upper wick highs
    for longs, lower wick lows for shorts), taken from bars BEFORE entry. Finds the
    nearest cluster of 2+ tips within CLUST points and returns (level, target_price),
    or None if no qualifying cluster sits at least MIN_TP_POINTS away.

    The TP is placed TP_FRONT_TICKS in front of the cluster (just short of it) so a
    resting limit fills before price has to punch through the level."""
    tips = [t for t in wick_tips if (t > entry if is_long else t < entry)]
    tips.sort(reverse=not is_long)   # ascending for longs, descending for shorts
    if len(tips) < 2:
        return None
    grp = [tips[0]]
    for t in tips[1:]:
        if abs(t - grp[-1]) <= CLUST:
            grp.append(t)
        else:
            hit = _finish_group(grp, entry, is_long)
            if hit:
                return hit
            grp = [t]
    return _finish_group(grp, entry, is_long)


def _finish_group(grp, entry, is_long):
    if len(grp) < 2:
        return None
    level = min(grp) if is_long else max(grp)
    tp = level - TP_FRONT_TICKS * TICK if is_long else level + TP_FRONT_TICKS * TICK
    dist = (tp - entry) if is_long else (entry - tp)
    if dist >= MIN_TP_POINTS:
        return level, round(tp, 2)
    return None


# ── BE@30 stop-move trigger ──────────────────────────────────────────────────
def breakeven_stop(entry, target, is_long):
    """Return the price level at which, once touched, the stop should move to
    breakeven (BE_LOCK_FRAC of the way to target — 0.0 = entry). Returns
    (trigger_price, new_stop_price).

    The trigger is BE_TRIGGER_FRAC of the way to target, but never CLOSER than
    BE_MIN_TRIGGER_PTS — see the note on that constant."""
    dist = abs(target - entry)
    need = max(BE_TRIGGER_FRAC * dist, BE_MIN_TRIGGER_PTS)
    if is_long:
        trigger = entry + need
        new_stop = entry + BE_LOCK_FRAC * dist
    else:
        trigger = entry - need
        new_stop = entry - BE_LOCK_FRAC * dist
    return round(trigger, 2), round(new_stop, 2)


def initial_stop(prev_2min_low, prev_2min_high, is_long):
    """One tick beyond the prior 2-min candle's extreme."""
    return round(prev_2min_low - TICK, 2) if is_long else round(prev_2min_high + TICK, 2)


# ── REMOVED 2026-08-12: BE-reachability filter ───────────────────────────────
# A filter was briefly added here that skipped trades where the BE@30 trigger
# distance exceeded a multiple of the stop distance. It was tuned on 27 tick-
# days (6/15-7/21) where it looked net positive, but on the full TopstepX
# window (6/16-8/11) it made things WORSE: 346 trades / $8,974 MES x3 filtered,
# vs roughly $9.7k unfiltered (Ruth's figure from the prior run; the filtered
# run is the measured one). That's overfitting to a small sample — the ratio
# flags trades that are structurally risky, but it does not predict which ones
# actually lose, and cutting them removes too many eventual winners. Do not
# re-add without validating on the full window first.
