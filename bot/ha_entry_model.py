"""
ha_entry_model.py — predicted-depth entry level for ha_mes_bot.py

WHAT THIS IS
------------
At T = 30 seconds before the 2-min signal candle closes, this predicts how far
price is about to travel AGAINST the signal direction, and returns the limit price
to rest at. Fill there and you buy the dip / sell the rip instead of paying the
close.

WHY IT IS ONE LINE OF ARITHMETIC AND NOT A MACHINE-LEARNING MODEL
-----------------------------------------------------------------
A 13-feature gradient-boosted model was tested against a one-feature linear fit on
the same walk-forward protocol. The one-feature version WON (R2 +0.218 vs +0.181).
Everything the big model learned beyond "deeper retracements happen when the tape
is moving faster" was noise it had memorised. So this ships the relationship that
actually generalised, and nothing else:

    predicted_depth_points = 1.22 * ATR14(1-minute bars)

Fitted on 321 signals across 43 sessions. Walk-forward out-of-sample R2 = +0.22,
correlation +0.47, which is 8.4 standard deviations from zero. The ATR-to-depth
correlation was 0.501 in the June-July window and 0.474 in the April-May window —
the stability across two opposite-trending regimes is the reason to believe it.

STATUS: NOT PROVEN PROFITABLE. The full walk-forward backtest returned +$847 over
31 days at MES x3 (PF 1.40, max drawdown -$574), but on only 91 filled trades that
is t = 1.10, p = 0.28 — consistent with no edge at all. It needs roughly 90 more
trading days to settle. Run it in DRY_RUN and let it collect them. Do not size it
on the strength of the backtest.
"""

# Fitted on 321 signals, 43 sessions (2026-04-22 .. 2026-07-21), MES/ES.
DEPTH_INTERCEPT = -0.14
DEPTH_PER_ATR = 1.22

# Fraction of the predicted depth to actually use. Below 1.0 on purpose: the error
# is one-sided. Resting shallower than the true extreme still FILLS (at a slightly
# worse price); resting deeper misses the trade completely. The walk-forward
# procedure selected 0.6-0.8 on every refit, so 0.7 is the middle of what the data
# chose rather than a number picked by hand.
ALPHA = 0.7

# Guard rails. A prediction outside these is a sign the ATR estimate is broken
# (thin tape, a gap, a bad seed) rather than a genuine forecast.
MIN_OFFSET_PTS = 0.75
MAX_OFFSET_PTS = 8.0


def predicted_depth(atr_1min):
    """Points price is expected to travel against the signal within ~5 minutes."""
    if atr_1min is None or atr_1min <= 0:
        return None
    return DEPTH_INTERCEPT + DEPTH_PER_ATR * float(atr_1min)


def entry_level(price_now, atr_1min, is_long, alpha=ALPHA):
    """Where to rest the entry limit. Returns None if ATR is unusable — the caller
    should then fall back to its existing entry rule rather than guessing."""
    d = predicted_depth(atr_1min)
    if d is None:
        return None
    off = alpha * d
    if off < MIN_OFFSET_PTS or off > MAX_OFFSET_PTS:
        return None
    lvl = price_now - off if is_long else price_now + off
    return round(round(lvl * 4) / 4, 2)          # snap to the 0.25 tick grid


def atr_from_bars(bars_1min, period=14):
    """bars_1min: newest-last sequence of dicts with h/l/c. Returns ATR or None."""
    if not bars_1min or len(bars_1min) < 3:
        return None
    trs = []
    for i in range(1, len(bars_1min)):
        h, l = bars_1min[i]["h"], bars_1min[i]["l"]
        pc = bars_1min[i - 1]["c"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    if not trs:
        return None
    w = trs[-period:]
    return sum(w) / len(w)
