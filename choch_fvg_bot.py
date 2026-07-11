"""
CHOCH/FVG Bot — Change of Character + Fair Value Gap + Fibonacci alignment
strategy for ES, 1 contract, 6:30-8:00 AM PT entry window.

Built from a detailed spec, with lessons applied from every bug found
across this project's other strategies:

  - FIXED A REAL BUG IN THE SPEC ITSELF: the provided Python skeleton's
    Fibonacci validity check (`bottom <= midpoint <= top and bottom <=
    fib_618 <= top`) is mathematically always true — midpoint and fib_618
    are computed FROM top/bottom, so they can never fail that check. This
    is the same shape of bug as the vacuous target-caps found in ES Base
    Hit and Zone Strategy. The REAL rule (from spec Section C) is that the
    FVG's midpoint must fall within a 50%-61.8% retracement of an EXTERNAL
    swing (the CHOCH's own swing structure), not compared to itself.
    Implemented that way here.
  - Stop/target checked via candle low/high, not just close.
  - Entries gated to the session window; an already-open trade is still
    managed normally after 8:00 AM until stop/target or the 1:10 PM PT
    forced flat, not silently abandoned.
  - Clean, distinct exit-reason labels — no mislabeling a breakeven
    scratch as a "stop loss."
  - BREAKEVEN TRIGGER RESCALED: the original spec's breakeven trigger was
    the CHOCH's own external swing high/low. Diagnostic replay showed that
    sat farther from entry than the target itself in 98.3% of trades
    (median 2.56x, up to 19x) — with sub-point-to-few-point targets, the
    trigger needed a far bigger move than these setups ever have to make,
    so breakeven fired in 0/181 trades over 18 months. Rescaled to
    EARLY_BREAKEVEN_PCT=0.30 of the trade's own risk instead, matching
    Dual Trail's validated early-breakeven design.
  - max_contracts = 1 is hard-enforced with no sizing tiers at all, so the
    contract-count/risk-cap coupling bug from Zone Strategy can't recur
    here (there's nothing to couple).
  - Trailing Max Drawdown is NOT implemented — it's an account-level,
    cross-day concept our per-day replay architecture can't track (same
    honest limitation flagged in ES Base Hit's "reduce risk after a
    losing streak" simplification). Daily Loss Limit IS implemented.
  - News filter NOT implemented — no news data source available.
  - SESSION_CLOSE WIDENED to 11:00 AM PT (from an original 8:00, via 10:30):
    initial sweeps widening the window kept improving both win rate and
    P&L up through 10:30-11:00 AM. A finer follow-up sweep (10:30/11:00/
    11:30/12:00/12:30/1:10) found expectancy PER TRADE actually peaks at
    11:00 (~$54/trade) and declines steadily past it (down to $49.54/trade
    by the 1:10 PM force-flat) — total P&L kept climbing past 11:00 only
    because trade count grew faster than per-trade quality shrank, not
    because later trades were equally good. 11:00 was chosen as the point
    that adds real volume (180 vs 160 trades/Jan-Jul-2026) without giving
    up per-trade edge, and deliberately over the higher-total-but-lower-
    quality later options given the account was rebuilding from a
    drawdown at the time — more trades/day compounds variance faster
    within a single session. MAX_STOP_POINTS was separately tested as an
    alternate lever for trade frequency and ruled out — relaxing it
    1.5->10pts only moved trade count 181->190 at the original 8:00 window.
"""

from datetime import time

import numpy as np

from topstep_bot_v5 import Candle, POINT_VALUE

SESSION_OPEN = time(6, 30)
SESSION_CLOSE = time(11, 0)  # no NEW entries after this — widened from 10:30 to 11:00 AM PT
                              # after a finer sweep (10:30/11:00/11:30/12:00/12:30/1:10) found
                              # expectancy PER TRADE peaks right around here, not later: 10:30
                              # $54.30/trade, 11:00 $53.87/trade (essentially the same), then a
                              # real, monotonic decline every step past this point down to
                              # $49.54/trade by 1:10 PM force-flat. Win rate similarly peaks at
                              # 11:00 (45.4%) before eroding to 42.0% by 1:10. Total P&L keeps
                              # climbing past 11:00 only because trade count outgrows the
                              # shrinking per-trade edge — that's more trades, more exposure,
                              # and lower quality per trade, not a free upgrade. 11:00 is the
                              # last point that adds real trade volume (180 vs. 160 at 10:30,
                              # $9,375 vs $8,462.50 over Jan-Jul 2026) without giving up
                              # per-trade quality. Chosen deliberately over the higher-total
                              # options further out given the account is currently rebuilding
                              # from a drawdown — more trades/day compounds variance within a
                              # session faster, which matters more right now than squeezing out
                              # the extra ~$2,650 the 1:10 PM version shows over the same window.
FORCE_FLAT_TIME = time(13, 10)  # 1:10 PM PT, matches Topstep's 3:10 PM CT rule

CONTRACTS = 2                 # 2 ES contracts — raised from 1 (tested via CHOCH_CONTRACTS=2
                               # override first, confirmed same 78 trades/Jan-Jul-2026 as the
                               # 1-contract run, P&L exactly 2.0x: $2,243.75 -> $4,487.50).
                               # MAX_TRADE_RISK_DOLLARS and DAILY_LOSS_LIMIT below are scaled
                               # 2x to match — see their comments. MAX_STOP_POINTS is NOT scaled:
                               # it's a pure point-distance filter, independent of contract count.
MAX_STOP_POINTS = 1.5         # 1R = $75/contract at ES's $50/point — the REAL quality filter,
                               # in points, independent of contract count or dollar value
MIN_STOP_POINTS = 0.25        # Floor on risk_points, promoted to the actual default after
                               # testing. A confirmed live problem: execution chasing costs
                               # roughly a FIXED amount of price movement regardless of a
                               # trade's own risk — July 8 (0.50pt risk) kept 87.5% of its
                               # theoretical edge after a real chase; July 9 (0.12pt risk) kept
                               # 0%. Swept 0.25/0.375/0.5 against Jan-Jul 2026: 0.25 cost only
                               # -6.9% total P&L with win rate flat (41.0%->41.3%, 139 trades);
                               # 0.375 and 0.5 both cratered win rate (down to 34-35%) — past
                               # 0.25, the filtered trades stop being marginal and start being
                               # the strategy's best performers, so this stops here rather than
                               # going further. A LIMIT-order entry approach was tried first and
                               # rejected: it filtered on whether price returned to the entry
                               # level after the signal, which turned out to correlate with
                               # LOSING trades, not fast ones (61 of 77 filtered trades in that
                               # test were winners) — a much worse selection bias than this.
MAX_TRADE_RISK_DOLLARS = 150.0 # 1R at 2 contracts ($75 * CONTRACTS). Scaled with CONTRACTS so
                               # this stays a redundant backstop consistent with MAX_STOP_POINTS
                               # (both should reject the same trades) rather than becoming a NEW,
                               # tighter, unintended filter that only 1-contract math satisfies.
RR_MULTIPLE = 7.0            # target = entry +/- 7R. Was 3.0, then 4.0; a full sweep
                               # (4/4.5/5/5.5/6/7/8) on the 18-month backtest found total P&L
                               # climbing at EVERY point tested, no peak found even at 8R
                               # ($33,975 vs $19,850 at 4R). Structural reason: EARLY_BREAKEVEN_PCT
                               # caps a trade's downside at $0 once triggered regardless of RR, so
                               # widening the target can only convert some winners into scratches
                               # (opportunity cost) while trades that still reach it pay more —
                               # not a normal risk/reward tradeoff. Checked whether FORCE_FLAT_TIME
                               # was starting to clip trades short of increasingly distant targets
                               # (the one real structural ceiling this architecture has) — zero
                               # forced-flat exits at RR=7 or RR=8, so that wall hadn't been hit
                               # yet either. 7R chosen as a judgment call, not a mathematical peak:
                               # realism of the move required and holding-time/unrealized-swing
                               # exposure both grow with RR in ways this backtest doesn't price in,
                               # so stopped just below the tested ceiling (8) rather than at it.
                               # 7R: 540 trades, 38.4% WR, $62.93/trade, $30,837.50 total/18mo.
MAX_TRADES_PER_DAY = 3
MAX_CONSECUTIVE_LOSSES = 2
DAILY_LOSS_LIMIT = 450.0     # 3x per-trade risk (3 losses = day done) at 2 contracts —
                               # scaled with CONTRACTS/MAX_TRADE_RISK_DOLLARS for the same reason
ATR_MAX_THRESHOLD = 8.0      # block trades if 1-min ATR exceeds this (spec: "extreme" volatility only)
ATR_WINDOW = 14

FIB_LOW = 0.50                 # confirmed via isolated A/B test: wide zone outperforms the
FIB_HIGH = 0.618                # narrowed 0.58-0.64 "golden pocket" test

USE_BREAKEVEN = False         # When False, the stop stays FIXED at its original level for the
                               # entire trade — no mid-trade breakeven relocation. This matches
                               # "pure fixed brackets": on entry, a fixed stop and fixed target are
                               # set and never moved, exactly what a broker-side stopLossBracket /
                               # takeProfitBracket does. Turned OFF as the default after the live
                               # breakeven mechanism caused a -$325 loss (July 10 2026): it fired a
                               # MARKET order into a fast reversal to "protect" the trade at
                               # breakeven, but the market order chased 3+ points past the intended
                               # scratch price. Fixed brackets remove that entire failure mode — the
                               # exchange manages the exit off live ticks, no polling-loop lag, no
                               # market-order chase. Set True to restore the old early-breakeven
                               # behavior for comparison testing.
EARLY_BREAKEVEN_PCT = 0.30    # Only used when USE_BREAKEVEN is True. Replaces the original spec's
                               # breakeven trigger (external CHOCH swing high/low). Diagnostic replay
                               # showed that trigger sat farther from entry than the target itself in
                               # 98.3% of trades (median 2.56x, up to 19x) — with sub-point-to-few-
                               # point targets, the trigger was calibrated to a much bigger move than
                               # these setups ever need to make, so breakeven fired in 0/181 trades
                               # over 18 months. Rescaled to the trade's OWN risk instead, matching
                               # Dual Trail's EARLY_BREAKEVEN_PCT=0.30 (early breakeven once 30% of
                               # risk is covered) rather than an unrelated structural swing level.




class FVG:
    def __init__(self, fvg_type: str, top: float, bottom: float):
        self.type = fvg_type  # "bullish" | "bearish"
        self.top = top
        self.bottom = bottom
        self.midpoint = (top + bottom) / 2
        self.consumed = False


class CHOCHFVGEngine:
    """One instance per replay day."""

    def __init__(self):
        self.candles: list[Candle] = []
        self.atr = None

        self.choch_bias = None          # "bullish" | "bearish" | None
        self.choch_swing_high = None
        self.choch_swing_low = None
        self.active_fvgs: list[FVG] = []

        self.trades_today = 0
        self.daily_pnl = 0.0
        self.consecutive_losses = 0

        self.in_trade = False
        self.position = None
        self.entry_price = None
        self.stop_price = None
        self.target_price = None
        self.breakeven_trigger = None
        self.breakeven_done = False
        self._last_theoretical_exit_pnl = 0.0

    def _update_atr(self):
        window = self.candles[-ATR_WINDOW:] if len(self.candles) >= ATR_WINDOW else self.candles
        ranges = [c.range for c in window if c.range > 0]
        self.atr = float(np.mean(ranges)) if ranges else None

    def _detect_choch(self, prev: Candle, curr: Candle):
        if prev.low < curr.low and curr.high > prev.high:
            self.choch_bias = "bullish"
            self.choch_swing_low = min(prev.low, curr.low)
            self.choch_swing_high = curr.high
            self.active_fvgs = []  # new structure — old FVGs are stale
        elif prev.high > curr.high and curr.low < prev.low:
            self.choch_bias = "bearish"
            self.choch_swing_high = max(prev.high, curr.high)
            self.choch_swing_low = curr.low
            self.active_fvgs = []

    def _detect_fvg(self, c1: Candle, c2: Candle, c3: Candle):
        if c1.high < c3.low:
            self.active_fvgs.append(FVG("bullish", top=c3.low, bottom=c1.high))
        elif c1.low > c3.high:
            self.active_fvgs.append(FVG("bearish", top=c1.low, bottom=c3.high))

    def _fib_valid(self, fvg: FVG) -> bool:
        """REAL fib alignment check — FVG midpoint vs. an EXTERNAL swing-based
        retracement zone, not the vacuous self-referential check in the
        original spec's skeleton (which was always true by construction)."""
        if self.choch_swing_high is None or self.choch_swing_low is None:
            return False
        swing_range = self.choch_swing_high - self.choch_swing_low
        if swing_range <= 0:
            return False
        if fvg.type == "bullish":
            # Retracement measured DOWN from the swing high
            fib_50 = self.choch_swing_high - FIB_LOW * swing_range
            fib_618 = self.choch_swing_high - FIB_HIGH * swing_range
            lo, hi = min(fib_50, fib_618), max(fib_50, fib_618)
        else:
            fib_50 = self.choch_swing_low + FIB_LOW * swing_range
            fib_618 = self.choch_swing_low + FIB_HIGH * swing_range
            lo, hi = min(fib_50, fib_618), max(fib_50, fib_618)
        return lo <= fvg.midpoint <= hi

    # Entry/exit orchestration (session-time-aware) lives in CHOCHFVGReplayWrapper below,
    # since it needs PT-time context that this bare engine intentionally doesn't hold.


class CHOCHFVGReplayWrapper:
    """Wraps CHOCHFVGEngine with the session-time-aware orchestration the
    bare engine needs (kept separate so the core engine stays testable
    without datetime/timezone plumbing baked in)."""

    def __init__(self):
        self.engine = CHOCHFVGEngine()

    def add_candle(self, candle: Candle, pt_time: time, is_last_candle_of_session: bool) -> dict | None:
        eng = self.engine
        eng.candles.append(candle)
        eng._update_atr()

        if eng.in_trade:
            exit_signal = eng._check_exit(candle)
            if exit_signal is None and (pt_time >= FORCE_FLAT_TIME or is_last_candle_of_session):
                return eng._exit(candle.close, "Forced flat (1:10 PM PT or session end)")
            return exit_signal

        if pt_time < SESSION_OPEN:
            return None

        if len(eng.candles) < 3:
            return None

        # CHOCH detection uses the last two candles
        eng._detect_choch(eng.candles[-2], eng.candles[-1])

        # FVG detection uses the last three candles, only relevant once we have a bias
        if eng.choch_bias is not None and len(eng.candles) >= 3:
            eng._detect_fvg(eng.candles[-3], eng.candles[-2], eng.candles[-1])

        if pt_time >= SESSION_CLOSE:
            return None  # no NEW entries past the session window

        if eng.trades_today >= MAX_TRADES_PER_DAY:
            return None
        if eng.consecutive_losses >= MAX_CONSECUTIVE_LOSSES:
            return None
        if eng.daily_pnl <= -DAILY_LOSS_LIMIT:
            return None
        if eng.atr is not None and eng.atr > ATR_MAX_THRESHOLD:
            return None
        if eng.choch_bias is None:
            return None

        return self._check_entry(eng, candle)

    def _check_entry(self, eng: CHOCHFVGEngine, curr: Candle) -> dict | None:
        for fvg in eng.active_fvgs:
            if fvg.consumed:
                continue
            if fvg.type != eng.choch_bias:
                continue
            if not eng._fib_valid(fvg):
                continue

            # Price must retrace INTO the FVG midpoint on this candle
            if not (curr.low <= fvg.midpoint <= curr.high):
                continue

            fvg.consumed = True
            entry = fvg.midpoint
            if fvg.type == "bullish":
                stop = fvg.bottom
                risk_points = entry - stop
            else:
                stop = fvg.top
                risk_points = stop - entry

            if risk_points <= 0 or risk_points > MAX_STOP_POINTS or risk_points < MIN_STOP_POINTS:
                continue  # quality filter — applies regardless of contract count
            total_risk = risk_points * POINT_VALUE * CONTRACTS
            if total_risk > MAX_TRADE_RISK_DOLLARS:
                continue  # backstop safety check, consistent with the point filter above

            direction = "long" if fvg.type == "bullish" else "short"
            target = entry + RR_MULTIPLE * risk_points if direction == "long" else entry - RR_MULTIPLE * risk_points
            breakeven_trigger = (entry + EARLY_BREAKEVEN_PCT * risk_points if direction == "long"
                                  else entry - EARLY_BREAKEVEN_PCT * risk_points)

            eng.in_trade = True
            eng.trades_today += 1
            eng.position = direction
            eng.entry_price = entry
            eng.stop_price = stop
            eng.target_price = target
            eng.breakeven_trigger = breakeven_trigger
            eng.breakeven_done = False

            return {"type": "entry", "direction": direction, "entry_price": entry,
                    "stop": stop, "target": target,
                    "reason": f"CHOCH {eng.choch_bias} + FVG retrace to midpoint {entry:.2f}, "
                              f"risk={risk_points:.2f}pts, {CONTRACTS} contracts (${total_risk:.2f} total risk)"}
        return None


def _check_exit(self: CHOCHFVGEngine, candle: Candle) -> dict | None:
    if self.position == "long":
        if candle.low <= self.stop_price:
            reason = "Stop loss hit" if self.stop_price < self.entry_price else "Breakeven exit"
            return self._exit(self.stop_price, reason)
        if candle.high >= self.target_price:
            return self._exit(self.target_price, "Target hit")
        if (USE_BREAKEVEN and not self.breakeven_done and self.breakeven_trigger is not None
                and candle.close > self.breakeven_trigger):
            self.breakeven_done = True
            if self.entry_price > self.stop_price:
                self.stop_price = self.entry_price
    else:
        if candle.high >= self.stop_price:
            reason = "Stop loss hit" if self.stop_price > self.entry_price else "Breakeven exit"
            return self._exit(self.stop_price, reason)
        if candle.low <= self.target_price:
            return self._exit(self.target_price, "Target hit")
        if (USE_BREAKEVEN and not self.breakeven_done and self.breakeven_trigger is not None
                and candle.close < self.breakeven_trigger):
            self.breakeven_done = True
            if self.entry_price < self.stop_price:
                self.stop_price = self.entry_price
    return None


def _exit(self: CHOCHFVGEngine, exit_price: float, reason: str) -> dict:
    pnl = ((exit_price - self.entry_price) if self.position == "long"
           else (self.entry_price - exit_price)) * POINT_VALUE * CONTRACTS
    pnl = round(pnl, 2)
    self.daily_pnl += pnl
    self._last_theoretical_exit_pnl = pnl  # so the live bracket path can back this exact
                                            # amount out when reconciling to the real fill
    if pnl < 0:
        self.consecutive_losses += 1
    else:
        self.consecutive_losses = 0
    result = {"type": "exit", "exit_price": exit_price, "reason": reason,
              "pnl": pnl, "entry_price": self.entry_price, "direction": self.position}
    self.in_trade = False
    self.position = None
    self.entry_price = None
    self.stop_price = None
    self.target_price = None
    self.breakeven_trigger = None
    return result


def _reconcile_actual_pnl(self: CHOCHFVGEngine, theoretical_pnl: float, actual_pnl: float):
    """Live-only correction. The engine computes daily_pnl and consecutive_losses from
    THEORETICAL fills (it's all the bare engine knows). But live execution slips, sometimes
    hugely (a 'breakeven exit' really filling -$325), so the theoretical running totals that
    drive the DAILY_LOSS_LIMIT and MAX_CONSECUTIVE_LOSSES safety gates can badly misstate the
    real account state. After each live exit, the live bot calls this with both numbers: we
    back out the theoretical pnl that _exit just added and add the real one instead, and
    re-decide the consecutive-loss flag on the real result. Backtests never call this, so
    replay behavior is unchanged (theoretical is the only truth available there anyway)."""
    self.daily_pnl = round(self.daily_pnl - theoretical_pnl + actual_pnl, 2)
    # Re-evaluate the consecutive-loss streak on the ACTUAL outcome, undoing the
    # theoretical-based decision _exit just made.
    theoretical_was_loss = theoretical_pnl < 0
    actual_is_loss = actual_pnl < 0
    if theoretical_was_loss and not actual_is_loss:
        # _exit incremented the streak on a theoretical loss that was actually a win/scratch
        self.consecutive_losses = 0
    elif not theoretical_was_loss and actual_is_loss:
        # _exit reset the streak on a theoretical win/scratch that was actually a loss
        self.consecutive_losses += 1
    # If both agree (loss/loss or nonloss/nonloss), _exit already did the right thing.


# Bind the exit methods onto the engine class (kept as module-level
# functions above for readability, attached here to avoid a forward-reference issue)
CHOCHFVGEngine._check_exit = _check_exit
CHOCHFVGEngine._exit = _exit
CHOCHFVGEngine._reconcile_actual_pnl = _reconcile_actual_pnl
