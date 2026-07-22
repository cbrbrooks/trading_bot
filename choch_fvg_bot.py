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
# NOTE: htf_bias (MultiTimeframeBias) is imported LAZILY inside CHOCHFVGReplayWrapper.__init__
# only when HTF_BIAS_MODE != "off". That filter tested poorly (-85% to -96% P&L) and is off by
# default, so the deployed bot does not need htf_bias.py present at all. Keeping the import lazy
# means the live bot runs with one fewer file to sync; htf_bias.py is only required if someone
# deliberately re-enables the experiment.

# ── Higher-timeframe structural bias filter ────────────────────────────────
# "off" | "15m" | "30m" | "hybrid"
# Only take 1-minute CHOCH+FVG entries that ALIGN with higher-timeframe structural
# bias (determined via CHOCH/BOS on that timeframe — see htf_bias.py). A NEUTRAL
# higher-timeframe bias means NO trade. "hybrid" requires 15m AND 30m to agree.
# Default "off" — this is a test harness, not yet a deployed feature.
HTF_BIAS_MODE = "off"

# ── Breakaway FVG filter (Arjo method) ─────────────────────────────────────
# When True, only take entries where the FVG is classified BREAKAWAY: candle 3
# closed beyond candle 2's high (bullish) / low (bearish). Default False (test only).
BREAKAWAY_ONLY = False

# ── 9-period moving-average mode for the VWAP/EMA filter ────────────────────
# Controls what the `ema9` line actually is, to match TradingView's EMA panel:
#   "sma"     -> Length 9 EMA with Smoothing Line=SMA, Length 9 => plotted line is a 9 SMA
#   "ema"     -> single 9 EMA of close (no smoothing)
#   "ema_ema" -> 9 EMA of close, then 9 EMA of that (Smoothing Line=EMA)
# The user's screenshots have shown SMA and EMA smoothing at different times; default
# "sma" matches the most recent screenshot. Override with CHOCH_MA_MODE.
MA_MODE = "sma"

# Source series for the 9-period MA (matches TradingView's EMA "Source" dropdown):
#   "close" | "open" | "high" | "low" | "hl2" | "hlc3" | "ohlc4"
# Default "close" matches the user's chart. Override with CHOCH_MA_SOURCE.
MA_SOURCE = "close"

# ── VWAP / 9-SMA price-location filter (ENABLED 2026-07-16) ─────────────────
# Skip a LONG only when current price is BELOW BOTH the 9-SMA and session VWAP;
# skip a SHORT only when price is ABOVE BOTH. The AND is essential — it isolates
# genuinely unconfirmed reversals (blocked group won just 15.4%, z=-3.86) instead
# of blindly blocking everything on one side of a line (VWAP-alone threw away 224
# profitable trades, -$8,901). Validated over 18 months: +$2,780 (+9.6%) vs
# baseline, positive in BOTH train (Jan25-Feb26) and test (Mar-Jun26), robust
# across MA source ($877 spread over 7 sources, Close best) and smoothing type
# (SMA $31,765 ~ single-EMA $31,934; double-EMA worst at $29,332). MA=SMA(9) on
# Close, session-anchored VWAP (5pm CT / 3pm PT prior-day anchor). Only ever
# removes trades. CAVEAT: 18-mo result is in-sample; true out-of-sample (test)
# gain was a modest +$276 — deployed for the sound mechanism, watch live results.
VWAP_EMA_FILTER = True

SESSION_OPEN = time(6, 30)
SESSION_CLOSE = time(12, 30) # no NEW entries after this — extended from 11:00 to 12:30 after
                              # a full sweep (11:00/11:30/12:00/12:30/12:45) showed 12:30 nearly
                              # maxes out total P&L while staying comfortably ahead of the 1:10 PT
                              # force-flat cutoff. Over the Jan 2025–Jun 2026 backtest under the
                              # current config (BROKER_MIN_STOP_POINTS=1.0, fixed brackets, fees),
                              # extending to 12:30 grew net P&L from $19,977 to $26,500 (+$6,523,
                              # +33%) with the win rate essentially unchanged (40.3% → 39.9%).
                              # 12:45 would add another ~$2,200 but pushes trade closes uncomfortably
                              # close to the force-flat, so 12:30 was chosen as the balance point.
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

CONTRACTS = 1                 # 1 ES contract. Set 2026-07-22 for the Topstep-safe config
                               # (2pt stop / 3pt target, 1 contract). Validated Jan 2025-Jul 2026:
                               # ~57% WR full-history (63% over Jan-Jul 2026), ~$976/mo at 0.25pt
                               # slippage, and MAX equity drawdown only ~$700 — fits any account.
                               # NOTE: 1 contract x 2.0pt stop = the SAME $100/loss dollar risk as
                               # the prior 2 contracts x 1.0pt, so MAX_TRADE_RISK_DOLLARS (110) and
                               # DAILY_LOSS_LIMIT (230) below stay correctly calibrated unchanged.
                               # (History: was 2 during the 1pt-stop phase; P&L scaled 2.0x then.)
BROKER_MIN_STOP_POINTS = 1.0  # TopstepX API accepts bracket stops >= 4 ticks (1.0pt). Confirmed
                               # ES) — this pads the stop out to that minimum when the setup's
                               # own risk_points is tighter. Set to 1.0 as the default after
                               # a live rejection on 2026-07-13 ("Invalid stop loss ticks (2).
                               # Price should be at least 4 ticks away"), backed by an 18mo
                               # backtest sweep: keeping all setups but widening tight stops
                               # to 1.0 gives $19,977 over Jan 2025-Jun 2026 (40.3% WR, 390
                               # trades) — a 15% haircut vs the invalid $23,619 baseline, but
                               # 25× better than "just filter tight setups out" ($804 at
                               # MIN_STOP_POINTS=1.0). Set to 0.0 to run the raw backtest
                               # without any broker-compliance widening.
FIXED_STOP_POINTS = 2.0       # Fixed 2.0pt stop on every trade (uniform geometry mode).
                               # Set 2026-07-22: 2pt stop + 3pt target (RR_MULTIPLE=1.5 below) was
                               # the best risk-adjusted config in the Jul-2026 stop/target sweep —
                               # a 1pt stop was getting tagged by intrabar noise before the move.
                               # 2.0pt x $50 x 1 contract = $100 risk/trade (matches the old cap).
                               # ---- historical note (prior 1.0pt setting) ----
                               # Fixed 1.0pt stop on every trade (uniform geometry mode).
                               # CONFIRMED 2026-07-14 via a live API test (test_bracket_ticks_live.py):
                               # a bracket order with stopLossBracket ticks=4 was ACCEPTED by the
                               # TopstepX API (orderId 3270048512, success=True). The ">5 ticks"
                               # rule in TopStep's help docs applies to the PLATFORM UI bracket
                               # settings, NOT to the API — the API enforces only the documented
                               # 4-tick minimum. So a 1.0pt stop (= 4 ticks) is valid via the API,
                               # and this is the strategy's validated geometry. Target scales via
                               # RR_MULTIPLE (1.0 * 3.0 = 3.0pt = 12 ticks), keeping the 3:1.
                               # Setup selection (which trades to take) is UNCHANGED — the
                               # pattern still identifies the same CHOCH+FVG setups; only the
                               # stop and target become fixed distances. Every trade: stop 1.0pt
                               # away, target RR_MULTIPLE * 1.0pt (currently 3.0pt) away.
                               # Set to 0.0 to revert to variable-stop mode (pattern-derived
                               # stop, BROKER_MIN_STOP_POINTS as floor).
                               #
                               # Chosen 2026-07-13 after backtest sweep (Jan 2025-Jun 2026):
                               # FIXED_STOP=1.0 + RR=3 → $27,957 net, 40.8% WR, 505 trades.
                               # Nearly identical to variable-stop config ($26,500 net, 39.9% WR),
                               # but with much cleaner mechanics: every stop is broker-compliant
                               # by construction, every trade has identical dollar risk/reward,
                               # and January (the ugliest month) turns from -$104 to +$158 net
                               # with a max loss streak of 9 instead of the higher-RR ranges of
                               # 11-17. Trade-off: higher RRs (5-7) show larger totals in backtest
                               # but with WR dropping to 23-30% — psychologically much harder to
                               # trade through. RR=3 fixed matches the current strategy's feel
                               # while getting uniform geometry.
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
MAX_TRADE_RISK_DOLLARS = 110.0 # Backstop cap on per-trade dollar risk (stop distance only, no
                               # fees; guard is `>`). At 1.0pt x $50 x 2 contracts the raw risk is
                               # exactly $100, so this sits $10 ABOVE it: never blocks a normal
                               # trade, but still rejects any genuinely wider stop. Deliberately NOT
                               # set to 100 — that would sit exactly on the value and a single extra
                               # rounding tick (1.01pt -> $101 > $100) would silently skip every trade.
                               # this stays a redundant backstop consistent with MAX_STOP_POINTS
                               # (both should reject the same trades) rather than becoming a NEW,
                               # tighter, unintended filter that only 1-contract math satisfies.
RR_MULTIPLE = 1.5            # target = entry +/- 1.5R. With FIXED_STOP_POINTS=2.0 that is a
                               # 3.0pt target ($150 at 1 contract) on every trade. Set 2026-07-22
                               # so the 2pt stop pairs with a 3pt target (the validated 2:3 geometry,
                               # ~57-63% WR). NOTE: RR is now 1.5 because the STOP widened to 2pt,
                               # not because the target shrank — target is still 3.0pt.
                               # ---- historical note (prior RR_MULTIPLE=3.0, 1pt-stop phase) ----
                               # target = entry +/- 3R (= 3.0pt on every trade in fixed-stop mode).
                               # Changed 2026-07-13 from 7.0 as part of the fixed-stop switch.
                               # Backtest sweep across RR=2/3/4/5/6/7 at FIXED_STOP=1.0:
                               #   RR=2: $17,444, 47.3% WR, 510 trades (Jan +$950, safest month)
                               #   RR=3: $27,957, 40.8% WR, 505 trades (Jan +$158, best balance)
                               #   RR=4: $29,603, 33.5% WR, 499 trades (Jan -$727, WR getting rough)
                               #   RR=5: $34,525, 29.6% WR, 496 trades (Jan -$827)
                               #   RR=6: $36,213, 26.1% WR, 491 trades (Jan -$1,127)
                               #   RR=7: $36,443, 23.0% WR, 487 trades (Jan -$827, streak of 17)
                               # RR=3 chosen: nearly identical expected P&L to previous variable-
                               # stop config ($27,957 vs $26,500), similar win rate feel (40.8% vs
                               # 39.9%), similar $/win ($292 vs $302), but with cleaner uniform
                               # geometry and a manageable January. Higher RRs offer more total
                               # P&L in backtest but at 23-30% WR which is much harder to trade
                               # through emotionally. Was 7.0 for variable-stop mode where a full
                               # sweep found totals climbing to 8R — but that math doesn't apply
                               # to fixed-stop mode, where higher RR just means harder-to-hit
                               # targets on every trade.
MAX_TRADES_PER_DAY = 3
MAX_CONSECUTIVE_LOSSES = 2
DAILY_LOSS_LIMIT = 230.0     # Backstop. Sits just above the worst reachable day, so under the
                               # CURRENT config it never actually fires — and that is the intent:
                               # the consecutive-loss gate is the real protection, this is the net
                               # beneath it.
                               #
                               # Why it can't fire today: in fixed-stop mode every loss is exactly
                               # -$107.56 net (1.0pt x $50/pt x 2 contracts, + $7.56 fees), so
                               # MAX_CONSECUTIVE_LOSSES=2 ends the day at -$215.12 — $14.88 short
                               # of this limit. Enumerating every possible day under the gates
                               # (MAX_TRADES_PER_DAY=3, MAX_CONSECUTIVE_LOSSES=2) yields exactly
                               # one losing outcome:
                               #     LL              -> -$215.12   <- true worst possible day
                               #     WLL / LWL       ->  +$77.32
                               #     LWW / WWL / WLW -> +$477.32
                               #     WWW            -> +$877.32
                               # Any day containing a single win finishes GREEN — one win (+$292.44)
                               # covers 2.7 losses.
                               #
                               # NOTE ON GATE SEMANTICS: this limit is checked BEFORE opening a
                               # trade, so it can only block the NEXT entry — it cannot prevent the
                               # loss that breaches it. Setting it at or below $215.12 would make it
                               # fire, but only at the exact moment MAX_CONSECUTIVE_LOSSES already
                               # stopped the day, so it would be pure redundancy, not extra safety.
                               #
                               # WHY KEEP IT AT ALL: it becomes live automatically if the geometry
                               # changes — more CONTRACTS, a wider FIXED_STOP_POINTS, or a higher
                               # MAX_CONSECUTIVE_LOSSES all raise the reachable daily loss past this
                               # number. Scaled with CONTRACTS by the replay harness, same as
                               # MAX_TRADE_RISK_DOLLARS.
                               #
                               # REAL MAX DAILY LOSS TODAY: $215.12 (2 losses), enforced by
                               # MAX_CONSECUTIVE_LOSSES — not by this constant.
ATR_MAX_THRESHOLD = 8.0      # block trades if 1-min ATR exceeds this (spec: "extreme" volatility only)
ATR_WINDOW = 14

# ── TopStep fees & commissions ─────────────────────────────────────────────
# ES round-turn $3.78 = $1.89 in + $1.89 out, PER CONTRACT. Every trade's real
# cost is FEE_PER_ROUND_TURN * CONTRACTS. At 2 contracts that's $7.56/trade.
# These are treated as PART OF the trade's real P&L: _exit() subtracts them from
# the reported pnl (headline number = net after fees), and daily_pnl / the
# DAILY_LOSS_LIMIT gate include them too — so the safety limits stay in sync
# with what actually happens to the account balance, not an idealized version.
# (The engine's theoretical pnl still starts as gross for math traceability;
# the subtraction happens once, in _exit, before booking.)
FEE_PER_ROUND_TURN = 3.78    # per contract, ES

FIB_LOW = 0.50                 # confirmed via isolated A/B test: wide zone outperforms the
FIB_HIGH = 0.618                # narrowed 0.58-0.64 "golden pocket" test

QUALITY_FILTERS_ENABLED = True # ENABLED 2026-07-13. Applies the SINGLE surviving filter in
                               # choch_quality_filters.py: block entries whose FVG quality score
                               # is < 45. In practice this rejects one specific class of setup —
                               # a WIDE gap (>= ~1.5pt) that has gone STALE (price took >= 2
                               # candles to return to it). Blocks ~4% of otherwise-valid setups.
                               #
                               # This is the only filter of five attempts that did not cost money.
                               # Validated with a chronological 80/20 split:
                               #   TRAIN (Jan 2025-Feb 2026): 388->373 trades, WR 36.3%->37.3%,
                               #                              P&L $14,542 -> $15,355 (+$813).
                               #                              Blocked group: 18 trades, 16.7% WR.
                               #   TEST  (Mar-Jun 2026, sealed, used once): 117->115 trades,
                               #                              WR 55.6%->56.5%, P&L $13,415 ->
                               #                              $13,631 (+$215). Blocked group:
                               #                              3 trades, 0% WR.
                               #
                               # HONEST CAVEAT: the blocked sample is only 21 trades total across
                               # both periods (3 wins). Combined z ~ -1.9, just under the 1.96
                               # significance bar. This is NOT statistically proven — it is weak
                               # evidence in a consistent direction plus a sound mechanism, with
                               # bounded downside. Expected impact ~+1-2% on P&L.
                               #
                               # THE TEST SET IS SPENT. Do not re-tune the 45 threshold against
                               # this backtest. Next honest evaluation comes from LIVE trades.
                               #
                               # NO momentum / time / trend / market-structure filters. All four
                               # were built, tested, and rejected — they reduced P&L.
                               # Set False to revert to the pure unfiltered baseline.
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
    def __init__(self, fvg_type: str, top: float, bottom: float, created_at_index: int = -1,
                 breakaway: bool = False):
        self.type = fvg_type  # "bullish" | "bearish"
        self.top = top
        self.bottom = bottom
        self.midpoint = (top + bottom) / 2
        self.consumed = False
        self.created_at_index = created_at_index  # engine candle index at creation, for
                                                    # age-based staleness scoring in the
                                                    # quality-filter overlay (-1 = untracked)
        self.breakaway = breakaway   # Arjo classification: candle 3 closed beyond candle 2's
                                      # high (bullish) / low (bearish). See _detect_fvg.


class CHOCHFVGEngine:
    """One instance per replay day."""

    def __init__(self):
        self.candles: list[Candle] = []
        self.atr = None

        # Session-anchored VWAP accumulators (reset per replay day = per engine instance)
        self._vwap_pv = 0.0     # sum of typical_price * volume
        self._vwap_vol = 0.0    # sum of volume
        self.vwap = None
        # 9-period MA of close (mode set by MA_MODE); _ema_base holds the inner EMA
        self.ema9 = None
        self._ema_base = None

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

    def _update_vwap_ema(self):
        """9 EMA (always engine-computed) + session-anchored VWAP.

        VWAP: CME futures sessions run 5:00 PM CT (prior day) to 4:00 PM CT, so a
        correct session VWAP must accumulate from ~3:00 PM PT the previous day — data
        the engine does NOT see (it only receives RTH candles from 6:30 AM PT). When a
        caller has precomputed the true session VWAP over the full 24h stream, it sets
        `self.session_vwap_override` on the newest candle's slot and we use that. If no
        override is supplied we fall back to accumulating from the first candle seen
        (RTH anchor) — approximate, and flagged as such."""
        c = self.candles[-1]

        override = getattr(self, "_session_vwap_next", None)
        if override is not None:
            self.vwap = override
            self._session_vwap_next = None
        else:
            typical = (c.high + c.low + c.close) / 3.0
            vol = c.volume if c.volume and c.volume > 0 else 1.0
            self._vwap_pv += typical * vol
            self._vwap_vol += vol
            self.vwap = self._vwap_pv / self._vwap_vol if self._vwap_vol > 0 else None

        # 9-period line matching the user's TradingView setup. Their EMA panel shows
        # Length 9, Smoothing Line = EMA, Smoothing Length 9 — i.e. a 9 EMA of close,
        # then a 9 EMA OF THAT EMA (a doubly-smoothed EMA-of-EMA). Kept on attribute
        # name `ema9` for continuity. Controlled by MA_MODE:
        #   "ema_ema" -> 9 EMA of close, then 9 EMA of that (the user's current setting)
        #   "ema"     -> single 9 EMA of close
        #   "sma"     -> 9 SMA of close (the prior tested setting)
        c = self.candles[-1]
        k = 2.0 / (9 + 1)
        mode = MA_MODE
        src = MA_SOURCE
        def _src(x):
            if src == "close": return x.close
            if src == "open": return x.open
            if src == "high": return x.high
            if src == "low": return x.low
            if src == "hl2": return (x.high + x.low) / 2
            if src == "hlc3": return (x.high + x.low + x.close) / 3
            if src == "ohlc4": return (x.open + x.high + x.low + x.close) / 4
            return x.close
        if mode == "sma":
            n = 9
            src_vals = [_src(x) for x in self.candles[-n:]] if len(self.candles) >= n else [_src(x) for x in self.candles]
            self.ema9 = sum(src_vals) / len(src_vals)
        else:
            cur_src = _src(c)
            if self._ema_base is None:
                self._ema_base = cur_src
            else:
                self._ema_base = cur_src * k + self._ema_base * (1 - k)
            if mode == "ema":
                self.ema9 = self._ema_base
            else:  # ema_ema
                if self.ema9 is None:
                    self.ema9 = self._ema_base
                else:
                    self.ema9 = self._ema_base * k + self.ema9 * (1 - k)

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
        idx = len(self.candles) - 1   # index of the newest candle (c3) at creation time
        if c1.high < c3.low:
            # Arjo Breakaway test (bullish): does candle 3 CLOSE above candle 2's HIGH?
            # If yes, the gap is a "Breakaway" — the move is decisive and price is (per the
            # method) less likely to return. Measured on ES over 18 months: Breakaway gaps
            # are mitigated 85.2% of the time vs 92.6% for Regular gaps (z=-21.4), so the
            # classification is directionally real, though the "won't return" framing is
            # much too strong — 85% of them do return.
            breakaway = c3.close > c2.high
            self.active_fvgs.append(FVG("bullish", top=c3.low, bottom=c1.high,
                                         created_at_index=idx, breakaway=breakaway))
        elif c1.low > c3.high:
            breakaway = c3.close < c2.low   # bearish mirror
            self.active_fvgs.append(FVG("bearish", top=c1.low, bottom=c3.high,
                                         created_at_index=idx, breakaway=breakaway))

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

    def __init__(self, htf_mode: str = None):
        self.engine = CHOCHFVGEngine()
        # Higher-timeframe structural bias tracker. Feeds on EVERY 1m candle (including
        # pre-session warmup bars) so that by the 6:30 AM session open the 15m/30m bias is
        # already established from the overnight/premarket structure. If it only saw
        # session candles, bias would be "neutral" for the first hour+ and — since neutral
        # means no-trade — the filter would silently suppress all early-session entries for
        # reasons that have nothing to do with structure.
        mode = htf_mode if htf_mode is not None else HTF_BIAS_MODE
        if mode != "off":
            from htf_bias import MultiTimeframeBias  # lazy: only needed when the filter is on
            self.htf = MultiTimeframeBias(mode)
        else:
            self.htf = None

    def add_candle(self, candle: Candle, pt_time: time, is_last_candle_of_session: bool,
                   session_vwap: float = None) -> dict | None:
        eng = self.engine

        # Feed the HTF tracker FIRST and unconditionally — before any session/gate checks —
        # so higher-timeframe structure is built from the full candle stream.
        if self.htf is not None:
            self.htf.add_1m_candle(candle)

        # If the caller precomputed the true CME-session VWAP (anchored to 5pm CT / 3pm PT
        # the prior day) over the full 24h stream, hand it to the engine for THIS candle.
        if session_vwap is not None:
            eng._session_vwap_next = session_vwap

        eng.candles.append(candle)
        eng._update_atr()
        eng._update_vwap_ema()

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

        return self._check_entry(eng, candle, pt_time)

    def _check_entry(self, eng: CHOCHFVGEngine, curr: Candle, pt_time: time) -> dict | None:
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

            # VWAP / 9-SMA price-location filter. Skip a LONG only if current price is BELOW
            # BOTH the 9-SMA and session VWAP; skip a SHORT only if price is ABOVE BOTH.
            # The AND is essential: tested VWAP-alone blocks 224 profitable trades (-$8,901,
            # same failure as every quality filter), because price dips below VWAP in plenty
            # of trades that recover. Requiring price below BOTH lines isolates the genuinely
            # unconfirmed reversals — that blocked group wins just 15.4% (z=-3.86). Only ever
            # REMOVES trades.
            if VWAP_EMA_FILTER and eng.vwap is not None and eng.ema9 is not None:
                price = curr.close
                if fvg.type == "bullish" and price < eng.ema9 and price < eng.vwap:
                    continue   # long blocked: price below both lines
                if fvg.type == "bearish" and price > eng.ema9 and price > eng.vwap:
                    continue   # short blocked: price above both lines

            # Breakaway FVG filter (Arjo method) — only take gaps where candle 3 closed
            # decisively beyond candle 2. Checked after the pattern's own hard gates so it
            # only ever REMOVES trades.
            if BREAKAWAY_ONLY and not fvg.breakaway:
                continue

            # Higher-timeframe STRUCTURAL bias gate. The 1m setup's direction must align with
            # the 15m/30m structural bias (established via CHOCH/BOS on that timeframe, not
            # SMA slope). A neutral HTF bias means no trade, per spec. This is checked here —
            # after the pattern's own hard gates — so it only ever REMOVES trades.
            if self.htf is not None:
                allowed_htf, htf_reason = self.htf.allows(fvg.type)
                if not allowed_htf:
                    continue  # HTF-blocked; try any other active FVG

            # Quality-filter overlay (optional) — four-filter scoring system. Runs after the
            # pattern's own hard gates so it only ever REMOVES trades. The FVG is consumed
            # either way (one shot per FVG), matching how the risk_points filter below already
            # behaves for setups it rejects.
            if QUALITY_FILTERS_ENABLED:
                from choch_quality_filters import evaluate_entry_quality
                fvg_age = (len(eng.candles) - 1 - fvg.created_at_index) if fvg.created_at_index >= 0 else 0
                allowed, q_score, q_detail = evaluate_entry_quality(
                    eng.candles, eng.choch_bias, eng.choch_swing_high, eng.choch_swing_low,
                    fvg.top - fvg.bottom, fvg_age, pt_time)
                if not allowed:
                    continue  # quality-blocked; try any other active FVG

            entry = fvg.midpoint
            if fvg.type == "bullish":
                stop = fvg.bottom
                risk_points = entry - stop
            else:
                stop = fvg.top
                risk_points = stop - entry

            if risk_points <= 0 or risk_points > MAX_STOP_POINTS or risk_points < MIN_STOP_POINTS:
                continue  # quality filter — applies regardless of contract count

            # FIXED_STOP_POINTS override: keep the setup, but replace its natural stop AND target
            # with fixed distances. Same trades get taken (the setup quality filter above is
            # unchanged), only the exit geometry changes. When 0 (default), the pattern's own
            # risk_points drives everything downstream.
            if FIXED_STOP_POINTS > 0:
                risk_points = FIXED_STOP_POINTS
                stop = entry - risk_points if fvg.type == "bullish" else entry + risk_points

            # Pad the stop out to the broker's minimum bracket distance if needed. TopstepX's
            # bracket API rejects stops closer than BROKER_MIN_STOP_POINTS ("Invalid stop loss
            # ticks — price should be at least 4 ticks away"), which killed a live entry on
            # 2026-07-13. The setup's ORIGINAL risk_points still drives the target math (kept
            # unchanged from the pattern's own geometry, i.e. the 7R the strategy was designed
            # around); we only widen the STOP for broker compliance. Effect on losing trades:
            # a stop that would have been 0.375pt now sits at 1.0pt, so a loss that would have
            # been -$37.50 becomes -$100. Effect on winning trades: unchanged, since the target
            # is unchanged. Whether this trade-off wins overall depends on how often the wider
            # stop actually gets hit — measured in the backtest at each configuration.
            # (Skipped when FIXED_STOP_POINTS is active, since fixed mode sets both directly.)
            if FIXED_STOP_POINTS == 0:
                broker_stop = max(risk_points, BROKER_MIN_STOP_POINTS)
                if broker_stop != risk_points:
                    stop = entry - broker_stop if fvg.type == "bullish" else entry + broker_stop
            else:
                broker_stop = risk_points   # already fixed to a broker-compliant value

            # Total-risk check runs on the ACTUAL submitted stop, not the pattern's ideal one,
            # so a widened stop that would blow through the dollar cap is still rejected.
            total_risk = broker_stop * POINT_VALUE * CONTRACTS
            if total_risk > MAX_TRADE_RISK_DOLLARS:
                continue  # backstop safety check

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
    gross_pnl = round(pnl, 2)
    fee = round(FEE_PER_ROUND_TURN * CONTRACTS, 2)   # e.g. $7.56 at 2 ES contracts
    pnl = round(gross_pnl - fee, 2)                    # NET of fees — the number the account actually sees
    self.daily_pnl += pnl
    self._last_theoretical_exit_pnl = pnl  # so the live bracket path can back this exact
                                            # amount out when reconciling to the real fill
    if pnl < 0:
        self.consecutive_losses += 1
    else:
        self.consecutive_losses = 0
    result = {"type": "exit", "exit_price": exit_price, "reason": reason,
              "pnl": pnl, "gross_pnl": gross_pnl, "fee": fee,
              "entry_price": self.entry_price, "direction": self.position}
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
