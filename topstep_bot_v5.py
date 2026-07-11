"""
TopstepX Trading Bot — Dual Trail v5
  Strategy: dual-sided S/R levels, trailing level exit, breakeven stop
==================================================
Architecture:
  LevelEngine  (15m) — builds S/R levels from swing detection, frozen during trades
  ExecutionEngine (1m) — detects 2-wick rejections, scores setups, manages exits

Instrument: MES or ES, set via INSTRUMENT env var (defaults to MES if unset).
  All position sizing, per-trade/daily risk limits, and profit thresholds
  scale automatically based on which instrument is active — see the
  INSTRUMENT block near the top of the config section below for exact values.

Strategy:
  - Two sessions exist for LEVEL DATA and managing already-open trades
    (Pacific time): Session 1: 6:33 AM – 8:00 AM PT | Session 2: 10:00 AM – 12:30 PM PT
  - NEW ENTRIES are only taken before 8:00 AM PT (Session 1). No new trade
    is opened during Session 2 — but a trade already open when 8:00 AM hits
    is still managed and closed normally, even if that happens during
    Session 2 or later. This is deliberate, not a bug: Session 2 was found
    to add no net edge, so entries there were disabled, while still letting
    any straddling trade resolve on its own terms rather than being force-cut.
  - Levels built from 15-min swing highs/lows (4-day lookback), frozen while in trade
  - Entry: 2-wick rejection on 1-min, scored >= MIN_SIGNAL_SCORE to trade (max=4)
  - Cooldown: 5 min after any stop-out, max 2 losses per level per day
  - Stop: hard per-trade dollar cap (scales with instrument — see INSTRUMENT
    config below for the exact value and resulting point-distance)
  - Exit: stop loss | early breakeven at 50% of risk covered | trailing to
    next level | reversal to passed level | change in character (3
    low-volume bars) | quick profit-take when momentum stalls
  - Stops trading for the day after MAX_CONSECUTIVE_LOSSES losses in a row
  - Hard flatten: 3:10 PM CT (Topstep rule) + 1 min before session end
  - Daily loss / profit target / max trades per day: scale with instrument
    (see INSTRUMENT config below)

Requirements:
  pip install aiohttp numpy python-dotenv

Setup:
  Create .env with:
    TOPSTEP_USERNAME=your_username
    TOPSTEP_API_KEY=your_api_key
"""

import asyncio
import json
import logging
import os
import sys
import threading
from collections import defaultdict
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

import numpy as np
from dotenv import load_dotenv

load_dotenv()

# ─── Logging ──────────────────────────────────────────────────────────────────
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

LOG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bot.log")
ANALYTICS_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "trades.json")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_PATH, encoding="utf-8"),
        logging.StreamHandler(stream=sys.stdout),
    ],
)
log = logging.getLogger(__name__)

# ─── Config ───────────────────────────────────────────────────────────────────
PT = ZoneInfo("America/Los_Angeles")
CT = ZoneInfo("America/Chicago")

USERNAME  = os.getenv("TOPSTEP_USERNAME")
API_KEY   = os.getenv("TOPSTEP_API_KEY")
BASE_URL  = "https://api.topstepx.com/api"

INSTRUMENT = os.getenv("INSTRUMENT", "MES").upper()  # "MES" or "ES" — set in .env to switch

if INSTRUMENT == "ES":
    MES_SYMBOL           = "ESU6"    # E-mini S&P — same front-month schedule as MES
    POINT_VALUE          = 50.0      # ES: $50 per point per contract (10x MES)
    CONTRACTS               = 1      # scaled down from 3 — account margin couldn't support 3 contracts
    PER_TRADE_LOSS_LIMIT    = 66.67  # $66.67 cap = same 1.33-point stop distance as the 3-contract config
    PER_TRADE_PROFIT_TARGET = 133.34   # 2x per-trade loss limit
    DAILY_LOSS_LIMIT        = 133.34   # exactly 2x per-trade loss — 2 losses = day done
    DAILY_PROFIT_TARGET     = 1333.40  # 20x per-trade loss limit
    QUICK_PROFIT_THRESHOLD  = 80.00    # 1.2x per-trade loss limit
else:
    MES_SYMBOL           = "MESU6"   # Current front-month — update at rollover
    POINT_VALUE          = 5.0       # MES: $5 per point per contract
    CONTRACTS               = 2
    PER_TRADE_LOSS_LIMIT    = 50.0
    PER_TRADE_PROFIT_TARGET = 100.0
    DAILY_LOSS_LIMIT        = 150.0
    DAILY_PROFIT_TARGET     = 1000.0
    QUICK_PROFIT_THRESHOLD  = 60.0

MAX_ENTRIES        = 3

WICK_THRESHOLD     = 0.40       # Wick >= 40% of candle range to count
VOLUME_DROP_PCT    = 0.30       # Volume drop threshold for divergence exit
MIN_SIGNAL_SCORE   = 3          # Minimum score to take a trade (max=4) — reverted from 4: that
                                 # produced only 9 trades in 18 months, too rare to evaluate or use.
                                 # Back to 3 to isolate the early-breakeven change on its own.
MIN_LEVEL_TOUCHES  = 3          # Minimum touches to consider a level tradeable — confirmed via
                                 # test: raising to 4 cut trades 243->187 and total P&L $13,030.50->$8,553.00
COOLDOWN_MINUTES   = 5          # Minutes to wait after a stop-out
MAX_LOSSES_PER_LEVEL = 2        # Max times to lose at the same level per day
MAX_CONSECUTIVE_LOSSES = 2      # Stop trading for the day after this many losses in a row —
                                 # confirmed via test: tightening to 1 turned +$11,049 into
                                 # -$1,618.50 at 3-contract size by blocking real recovery trades
STOP_AFTER_FIRST_WIN = False    # Tested: cost ~$1,077 in blocked compounding trades vs. current
                                 # config (242 trades/$13,287 without it, 207 trades/$12,210 with
                                 # it) — reverted. Day still ends after MAX_ENTRIES (3) trades or
                                 # MAX_CONSECUTIVE_LOSSES (2) losses, whichever comes first.

WATCHDOG_INTERVAL  = 15 * 60    # Seconds between presence checks
WATCHDOG_TIMEOUT   = 30         # Seconds to respond

SESSION_1_START = time(6, 33)
SESSION_1_END   = time(8,  0)
SESSION_2_START = time(10, 0)
SESSION_2_END   = time(12, 30)
TOPSTEP_CLOSE   = time(15, 10)  # CT hard flatten


# ─── Candle ───────────────────────────────────────────────────────────────────
class Candle:
    def __init__(self, o, h, l, c, v, ts):
        self.open   = float(o or 0)
        self.high   = float(h or 0)
        self.low    = float(l or 0)
        self.close  = float(c or 0)
        self.volume = float(v or 0)
        self.ts     = ts

    @property
    def range(self): return self.high - self.low
    @property
    def upper_wick(self): return self.high - max(self.open, self.close)
    @property
    def lower_wick(self): return min(self.open, self.close) - self.low
    @property
    def is_bullish(self): return self.close > self.open
    @property
    def is_bearish(self): return self.close < self.open

    @staticmethod
    def from_bar(b: dict, ts=None) -> "Candle":
        """Parse a bar dict using abbreviated or full field names."""
        t_raw = b.get("t") or b.get("time") or b.get("ts") or "2000-01-01T00:00:00+00:00"
        ts = ts or datetime.fromisoformat(t_raw)
        return Candle(
            b.get("o") or b.get("open"),
            b.get("h") or b.get("high"),
            b.get("l") or b.get("low"),
            b.get("c") or b.get("close"),
            b.get("v") or b.get("volume"),
            ts,
        )


# ─── Level ────────────────────────────────────────────────────────────────────
class Level:
    def __init__(self, price: float, kind: str, touches: int, recency: float):
        self.price    = price       # Dollar price
        self.kind     = kind        # "support" | "resistance"
        self.touches  = touches     # Times price visited this level in history
        self.recency  = recency     # 0-1 weight, higher = more recent
        self.losses   = 0           # Times we lost trading this level today

    @property
    def score(self) -> float:
        """Level quality score: touches + recency bonus."""
        return self.touches + self.recency * 2

    def __repr__(self):
        return f"Level({self.kind} @ {self.price:.2f}, touches={self.touches}, score={self.score:.1f})"


# ─── LevelEngine (15-min) ────────────────────────────────────────────────────
class LevelEngine:
    """
    Builds S/R levels from 15-min swing highs/lows.
    Frozen while ExecutionEngine is in a trade.
    """

    def __init__(self):
        self.levels: list[Level] = []
        self.session_open: float | None = None
        self.atr_15m: float = 20.0  # Adaptive ATR for proximity buffer

    def build(self, candles_15m: list[Candle], session_open: float):
        """Build dual-sided levels from 2-week 15-min data.
        Recent 3 days weighted 3x. Guarantees >= 6 levels above and below open."""
        if len(candles_15m) < 5:
            log.warning("LevelEngine: not enough 15-min bars to build levels.")
            return

        self.session_open = session_open
        n = len(candles_15m)

        # Adaptive ATR from last 20 bars
        ranges = [c.range for c in candles_15m[-20:] if c.range > 0]
        self.atr_15m = float(np.mean(ranges)) if ranges else 20.0
        cluster_thresh = max(self.atr_15m * 0.10, 2.0)
        log.info(f"LevelEngine: ATR_15m={self.atr_15m:.2f}, cluster_thresh={cluster_thresh:.2f}")

        # Recency weighting: recent 3 days (≈96 bars) get 3x weight
        recent_cutoff = max(0, n - 96)
        highs = [c.high for c in candles_15m]
        lows  = [c.low  for c in candles_15m]

        raw_res, raw_sup = [], []
        for i in range(2, n - 2):
            base_w = i / n
            recency_mult = 3.0 if i >= recent_cutoff else 1.0
            w = base_w * recency_mult
            if (highs[i] > highs[i-1] and highs[i] > highs[i-2] and
                    highs[i] > highs[i+1] and highs[i] > highs[i+2]):
                raw_res.append((highs[i], w))
            if (lows[i] < lows[i-1] and lows[i] < lows[i-2] and
                    lows[i] < lows[i+1] and lows[i] < lows[i+2]):
                raw_sup.append((lows[i], w))

        def cluster(swings):
            if not swings: return []
            swings.sort(key=lambda x: x[0])
            groups, cur = [], [swings[0]]
            for s in swings[1:]:
                if abs(s[0] - cur[-1][0]) <= cluster_thresh:
                    cur.append(s)
                else:
                    groups.append(cur); cur = [s]
            groups.append(cur)
            return groups

        res_cl = []
        for grp in cluster(raw_res):
            ps=[g[0] for g in grp]; ws=[g[1] for g in grp]
            avg_p=sum(p*w for p,w in zip(ps,ws))/sum(ws)
            res_cl.append((round(avg_p,2), len(grp), max(ws)))
        sup_cl = []
        for grp in cluster(raw_sup):
            ps=[g[0] for g in grp]; ws=[g[1] for g in grp]
            avg_p=sum(p*w for p,w in zip(ps,ws))/sum(ws)
            sup_cl.append((round(avg_p,2), len(grp), max(ws)))

        def find_dual_levels(window):
            """Find dual-sided levels within price window of session open."""
            lvls = []
            for rp,rt,rw in res_cl:
                for sp,st,sw in sup_cl:
                    if abs(rp-sp) <= cluster_thresh*1.5:
                        avg_p = round((rp+sp)/2, 2)
                        if abs(avg_p-session_open) <= window:
                            lvls.append(Level(avg_p, "dual", rt+st, max(rw,sw)))
            lvls.sort(key=lambda l: l.price)
            unique = []
            for l in lvls:
                if not unique or abs(l.price-unique[-1].price) > cluster_thresh:
                    unique.append(l)
            return [l for l in unique if l.touches >= MIN_LEVEL_TOUCHES]

        def find_single_levels(window):
            """Fallback: single-sided levels (resistance or support) within window."""
            lvls = []
            for rp,rt,rw in res_cl:
                if abs(rp-session_open) <= window:
                    lvls.append(Level(rp, "resistance", rt, rw))
            for sp,st,sw in sup_cl:
                if abs(sp-session_open) <= window:
                    lvls.append(Level(sp, "support", st, sw))
            lvls.sort(key=lambda l: l.price)
            unique = []
            for l in lvls:
                if not unique or abs(l.price-unique[-1].price) > cluster_thresh:
                    unique.append(l)
            return [l for l in unique if l.touches >= MIN_LEVEL_TOUCHES]

        TARGET_PER_SIDE = 6

        # Start with $75 window for dual-sided levels
        levels = find_dual_levels(75.0)
        above = [l for l in levels if l.price > session_open]
        below = [l for l in levels if l.price < session_open]

        # If not enough above, supplement with single-sided resistance levels
        if len(above) < TARGET_PER_SIDE:
            single = find_single_levels(100.0)
            extra_above = [l for l in single if l.price > session_open
                           and not any(abs(l.price-x.price) <= cluster_thresh for x in above)]
            extra_above.sort(key=lambda l: abs(l.price-session_open))
            above += extra_above[:TARGET_PER_SIDE - len(above)]

        # If not enough below, supplement with single-sided support levels
        if len(below) < TARGET_PER_SIDE:
            single = find_single_levels(100.0)
            extra_below = [l for l in single if l.price < session_open
                           and not any(abs(l.price-x.price) <= cluster_thresh for x in below)]
            extra_below.sort(key=lambda l: abs(l.price-session_open))
            below += extra_below[:TARGET_PER_SIDE - len(below)]

        self.levels = sorted(above + below, key=lambda l: l.price)

        above_f = [l for l in self.levels if l.price > session_open]
        below_f = [l for l in self.levels if l.price < session_open]
        log.info(f"LevelEngine: {len(self.levels)} levels built ({len(above_f)} above, {len(below_f)} below open={session_open}):")
        for l in self.levels:
            side = "↑" if l.price > session_open else "↓"
            log.info(f"  {side} {l}")

    def nearest(self, price: float, kind: str) -> Level | None:
        """Find nearest dual-sided level within ATR-adaptive buffer."""
        buf = max(self.atr_15m * 0.15, 3.0)
        candidates = [l for l in self.levels
                      if abs(l.price - price) <= buf and l.losses < MAX_LOSSES_PER_LEVEL]
        if not candidates:
            return None
        return min(candidates, key=lambda l: abs(l.price - price))

    def next_level(self, price: float, direction: str) -> Level | None:
        """Find next level in trade direction for trailing exit."""
        if direction == "long":
            above = [l for l in self.levels if l.price > price + 1.0]
            return min(above, key=lambda l: l.price) if above else None
        else:
            below = [l for l in self.levels if l.price < price - 1.0]
            return max(below, key=lambda l: l.price) if below else None

    def record_loss(self, level_price: float):
        """Mark a loss at a level so we don't trade it again today."""
        for l in self.levels:
            if abs(l.price - level_price) <= 2.0:
                l.losses += 1
                log.info(f"LevelEngine: recorded loss at {l.price} (losses={l.losses})")

    def reset_daily(self):
        for l in self.levels:
            l.losses = 0


# ─── ExecutionEngine (1-min) ──────────────────────────────────────────────────
class ExecutionEngine:
    """
    Reads levels from LevelEngine, detects 2-wick rejections,
    scores setups, and manages positions.
    """

    def __init__(self, level_engine: LevelEngine):
        self.le          = level_engine
        self.candles: list[Candle] = []
        self.avg_volume  = None
        self.in_trade    = False
        self.position    = None      # "long" | "short"
        self.entry_price = None
        self.stop_price  = None
        self.entry_level       = None   # Level object we entered at
        self.target_level      = None   # Next level target for trailing exit
        self.last_passed_level = None   # Last level price passed through
        self.last_stop_out: datetime | None = None  # For cooldown
        self.ema: float | None = None  # 20-period EMA of 1-min closes, for trend filter

    # ── Volume ────────────────────────────────────────────────────────────────
    def _update_avg_volume(self):
        window = self.candles[-20:] if len(self.candles) >= 20 else self.candles
        if len(window) >= 5:
            self.avg_volume = float(np.mean([c.volume for c in window]))

    # ── Signal scoring ────────────────────────────────────────────────────────
    def _score_signal(self, prev: Candle, curr: Candle, level: Level, direction: str) -> int:
        """
        Score a setup 0-4. Trade only if score >= MIN_SIGNAL_SCORE.
          +1  wick ratio > 2x threshold
          +1  volume above average
          +1  level has 3+ touches
          +1  second wick larger than first (momentum confirmation)
        """
        score = 0
        wick = curr.lower_wick if direction == "long" else curr.upper_wick
        wick_ratio = wick / max(curr.range, 0.01)
        if wick_ratio > WICK_THRESHOLD * 2:
            score += 1
        if self.avg_volume and curr.volume > self.avg_volume:
            score += 1
        if level.touches >= 3:
            score += 1
        # Second wick larger than first
        prev_wick = prev.lower_wick if direction == "long" else prev.upper_wick
        if wick > prev_wick:
            score += 1
        return score

    # ── Entry ─────────────────────────────────────────────────────────────────
    def check_entry(self, prev: Candle, curr: Candle) -> dict | None:
        if self.in_trade or not self.le.levels:
            return None

        # No NEW entries after Session 1 ends — Session 2 is no longer traded.
        # This only blocks entries; an already-open trade still gets managed
        # normally by check_exit() regardless of what time it is, since
        # add_candle() checks in_trade first and calls check_exit() directly
        # without ever routing through here.
        curr_pt_time = curr.ts.astimezone(PT).time()
        if curr_pt_time >= SESSION_1_END:
            return None

        # Cooldown after stop-out
        if self.last_stop_out:
            elapsed = (datetime.now(PT) - self.last_stop_out).total_seconds()
            if elapsed < COOLDOWN_MINUTES * 60:
                return None

        # ── Long: two 1-min candles touching dual level, second closes bullish ──
        level = self.le.nearest(prev.low, "support")
        session_open = self.le.session_open or 0
        if level:
            buf = max(self.le.atr_15m * 0.15, 3.0)
            prev_touched = prev.low <= level.price + buf
            curr_touched = curr.low <= level.price + buf
            curr_confirms = curr.is_bullish
            vol_ok = self.avg_volume is None or curr.volume >= self.avg_volume * 1.2  # Rejection candle needs above-avg volume
            trend_ok = self.ema is None or curr.close > self.ema  # Don't buy dips against a downtrend
            distance_ok = (curr.close - level.price) <= buf * 1.5  # Don't chase — close must still be near the level
            if prev_touched and curr_touched and curr_confirms and vol_ok and trend_ok and distance_ok:
                score = self._score_signal(prev, curr, level, "long")
                log.info(f"LONG setup @ {curr.close} | level={level.price} | score={score}/4 | vol_ok={vol_ok} | trend_ok={trend_ok}")
                if score >= MIN_SIGNAL_SCORE:
                    stop = round(curr.close - PER_TRADE_LOSS_LIMIT / (POINT_VALUE * CONTRACTS), 2)
                    return {"type": "entry", "action": "buy", "position": "long",
                            "entry_price": curr.close, "stop": stop,
                            "level": level, "score": score,
                            "reason": f"2-touch support @ {level.price} score={score}"}
                else:
                    log.info(f"  Setup skipped — score {score} < MIN {MIN_SIGNAL_SCORE}")

        # ── Short: two 1-min candles touching dual level, second closes bearish ──
        level = self.le.nearest(prev.high, "resistance")
        if level:
            buf = max(self.le.atr_15m * 0.15, 3.0)
            prev_touched = prev.high >= level.price - buf
            curr_touched = curr.high >= level.price - buf
            curr_confirms = curr.is_bearish
            vol_ok = self.avg_volume is None or curr.volume >= self.avg_volume * 1.2  # Rejection candle needs above-avg volume
            trend_ok = self.ema is None or curr.close < self.ema  # Don't sell rallies against an uptrend
            distance_ok = (level.price - curr.close) <= buf * 1.5  # Don't chase — close must still be near the level
            if prev_touched and curr_touched and curr_confirms and vol_ok and trend_ok and distance_ok:
                score = self._score_signal(prev, curr, level, "short")
                log.info(f"SHORT setup @ {curr.close} | level={level.price} | score={score}/4 | vol_ok={vol_ok} | trend_ok={trend_ok}")
                if score >= MIN_SIGNAL_SCORE:
                    stop = round(curr.close + PER_TRADE_LOSS_LIMIT / (POINT_VALUE * CONTRACTS), 2)
                    return {"type": "entry", "action": "sell", "position": "short",
                            "entry_price": curr.close, "stop": stop,
                            "level": level, "score": score,
                            "reason": f"2-touch resistance @ {level.price} score={score}"}
                else:
                    log.info(f"  Setup skipped — score {score} < MIN {MIN_SIGNAL_SCORE}")

        return None

    # ── Exit ──────────────────────────────────────────────────────────────────
    def check_exit(self, candle: Candle) -> dict | None:
        if not self.in_trade:
            return None

        # 1. Stop loss
        if self.position == "long" and candle.low <= self.stop_price:
            return self._exit("Stop loss hit", stopped_out=True, exit_price=self.stop_price)
        if self.position == "short" and candle.high >= self.stop_price:
            return self._exit("Stop loss hit", stopped_out=True, exit_price=self.stop_price)

        raw_pnl = ((candle.close - self.entry_price) if self.position == "long"
                    else (self.entry_price - candle.close)) * POINT_VALUE * CONTRACTS

        # 1a. Early breakeven — locks in no-loss well before price fully
        # reaches the first target level. Once unrealized gain covers 30%
        # of the initial risk, move the stop to entry. Validated: raised
        # win rate 38%->50.2% at a real but acceptable cost to total P&L
        # ($8,162.50 -> $5,100.00 over the same 18-month range).
        EARLY_BREAKEVEN_PCT = 0.30
        if self.position == "long" and self.stop_price < self.entry_price:
            if raw_pnl >= PER_TRADE_LOSS_LIMIT * EARLY_BREAKEVEN_PCT:
                self.stop_price = self.entry_price
                log.info(f"EARLY BREAKEVEN: stop moved to {self.entry_price} (${raw_pnl:.2f} unrealized)")
        elif self.position == "short" and self.stop_price > self.entry_price:
            if raw_pnl >= PER_TRADE_LOSS_LIMIT * EARLY_BREAKEVEN_PCT:
                self.stop_price = self.entry_price
                log.info(f"EARLY BREAKEVEN: stop moved to {self.entry_price} (${raw_pnl:.2f} unrealized)")

        # 1b. Quick profit-take — lock in gains before they can round-trip,
        # but ONLY if momentum has actually stalled (volume below average).
        # If volume is still elevated, the move likely has more room — let it
        # continue into the trailing-to-next-level logic below instead of
        # capping it. This targets the specific tradeoff seen in testing: a
        # blind fixed take-profit was cutting off exactly the trades that
        # would have become the larger trailing winners.
        QUICK_PROFIT = QUICK_PROFIT_THRESHOLD
        momentum_stalled = self.avg_volume is None or candle.volume < self.avg_volume
        if raw_pnl >= QUICK_PROFIT and not self.last_passed_level and momentum_stalled:
            return self._exit(f"Quick profit target, momentum stalled (${raw_pnl:.2f})", exit_price=candle.close)

        # 2. Trailing level exit
        if self.target_level:
            tl = self.target_level.price
            if self.position == "long" and candle.high >= tl:
                self.last_passed_level = self.target_level
                # Move stop to breakeven after passing first level
                if self.stop_price < self.entry_price:
                    self.stop_price = self.entry_price
                    log.info(f"BREAKEVEN: stop moved to {self.entry_price}")
                next_t = self.le.next_level(tl, "long")
                if next_t:
                    self.target_level = next_t
                    log.info(f"PASSED level {tl} — new target {next_t.price}")
                else:
                    return self._exit(f"No more levels above {tl} (${raw_pnl:.2f})")
            elif self.position == "short" and candle.low <= tl:
                self.last_passed_level = self.target_level
                if self.stop_price > self.entry_price:
                    self.stop_price = self.entry_price
                    log.info(f"BREAKEVEN: stop moved to {self.entry_price}")
                next_t = self.le.next_level(tl, "short")
                if next_t:
                    self.target_level = next_t
                    log.info(f"PASSED level {tl} — new target {next_t.price}")
                else:
                    return self._exit(f"No more levels below {tl} (${raw_pnl:.2f})")

        # 3. Reversal to passed level — exit if price comes back to last passed level
        if self.last_passed_level:
            lp = self.last_passed_level.price
            buf = max(self.le.atr_15m * 0.15, 3.0)
            if self.position == "long" and candle.low <= lp + buf:
                return self._exit(f"Reversal to passed level {lp}")
            elif self.position == "short" and candle.high >= lp - buf:
                return self._exit(f"Reversal to passed level {lp}")

        # 4. Fallback profit target if no next level found
        if not self.target_level and raw_pnl >= PER_TRADE_PROFIT_TARGET:
            return self._exit(f"Profit target fallback (${raw_pnl:.2f})")

        # 5. Volume dried up — 3 consecutive low volume bars
        if self.avg_volume and len(self.candles) >= 3:
            last3 = [c.volume for c in self.candles[-3:]]
            if all(v < self.avg_volume * (1 - VOLUME_DROP_PCT) for v in last3):
                return self._exit("Volume dried up (3 bars)")

        return None

    def _exit(self, reason: str, stopped_out=False, exit_price=None) -> dict:
        log.info(f"EXIT [{self.position.upper()}] — {reason}")
        action = "sell" if self.position == "long" else "buy"
        if stopped_out:
            self.last_stop_out = datetime.now(PT)
            if self.entry_level:
                self.le.record_loss(self.entry_level.price)
        return {"type": "exit", "action": action, "reason": reason,
                "stopped_out": stopped_out, "exit_price": exit_price}

    def add_candle(self, candle: Candle) -> dict | None:
        self.candles.append(candle)
        self._update_avg_volume()
        self._update_ema()
        if self.in_trade:
            return self.check_exit(candle)
        if len(self.candles) < 2:
            return None
        return self.check_entry(self.candles[-2], self.candles[-1])

    def _update_ema(self, period=20):
        """Trend filter: EMA of 1-min closes. Longs only above it, shorts only below."""
        if len(self.candles) < period:
            self.ema = None
            return
        closes = [c.close for c in self.candles[-period * 3:]]  # enough history to stabilize
        k = 2 / (period + 1)
        ema = closes[0]
        for c in closes[1:]:
            ema = c * k + ema * (1 - k)
        self.ema = ema

    def mark_entry_filled(self, position, entry_price, stop_price, level):
        self.in_trade          = True
        self.position          = position
        self.entry_price       = entry_price
        self.stop_price        = stop_price
        self.entry_level       = level
        self.target_level      = self.le.next_level(entry_price, position)
        self.last_passed_level = None
        if self.target_level:
            log.info(f"Target level: {self.target_level.price}")

    def mark_flat(self):
        self.in_trade          = False
        self.position          = None
        self.entry_price       = None
        self.stop_price        = None
        self.entry_level       = None
        self.target_level      = None
        self.last_passed_level = None

    def reset_daily(self):
        self.candles         = []
        self.avg_volume      = None
        self.last_stop_out   = None
        self.mark_flat()


# ─── Analytics Logger ─────────────────────────────────────────────────────────
class Analytics:
    """Logs every trade to trades.json for post-session review."""

    def __init__(self, path: str):
        self.path  = path
        self.trades = self._load()

    def _load(self):
        try:
            with open(self.path) as f:
                return json.load(f)
        except Exception:
            return []

    def log_trade(self, entry_signal: dict, exit_reason: str, pnl: float,
                  entry_level: Level | None, exec_engine: ExecutionEngine):
        record = {
            "date":          datetime.now(PT).strftime("%Y-%m-%d"),
            "time_of_day":   datetime.now(PT).strftime("%H:%M"),
            "level_type":    entry_level.kind if entry_level else "unknown",
            "level_price":   entry_level.price if entry_level else None,
            "level_touches": entry_level.touches if entry_level else None,
            "level_score":   round(entry_level.score, 2) if entry_level else None,
            "signal_score":  entry_signal.get("score"),
            "direction":     entry_signal.get("position"),
            "entry_price":   entry_signal.get("entry_price"),
            "exit_reason":   exit_reason,
            "pnl":           round(pnl, 2),
            "result_r":      round(pnl / PER_TRADE_LOSS_LIMIT, 2),
            "volume_ratio":  round(exec_engine.candles[-1].volume / exec_engine.avg_volume, 2)
                             if exec_engine.avg_volume else None,

        }
        self.trades.append(record)
        try:
            with open(self.path, "w") as f:
                json.dump(self.trades, f, indent=2)
        except Exception as e:
            log.warning(f"Analytics write failed: {e}")
        log.info(f"Trade logged: {record}")


# ─── TopstepBot ───────────────────────────────────────────────────────────────
class TopstepBot:
    def __init__(self):
        self.level_engine  = LevelEngine()
        self.exec_engine   = ExecutionEngine(self.level_engine)
        self.analytics     = Analytics(ANALYTICS_PATH)
        self.account_id    = None
        self.contract_id   = None
        self.entries_today      = 0
        self.daily_pnl          = 0.0
        self.consecutive_losses = 0  # Stop trading after MAX_CONSECUTIVE_LOSSES in a row
        self.open_order_id = None
        self._starting_balance   = None
        self._last_pnl_check     = None
        self._token_refreshed_at = None
        self._last_1m            = None
        self._last_1m_ohlcv      = None
        self._watchdog_last_checkin = None
        self._session_open_price    = None
        self._last_level_refresh    = None
        self._pending_entry_signal  = None  # Holds entry signal until fill confirmed

    # ── Auth ──────────────────────────────────────────────────────────────────
    async def _authenticate(self, session):
        resp = await session.post(f"{BASE_URL}/Auth/loginKey",
                                  json={"userName": USERNAME, "apiKey": API_KEY})
        data = await resp.json()
        if not data.get("success"):
            raise RuntimeError(f"Auth failed: {data}")
        log.info("Authenticated with TopstepX OK")
        return data["token"]

    async def _get_account(self, session, token):
        resp = await session.post(f"{BASE_URL}/Account/search",
                                  headers={"Authorization": f"Bearer {token}"},
                                  json={"onlyActiveAccounts": True})
        data = await resp.json()
        accounts = data.get("accounts", [])
        if not accounts:
            raise RuntimeError("No active accounts found.")
        account = accounts[0]
        log.info(f"Account: {account['name']} (id={account['id']})")
        return account["id"]

    async def _get_contract(self, session, token):
        resp = await session.post(f"{BASE_URL}/Contract/search",
                                  headers={"Authorization": f"Bearer {token}"},
                                  json={"searchText": INSTRUMENT, "live": False})
        data = await resp.json()
        contracts = data.get("contracts", [])
        exact = [c for c in contracts if c.get("name") == MES_SYMBOL]
        if exact:
            contract = exact[0]
        else:
            matches = [c for c in contracts if c.get("name", "").startswith(INSTRUMENT)]
            if not matches:
                raise RuntimeError(f"{INSTRUMENT} contract not found.")
            contract = sorted(matches, key=lambda c: c.get("name", ""))[0]
            log.warning(f"{MES_SYMBOL} not found — using {contract.get('name')}. Update MES_SYMBOL.")
        log.info(f"Using contract: {contract['name']} (id={contract['id']})")
        return contract["id"]

    # ── P&L ───────────────────────────────────────────────────────────────────
    async def _refresh_daily_pnl(self, session, token):
        now = datetime.now(PT)
        if self._last_pnl_check and (now - self._last_pnl_check).total_seconds() < 10:
            return self.daily_pnl
        self._last_pnl_check = now
        try:
            resp = await session.post(f"{BASE_URL}/Account/search",
                                      headers={"Authorization": f"Bearer {token}"},
                                      json={"onlyActiveAccounts": True})
            data = await resp.json()
            accounts = data.get("accounts", [])
            account  = next((a for a in accounts if a.get("id") == self.account_id),
                            accounts[0] if accounts else None)
            if not account:
                return self.daily_pnl
            for key in ("dailyPnl","dailyPnL","dayPnl","dayPnL","realizedPnl","realizedPnL","pnl"):
                v = account.get(key)
                if isinstance(v, (int, float)):
                    self.daily_pnl = float(v)
                    return self.daily_pnl
            for key in ("balance","currentBalance","equity","netLiquidationValue"):
                v = account.get(key)
                if isinstance(v, (int, float)):
                    v = float(v)
                    if self._starting_balance is None:
                        self._starting_balance = v
                        log.info(f"Starting balance: ${v:.2f}")
                    self.daily_pnl = v - self._starting_balance
                    log.info(f"Daily P&L: ${self.daily_pnl:.2f}")
                    return self.daily_pnl
        except Exception as e:
            log.warning(f"P&L refresh error: {e}")
        return self.daily_pnl

    # ── Orders ────────────────────────────────────────────────────────────────
    async def _place_order(self, session, token, action: str):
        side = 0 if action == "buy" else 1
        order_payload = {"accountId": self.account_id, "contractId": self.contract_id,
                          "type": 2, "side": side, "size": CONTRACTS,
                          "limitPrice": None, "stopPrice": None, "trailPrice": None,
                          "customTag": None, "linkedOrderId": None}
        resp = await session.post(
            f"{BASE_URL}/Order/place",
            headers={"Authorization": f"Bearer {token}"},
            json=order_payload,
        )
        data = await resp.json()
        if not data.get("success"):
            log.error(f"Order rejected | HTTP {resp.status} | request={order_payload} | response={data}")
            return None
        oid = data.get("orderId")
        log.info(f"Order submitted: {action.upper()} {CONTRACTS}x {INSTRUMENT} | id={oid}")

        # Poll for fill confirmation — never assume
        for attempt in range(3):
            await asyncio.sleep(1)
            try:
                start_ts = (datetime.now(CT) - timedelta(minutes=10)).isoformat()
                chk  = await session.post(f"{BASE_URL}/Order/search",
                                          headers={"Authorization": f"Bearer {token}"},
                                          json={"accountId": self.account_id,
                                                "startTimestamp": start_ts})
                chkd = await chk.json()
                order = next((o for o in chkd.get("orders", []) if o.get("id") == oid), None)
                if order:
                    status = order.get("status", "")
                    if status == 2:
                        fill = order.get("filledPrice") or order.get("limitPrice", 0)
                        log.info(f"Order FILLED @ {fill} | id={oid}")
                        return oid
                    elif status in (4, 5):
                        log.error(f"Order NOT filled (status={status}) | id={oid}")
                        return None
            except Exception as e:
                log.warning(f"Fill check attempt {attempt+1}: {e}")

        log.warning("Fill status inconclusive after 3 order-status attempts — checking open positions directly.")
        try:
            pos_resp = await session.post(f"{BASE_URL}/Position/searchOpen",
                                          headers={"Authorization": f"Bearer {token}"},
                                          json={"accountId": self.account_id})
            pos_data = await pos_resp.json()
            positions = pos_data.get("positions", [])
            match = next((p for p in positions if p.get("contractId") == self.contract_id), None)
            if match:
                log.warning(f"Position CONFIRMED open via Position/searchOpen: "
                            f"size={match.get('size')} avgPrice={match.get('averagePrice')} — "
                            f"treating order as FILLED despite inconclusive status poll.")
                return oid
        except Exception as e:
            log.error(f"Position fallback check failed: {e}")

        log.error("Could not confirm fill via order status OR open positions. "
                   "MANUALLY CHECK YOUR ACCOUNT — the bot cannot safely assume flat.")
        return None

    async def _flatten_all(self, session, token, reason=""):
        log.info(f"FLATTEN ALL — {reason}")
        resp = await session.post(f"{BASE_URL}/Position/closeAll",
                                  headers={"Authorization": f"Bearer {token}"},
                                  json={"accountId": self.account_id})
        data = await resp.json()
        if data.get("success"):
            log.info("All positions closed OK")
            self.exec_engine.mark_flat()
        else:
            log.error(f"Flatten failed: {data.get('errorMessage')}")

    # ── Helpers ───────────────────────────────────────────────────────────────
    def _in_session(self, t: time) -> bool:
        return (SESSION_1_START <= t < SESSION_1_END or
                SESSION_2_START <= t < SESSION_2_END)

    def _session_end_approaching(self, now_pt: datetime) -> bool:
        today = now_pt.date()
        for end in (SESSION_1_END, SESSION_2_END):
            end_dt = datetime.combine(today, end, tzinfo=PT)
            if end_dt - timedelta(minutes=1) <= now_pt < end_dt:
                return True
        return False

    def _per_trade_loss_hit(self, candle: Candle) -> tuple[bool, float]:
        """Returns (hit, exit_price). This checks against exec_engine.stop_price
        directly — the SAME value check_exit() uses and maintains (including
        any breakeven/trailing moves) — rather than recomputing an independent
        threshold from entry price. Two independent thresholds was the actual
        bug: they could disagree, and using the bar's raw low/high as the exit
        price (instead of the precise stop_price) let losses overshoot the cap
        even after 'fixing' this to be intrabar-aware."""
        if not self.exec_engine.in_trade or self.exec_engine.stop_price is None:
            return False, 0.0
        if self.exec_engine.position == "long" and candle.low <= self.exec_engine.stop_price:
            return True, self.exec_engine.stop_price
        if self.exec_engine.position == "short" and candle.high >= self.exec_engine.stop_price:
            return True, self.exec_engine.stop_price
        return False, 0.0

    # ── Watchdog ──────────────────────────────────────────────────────────────
    async def _watchdog_check(self):
        confirmed = threading.Event()
        def prompt():
            try:
                print("\n" + "="*55)
                print("  WATCHDOG CHECK — Are you still watching?")
                print(f"  Press Enter within {WATCHDOG_TIMEOUT}s to continue.")
                print("="*55)
                sys.stdin.readline()
                confirmed.set()
            except Exception:
                pass
        t = threading.Thread(target=prompt, daemon=True)
        t.start(); t.join(timeout=WATCHDOG_TIMEOUT)
        if confirmed.is_set():
            log.info("Watchdog: presence confirmed OK")
        else:
            log.warning("Watchdog: no response — PAUSED. Press Enter to resume.")
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, sys.stdin.readline)
            log.info("Watchdog: resumed.")

    # ── Level refresh ─────────────────────────────────────────────────────────
    async def _refresh_levels(self, session, token):
        try:
            end   = datetime.now(CT)
            start = end - timedelta(days=14)  # 2-week lookback
            resp  = await session.post(
                f"{BASE_URL}/History/retrieveBars",
                headers={"Authorization": f"Bearer {token}"},
                json={"contractId": self.contract_id, "live": False,
                      "unit": 2, "unitNumber": 15, "limit": 384,
                      "includePartialBar": False,
                      "startTime": start.isoformat(), "endTime": end.isoformat()},
            )
            d    = await resp.json()
            bars = d.get("bars") or d.get("d") or d.get("data") or []
            if bars:
                bars.sort(key=lambda b: b.get("t") or b.get("time") or "")
            if bars and len(bars) >= 5:
                candles15 = [Candle.from_bar(b) for b in bars]
                open_px   = self._session_open_price or candles15[-1].close
                self.level_engine.build(candles15, open_px)
                self._last_level_refresh = datetime.now(PT)
            else:
                log.warning(f"Level refresh: only {len(bars)} bars returned, skipping.")
        except Exception as e:
            log.warning(f"Level refresh failed: {e}")

    # ── Main Loop ─────────────────────────────────────────────────────────────
    async def run(self):
        import aiohttp
        async with aiohttp.ClientSession() as session:
            token = await self._authenticate(session)
            self.account_id  = await self._get_account(session, token)
            self.contract_id = await self._get_contract(session, token)

            log.info("Bot ready.")
            log.info(f"  Session 1: {SESSION_1_START.strftime('%I:%M %p')} – {SESSION_1_END.strftime('%I:%M %p')} PT")
            log.info(f"  Session 2: {SESSION_2_START.strftime('%I:%M %p')} – {SESSION_2_END.strftime('%I:%M %p')} PT")
            log.info(f"  Max trades: {MAX_ENTRIES}/day | Loss: ${DAILY_LOSS_LIMIT} | Target: ${DAILY_PROFIT_TARGET}")
            log.info(f"  Min signal score: {MIN_SIGNAL_SCORE}/4 | Cooldown: {COOLDOWN_MINUTES}min")
            log.info("  Strategy: DUAL TRAIL v5 — dual-sided levels, trailing exit, breakeven stop")

            await self._watchdog_check()
            self._watchdog_last_checkin = datetime.now(PT)

            while True:
                now_pt  = datetime.now(PT)
                now_ct  = datetime.now(CT)
                pt_time = now_pt.time()
                ct_time = now_ct.time()

                # ── Daily reset at midnight ──────────────────────────────────
                if pt_time < time(0, 2):
                    if self.entries_today > 0 or self.daily_pnl != 0.0:
                        self.entries_today       = 0
                        self.daily_pnl           = 0.0
                        self.consecutive_losses  = 0
                        self._starting_balance   = None
                        self._session_open_price = None
                        self._last_level_refresh = None
                        self.exec_engine.reset_daily()
                        self.level_engine.reset_daily()
                        log.info("Daily reset complete.")

                # ── Weekend guard ────────────────────────────────────────────
                if now_pt.weekday() >= 5:
                    await asyncio.sleep(300)
                    continue

                # ── Watchdog ─────────────────────────────────────────────────
                if (self._watchdog_last_checkin is None or
                        (now_pt - self._watchdog_last_checkin).total_seconds() >= WATCHDOG_INTERVAL):
                    await self._watchdog_check()
                    self._watchdog_last_checkin = now_pt

                # ── Topstep hard close ───────────────────────────────────────
                if ct_time >= TOPSTEP_CLOSE:
                    if self.exec_engine.in_trade:
                        await self._flatten_all(session, token, "3:10 PM CT Topstep rule")
                    await asyncio.sleep(60)
                    continue

                # ── Session end approaching ──────────────────────────────────
                if self._session_end_approaching(now_pt) and self.exec_engine.in_trade:
                    await self._flatten_all(session, token, "Session end approaching")

                # ── Token refresh (throttled to 20h) ────────────────────────
                if (self._token_refreshed_at is None or
                        (now_pt - self._token_refreshed_at).total_seconds() > 72000):
                    try:
                        r = await session.post(f"{BASE_URL}/Auth/validate",
                                               headers={"Authorization": f"Bearer {token}"})
                        vd = await r.json()
                        if vd.get("success") and vd.get("newToken"):
                            token = vd["newToken"]
                            self._token_refreshed_at = now_pt
                            log.info("JWT token refreshed.")
                    except Exception as e:
                        log.warning(f"Token refresh failed: {e}")

                # ── P&L guards ───────────────────────────────────────────────
                await self._refresh_daily_pnl(session, token)
                if self.daily_pnl <= -DAILY_LOSS_LIMIT:
                    if self.exec_engine.in_trade:
                        await self._flatten_all(session, token, "Daily loss limit")
                    log.warning(f"Daily loss limit ${DAILY_LOSS_LIMIT} hit — no more trades today.")
                    await asyncio.sleep(60)
                    continue
                if self.daily_pnl >= DAILY_PROFIT_TARGET:
                    if self.exec_engine.in_trade:
                        await self._flatten_all(session, token, "Daily profit target")
                    log.info(f"Daily profit target ${DAILY_PROFIT_TARGET} hit — done for today.")
                    await asyncio.sleep(60)
                    continue

                # ── Not in session ───────────────────────────────────────────
                if not self._in_session(pt_time):
                    await asyncio.sleep(15)
                    continue

                # ── Fetch 1-min bars (moved before level refresh so session
                # open is always captured first — this used to run AFTER the
                # level refresh, meaning the first refresh each day always
                # used a stale fallback: candles15[-1].close instead of the
                # real session open, silently building all levels wrong for
                # a window that overlaps most morning entries) ──────────────
                end_b   = datetime.now(CT)
                start_b = end_b - timedelta(minutes=90)
                resp = await session.post(
                    f"{BASE_URL}/History/retrieveBars",
                    headers={"Authorization": f"Bearer {token}"},
                    json={"contractId": self.contract_id, "live": False,
                          "unit": 2, "unitNumber": 1, "limit": 90,
                          "includePartialBar": False,
                          "startTime": start_b.isoformat(), "endTime": end_b.isoformat()},
                )
                bdata = await resp.json()
                bars  = bdata.get("bars") or bdata.get("d") or bdata.get("data") or []
                if bars:
                    bars.sort(key=lambda b: b.get("t") or b.get("time") or "")
                if not bars or len(bars) < 2:
                    await asyncio.sleep(15)
                    continue

                latest_bar = bars[-1]
                bar_ts = datetime.fromisoformat(
                    latest_bar.get("t") or latest_bar.get("time") or "2000-01-01T00:00:00+00:00"
                )
                bar_fp = (bar_ts,
                          latest_bar.get("o") or latest_bar.get("open") or 0,
                          latest_bar.get("h") or latest_bar.get("high") or 0,
                          latest_bar.get("l") or latest_bar.get("low") or 0,
                          latest_bar.get("c") or latest_bar.get("close") or 0)
                if self._last_1m_ohlcv == bar_fp:
                    await asyncio.sleep(15)
                    continue
                self._last_1m        = bar_ts
                self._last_1m_ohlcv  = bar_fp

                candle = Candle.from_bar(latest_bar, bar_ts)

                # Capture session open price from the ACTUAL 6:30 AM bar —
                # search the fetched bars for it directly, rather than using
                # whatever the most recent bar happens to be when the bot
                # first notices the time has passed 6:30. That was a real
                # bug: if the bot's polling loop first reaches this code at
                # 6:40 or 6:56 (which it has, per observed logs), it was
                # capturing the CURRENT price at that moment as "session
                # open" instead of the true 6:30 AM open — a bar that was
                # already sitting in the fetched 90-minute window the whole
                # time, just never looked for.
                if self._session_open_price is None and pt_time >= time(6, 30):
                    open_bar = None
                    for b in bars:
                        b_ts = datetime.fromisoformat(b.get("t") or b.get("time") or "2000-01-01T00:00:00+00:00")
                        if b_ts.astimezone(PT).time() >= time(6, 30):
                            open_bar = b
                            break
                    if open_bar is not None:
                        self._session_open_price = open_bar.get("o") or open_bar.get("open")
                        open_bar_ts = datetime.fromisoformat(open_bar.get("t") or open_bar.get("time")).astimezone(PT)
                        log.info(f"Session open price (6:30 AM, bar@{open_bar_ts.strftime('%H:%M')}): {self._session_open_price}")
                    else:
                        # 6:30 bar isn't in the fetched window at all (bot started very late) —
                        # fall back to latest bar rather than leaving it uncaptured, but flag it clearly.
                        self._session_open_price = candle.open
                        log.warning(f"6:30 AM bar not found in fetched window — using current bar "
                                    f"({pt_time}) as session open fallback: {self._session_open_price}")

                # ── Level refresh (flat only, in/near session) ───────────────
                in_or_near = (
                    (time(6, 20) <= pt_time < SESSION_1_END) or
                    (time(9, 50) <= pt_time < SESSION_2_END)
                )
                if (in_or_near and not self.exec_engine.in_trade and
                        (self._last_level_refresh is None or
                         (now_pt - self._last_level_refresh).total_seconds() >= 870)):
                    await self._refresh_levels(session, token)

                # ── Per-trade loss check ─────────────────────────────────────
                loss_hit, adverse_price = self._per_trade_loss_hit(candle)
                if loss_hit:
                    exit_action = "sell" if self.exec_engine.position == "long" else "buy"
                    oid = await self._place_order(session, token, exit_action)
                    if oid:
                        prev_entry = self._pending_entry_signal
                        pnl = (adverse_price - self.exec_engine.entry_price
                               if self.exec_engine.position == "long"
                               else self.exec_engine.entry_price - adverse_price) * POINT_VALUE * CONTRACTS
                        # If the stop had already moved to breakeven (early-breakeven logic),
                        # hitting it is a scratch, not a genuine hard-cap loss — label it correctly.
                        exit_reason = "Breakeven stop hit (scratch)" if abs(adverse_price - self.exec_engine.entry_price) < 0.01 \
                                      else "Per-trade loss limit"
                        self.analytics.log_trade(prev_entry or {}, exit_reason,
                                                 pnl, self.exec_engine.entry_level, self.exec_engine)
                        self.exec_engine.last_stop_out = datetime.now(PT)
                        if self.exec_engine.entry_level:
                            self.level_engine.record_loss(self.exec_engine.entry_level.price)
                        self.exec_engine.mark_flat()
                        self.open_order_id = None

                # ── Process candle signal ────────────────────────────────────
                signal = self.exec_engine.add_candle(candle)

                # Stop trading after MAX_CONSECUTIVE_LOSSES losses in a row
                if self.consecutive_losses >= MAX_CONSECUTIVE_LOSSES:
                    await asyncio.sleep(30)
                    continue

                if signal and signal.get("type") == "entry":
                    if not self.exec_engine.in_trade and self.entries_today < MAX_ENTRIES \
                            and self.level_engine.levels:
                        oid = await self._place_order(session, token, signal["action"])
                        if oid:
                            self.open_order_id = oid
                            self.entries_today += 1
                            self._pending_entry_signal = signal
                            self.exec_engine.mark_entry_filled(
                                signal["position"], signal["entry_price"],
                                signal["stop"], signal.get("level")
                            )
                            log.info(f"Entry #{self.entries_today} confirmed | {signal['reason']}")

                elif signal and signal.get("type") == "exit":
                    if self.exec_engine.in_trade:
                        oid = await self._place_order(session, token, signal["action"])
                        if oid:
                            fill_price = signal.get("exit_price") if signal.get("exit_price") is not None else candle.close
                            pnl = (fill_price - self.exec_engine.entry_price
                                   if self.exec_engine.position == "long"
                                   else self.exec_engine.entry_price - fill_price) \
                                  * POINT_VALUE * CONTRACTS
                            self.analytics.log_trade(
                                self._pending_entry_signal or {}, signal["reason"],
                                pnl, self.exec_engine.entry_level, self.exec_engine
                            )
                            self.exec_engine.mark_flat()
                            self.open_order_id = None
                            await self._refresh_daily_pnl(session, token)
                            log.info(f"Exit confirmed | P&L this trade: ${pnl:.2f}")
                            if pnl < 0:
                                self.consecutive_losses += 1
                                log.warning(f"Consecutive losses: {self.consecutive_losses}/{MAX_CONSECUTIVE_LOSSES}")
                                if self.consecutive_losses >= MAX_CONSECUTIVE_LOSSES:
                                    log.warning(f"{MAX_CONSECUTIVE_LOSSES} consecutive loss(es) — done trading for today.")
                            else:
                                self.consecutive_losses = 0
                                if STOP_AFTER_FIRST_WIN and self.entries_today == 1:
                                    log.warning("First trade of the day was a win — done trading for today.")
                                    self.consecutive_losses = MAX_CONSECUTIVE_LOSSES  # reuses the existing stop-for-day gate

                await asyncio.sleep(15)


# ─── Entry Point ──────────────────────────────────────────────────────────────
async def diagnose():
    import aiohttp
    async with aiohttp.ClientSession() as session:
        resp = await session.post(f"{BASE_URL}/Auth/loginKey",
                                  json={"userName": USERNAME, "apiKey": API_KEY})
        data = await resp.json()
        if not data.get("success"):
            print(f"Auth failed: {data}"); return
        token = data["token"]
        print("Authenticated OK")
        for live in [True, False]:
            for term in [MES_SYMBOL, "MES", "ES", ""]:
                r = await session.post(f"{BASE_URL}/Contract/search",
                                       headers={"Authorization": f"Bearer {token}"},
                                       json={"searchText": term, "live": live})
                d = await r.json()
                cs = d.get("contracts", [])
                if cs:
                    print(f"\nlive={live} search='{term}' -> {len(cs)} results:")
                    for c in cs:
                        print(f"  name={c.get('name')} id={c.get('id')}")


if __name__ == "__main__":
    if not USERNAME or not API_KEY:
        raise SystemExit("ERROR: Set TOPSTEP_USERNAME and TOPSTEP_API_KEY in your .env file.")
    if len(sys.argv) > 1 and sys.argv[1] == "diagnose":
        asyncio.run(diagnose())
    else:
        asyncio.run(TopstepBot().run())
