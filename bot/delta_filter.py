"""
Order-flow delta filter for the CHOCH/FVG intrabar bot.
=======================================================
Backtest finding (3 months ES trades, Databento, 2026-04..07): a CHOCH midpoint
fill is only worth taking when trailing 5-min cumulative delta CONFIRMS it.
delta AGREES with the trade => higher win rate; delta OPPOSES => the false starts.

LIVE FEED: TopstepX GatewayTrade sends {price, volume, type, ...} where `type`
is the aggressor side as a NUMBER (observed 0 and 1). We do NOT hard-code which
of 0/1 is the buyer. Instead the tracker maps type->tentative sign and then
AUTO-CALIBRATES the polarity from the tape: aggressive buying should push price
UP, so it accumulates cov(sign, price-change) over the first few hundred moving
ticks and flips the sign if the tape disagrees. This makes the delta sign
self-correcting and impossible to leave inverted. Falls back to the tick rule
only if no side/type field is present.
"""
import time as _time
from collections import deque

DELTA_WINDOW_SECS = 300          # 5-min trailing window (matches backtest cd5)
_CALIB_MIN_MOVES  = 300          # price-moving ticks needed to lock polarity

class DeltaTracker:
    TYPE_KEYS = ("type", "tradeType", "aggressor", "aggressorSide", "side", "takerSide")
    BUY_TOKENS  = {"0", "B", "BID", "BUY", "BOT", "BUYER"}
    SELL_TOKENS = {"1", "A", "ASK", "SELL", "SLD", "SELLER"}

    def __init__(self, window_secs=DELTA_WINDOW_SECS, log=None, sample_n=20):
        self.window = window_secs
        self.log = log
        self.sample_n = sample_n
        self.events = deque()        # (epoch_secs, raw_signed_size)  raw = pre-polarity
        self._last_price = None
        self._samples = 0
        self.field = None
        self.method = None
        # polarity self-calibration
        self.sign_mult = +1          # multiplies raw delta; flips to -1 if tape disagrees
        self.calibrated = False
        self._cov = 0.0
        self._nmoves = 0

    def _raw_sign(self, tr):
        """Tentative +1 (buy) / -1 (sell) from a recognized field; None if absent.
        Polarity here is only a guess -- calibration fixes it from price action."""
        if not isinstance(tr, dict):
            return None
        for k in self.TYPE_KEYS:
            if k in tr and tr[k] is not None:
                self.field = k
                v = str(tr[k]).strip().upper()
                if v in self.BUY_TOKENS:
                    return +1
                if v in self.SELL_TOKENS:
                    return -1
                return None          # recognized key but unknown value -> no side
        return None

    def add_trade(self, price, size, tr=None, now=None):
        now = _time.time() if now is None else now
        try:
            price = float(price)
        except (TypeError, ValueError):
            return
        try:
            size = float(size)
        except (TypeError, ValueError):
            size = 1.0
        if size <= 0:
            size = 1.0

        rs = self._raw_sign(tr)
        self.method = "side"
        if rs is None:
            self.method = "tick"
            if self._last_price is None:
                rs = 0
            elif price > self._last_price:
                rs = +1
            elif price < self._last_price:
                rs = -1
            else:
                rs = 0

        # Polarity calibration -- only on real side data, only on price moves.
        if (self._last_price is not None and not self.calibrated
                and self.method == "side"):
            dp = price - self._last_price
            if dp != 0.0:
                self._cov += rs * dp
                self._nmoves += 1
                if self._nmoves >= _CALIB_MIN_MOVES:
                    self.calibrated = True
                    if self._cov < 0:
                        self.sign_mult = -1
                    if self.log is not None:
                        self.log.info(
                            f"[DELTA-CALIB] locked after {self._nmoves} moving ticks | "
                            f"cov={self._cov:+.1f} -> sign_mult={self.sign_mult} "
                            f"(field={self.field}). "
                            + ("Polarity FLIPPED — this feed's 0=sell/1=buy."
                               if self.sign_mult < 0 else
                               "Polarity confirmed — 0=buy/1=sell."))

        self._last_price = price
        self.events.append((now, rs * size))
        self._evict(now)

        if self.log is not None and self._samples < self.sample_n:
            self._samples += 1
            self.log.info(f"[DELTA-TICK-SAMPLE {self._samples}/{self.sample_n}] raw={tr} "
                          f"-> price={price} size={size} raw_sign={rs} "
                          f"method={self.method} field={self.field}")

    def _evict(self, now):
        cutoff = now - self.window
        while self.events and self.events[0][0] < cutoff:
            self.events.popleft()

    def delta(self, now=None):
        now = _time.time() if now is None else now
        self._evict(now)
        return self.sign_mult * sum(v for _, v in self.events)

    def agrees(self, direction, now=None):
        """(agrees, delta). Long wants delta>0, short wants delta<0. delta==0
        (warmup / no flow) does NOT agree, so a cold tape never fires an entry."""
        d = self.delta(now)
        if direction == "long":
            return (d > 0), d
        return (d < 0), d
