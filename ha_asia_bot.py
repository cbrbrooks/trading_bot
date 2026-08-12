"""
ha_asia_bot.py — LIVE volatility-gated Asia bot for TopstepX (MES, 3 contracts)
===============================================================================
The Asia-session sibling of ha_mes_bot.py. It reuses that bot's tested TopstepX
plumbing (auth, account, SignalR tick feed, bracket orders, the BE@30 stop move,
flatten) by subclassing HAMesBot, and swaps in the Asia strategy from
ha_strategy.py: 5-minute HA candles, VWAP anchored at the 3pm PT CME open, target
the nearest key level pooled from the prior NY session, and — the whole point —
only enter when 5-minute ATR >= ASIA_ATR_MIN (trade the active nights, skip the
dead chop). Trades 5:00–7:30pm PT, flat by 8pm.

⚠️  DO NOT RUN THIS LIVE YET. Sequence, in order:
   1. Validate the edge OUT-OF-SAMPLE first: run ha_asia_topstep.py on data after
      your tick sample (e.g. 2026-07-22..today) and confirm it's still positive.
      The Asia edge is fit on a small in-sample window; it must clear OOS first.
   2. Then run THIS bot with DRY_RUN=True for a few nights and check the log:
      entries and ATR-gate skips should match ha_asia_replay.py.
   3. The live breakeven move is the one piece unverifiable offline (see
      ha_mes_bot.py header) — confirm it on a practice account before funded.
DRY_RUN defaults to True. Needs the same .env as your other bots.
"""

import asyncio
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

import csv, os
import ha_strategy as S
import ha_mes_bot as base
from ha_mes_bot import HAMesBot, log, BASE_URL, CT, DRY_RUN, BE_ENABLED

PT = ZoneInfo("America/Los_Angeles")

# ── Forward-test config ──────────────────────────────────────────────────────
# The ATR gate is tunable here. We're forward-testing 3.5 (slightly better + more
# defensible in-sample than 4.0); override with env ASIA_ATR_MIN if you want.
S.ASIA_ATR_MIN = float(os.getenv("ASIA_ATR_MIN", "3.5"))
FORWARD_LOG = os.getenv("ASIA_FORWARD_LOG", "ha_asia_forward_log.csv")
_LOG_COLS = ["date", "entry_time", "exit_time", "dir", "entry", "stop", "target",
             "exit", "reason", "points", "net", "atr", "mins_into_session", "window", "mode"]


def _session_window(mins_in):
    """Tag the entry by minutes since 5pm. 30-60min is the in-sample loss zone —
    we TAKE these trades in the forward test but flag them so we can judge later."""
    if 30 <= mins_in < 60:
        return "WATCH_530_600"      # in-sample loss zone — logged, not filtered
    return f"{(mins_in // 30) * 30}-{(mins_in // 30) * 30 + 30}min"


class AsiaBars:
    """5-minute HA bars with VWAP anchored at 3pm PT, plus a 5-min high/low history
    for the ATR gate and a 1-min session wick list for targets. Seed it with the
    3pm→now 1-minute bars at startup so VWAP matches TopstepX, then feed live ticks."""

    def __init__(self):
        self.cur5 = None; self.cur5_key = None
        self.cur1 = None; self.cur1_key = None
        self.prev_ha = None
        self.last5 = None; self.prev2 = None
        self.vnum = 0.0; self.vden = 0.0; self.vwap = None
        self.highs5 = []; self.lows5 = []           # closed 5-min ranges → ATR
        self.upper_wicks = []; self.lower_wicks = []  # session 1-min wick tips

    @staticmethod
    def _key(ts, m):
        return (ts.year, ts.month, ts.day, ts.hour, (ts.minute // m) * m)

    def _roll1(self, price):
        b = self.cur1
        if (b["h"] - max(b["o"], b["c"])) >= S.MIN_WICK: self.upper_wicks.append(b["h"])
        if (min(b["o"], b["c"]) - b["l"]) >= S.MIN_WICK: self.lower_wicks.append(b["l"])

    def _roll5(self):
        b = self.cur5
        ho, hh, hl, hc = S.heikin(b["o"], b["h"], b["l"], b["c"],
                                  self.prev_ha[0] if self.prev_ha else None,
                                  self.prev_ha[1] if self.prev_ha else None)
        self.prev_ha = (ho, hc)
        tp = (b["h"] + b["l"] + b["c"]) / 3.0
        self.vnum += tp * b["v"]; self.vden += b["v"]
        self.vwap = self.vnum / self.vden if self.vden else b["c"]
        self.highs5.append(b["h"]); self.lows5.append(b["l"])
        hb = {"ha_open": ho, "ha_high": hh, "ha_low": hl, "ha_close": hc,
              "raw_high": b["h"], "raw_low": b["l"], "vwap": self.vwap}
        self.prev2 = self.last5; self.last5 = hb
        return hb

    def _apply(self, ts, price, size, emit):
        """Update 1-min and 5-min bars. Returns a closed 5-min HA dict on rollover
        (only when emit=True — seeding passes emit=False to build state silently)."""
        closed = None
        k1 = self._key(ts, 1)
        if self.cur1_key != k1:
            if self.cur1 is not None: self._roll1(price)
            self.cur1 = {"o": price, "h": price, "l": price, "c": price}; self.cur1_key = k1
        else:
            self.cur1["h"] = max(self.cur1["h"], price); self.cur1["l"] = min(self.cur1["l"], price); self.cur1["c"] = price
        k5 = self._key(ts, 5)
        if self.cur5_key != k5:
            if self.cur5 is not None: closed = self._roll5()
            self.cur5 = {"o": price, "h": price, "l": price, "c": price, "v": size}; self.cur5_key = k5
        else:
            self.cur5["h"] = max(self.cur5["h"], price); self.cur5["l"] = min(self.cur5["l"], price)
            self.cur5["c"] = price; self.cur5["v"] += size
        return closed if emit else None

    def add_tick(self, ts, price, size):
        return self._apply(ts, price, size, emit=True)

    def seed_1min(self, o, h, l, c, v, ts):
        # feed a historical 1-min bar as o->h->l->c ticks so 5-min OHLC/VWAP/wicks build correctly
        for p in (o, h, l, c):
            self._apply(ts, p, v / 4.0, emit=False)


class HAAsiaBot(HAMesBot):
    def __init__(self):
        super().__init__()
        self.bars = AsiaBars()
        self.ny_up = []; self.ny_lo = []
        self.session_seeded_for = None

    async def _fetch_1m(self, session, a_ct, b_ct):
        r = await session.post(f"{BASE_URL}/History/retrieveBars", headers=self._hdr(),
                               json={"contractId": self.contract_id, "live": False, "unit": 2,
                                     "unitNumber": 1, "limit": 5000, "includePartialBar": False,
                                     "startTime": a_ct.isoformat(), "endTime": b_ct.isoformat()})
        d = await r.json()
        out = []
        for x in d.get("bars") or d.get("data") or []:
            ts = x.get("t") or x.get("timestamp")
            o = x.get("o", x.get("open"))
            if ts is None or o is None: continue
            import pandas as pd
            out.append({"ts": pd.to_datetime(ts, utc=True).tz_convert(PT), "o": float(o),
                        "h": float(x.get("h", x.get("high"))), "l": float(x.get("l", x.get("low"))),
                        "c": float(x.get("c", x.get("close"))), "v": float(x.get("v", x.get("volume", 0)) or 0)})
        out.sort(key=lambda z: z["ts"]); return out

    async def _seed_session(self, session, day):
        """Build NY key levels (today's 6:30–1pm) and seed VWAP/bars from 3pm→now."""
        self.bars = AsiaBars()
        ny_a = datetime.combine(day, S.SESSION_OPEN, tzinfo=PT).astimezone(CT)
        ny_b = datetime.combine(day, time(13, 0), tzinfo=PT).astimezone(CT)
        ny = await self._fetch_1m(session, ny_a, ny_b)
        self.ny_up = [m["h"] for m in ny if (m["h"] - max(m["o"], m["c"])) >= S.MIN_WICK]
        self.ny_lo = [m["l"] for m in ny if (min(m["o"], m["c"]) - m["l"]) >= S.MIN_WICK]
        anchor = datetime.combine(day, S.ASIA_VWAP_ANCHOR, tzinfo=PT).astimezone(CT)
        now_ct = datetime.now(CT)
        pre = await self._fetch_1m(session, anchor, now_ct)
        for m in pre:
            if m["ts"].time() >= S.ASIA_VWAP_ANCHOR:
                self.bars.seed_1min(m["o"], m["h"], m["l"], m["c"], m["v"], m["ts"])
        log.info(f"Seeded Asia session {day}: {len(self.ny_up)+len(self.ny_lo)} NY levels, "
                 f"VWAP={self.bars.vwap}, {len(self.bars.highs5)} 5-min bars pre-rolled")
        self.session_seeded_for = day

    async def _on_bar_close(self, session, closed, now_pt):
        if self.in_trade or self.bars.prev2 is None: return
        if now_pt.time() < S.ASIA_TRADE_OPEN or now_pt.time() > S.ASIA_ENTRY_CUTOFF: return
        if self.trades_today >= base.MAX_TRADES_PER_DAY: return
        sig = S.entry_signal(self.bars.prev2, closed, closed["vwap"])
        if sig is None: return
        # ── ACTIVITY GATE ──
        atr = S.atr_points(self.bars.highs5, self.bars.lows5, len(self.bars.highs5) - 1)
        if atr < S.ASIA_ATR_MIN:
            log.info(f"{sig.upper()} signal skipped — ATR {atr:.2f} < {S.ASIA_ATR_MIN} (dead-chop gate)"); return
        is_long = sig == "long"
        cutoff = datetime.now(PT) - timedelta(seconds=S.CONSOL_WINDOW_SEC)
        with self.tick_lock:
            recent = [p for (t, p, _) in self.ticks if t >= cutoff]
        entry = S.consolidation_entry(recent, is_long)
        if entry is None: log.info(f"{sig.upper()} signal but no consolidation ticks — skip"); return
        stop = S.initial_stop(closed["raw_low"], closed["raw_high"], is_long)
        pool = (self.ny_up if is_long else self.ny_lo) + (self.bars.upper_wicks if is_long else self.bars.lower_wicks)
        tgt = S.cluster_target(list(pool), entry, is_long)
        if tgt is None: log.info(f"{sig.upper()} signal but no key-level target — skip"); return
        level, target = tgt
        if abs(target - entry) > S.ASIA_MAX_TP_POINTS:
            log.info(f"{sig.upper()} target {abs(target-entry):.1f}pt > {S.ASIA_MAX_TP_POINTS}pt cap — skip"); return
        stop_ticks_mag = round(abs(entry - stop) / S.TICK); tgt_ticks_mag = round(abs(target - entry) / S.TICK)
        stop_ticks = -stop_ticks_mag if is_long else stop_ticks_mag
        target_ticks = tgt_ticks_mag if is_long else -tgt_ticks_mag
        self.be_trigger, self.be_stop = S.breakeven_stop(entry, target, is_long)
        log.info(f"ASIA SIGNAL {sig.upper()} | ATR {atr:.1f} | entry~{entry} stop {stop} tgt {target} "
                 f"(level {level}) | BE {self.be_trigger}->{self.be_stop}")
        action = "buy" if is_long else "sell"
        oid, fill = await self._place_bracket_entry(session, action, stop_ticks, target_ticks)
        if oid is None: return
        self.in_trade = True; self.direction = sig
        self.entry = fill if fill else entry; self.stop = stop; self.target = target
        self.be_armed = False; self.entry_oid = oid
        self.stop_child_id = None if DRY_RUN else await self._find_stop_child(session)
        self.trades_today += 1
        # forward-test context
        self.entry_atr = round(atr, 2)
        mins_in = (now_pt.hour - 17) * 60 + now_pt.minute
        self.entry_mins_in = mins_in
        self.entry_window = _session_window(mins_in)
        self.entry_time_str = now_pt.strftime("%H:%M")
        if self.entry_window == "WATCH_530_600":
            log.info("  ⚠ WATCH: entry falls in the 5:30-6:00pm in-sample loss zone — "
                     "TAKING it anyway and tagging it, so forward data can judge that pattern")
        await self._alert(session, f"{'🟢 LONG' if is_long else '🔴 SHORT'} {base.INSTRUMENT} x{base.CONTRACTS} (Asia)"
                          f"{' [DRY]' if DRY_RUN else ''}\nentry {self.entry}  stop {stop}  target {target}  ATR {atr:.1f}"
                          + ("\n⚠ 5:30-6pm watch-window (taken, flagged)" if self.entry_window == "WATCH_530_600" else ""))

    def _record_trade(self, reason, exit_price, exit_time_str):
        """Append a completed (paper or live) trade to the forward-test CSV."""
        is_long = self.direction == "long"
        pts = (exit_price - self.entry) if is_long else (self.entry - exit_price)
        net = round(pts * base.POINT_VALUE * base.CONTRACTS - base.FEE_PER_RT * base.CONTRACTS, 2)
        row = {"date": self.session_seeded_for, "entry_time": getattr(self, "entry_time_str", ""),
               "exit_time": exit_time_str, "dir": self.direction, "entry": round(self.entry, 2),
               "stop": round(self.stop, 2), "target": round(self.target, 2), "exit": round(exit_price, 2),
               "reason": reason, "points": round(pts, 2), "net": net, "atr": getattr(self, "entry_atr", ""),
               "mins_into_session": getattr(self, "entry_mins_in", ""), "window": getattr(self, "entry_window", ""),
               "mode": "DRY" if DRY_RUN else "LIVE"}
        new = not os.path.exists(FORWARD_LOG)
        with open(FORWARD_LOG, "a", newline="") as f:
            w = csv.DictWriter(f, fieldnames=_LOG_COLS)
            if new: w.writeheader()
            w.writerow(row)
        tag = "  [WATCH 5:30-6pm]" if row["window"] == "WATCH_530_600" else ""
        log.info(f"[{'PAPER' if DRY_RUN else 'LIVE'}] TRADE CLOSED {reason} @ {exit_price} "
                 f"| {pts:+.2f}pt ${net:+.0f}{tag}  -> logged to {FORWARD_LOG}")

    async def _paper_manage(self, session, last_price):
        """DRY_RUN fill simulation: walk the live tick against target/stop/BE and record
        the paper trade. (In live mode the broker bracket + _manage handle this instead.)"""
        if not self.in_trade:
            return
        is_long = self.direction == "long"
        if BE_ENABLED and not self.be_armed:
            if (last_price >= self.be_trigger) if is_long else (last_price <= self.be_trigger):
                self.be_armed = True
                log.info(f"[PAPER] breakeven armed @ {last_price} (stop -> {self.be_stop})")
        cur_stop = self.be_stop if self.be_armed else self.stop
        now_str = datetime.now(PT).strftime("%H:%M")
        outcome = None
        if (last_price >= self.target) if is_long else (last_price <= self.target):
            outcome = ("target", self.target)
        elif (last_price <= cur_stop) if is_long else (last_price >= cur_stop):
            outcome = ("be" if self.be_armed else "stop", cur_stop)
        if outcome:
            reason, xp = outcome
            pts = (xp - self.entry) if is_long else (self.entry - xp)
            net = round(pts * base.POINT_VALUE * base.CONTRACTS - base.FEE_PER_RT * base.CONTRACTS, 2)
            icon = {"target": "✅", "be": "➖", "stop": "🛑"}.get(reason, "⚪")
            tag = "  [5:30-6pm watch]" if getattr(self, "entry_window", "") == "WATCH_530_600" else ""
            self._record_trade(reason, xp, now_str); self._go_flat()
            await self._alert(session, f"{icon} PAPER {reason.upper()} {base.INSTRUMENT} (Asia)  "
                              f"{pts:+.2f}pt  ${net:+.0f}{tag}")

    async def run(self):
        import aiohttp, os
        if not base.USERNAME or not base.API_KEY: raise SystemExit("Set TOPSTEP_USERNAME / TOPSTEP_API_KEY in .env")
        log.info("=" * 60)
        log.info(f"  HA ASIA BOT — {base.INSTRUMENT} x{base.CONTRACTS} | 5-min | ATR>={S.ASIA_ATR_MIN} gate "
                 f"| BE@{int(S.BE_TRIGGER_FRAC*100)} {'ON' if BE_ENABLED else 'OFF'} | DRY_RUN={DRY_RUN}")
        log.info("  Trades 5:00-7:30pm PT, flat 8pm. VALIDATE OOS + DRY_RUN before funded.")
        log.info("=" * 60)
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=25)) as session:
            await self._auth(session); await self._get_account(session); await self._get_contract(session)
            self._open_market_hub()
            await self._alert(session, f"🌙 HA ASIA bot started — {base.INSTRUMENT} x{base.CONTRACTS} | "
                              f"ATR>={S.ASIA_ATR_MIN} | DRY_RUN={DRY_RUN}\n"
                              f"Pings on every entry/exit; 5:30-6pm trades are flagged.")
            last_price = None
            last_hb = datetime.now(PT); tick_count = 0
            while True:
                now = datetime.now(PT)
                # (re)seed at/after 5pm for the day if not done
                if now.time() >= S.ASIA_TRADE_OPEN and now.time() < S.ASIA_CLOSE and self.session_seeded_for != now.date():
                    try:
                        await self._seed_session(session, now.date())
                        self.trades_today = 0
                    except Exception as e:
                        log.error(f"seed failed: {e}")
                with self.tick_lock:
                    batch = list(self.ticks); self.ticks.clear()
                for ts_pt, price, size in batch:
                    last_price = price; tick_count += 1
                    if not (S.ASIA_VWAP_ANCHOR <= ts_pt.time() < S.ASIA_CLOSE): continue
                    if DRY_RUN and self.in_trade:                 # paper-manage on every tick
                        await self._paper_manage(session, price)
                    closed = self.bars.add_tick(ts_pt, price, size)
                    if closed is not None and not self.in_trade and self.session_seeded_for == ts_pt.date():
                        await self._on_bar_close(session, closed, ts_pt)
                # heartbeat every ~60s so you can SEE it's alive and receiving ticks
                if (now - last_hb).total_seconds() >= 60:
                    st = ("IN TRADE" if self.in_trade else
                          "watching 5-7:30pm" if S.ASIA_TRADE_OPEN <= now.time() <= S.ASIA_ENTRY_CUTOFF else
                          "idle (outside 5-7:30 window)")
                    atr_now = (S.atr_points(self.bars.highs5, self.bars.lows5, len(self.bars.highs5) - 1)
                               if self.bars.highs5 else 0.0)
                    vw = f"{self.bars.vwap:.2f}" if self.bars.vwap else "-"
                    log.info(f"♥ {now.strftime('%H:%M:%S')} | last {last_price} | {tick_count} ticks/min "
                             f"| vwap {vw} ATR {atr_now:.1f} | {st}")
                    last_hb = now; tick_count = 0
                if not DRY_RUN and last_price is not None and self.in_trade:  # live: broker manages, poll per loop
                    await self._manage(session, last_price)
                if now.time() >= S.ASIA_CLOSE and self.in_trade:
                    if DRY_RUN:
                        self._record_trade("flat_eod", last_price, now.strftime("%H:%M")); self._go_flat()
                    else:
                        await self._flatten(session, "Asia session close"); self._go_flat()
                await asyncio.sleep(0.25)


if __name__ == "__main__":
    try:
        asyncio.run(HAAsiaBot().run())
    except KeyboardInterrupt:
        log.info("Stopped by user.")
