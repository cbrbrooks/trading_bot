# Go-Live Checklist — ES bot (1 contract, 2pt/3pt, delta off)
_Created 2026-07-23. Don't trade real money until every gate below is green. The config is correct
and the logic is sound — these gates are about proving stability and that the live edge is real._

---

## Gate 1 — Alerts working  ✅ DONE
- [x] Watchdog alert received on phone (confirmed 2026-07-23) — Telegram pipeline works, and the
      external watchdog fires when the bot goes quiet.
- [ ] Confirm you also get the **bot's own** startup Telegram ("…startup test…") when you launch
      `intrabar_es_bot.py` — that's a different sender than the watchdog.

## Gate 2 — Connection stability (the thing that bit you this morning)
- [ ] Bot runs in **Windows Terminal** (not the legacy console) — kills the QuickEdit freeze.
- [ ] `watchdog.py` running in a **separate** window every session.
- [ ] **At least 3–5 full dry-run sessions with ZERO freezes.** If it freezes or you have to press
      Enter to un-stick it, this gate is NOT met — fix it first.
- [ ] Any SignalR drops **auto-reconnect cleanly** (log shows "Data feed RECOVERED" within a few
      minutes, no manual intervention). This morning it dropped at 08:23 and 08:43 — watch that it
      recovers on its own now that the process isn't freezing.
- [ ] Never click inside the bot's window while it runs (use the log-tail window instead).

## Gate 3 — Forward-test sample (prove the edge is live, not just the plumbing)
- [ ] **1–2 weeks** of dry-run trades (one winner is not a sample).
- [ ] Run `python analyze_choch_log.py choch_bot.log --since <start-date>` at the end of the stretch.
- [ ] Live-dry **win rate** in the ballpark of the backtest (~55–63% recent) — not one or two bad
      days, the whole stretch.
- [ ] **Slippage check (most important):** compare where dry-run says it filled (the FVG midpoint /
      the exact target) against where price actually was. If entries/exits look reachable at those
      prices, good. If the tape was blowing past them, real slippage will be worse than the 0.25pt
      the backtest assumed — and that's what quietly eats the edge.
- [ ] Behavior sane: ~1–3 trades/day, brackets at 2pt/3pt, `KEPT (NOT flattened)` if it ever
      appears (healthy), and **never** a live `FLATTENING the false start` line.

## Gate 4 — Account / risk fit (Topstep)
- [ ] Confirm your account's **trailing drawdown / max-loss limit** comfortably exceeds the config's
      realistic drawdown (~$700 recent, up to ~$2,500 over 5 years, at 1 contract). If your limit is
      tight, size and expectations need to match.
- [ ] `.env` API key valid; account active/entitled for API trading.
- [ ] Bot runs on **your own machine only** (Topstep prohibits VPS/cloud automation).

## Gate 5 — First LIVE session protocol (when Gates 1–4 are green)
Going live is NOT "flip the switch and walk away." First real day:
- [ ] Pick a day you can **watch the entire session** (6:30 AM–1:10 PM PT). Do not leave it alone.
- [ ] Launch **without** `--dry-run`:  `python intrabar_es_bot.py --no-delta-filter`
- [ ] Confirm the banner: `ES 1 CONTRACT`, `stop 2.0pt / target 3.0pt`, `Delta filter: OFF`,
      `Version B ENFORCED ON`, `LIVE MODE — REAL orders`.
- [ ] On the **first real entry**, verify in the log/platform that the order actually placed and the
      **broker bracket attached** (a real stop + target on the position — not a rejection like the
      7/13 "Invalid stop loss ticks"). A 2pt stop = 8 ticks, so it should pass, but watch it.
- [ ] After the first trade closes, check your **actual fill prices** vs the intended entry/stop/
      target. That's your real slippage number — write it down.
- [ ] If the first watched day behaves, you can start letting it run on its own. If anything is off,
      back to dry-run.

---

## The one-line summary
Config: ✅ right. Logic: ✅ sound. Alerts: ✅ working. **Not yet proven:** freeze-free stability
over several sessions, a real forward-test sample, real order placement, and your live slippage.
Clear those, go live small and watched, then let it run. The edge is modest and the account is
unforgiving — proving it first is worth far more than the few hundred dollars of jumping early.
