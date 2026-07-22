# ES bot — 1-contract config (2pt stop / 3pt target)   ·   set 2026-07-22

This configures your **existing, validated** live bot (`intrabar_es_bot.py` + engine) for the
Topstep-safe setup we backtested. **No trading logic was rewritten** — only three strategy
constants changed, plus a cosmetic banner fix. All the hardened broker/auth/bracket/SignalR
code is untouched.

## What changed (only in `choch_fvg_bot.py`)
| constant | was | now | why |
|---|---|---|---|
| `CONTRACTS` | 2 | **1** | Topstep-safe size; drawdown ~$700 fits any account |
| `FIXED_STOP_POINTS` | 1.0 | **2.0** | 2pt stop stops giving away winners to intrabar noise |
| `RR_MULTIPLE` | 3.0 | **1.5** | with the 2pt stop → **3.0pt target** (unchanged target, wider stop) |

Everything else is intentionally the same and stays correctly calibrated:
- `MAX_TRADES_PER_DAY = 3`, `MAX_CONSECUTIVE_LOSSES = 2` — your 3/day + stop-at-2-losses rules.
- `DAILY_LOSS_LIMIT = 230` and `MAX_TRADE_RISK_DOLLARS = 110` — still correct, because
  **1 contract × 2pt = the same $100/loss** as the old 2 contracts × 1pt. Worst day = −$207.56.
- `MAX_STOP_POINTS = 1.5` — the setup selector, unchanged (same trades get picked).
- `VWAP_EMA_FILTER = True` — Version B, still enforced on every entry (mandatory).

Also fixed the startup banner in `intrabar_es_bot.py` so it prints the **real** size/geometry
(it used to hard-print "2 CONTRACTS", which would be dangerously misleading now).

Per trade now: **stop 2.0pt = $100 risk, target 3.0pt = $150** (before fees; ES fee $3.78/RT).

## Delta filter
The validated config runs with the **delta filter OFF** (3 studies showed no edge at the live
window). Run with `--no-delta-filter`. If you'd rather keep collecting live delta data to
reconcile against the backtest, drop that flag — but the numbers we validated are delta-off.

## How to run

**Tomorrow — dry run first (no real orders). Do this before anything live:**
```
python intrabar_es_bot.py --dry-run --no-delta-filter
```
Confirm the banner shows `ES 1 CONTRACT`, `stop 2.0pt / target 3.0pt`, `Max trades 3/day`,
`Max consec L 2`, `Version B ENFORCED ON`, and `Delta filter: OFF`. Let it run a session and
check `choch_bot.log` looks sane (entries at 2pt/3pt brackets, no errors).

**Live — only after the dry run looks right (real money):**
```
python intrabar_es_bot.py --no-delta-filter
```

## Safety notes
- **Review the git diff before running** (`git diff choch_fvg_bot.py intrabar_es_bot.py`) — it's
  3 constants + 2 banner lines. Revert anytime with git.
- The bot must run **on your own machine** (Topstep prohibits automated trading from a VPS/cloud).
- `.env` (API key) is unchanged and must never be committed.
- Expectations (honest): ~$1,000/mo at 1 contract with realistic slippage; the edge is real but
  modest and softening — this is a backtest, the dry run is the real proof.
