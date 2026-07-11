# CHOCH/FVG Trading Bot

Automated ES futures trading bot for TopstepX, implementing a CHOCH (Change of
Character) + FVG (Fair Value Gap) strategy, plus the backtesting and analysis
tooling used to develop and validate it.

## ⚠️ Before anything else

- **Never commit your `.env` file.** It holds your TopStep API key. Anyone with
  it can trade your account. It is git-ignored; keep it that way.
- **TopStep does not permit automated trading from a VPS / remote server.** Run
  the bot on your own machine only. Confirm current policy with TopStep support.
- **This bot trades real money.** Backtest results are theoretical and run
  somewhat better than live fills. Watch early trades closely.

## Setup

```bash
pip install -r requirements.txt
cp .env.example .env        # then edit .env with your real TopStep API key
```

Confirm your settings loaded correctly:

```bash
python -c "import choch_fvg_bot as m; print('CONTRACTS:', m.CONTRACTS, '| RR:', m.RR_MULTIPLE, '| SESSION_CLOSE:', m.SESSION_CLOSE, '| MIN_STOP:', m.MIN_STOP_POINTS, '| USE_BREAKEVEN:', m.USE_BREAKEVEN)"
```

## Running

```bash
python choch_bot_live.py --dry-run   # paper mode — logs decisions, places NO orders
python choch_bot_live.py             # LIVE — places real bracket orders
```

Start before 6:30 AM PT (the entry window is 6:30-11:00 AM PT). Press Enter at
the startup watchdog prompt. Exits are managed broker-side via bracket orders,
so an open position stays protected even if the bot or its connection drops.

## Telegram alerts (optional)

Set `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID` in `.env` (see `.env.example`
for how to get them) and the bot pushes messages to your phone for: trade
entries, trade exits (with real P&L), safety-gate stops (daily loss limit /
consecutive losses), errors needing attention, and a data-feed stall alert if
no market data arrives for 3+ minutes during the session (plus a recovery
message when it resumes). A `[STARTUP]` test message is sent when the bot
launches — if you don't receive it, fix the token/chat ID before relying on
alerts. Alerts are fail-safe: a Telegram outage can never interrupt trading.

## Files

**The bot:**
- `choch_fvg_bot.py` — the strategy engine (all tunable settings live here)
- `choch_bot_live.py` — the live trading bot (bracket orders, fill tracking)
- `topstep_bot_v5.py` — shared dependency: auth, Candle class, constants
- `databento_loader.py` — shared dependency: historical data loading

**Backtesting & analysis:**
- `choch_databento_replay.py` — backtest against the local Databento file
- `choch_batch_replay.py` — replay recent days from the TopstepX API
- `build_daily_summary.py` — build the monthly .xlsx summary workbooks

**Tests:**
- `test_bracket_orders.py` — full bracket-order lifecycle (mocked broker)
- `test_limit_orders.py` — limit-order state machine (mocked broker)

## Not in this repo (git-ignored, by design)

- `.env` — your credentials
- `*.log`, `choch_trades.json` — live logs and trade history
- `glbx-*.zst` — the Databento market data file (large, licensed; supply your own)
- backtest output JSON / XLSX — regenerated on demand

## Current strategy configuration

2 ES contracts · RR 7.0 · entry window 6:30-11:00 AM PT · fixed brackets
(no breakeven move) · 0.25pt minimum stop filter · broker-side stop (type 4) +
target (type 1) brackets managing every exit.
