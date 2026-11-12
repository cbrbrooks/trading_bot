# CHOCH/FVG Trading Bot

An automated ES futures trading bot for TopstepX. It trades a CHOCH (Change of
Character) + FVG (Fair Value Gap) strategy on live tick data, and the repo also
holds the backtesting and research tools used to build and validate it.

## How it works

1. **Market data.** A SignalR connection streams live ES ticks from the TopstepX API.
2. **Signal.** The strategy engine builds candles, detects a change of character,
   and waits for price to return into a fair value gap. Quality filters (price
   location vs. VWAP/9 SMA, minimum stop size, entry window) reject weak setups.
3. **Order.** Each entry goes out as a broker-side bracket order (stop + target),
   so an open position stays protected even if the bot or its connection drops.
4. **Safety.** Daily loss limit, consecutive-loss stop, a forced flat time, a
   post-close sweep that cancels stray orders, and a separate watchdog process
   that alerts if the bot freezes or crashes.
5. **Alerts.** Optional Telegram messages for entries, exits, safety stops, and
   data-feed stalls. A Telegram outage can never interrupt trading.

## Repo layout

```
bot/          live trading code: strategy engine, live bots, watchdogs
backtests/    replay engines and backtest runners (TopstepX API + Databento data)
research/     one-off studies: delta filter validation, MAE study, log analysis
tests/        mocked-broker tests for bracket orders, limit orders, entry fixes
docs/         backtest write-ups, audits, and the go-live checklist
```

**Key files in `bot/`:**
- `choch_fvg_bot.py`: strategy engine (all tunable settings live here)
- `choch_bot_live.py`: live bot with bracket orders and fill tracking
- `intrabar_es_bot.py`: tick-level entry version (also `_2ct` / `_3ct` sizes)
- `topstep_bot_v5.py`: shared auth, Candle class, and constants
- `watchdog.py`: independent freeze/crash monitor (one per bot size)

## Setup

```bash
pip install -r requirements.txt
cp bot/.env.example bot/.env      # then add your TopstepX API key
```

The `.env` file lives in `bot/` because the bots and watchdogs read it from
their own folder. It is git-ignored and never committed.

## Running (from the repo root)

```bash
python bot/choch_bot_live.py --dry-run     # paper mode: logs decisions, places NO orders
python bot/intrabar_es_bot.py --dry-run    # tick-level version, paper mode
python bot/watchdog.py                     # run alongside the bot in a second window
```

Logs (`choch_bot.log`) and trade history are written into `bot/`.

**Backtests** take a Databento file path or a date range, for example:

```bash
python backtests/choch_databento_replay.py data/<file>.csv.zst 2026-01-01 2026-06-01
python backtests/choch_batch_replay.py 2026-10-01 2026-10-30
```

## Notes

- **TopstepX does not permit automated trading from a VPS.** Run it on your own machine.
- **This bot trades real money.** Backtests run somewhat better than live fills.
- Not in the repo, by design: `.env`, logs, trade history, market data (`data/`),
  and backtest outputs (`results/`).
- Built with Claude as a coding partner: strategy design, debugging, and backtests.
