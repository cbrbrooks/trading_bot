"""
Databento ES Data Loader

Handles a Databento OHLCV-1m historical CSV export for the "ES.FUT" parent
symbol (which contains ALL ES contract months mixed together, distinguished
by the `symbol` column). This picks the front-month contract at each point
in time using volume as the signal — the actively-traded contract always has
dramatically higher volume than back months, so per-minute "pick the row
with the highest volume" is a simple, reliable proxy for a continuous
front-month series without needing an explicit roll calendar.

Usage as a library:
    from databento_loader import ensure_decompressed, load_range
    csv_path = ensure_decompressed("glbx-mdp3-...-ohlcv-1m.csv.zst")
    df = load_range(csv_path, start_utc, end_utc)
"""
# --- path bootstrap (repo reorganized into folders) ---
import sys as _sys, pathlib as _pathlib
_ROOT = _pathlib.Path(__file__).resolve().parents[1]
_sys.path[:0] = [str(_ROOT / _d) for _d in ("bot", "backtests", "research")]
# ---------------------------------------------------------

import os
import pandas as pd


def ensure_decompressed(zst_path: str) -> str:
    """Decompress the .zst file once, caching the .csv next to it for reuse
    across multiple runs (decompression of ~900MB takes a while — no reason
    to redo it every time)."""
    if not zst_path.endswith(".zst"):
        return zst_path  # already a plain CSV

    csv_path = zst_path[:-4]  # strip ".zst"
    if os.path.exists(csv_path):
        print(f"Using cached decompressed file: {csv_path}")
        return csv_path

    import zstandard as zstd
    print(f"Decompressing {zst_path} -> {csv_path} (one-time, may take a minute)...")
    dctx = zstd.ZstdDecompressor()
    with open(zst_path, "rb") as fh_in, open(csv_path, "wb") as fh_out:
        with dctx.stream_reader(fh_in) as reader:
            while True:
                chunk = reader.read(1024 * 1024)
                if not chunk:
                    break
                fh_out.write(chunk)
    print("Decompression complete.")
    return csv_path


import datetime as _dt

QUARTER_MONTHS = [(3, "H"), (6, "M"), (9, "U"), (12, "Z")]  # standard CME equity index quarterly cycle
ROLL_OFFSET_DAYS = 14  # roll to next contract ~2 weeks before expiration — widened after
                       # checking against confirmed ground truth (ESU6 was already the
                       # active contract by 2026-06-10, 9 days before ESM6's expiration,
                       # so the true roll happened earlier than a naive 8-day assumption)


def _third_friday(year: int, month: int) -> _dt.date:
    d = _dt.date(year, month, 1)
    first_friday = d + _dt.timedelta(days=(4 - d.weekday()) % 7)
    return first_friday + _dt.timedelta(weeks=2)


def contract_symbol_for_date(d: _dt.date) -> str:
    """Returns the ES contract symbol (e.g. 'ESU6') that was the front-month
    contract on a given date, using a standard roll-ahead-of-expiration
    calendar. A contract is front-month from just after the PREVIOUS
    contract's roll date until its OWN roll date (roll_start = expiration -
    ROLL_OFFSET_DAYS) — at which point trading has already moved to the next
    contract. Calibrated against confirmed ground truth: TopstepX was
    already trading ESU6 on 2026-06-10, before ESM6's raw exchange volume
    had crossed over — brokers roll ahead of expiration, not on volume."""
    candidates = []
    for y in (d.year - 1, d.year, d.year + 1, d.year + 2):
        for m, letter in QUARTER_MONTHS:
            exp = _third_friday(y, m)
            roll_start = exp - _dt.timedelta(days=ROLL_OFFSET_DAYS)
            candidates.append((roll_start, letter, y))
    candidates.sort(key=lambda c: c[0])

    # The active front-month contract is the EARLIEST one whose own roll
    # date hasn't happened yet (d is still before that contract rolls out).
    for roll_start, letter, y in candidates:
        if d < roll_start:
            return f"ES{letter}{str(y)[-1]}"
    return None


def load_range(csv_path: str, start_utc: pd.Timestamp, end_utc: pd.Timestamp) -> pd.DataFrame:
    """
    Reads the CSV in chunks (keeps memory bounded regardless of total file
    size — this file covers 16 years, but any single backtest only needs a
    slice of it) and returns front-month-selected 1-minute bars as a
    DataFrame indexed by UTC timestamp, columns: open, high, low, close, volume.

    Front-month selection uses a deterministic roll calendar (see
    contract_symbol_for_date), not volume — volume-based selection lags
    behind real broker roll timing by days, confirmed by comparing against
    a known real trade (TopstepX was trading ESU6 while ESM6 still had more
    raw exchange volume). Calendar spread instruments (symbols containing
    "-", e.g. "ESM6-ESU6") are excluded — their prices are basis
    differentials, not index levels.
    """
    matched_chunks = []
    chunksize = 1_000_000

    for chunk in pd.read_csv(
        csv_path,
        chunksize=chunksize,
        usecols=["ts_event", "open", "high", "low", "close", "volume", "symbol"],
        dtype={"symbol": "category"},
    ):
        chunk["ts_event"] = pd.to_datetime(chunk["ts_event"], utc=True)
        mask = (chunk["ts_event"] >= start_utc) & (chunk["ts_event"] <= end_utc)
        if mask.any():
            matched_chunks.append(chunk.loc[mask])
        if chunk["ts_event"].min() > end_utc:
            break

    if not matched_chunks:
        return pd.DataFrame(columns=["open", "high", "low", "close", "volume"])

    df = pd.concat(matched_chunks, ignore_index=True)
    df = df[~df["symbol"].astype(str).str.contains("-")]  # drop calendar spreads

    df["_day"] = df["ts_event"].dt.date
    df["_front_symbol"] = df["_day"].apply(contract_symbol_for_date)
    front = df[df["symbol"].astype(str) == df["_front_symbol"]].copy()
    front = front.sort_values("ts_event").set_index("ts_event")
    return front[["open", "high", "low", "close", "volume"]]


def resample(df: pd.DataFrame, rule: str) -> pd.DataFrame:
    """Aggregate 1-minute bars up to a coarser interval (e.g. '15min')."""
    if df.empty:
        return df
    agg = df.resample(rule, label="left", closed="left").agg({
        "open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum",
    })
    return agg.dropna(subset=["open"])
