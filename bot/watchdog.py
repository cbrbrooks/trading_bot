#!/usr/bin/env python3
"""
watchdog.py — independent freeze/crash monitor for the ES bot.
=============================================================
WHY THIS IS A SEPARATE SCRIPT: the bot already has an in-process data-feed stall
alert, but if the bot's process FREEZES (Windows QuickEdit console pause) or CRASHES,
that in-process alert can't fire — a frozen program can't send anything. This watchdog
runs as its OWN process in a second window, so it keeps working when the bot doesn't.

WHAT IT DOES: it watches the bot's log file. During the trading window it expects the
log to keep getting written (the bot logs a bar / tick-health line about every minute).
If the log goes silent for longer than STALE_SECONDS, it sends you a Telegram alert.
When the log starts updating again, it sends an all-clear.

RUN IT (in a SEPARATE Windows Terminal / PowerShell window, alongside the bot):
    cd C:\\Users\\Ruth\\topstep_bot
    python watchdog.py

It reads the SAME TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID from your .env as the bot.
Leave this window running whenever the bot is running. Safe to Ctrl-C to stop.
"""
import os, sys, time, urllib.request, urllib.parse
from datetime import datetime, time as dtime
from zoneinfo import ZoneInfo

# ── config (edit here if you want) ─────────────────────────────────────────────
LOG_PATH        = os.path.join(os.path.dirname(os.path.abspath(__file__)), "choch_bot.log")
STALE_SECONDS   = 240          # alert if the log hasn't updated in this many seconds (4 min)
CHECK_EVERY     = 30           # how often the watchdog checks, in seconds
ACTIVE_START    = dtime(6, 0)  # only watch during the trading window (PT) — avoids overnight false alarms
ACTIVE_END      = dtime(13, 15)
ACTIVE_WEEKDAYS = {0, 1, 2, 3, 4}   # Mon-Fri (0=Mon)
PT              = ZoneInfo("America/Los_Angeles")

# ── load Telegram creds from .env (same keys the bot uses) ─────────────────────
def load_env():
    tok = os.getenv("TELEGRAM_BOT_TOKEN"); cid = os.getenv("TELEGRAM_CHAT_ID")
    if tok and cid:
        return tok, cid
    envp = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    if os.path.exists(envp):
        for line in open(envp, encoding="utf-8", errors="replace"):
            line = line.strip()
            if line.startswith("TELEGRAM_BOT_TOKEN"):
                tok = line.split("=", 1)[1].strip().strip('"').strip("'")
            elif line.startswith("TELEGRAM_CHAT_ID"):
                cid = line.split("=", 1)[1].strip().strip('"').strip("'")
    return tok, cid

TOKEN, CHAT_ID = load_env()

def send(text):
    """Fail-safe Telegram send (stdlib only). Never raises."""
    if not (TOKEN and CHAT_ID):
        print(f"[watchdog] (no telegram configured) would send: {text}")
        return
    try:
        url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
        data = urllib.parse.urlencode({"chat_id": CHAT_ID, "text": text}).encode()
        with urllib.request.urlopen(urllib.request.Request(url, data=data), timeout=15) as r:
            r.read()
    except Exception as e:
        print(f"[watchdog] telegram send failed ({type(e).__name__}: {e})")

def in_active_window(now):
    return now.weekday() in ACTIVE_WEEKDAYS and ACTIVE_START <= now.time() <= ACTIVE_END

def log_age_seconds():
    """Seconds since the bot's log was last written. None if the file is missing."""
    try:
        return time.time() - os.path.getmtime(LOG_PATH)
    except OSError:
        return None

def main():
    print(f"[watchdog] monitoring {LOG_PATH}")
    print(f"[watchdog] alert if silent > {STALE_SECONDS}s during {ACTIVE_START}-{ACTIVE_END} PT, Mon-Fri")
    if not (TOKEN and CHAT_ID):
        print("[watchdog] WARNING: no TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID found in .env — "
              "alerts will only print to this window. Set them in .env to get phone alerts.")
    send("🐕 Watchdog started — I'll alert you if the bot's log goes silent during trading hours.")

    alerted = False           # currently in a "bot is silent" alert state
    last_day = None
    while True:
        now = datetime.now(PT)
        # reset the alert latch at the start of each new day
        if last_day != now.date():
            last_day = now.date(); alerted = False

        if in_active_window(now):
            age = log_age_seconds()
            if age is None:
                if not alerted:
                    alerted = True
                    send(f"⚠️ BOT WATCHDOG: log file not found ({LOG_PATH}). "
                         f"Is the bot running? (checked {now:%H:%M PT})")
            elif age > STALE_SECONDS:
                if not alerted:
                    alerted = True
                    send(f"⚠️ BOT WATCHDOG: no log activity for {age/60:.1f} minutes — the bot may be "
                         f"FROZEN or STOPPED. Check the bot window. (as of {now:%H:%M PT})")
            else:
                if alerted:
                    alerted = False
                    send(f"✅ BOT WATCHDOG: log is updating again — bot recovered. (as of {now:%H:%M PT})")
        time.sleep(CHECK_EVERY)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[watchdog] stopped.")
