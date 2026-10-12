#!/usr/bin/env python3
"""
watchdog_3ct.py — independent freeze/crash monitor for the 3-CONTRACT ES bot
(intrabar_es_bot_3ct.py). Copy of watchdog.py, same syntax/structure, only the
docstring, alert text, and startup message are relabeled for this bot copy.
=============================================================================
NOTE ON THE LOG FILE: intrabar_es_bot_3ct.py imports its `log` object straight
from choch_bot_live.py, same as the original intrabar_es_bot.py does — so it
writes to the exact same "choch_bot.log" as the 1-contract bot. The ORIGINAL
watchdog.py would technically already monitor this bot's log activity
unchanged. This copy exists so the Telegram alerts are clearly labeled
"3-CONTRACT" — useful since you should only run ONE of the two intrabar bots
at a time (TopstepX allows one session per username), so whichever watchdog
you run should match whichever bot is actually running that day.

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
    python watchdog_3ct.py

It reads the SAME TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID from your .env as the bot.
Leave this window running whenever the 3-contract bot is running. Safe to Ctrl-C to stop.
"""
import os, sys, time, urllib.request, urllib.parse
from datetime import datetime, time as dtime
from zoneinfo import ZoneInfo

# ── config (edit here if you want) ─────────────────────────────────────────────
# Same log file as the 1-contract bot's watchdog.py -- see the note at the top
# of the docstring for why. Only one of the two intrabar bots should be
# running (and therefore writing to this file) at any given time.
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
        print(f"[watchdog-3ct] (no telegram configured) would send: {text}")
        return
    try:
        url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
        data = urllib.parse.urlencode({"chat_id": CHAT_ID, "text": text}).encode()
        with urllib.request.urlopen(urllib.request.Request(url, data=data), timeout=15) as r:
            r.read()
    except Exception as e:
        print(f"[watchdog-3ct] telegram send failed ({type(e).__name__}: {e})")

def in_active_window(now):
    return now.weekday() in ACTIVE_WEEKDAYS and ACTIVE_START <= now.time() <= ACTIVE_END

def log_age_seconds():
    """Seconds since the bot's log was last written. None if the file is missing."""
    try:
        return time.time() - os.path.getmtime(LOG_PATH)
    except OSError:
        return None

def main():
    print(f"[watchdog-3ct] monitoring {LOG_PATH} (3-CONTRACT bot)")
    print(f"[watchdog-3ct] alert if silent > {STALE_SECONDS}s during {ACTIVE_START}-{ACTIVE_END} PT, Mon-Fri")
    if not (TOKEN and CHAT_ID):
        print("[watchdog-3ct] WARNING: no TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID found in .env — "
              "alerts will only print to this window. Set them in .env to get phone alerts.")
    send("🐕 Watchdog (3-CONTRACT bot) started — I'll alert you if the bot's log goes silent during trading hours.")

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
                    send(f"⚠️ BOT WATCHDOG (3-CONTRACT): log file not found ({LOG_PATH}). "
                         f"Is the bot running? (checked {now:%H:%M PT})")
            elif age > STALE_SECONDS:
                if not alerted:
                    alerted = True
                    send(f"⚠️ BOT WATCHDOG (3-CONTRACT): no log activity for {age/60:.1f} minutes — the bot may be "
                         f"FROZEN or STOPPED. Check the bot window. (as of {now:%H:%M PT})")
            else:
                if alerted:
                    alerted = False
                    send(f"✅ BOT WATCHDOG (3-CONTRACT): log is updating again — bot recovered. (as of {now:%H:%M PT})")
        time.sleep(CHECK_EVERY)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[watchdog-3ct] stopped.")
