#!/usr/bin/env python3
"""
log_watchdog.py — independent freeze/crash monitor for ANY of the trading bots.
==============================================================================
Renamed from ha_watchdog.py on 2026-08-18. It was written for ha_mes_bot.py and
kept that name after it became general-purpose, which was actively misleading
once it was also guarding mes5_bot.log. Nothing in here is HA-specific: point it
at a log with --log and it watches that.

    python log_watchdog.py --log mes5_bot.log     # CHOCH / MES5 bot
    python log_watchdog.py --log ha_mes_bot.log   # HA NY/RTH bot

WHY IT IS A SEPARATE PROCESS: the bots send their own Telegram alerts, but a
FROZEN process (Windows QuickEdit console pause — click in the window and it
stops dead) or a CRASHED one cannot send anything. This runs in its own window,
so it still works when the bot does not.

WHAT IT WATCHES
  1 SILENCE       the log's modified-time. During the session the bot writes at
                  least once a minute, so >4 min of silence means frozen, dead,
                  or never started.
  2 BLINDNESS     the log TEXT, for DATA FEED STALL / Bar fetch failed / Bar gap
                  detected. On 2026-08-14 the bot lost its data feed for 32
                  minutes while still logging normally — from the outside it
                  looked perfectly healthy, and silence detection could never
                  have caught it. The bot knows it is blind and says so; this
                  just reads it.

DELIVERY, and why the retry matters
-----------------------------------
send() reports whether the message actually reached Telegram, and an alert is
only marked delivered when it did. On 2026-08-14 the bot went silent because the
machine lost DNS — and the first version tried to report that THROUGH THE SAME
DEAD NETWORK, marked the failed send as delivered, and the only message that ever
arrived was "recovered" once DNS came back. A monitor that needs the network to
tell you the network is down has to keep retrying.

WHAT IT CANNOT DO: if the machine is off or asleep, this is off too. It is a
process on the same computer and cannot report that the computer stopped
existing.
"""
import argparse
import os
import time
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, time as dtime
from zoneinfo import ZoneInfo

HERE = os.path.dirname(os.path.abspath(__file__))

# ── config ───────────────────────────────────────────────────────────────────
DEFAULT_LOG     = os.path.join(HERE, "ha_mes_bot.log")   # only a default; pass --log
STALE_SECONDS   = 240            # alert if the log hasn't updated in this long (4 min)
CHECK_EVERY     = 30             # how often to check, seconds
ACTIVE_START    = dtime(6, 0)    # only watch the trading window (PT) — no overnight false alarms
ACTIVE_END      = dtime(13, 15)  # a bit past SESSION_CLOSE 13:00 so the wind-down is covered
ACTIVE_WEEKDAYS = {0, 1, 2, 3, 4}   # Mon-Fri (0=Mon)
PT              = ZoneInfo("America/Los_Angeles")


def load_env():
    """Read Telegram creds from the environment, falling back to .env.
    Deliberately does NOT print or log the token."""
    tok = os.getenv("TELEGRAM_BOT_TOKEN")
    cid = os.getenv("TELEGRAM_CHAT_ID")
    if tok and cid:
        return tok, cid
    envp = os.path.join(HERE, ".env")
    if os.path.exists(envp):
        for line in open(envp, encoding="utf-8", errors="replace"):
            line = line.strip()
            if line.startswith("TELEGRAM_BOT_TOKEN"):
                tok = line.split("=", 1)[1].strip().strip('"').strip("'")
            elif line.startswith("TELEGRAM_CHAT_ID"):
                cid = line.split("=", 1)[1].strip().strip('"').strip("'")
    return tok, cid


TOKEN, CHAT_ID = load_env()


def send(text, prefix=""):
    """Fail-safe Telegram send (stdlib only — no deps, so the watchdog can never be
    broken by the bot's virtualenv). Never raises.

    RETURNS True only if the message actually reached Telegram. That return value
    matters more than it looks: see the alert loop. On 2026-08-14 the bot went silent
    because the machine lost DNS -- and the watchdog then tried to report it THROUGH
    THE SAME DEAD NETWORK. The send failed, the alert was marked as delivered anyway,
    and the only message that ever arrived was the 'recovered' one once DNS came back.
    A monitor that needs the network to tell you the network is down has to assume the
    send can fail and keep retrying."""
    msg = f"{prefix} {text}".strip()
    if not (TOKEN and CHAT_ID):
        print(f"[watchdog] (telegram not configured) would send: {msg}")
        return False
    try:
        url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
        data = urllib.parse.urlencode({"chat_id": CHAT_ID, "text": msg}).encode()
        with urllib.request.urlopen(urllib.request.Request(url, data=data), timeout=15) as r:
            r.read()
        return True
    except Exception as e:
        print(f"[watchdog] telegram send failed ({type(e).__name__}: {e}) — will retry")
        return False


def in_active_window(now):
    return now.weekday() in ACTIVE_WEEKDAYS and ACTIVE_START <= now.time() <= ACTIVE_END


def log_age_seconds(path):
    """Seconds since the log was last written. None if the file is missing."""
    try:
        return time.time() - os.path.getmtime(path)
    except OSError:
        return None


def tail(path, n=3):
    """Last n non-blank lines of the log, to include in the alert so you can see
    what the bot was doing when it stopped without opening the machine."""
    try:
        with open(path, "rb") as fh:
            fh.seek(0, os.SEEK_END)
            size = fh.tell()
            block = min(size, 8192)
            fh.seek(size - block)
            lines = [l.strip() for l in fh.read().decode("utf-8", "replace").splitlines() if l.strip()]
        return "\n".join(lines[-n:]) if lines else "(log is empty)"
    except Exception:
        return "(couldn't read the log)"



def recent_stall(path, window_secs):
    """Return the most recent DATA-FEED problem line the bot logged in the last
    `window_secs`, or None.

    Silence detection cannot catch this case. On 2026-08-14 the bot lost its data
    feed for 32 minutes while still writing to the log -- from the outside it looked
    perfectly healthy. The bot knows it is blind and says so; the watchdog just has to
    read it."""
    import re, time as _t
    markers = ("DATA FEED STALL", "Bar fetch failed", "Bar gap detected")
    try:
        with open(path, "rb") as fh:
            fh.seek(0, os.SEEK_END)
            size = fh.tell()
            block = min(size, 16384)
            fh.seek(size - block)
            lines = fh.read().decode("utf-8", "replace").splitlines()
    except Exception:
        return None
    cutoff = datetime.now(PT) - timedelta(seconds=window_secs)
    for line in reversed(lines):
        if not any(m in line for m in markers):
            continue
        m = re.match(r"(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d)", line)
        if not m:
            continue
        try:
            ts = datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S").replace(tzinfo=PT)
        except Exception:
            continue
        if ts >= cutoff:
            return line.strip()[:300]
    return None


def main():
    ap = argparse.ArgumentParser(description="Freeze/crash monitor for any of the trading bots")
    ap.add_argument("--log", default=DEFAULT_LOG, help="log file to watch")
    ap.add_argument("--stale", type=int, default=STALE_SECONDS, help="alert threshold, seconds")
    ap.add_argument("--quiet-start", action="store_true", help="skip the startup ping")
    args = ap.parse_args()

    log_path, stale = args.log, args.stale
    print(f"[watchdog] monitoring {log_path}")
    print(f"[watchdog] alert if silent > {stale}s during "
          f"{ACTIVE_START:%H:%M}-{ACTIVE_END:%H:%M} PT, Mon-Fri")
    print(f"[watchdog] checking every {CHECK_EVERY}s — leave this window open. Ctrl-C to stop.")
    if not (TOKEN and CHAT_ID):
        print("[watchdog] WARNING: no TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID found — "
              "alerts will only print here. Set them in .env for phone alerts.")
    if not args.quiet_start:
        send(f"Watchdog started — monitoring {os.path.basename(log_path)}. "
             f"I'll alert you if the bot goes silent for {stale//60}+ minutes "
             f"during {ACTIVE_START:%H:%M}-{ACTIVE_END:%H:%M} PT.", prefix="[WATCHDOG]")

    alerted = False        # a down-alert has been DELIVERED (not merely attempted)
    down_since = None      # when the current silence began, for the recovery message
    last_stall_seen = None # de-dupe the data-feed alerts
    last_day = None
    was_active = False
    while True:
        now = datetime.now(PT)

        # reset the latch each new day so a stale alert doesn't carry over
        if last_day != now.date():
            last_day = now.date()
            alerted = False
            down_since = None
            last_stall_seen = None

        active = in_active_window(now)
        if active and not was_active:
            print(f"[watchdog] {now:%H:%M} PT — entering active window")
        if was_active and not active:
            print(f"[watchdog] {now:%H:%M} PT — leaving active window, standing down")
            alerted = False
            down_since = None
        was_active = active

        if active:
            age = log_age_seconds(log_path)
            if age is None:
                # `alerted` is now set ONLY when the message got through, so a failed
                # send is retried on the next tick instead of being silently swallowed.
                if not alerted:
                    if down_since is None:
                        down_since = now
                    alerted = send(f"Log file not found:\n{log_path}\n"
                                   f"Is the bot actually running? (checked {now:%H:%M PT})",
                                   prefix="[WATCHDOG]")
            elif age > stale:
                if down_since is None:
                    down_since = now
                if not alerted:
                    alerted = send(f"No log activity for {age/60:.1f} minutes — the bot may be "
                                   f"FROZEN or STOPPED. Check the bot window.\n"
                                   f"(as of {now:%H:%M PT})\n\n"
                                   f"Last log lines:\n{tail(log_path)}",
                                   prefix="[WATCHDOG]")
            else:
                if alerted or down_since is not None:
                    mins = (now - down_since).total_seconds() / 60 if down_since else 0.0
                    # State the outage length explicitly. If the down alert never made
                    # it out (dead network), this is the only message that will arrive,
                    # so it has to carry the whole story rather than just say "recovered".
                    note = ("." if alerted else
                            " -- and the DOWN alert never reached you at the time, most "
                            "likely because the network was the thing that failed.")
                    send(f"Bot is logging again after {mins:.1f} minutes of silence{note}\n"
                         f"(as of {now:%H:%M PT})",
                         prefix="[WATCHDOG OK]")
                    alerted = False
                    down_since = None
                # separately: the bot may be ALIVE but BLIND. It logs these itself,
                # and a healthy-looking log would otherwise hide them completely.
                stall = recent_stall(log_path, CHECK_EVERY * 2)
                if stall and stall != last_stall_seen:
                    if send(f"The bot is running but reporting a DATA problem:\n\n{stall}\n\n"
                            f"(as of {now:%H:%M PT})", prefix="[WATCHDOG DATA]"):
                        last_stall_seen = stall
        time.sleep(CHECK_EVERY)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[watchdog] stopped.")
