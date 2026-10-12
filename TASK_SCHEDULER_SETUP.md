# Auto-launching the 3-contract bot with Windows Task Scheduler

## Is this actually allowed under Topstep's rules?

Yes, with one important distinction. I checked Topstep's own Help Center docs (not
a third-party blog):

- **Your own PC:** allowed. TopstepX's API-access docs state custom automated
  strategies/bots are permitted via the API "subject to standard platform rules."
- **VPS / cloud / remote servers:** prohibited. Their Terms of Use bar running
  automation on a VPS — the line they draw is *order transmission*: a remote
  server can watch/log data, but it can't be the thing placing the orders.
- Your own `README.md` and `GO_LIVE_CHECKLIST.md` already say the same thing
  ("Topstep does not permit automated trading from a VPS / remote server... run
  the bot on your own machine only").

Windows Task Scheduler running on your own physical PC in `C:\Users\Ruth\topstep_bot`
is exactly "your own machine" — it's not a VPS, it's not a remote server, it's just
automating the click you'd otherwise do by hand every morning. So this is fine.
(Sources: [Topstep — Prohibited Trading Strategies](https://help.topstep.com/en/articles/10305426-prohibited-trading-strategies-at-topstep),
[Topstep — TopstepX API Access](https://help.topstep.com/en/articles/11187768-topstepx-api-access))

## One thing I checked before building this

Your `README.md` mentions pressing Enter at a "startup watchdog prompt." I read
through `intrabar_es_bot_3ct.py`, `watchdog_3ct.py`, and `choch_bot_live.py`
looking for any `input()` call that would block and wait for a keypress — there
isn't one in any of the three. That line in the README looks stale from an
earlier version of the workflow. I'm flagging this rather than assuming: if
there *is* some interactive prompt I missed elsewhere in the import chain, a
scheduled launch would just hang silently with no error, so the very first
scheduled run should be one you watch happen (see "Testing it" below) rather
than one you trust blind.

## Files

- **`launch_3ct_session.bat`** — put this in `C:\Users\Ruth\topstep_bot\`. It
  `cd`s into the folder, opens `intrabar_es_bot_3ct.py --no-delta-filter` in its
  own console window (title "ES 3CT BOT"), waits 10 seconds, then opens
  `watchdog_3ct.py` in its own window (title "ES 3CT WATCHDOG") — same as
  starting both by hand, just automated. It also appends a timestamp line to
  `scheduled_launch.log` each time it fires, so you can confirm it actually ran.

## Setting up the scheduled task (GUI)

1. Open **Task Scheduler** (Start menu → type "Task Scheduler").
2. **Action → Create Task…** (not "Create Basic Task" — you need the extra tabs).
3. **General tab:**
   - Name: `ES 3CT Bot Auto-Launch`
   - Select **"Run whether user is logged on or not"** only if you want it to
     survive a full logoff/reboot to the login screen — see the trade-off note
     below. Otherwise leave **"Run only when user is logged on."**
   - Check **"Run with highest privileges."**
4. **Triggers tab → New…:**
   - Begin the task: **On a schedule**
   - Weekly, recur every 1 week, check **Mon–Fri** only
   - Start time: **6:25:00 AM**
   - Under **Advanced settings**, check **"Enabled."**
5. **Actions tab → New…:**
   - Action: **Start a program**
   - Program/script: `C:\Users\Ruth\topstep_bot\launch_3ct_session.bat`
   - Start in: `C:\Users\Ruth\topstep_bot`
6. **Conditions tab:**
   - Check **"Wake the computer to run this task."**
   - Under Power, **uncheck** "Start the task only if the computer is on AC
     power" — you don't want a low/no-battery state to silently skip the
     launch (though see the caveat below — this doesn't fix a truly dead
     battery).
7. **Settings tab:**
   - Check **"Run task as soon as possible after a scheduled start is missed"**
     (covers the PC waking a few minutes late).
   - Consider checking **"If the task fails, restart every: 1 minute"** with a
     couple of retry attempts.
8. Save. Windows will prompt for your account password if you chose "whether
   logged on or not."

## Equivalent one-line command (PowerShell, run as Administrator)

```powershell
schtasks /create /tn "ES 3CT Bot Auto-Launch" /tr "C:\Users\Ruth\topstep_bot\launch_3ct_session.bat" /sc weekly /d MON,TUE,WED,THU,FRI /st 06:25 /rl HIGHEST
```

This creates the basic schedule; the wake-computer and missed-run options still
need to be set once via the GUI (Conditions/Settings tabs above) — `schtasks`
can't set those from the command line as cleanly.

## "Run only when logged on" vs. "whether logged on or not"

- **Only when logged on** (simpler, recommended to start): the console windows
  are visible on your screen when you check in, same as launching by hand. If
  your PC is merely *locked* (not logged off), this still fires fine — locked
  is not logged off.
- **Whether logged on or not**: needed if your PC sometimes fully logs off or
  reboots overnight. Trade-off: it runs in a hidden session, so you won't see
  the console windows at all — you'd rely entirely on the Telegram alerts and
  `scheduled_launch.log` to know it started. Also stores your Windows password
  in Task Scheduler.

Given you're mainly trying to avoid having to manually type the launch command
each morning (not trying to survive full logoffs), start with "only when
logged on."

## What this does *not* fix

Be clear-eyed about this: Task Scheduler + "wake the computer" only works if the
PC still has power and is asleep/hibernating — it cannot power on a machine
that's fully shut down, and it cannot help if the battery is dead or the power
cord is unplugged. That's exactly what happened on 2026-08-27 (laptop lost
power, logged a bare `KeyboardInterrupt`, no alert went out because the
machine itself was gone, not just the bot process). This task only removes the
"I have to remember to click launch" failure mode — it does nothing for a real
power-loss event. If you want that covered too, the earlier idea from last
session (a phone-side "haven't heard from the bot by X:XX" check) is still the
only real fix, since nothing running on the same machine can alert on that
machine dying.

Practical mitigation in the meantime: keep the laptop plugged in and set Windows
power settings (Settings → System → Power) so it never sleeps on AC power
during trading hours, and set "when I close the lid" to "Do nothing" if you
ever close it while the bot's running.

## Testing it before trusting it

Don't let the first real run of this be an actual trading morning:

1. Right-click the task in Task Scheduler → **Run**, at a time when the market
   is closed (or in a moment you can watch), and confirm both console windows
   open and both bot + watchdog start cleanly.
2. Check `scheduled_launch.log` got a new timestamp line.
3. Confirm you got the bot's own `[STARTUP-3CT]` Telegram message.
4. Manually stop both (Ctrl-C in each window) once you've confirmed it works.

Only after that, let it fire on the real 6:25 AM schedule.
