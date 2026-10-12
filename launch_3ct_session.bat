@echo off
REM ============================================================
REM launch_3ct_session.bat
REM Starts the 3-contract ES bot + its matching watchdog, each in
REM its own visible console window, exactly like doing it by hand.
REM Meant to be fired by Windows Task Scheduler at 6:25 AM PT.
REM ============================================================

cd /d C:\Users\Ruth\topstep_bot

echo [%date% %time%] Launching intrabar_es_bot_3ct.py >> scheduled_launch.log

start "ES 3CT BOT" cmd /k python intrabar_es_bot_3ct.py --no-delta-filter

REM Small delay so the bot's own process/log exists before the
REM watchdog starts watching it.
timeout /t 10 /nobreak >nul

echo [%date% %time%] Launching watchdog_3ct.py >> scheduled_launch.log

start "ES 3CT WATCHDOG" cmd /k python watchdog_3ct.py
