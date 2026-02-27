@echo off
:: ============================================================
:: Vertex Football Scraper — Daemon Mode (24/7)
:: ============================================================
:: Chay lien tuc, tu dong cap nhat du lieu.
:: Dung Ctrl+C de dung daemon.
:: Xem docs/05_autorun.md de biet cach cau hinh.
:: ============================================================

set PROJECT_DIR=%~dp0
set PYTHON=%PROJECT_DIR%.venv\Scripts\python.exe

:: Tao thu muc log neu chua co
if not exist "%PROJECT_DIR%logs" mkdir "%PROJECT_DIR%logs"

cd /d "%PROJECT_DIR%"

echo ============================================
echo  Vertex Football Scraper - DAEMON MODE
echo ============================================
echo  Nhan Ctrl+C de dung.
echo  Log: logs\daemon.log
echo ============================================
echo.

:: Chay daemon — sua flags theo nhu cau
:: Mac dinh: EPL, SS limit 5, match hours 11-23 UTC
"%PYTHON%" run_daemon.py --league EPL --ss-match-limit 5

pause
