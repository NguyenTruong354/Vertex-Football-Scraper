@echo off
:: ============================================================
:: Vertex Football Scraper — Daily Auto Run
:: ============================================================
:: Dung voi Windows Task Scheduler de chay tu dong hang ngay.
:: Xem docs/05_autorun.md de biet cach cau hinh.
:: ============================================================

for %%I in ("%~dp0..\..") do set PROJECT_DIR=%%~fI
set PYTHON=%PROJECT_DIR%\.venv\Scripts\python.exe
set LOG_DIR=%PROJECT_DIR%\logs

:: Tao thu muc log neu chua co
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

:: Timestamp cho log file (YYYY-MM-DD_HHMM)
for /f "tokens=2 delims==" %%I in ('wmic os get localdatetime /value') do set DATETIME=%%I
set DATESTAMP=%DATETIME:~0,4%-%DATETIME:~4,2%-%DATETIME:~6,2%
set TIMESTAMP=%DATETIME:~8,2%%DATETIME:~10,2%
set LOGFILE=%LOG_DIR%\autorun_%DATESTAMP%_%TIMESTAMP%.log

echo [%date% %time%] ====== Pipeline started ====== >> "%LOGFILE%"

cd /d "%PROJECT_DIR%"

:: Chay pipeline — sua flags theo nhu cau
:: Mac dinh: EPL, SofaScore gioi han 10 tran
"%PYTHON%" run_pipeline.py --league EPL --ss-match-limit 10 >> "%LOGFILE%" 2>&1

set EXIT_CODE=%ERRORLEVEL%

echo [%date% %time%] ====== Pipeline finished (exit code: %EXIT_CODE%) ====== >> "%LOGFILE%"

exit /b %EXIT_CODE%
