@echo off
setlocal

REM ============================================================
REM Livestatus to Smartsheet Automation - Windows Task Scheduler Runner
REM ============================================================

echo ============================================================
echo Livestatus REPORTS AUTOMATION
echo ============================================================
echo Start Time: %date% %time%
echo.

REM Move to project directory
cd /d "C:\Users\SAMA\3D Objects\Smartsheet"

REM Activate virtual environment
call "C:\Users\SAMA\3D Objects\Smartsheet\venv\Scripts\activate.bat"

echo Running Livestatus reports automation...
echo.

REM Run script using venv python
python live_status_to_smartsheet.py

REM Check if successful
if %ERRORLEVEL% EQU 0 (
    echo.
    echo ============================================================
    echo LIVE STATUS AUTOMATION COMPLETED SUCCESSFULLY
    echo ============================================================
) else (
    echo.
    echo ============================================================
    echo ERROR: Live status automation failed with error code %ERRORLEVEL%
    echo ============================================================
)

echo End Time: %date% %time%
echo.

REM Keep window open if run manually
if "%1"=="" (
    pause
)
