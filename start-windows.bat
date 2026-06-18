@echo off
REM Double-click this on Windows to start the Ring Video Archiver.
cd /d "%~dp0"

if not exist ".venv" (
  echo First-time setup ^(one minute^)...
  py -3 -m venv .venv || python -m venv .venv
)
call .venv\Scripts\activate.bat
pip install --quiet --upgrade pip
pip install --quiet -r requirements.txt

REM To preview with no Ring account:  set RING_ARCHIVER_DEMO=1  then run this file.
python app.py
echo.
echo Stopped. You can close this window.
pause
