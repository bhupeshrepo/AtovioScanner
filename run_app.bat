@echo off
setlocal
title atovio Label Uploader

REM Ensure we run from this script's directory
cd /d "%~dp0"

REM Activate the virtual environment (adjust folder name if needed)
if exist "%~dp0env\Scripts\activate.bat" (
  call "%~dp0env\Scripts\activate.bat"
)

REM Start Flask in a new console (so logs are visible)
start "" cmd /k python app.py

REM Give it a moment to boot, then open browser
timeout /t 3 /nobreak >nul
start "" http://127.0.0.1:5000
