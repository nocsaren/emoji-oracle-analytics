@echo off
REM Activate the venv
call "%~dp0.venv/Scripts/activate.bat"

REM Run the Python script
python update_data.py

REM Keep the window open to see output/errors
pause
