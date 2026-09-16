@echo off
setlocal
cd /d "%~dp0"

where py >nul 2>&1
if errorlevel 1 (
  echo Python launcher was not found. Install a supported Python 3 release first.
  exit /b 1
)

if not exist venv\Scripts\python.exe (
  py -3 -m venv venv
  if errorlevel 1 exit /b 1
)

call venv\Scripts\activate.bat
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
if errorlevel 1 exit /b 1

python configure_agent.py
if errorlevel 1 exit /b 1

(
  echo @echo off
  echo cd /d "%%~dp0"
  echo call venv\Scripts\activate.bat
  echo python agent_program.py
  echo pause
) > Run_Agent.bat

echo Setup complete. Double-click Run_Agent.bat to start the agent.
endlocal
