@echo off
setlocal EnableExtensions EnableDelayedExpansion
cd /d "%~dp0"

REM CCD Agent Windows installer (normal edition).
REM Uses curl.exe when available and Windows PowerShell only as a download
REM fallback. All application files are verified before the installed copy is
REM replaced. Existing daemon_logs and delta-cache files are never touched.

set "PYTHON_INSTALLER_URL=https://www.python.org/ftp/python/3.13.10/python-3.13.10-amd64.exe"
if not defined CCD_AGENT_ARTIFACT_BASE_URL set "CCD_AGENT_ARTIFACT_BASE_URL=https://raw.githubusercontent.com/cuhkfyp/agent_program_new_setting/main/agent"

set "AGENT_SHA256=d778281060ae52abd8ddb99491ea3b1a1720ea91f16c1486e000c8219e00380a"
set "CONFIGURE_SHA256=dfa718414aa9498ed20cf3513b2cbfd024fae3bf29f7bf0a7065a4d58ec0ec2e"
set "REQUIREMENTS_SHA256=ae5ff78ab54babe81b04653a244af8b2ae70c9a87a89f20c14de788a7ea40c87"
set "STAGE_DIR=%TEMP%\ccd_agent_setup_%RANDOM%_%RANDOM%"
set "PYTHON_EXE="
set "CURL_EXE="

echo ===========================================
echo    CCD Agent Setup for Windows
echo ===========================================
echo This installer keeps existing logs and delta caches.
echo.

call :find_curl
call :find_python
if not defined PYTHON_EXE call :install_python
if errorlevel 1 goto :failed
if not defined PYTHON_EXE (
    echo ERROR: A working Python 3 installation was not found.
    goto :failed
)

if exist "%STAGE_DIR%" rmdir /s /q "%STAGE_DIR%"
mkdir "%STAGE_DIR%"
if errorlevel 1 (
    echo ERROR: Could not create the temporary download directory.
    goto :failed
)

echo [2/5] Downloading the verified CCD Agent package...
call :download "%CCD_AGENT_ARTIFACT_BASE_URL%/agent_program.py" "%STAGE_DIR%\agent_program.py"
if errorlevel 1 goto :download_failed
call :download "%CCD_AGENT_ARTIFACT_BASE_URL%/configure_agent.py" "%STAGE_DIR%\configure_agent.py"
if errorlevel 1 goto :download_failed
call :download "%CCD_AGENT_ARTIFACT_BASE_URL%/requirements.txt" "%STAGE_DIR%\requirements.txt"
if errorlevel 1 goto :download_failed

echo [3/5] Verifying package checksums...
call :verify_sha256 "%STAGE_DIR%\agent_program.py" "%AGENT_SHA256%" "agent_program.py"
if errorlevel 1 goto :checksum_failed
call :verify_sha256 "%STAGE_DIR%\configure_agent.py" "%CONFIGURE_SHA256%" "configure_agent.py"
if errorlevel 1 goto :checksum_failed
call :verify_sha256 "%STAGE_DIR%\requirements.txt" "%REQUIREMENTS_SHA256%" "requirements.txt"
if errorlevel 1 goto :checksum_failed

echo [4/5] Preparing the isolated Python environment...
if not exist "venv\Scripts\python.exe" (
    "%PYTHON_EXE%" -m venv venv
    if errorlevel 1 (
        echo ERROR: Could not create the Python virtual environment.
        goto :failed
    )
)
"venv\Scripts\python.exe" -m pip install --upgrade pip
if errorlevel 1 goto :dependency_failed
"venv\Scripts\python.exe" -m pip install -r "%STAGE_DIR%\requirements.txt"
if errorlevel 1 goto :dependency_failed

echo [5/5] Installing the CCD Agent files...
if exist "agent_program.py" copy /y "agent_program.py" "agent_program.py.previous" >nul
if exist "configure_agent.py" copy /y "configure_agent.py" "configure_agent.py.previous" >nul
if exist "requirements.txt" copy /y "requirements.txt" "requirements.txt.previous" >nul
copy /y "%STAGE_DIR%\agent_program.py" "agent_program.py" >nul
if errorlevel 1 goto :install_failed
copy /y "%STAGE_DIR%\configure_agent.py" "configure_agent.py" >nul
if errorlevel 1 goto :install_failed
copy /y "%STAGE_DIR%\requirements.txt" "requirements.txt" >nul
if errorlevel 1 goto :install_failed

set "NEED_CONFIGURATION=1"
"venv\Scripts\python.exe" -c "import keyring,sys; values=[keyring.get_password('ccd_agent', key) for key in ('erpnext_url','erpnext_user','erpnext_pass')]; sys.exit(0 if all(values) else 1)" >nul 2>&1
if not errorlevel 1 set "NEED_CONFIGURATION=0"

if "%NEED_CONFIGURATION%"=="0" (
    set "RECONFIGURE="
    set /p "RECONFIGURE=Existing CCD Agent credentials found. Replace them? [y/N]: "
    if /i "!RECONFIGURE!"=="Y" set "NEED_CONFIGURATION=1"
    if /i "!RECONFIGURE!"=="YES" set "NEED_CONFIGURATION=1"
)

if "%NEED_CONFIGURATION%"=="1" (
    "venv\Scripts\python.exe" configure_agent.py
    if errorlevel 1 (
        echo ERROR: Agent configuration was not completed.
        goto :failed
    )
) else (
    echo Existing Windows Credential Manager settings retained.
)

(
    echo @echo off
    echo setlocal
    echo cd /d "%%~dp0"
    echo "venv\Scripts\python.exe" agent_program.py
    echo set "CCD_AGENT_EXIT_CODE=%%ERRORLEVEL%%"
    echo echo.
    echo echo CCD Agent exited with code %%CCD_AGENT_EXIT_CODE%%.
    echo pause
    echo exit /b %%CCD_AGENT_EXIT_CODE%%
) > "Run_Agent.bat"

rmdir /s /q "%STAGE_DIR%" 2>nul
echo.
echo Setup complete. Double-click Run_Agent.bat to start the agent.
echo No inbound client port is required; the agent uses outbound HTTPS and WebSocket.
endlocal
exit /b 0

:find_curl
if exist "%~dp0curl.exe" set "CURL_EXE=%~dp0curl.exe"
if defined CURL_EXE exit /b 0
if exist "%SystemRoot%\System32\curl.exe" set "CURL_EXE=%SystemRoot%\System32\curl.exe"
if defined CURL_EXE exit /b 0
for /f "delims=" %%I in ('where curl.exe 2^>nul') do if not defined CURL_EXE set "CURL_EXE=%%I"
exit /b 0

:find_python
set "PYTHON_EXE="
for /f "delims=" %%I in ('py -3 -c "import sys; print(sys.executable)" 2^>nul') do if not defined PYTHON_EXE set "PYTHON_EXE=%%I"
if defined PYTHON_EXE exit /b 0
for /f "delims=" %%I in ('where python.exe 2^>nul') do if not defined PYTHON_EXE set "PYTHON_EXE=%%I"
if defined PYTHON_EXE "%PYTHON_EXE%" -c "import sys; raise SystemExit(0 if sys.version_info.major == 3 else 1)" >nul 2>&1
if errorlevel 1 set "PYTHON_EXE="
exit /b 0

:install_python
echo [1/5] Python 3 was not found. Downloading Python 3.13...
set "PYTHON_INSTALLER=%TEMP%\ccd_agent_python_%RANDOM%.exe"
call :download "%PYTHON_INSTALLER_URL%" "%PYTHON_INSTALLER%"
if errorlevel 1 (
    echo ERROR: Could not download the Python installer.
    exit /b 1
)
"%PYTHON_INSTALLER%" /quiet InstallAllUsers=0 PrependPath=1 Include_test=0
set "INSTALL_RESULT=%ERRORLEVEL%"
del "%PYTHON_INSTALLER%" 2>nul
if not "%INSTALL_RESULT%"=="0" (
    echo ERROR: Python installation returned code %INSTALL_RESULT%.
    exit /b 1
)
call :find_python
if not defined PYTHON_EXE if exist "%LocalAppData%\Programs\Python\Python313\python.exe" set "PYTHON_EXE=%LocalAppData%\Programs\Python\Python313\python.exe"
if not defined PYTHON_EXE (
    echo ERROR: Python was installed but python.exe could not be located.
    exit /b 1
)
exit /b 0

:download
set "DOWNLOAD_URL=%~1"
set "DOWNLOAD_TARGET=%~2"
if defined CURL_EXE (
    "%CURL_EXE%" --fail --location --retry 3 --connect-timeout 30 "%DOWNLOAD_URL%" --output "%DOWNLOAD_TARGET%"
    exit /b !ERRORLEVEL!
)
powershell -NoLogo -NoProfile -NonInteractive -ExecutionPolicy Bypass -Command "[Net.ServicePointManager]::SecurityProtocol=[Net.SecurityProtocolType]::Tls12; (New-Object Net.WebClient).DownloadFile($env:DOWNLOAD_URL,$env:DOWNLOAD_TARGET)"
exit /b %ERRORLEVEL%

:verify_sha256
"%PYTHON_EXE%" -c "import hashlib,sys; actual=hashlib.sha256(open(sys.argv[1],'rb').read()).hexdigest(); print(sys.argv[3]+': '+actual); raise SystemExit(0 if actual.lower()==sys.argv[2].lower() else 1)" "%~1" "%~2" "%~3"
exit /b %ERRORLEVEL%

:download_failed
echo ERROR: The CCD Agent package could not be downloaded.
goto :failed

:checksum_failed
echo ERROR: Package checksum validation failed. No downloaded file was installed.
goto :failed

:dependency_failed
echo ERROR: A required Python package could not be installed.
goto :failed

:install_failed
echo ERROR: The verified CCD Agent files could not be installed.
goto :failed

:failed
if exist "%STAGE_DIR%" rmdir /s /q "%STAGE_DIR%" 2>nul
echo.
echo CCD Agent setup did not complete. Existing logs and delta caches were not removed.
pause
endlocal
exit /b 1
