@echo off
setlocal

cd /d "%~dp0"

set "REPO_ROOT=%CD%"
set "VENV_PYTHON=%REPO_ROOT%\.venv\Scripts\python.exe"
set "PYTHONPATH=%REPO_ROOT%\src"

if exist "%VENV_PYTHON%" (
    "%VENV_PYTHON%" -m pbdata.gui
    set "EXIT_CODE=%ERRORLEVEL%"
) else (
    where py >nul 2>nul
    if %ERRORLEVEL%==0 (
        py -m pbdata.gui
        set "EXIT_CODE=%ERRORLEVEL%"
    ) else (
        echo Could not find .venv\Scripts\python.exe or the Windows "py" launcher.
        echo Create the virtual environment and install dependencies first.
        set "EXIT_CODE=1"
    )
)

if not "%EXIT_CODE%"=="0" (
    echo.
    echo GUI launch failed with exit code %EXIT_CODE%.
    pause
)

exit /b %EXIT_CODE%
