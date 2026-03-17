@echo off
setlocal

cd /d "%~dp0"
set "BOOTSTRAP=%CD%\scripts\launch_pbdata_winui.ps1"

if not exist "%BOOTSTRAP%" (
    echo Could not find bootstrap script:
    echo %BOOTSTRAP%
    pause
    exit /b 1
)

powershell -NoProfile -ExecutionPolicy Bypass -File "%BOOTSTRAP%"
if errorlevel 1 (
    echo.
    echo ProteoSphere WinUI launch failed.
    echo Check logs\winui_launcher_build.log and logs\winui_launcher_runtime.log for details.
    pause
    exit /b 1
)

exit /b 0
