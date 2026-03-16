@echo off
setlocal

set "REPO_ROOT=%~dp0"
pushd "%REPO_ROOT%" >nul

set "PYTHON_EXE=%REPO_ROOT%\.venv\Scripts\python.exe"
if not exist "%PYTHON_EXE%" (
  set "PYTHON_EXE=python"
)

"%PYTHON_EXE%" "%REPO_ROOT%scripts\run_repo_smoke.py" %*
set "EXIT_CODE=%ERRORLEVEL%"

popd >nul
exit /b %EXIT_CODE%
