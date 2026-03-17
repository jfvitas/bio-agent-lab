@echo off
setlocal

cd /d "%~dp0"
call "%~dp0Run PBData Smoke Check.bat"
exit /b %errorlevel%
