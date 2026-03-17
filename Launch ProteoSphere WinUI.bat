@echo off
setlocal

cd /d "%~dp0"
call "%~dp0Launch PBData WinUI.bat"
exit /b %errorlevel%
