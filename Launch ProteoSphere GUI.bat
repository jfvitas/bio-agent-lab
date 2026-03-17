@echo off
setlocal

cd /d "%~dp0"
call "%~dp0Launch PBData GUI.bat"
exit /b %errorlevel%
