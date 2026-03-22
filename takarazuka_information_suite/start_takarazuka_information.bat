@echo off
setlocal

echo.
echo ==============================================
echo   Takarazuka Information Suite v0.1.0
echo ==============================================
echo.

cd /d "%~dp0"

echo [1/3] Installing requirements...
py -m pip install -r requirements.txt
if errorlevel 1 goto end

echo.
echo [2/3] Collecting data and building dashboards...
py takarazuka_info\takarazuka_info.py
if errorlevel 1 goto end

echo.
echo [3/3] Opening main dashboard...
start "" "%~dp0takarazuka_info\output\index.html"

:end
echo.
pause
