@echo off
:: ====================================================================================
:: CSE449: Parallel, Distributed & High-Performance Computing
:: Project: High Speed Campus Downloader for Students (EdgeMesh Distributed Downloader)
:: Authors: Tanjila Afsari Rubina (24241310) & Sandip Kumar Paul (24241311)
:: ====================================================================================
title Build Campus Downloader EXE
cd /d "%~dp0"
echo ========================================================
echo   Compiling High Speed Campus Downloader Standalone EXE
echo ========================================================
echo.

:: Prefer the project's virtual environment when there is one
set "PY=python"
if exist ".venv\Scripts\python.exe" set "PY=.venv\Scripts\python.exe"

"%PY%" -m PyInstaller --noconsole --onefile --name "CampusDownloader" --collect-all customtkinter main.py
if errorlevel 1 (
    echo.
    echo  Build FAILED. Is PyInstaller installed?  pip install pyinstaller
    pause
    exit /b 1
)

echo.
echo ========================================================
echo  Build Complete! Standalone EXE is in the 'dist' folder.
echo ========================================================
pause
