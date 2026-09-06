@echo off
:: ====================================================================================
:: CSE449: Parallel, Distributed & High-Performance Computing
:: Project: High Speed Campus Downloader for Students (EdgeMesh Distributed Downloader)
:: Authors: Tanjila Afsari Rubina (24241310) & Sandip Kumar Paul (24241311)
:: ====================================================================================
title Build Campus Downloader EXE
echo ========================================================
echo   Compiling High Speed Campus Downloader Standalone EXE
echo ========================================================
echo.

python -m PyInstaller --noconsole --onefile --name "CampusDownloader" --collect-all customtkinter main.py

echo.
echo ========================================================
echo  Build Complete! Standalone EXE is in the 'dist' folder.
echo ========================================================
pause
