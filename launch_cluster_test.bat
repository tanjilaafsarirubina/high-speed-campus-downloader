@echo off
:: ====================================================================================
:: CSE449: Parallel, Distributed & High-Performance Computing
:: Project: High Speed Campus Downloader for Students (EdgeMesh Distributed Downloader)
:: Authors: Tanjila Afsari Rubina (24241310) & Sandip Kumar Paul (24241311)
:: ====================================================================================
title EdgeMesh Local Cluster Launcher
echo =======================================================
echo   High Speed Campus Downloader (EdgeMesh) Local Test
echo   CSE449 - Parallel & Distributed Systems Project
echo =======================================================
echo.
echo Launching standalone GUI for demonstration...
echo.

start python main.py

echo [1] Launched GUI: python main.py
echo [2] To run speedup benchmarks: python benchmark.py
echo [3] To run formal QA verification suite: python test_qa_suite.py
echo.
pause
