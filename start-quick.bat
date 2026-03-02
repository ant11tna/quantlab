@echo off
chcp 65001 >nul
title QuantLab - Quick Start

:: Quick start - directly launch Chart Demo (most visual)
set PYTHONPATH=%CD%;%CD%\src

python --version >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Python not found. Please install Python 3.10+
    pause
    exit /b 1
)

echo Starting QuantLab Chart Demo...
python -m streamlit run ui/pages/3_chart_demo.py

pause
