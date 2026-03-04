@echo off
chcp 65001 >nul
title QuantLab - Install Dependencies
echo.
echo Installing QuantLab dependencies...
echo.

python -m pip install --upgrade pip

pip install -q streamlit plotly pandas pyarrow pyyaml loguru
pip install -q numpy exchange_calendars

echo.
echo Optional dependencies (for enhanced charts):
echo   pip install streamlit-echarts
echo.
echo Installation complete!
pause
