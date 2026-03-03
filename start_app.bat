@echo off
setlocal
cd /d %~dp0
set PYTHONPATH=%CD%\src;%PYTHONPATH%
python -m streamlit run ui/app.py --server.headless false
pause
