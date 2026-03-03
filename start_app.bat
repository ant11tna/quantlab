@echo off
setlocal
cd /d %~dp0
set PYTHONPATH=%CD%\src;%PYTHONPATH%

python -m streamlit run ui/app.py --server.port 8501 --server.headless false
if errorlevel 1 (
    echo [ERROR] Streamlit 启动失败
    pause
    exit /b 1
)

pause
