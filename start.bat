@echo off
setlocal
cd /d %~dp0
chcp 65001 >nul
title QuantLab - 量化研究平台

echo.
echo  ╔══════════════════════════════════════════════════════════════╗
echo  ║                                                              ║
echo  ║                 QuantLab 量化研究平台                        ║
echo  ║                                                              ║
echo  ║         Quantitative Research Platform with Streamlit        ║
echo  ║                                                              ║
echo  ╚══════════════════════════════════════════════════════════════╝
echo.

if not exist "src\quantlab" (
    echo [错误] 请在 quantlab 根目录下运行此脚本
    pause
    exit /b 1
)

python --version >nul 2>&1
if errorlevel 1 (
    echo [错误] Python 未安装或未添加到 PATH
    pause
    exit /b 1
)

for /f "tokens=2" %%a in ('python --version 2^>^&1') do set PYTHON_VERSION=%%a
echo [信息] 检测到 Python 版本: %PYTHON_VERSION%
echo.

set PYTHONPATH=%CD%\src;%PYTHONPATH%

echo.
echo ════════════════════════════════════════════════════════════════
echo  选择要启动的页面 Select page to launch:
echo ════════════════════════════════════════════════════════════════
echo.
echo  [1] Runs 页面 - 实验列表 (Runs list)
echo  [2] Chart Demo - 图表演示 (Chart visualization)
echo  [3] Run Detail - 回测详情 (Run details)
echo  [4] Main App - 主入口 (Main entry)
echo.
echo  [0] 退出 Exit
echo.

set /p choice="请输入选项 Enter choice [0-4]: "

if "%choice%"=="0" exit /b 0
if "%choice%"=="1" set STREAMLIT_ENTRY=ui/pages/1_runs.py& goto :launch
if "%choice%"=="2" set STREAMLIT_ENTRY=ui/pages/3_chart_demo.py& goto :launch
if "%choice%"=="3" set STREAMLIT_ENTRY=ui/pages/2_run_detail.py& goto :launch
if "%choice%"=="4" set STREAMLIT_ENTRY=ui/app.py& goto :launch

echo [ERROR] 无效选项 Invalid choice: %choice%
pause
exit /b 1

:launch
echo [INFO] Entry: %STREAMLIT_ENTRY%
python -m streamlit run %STREAMLIT_ENTRY% --server.headless false
if errorlevel 1 (
    echo [ERROR] Streamlit 启动失败
    pause
    exit /b 1
)

pause
