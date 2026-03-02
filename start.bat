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

:: Check if we're in the right directory
if not exist "src\quantlab" (
    echo [错误] 请在 quantlab 根目录下运行此脚本
    pause
    exit /b 1
)

:: Check Python installation
python --version >nul 2>&1
if errorlevel 1 (
    echo [错误] Python 未安装或未添加到 PATH
    echo.
    echo 请安装 Python 3.10+ 并确保添加到系统 PATH
    pause
    exit /b 1
)

for /f "tokens=2" %%a in ('python --version 2^>^&1') do set PYTHON_VERSION=%%a
echo [信息] 检测到 Python 版本: %PYTHON_VERSION%
echo.

:: Set PYTHONPATH
set PYTHONPATH=%CD%;%CD%\src

:: Check and install dependencies
echo [INFO] 检查依赖...
echo [INFO] Checking dependencies...

python -c "import streamlit" >nul 2>&1
if errorlevel 1 (
    echo [WARN] Streamlit 未安装，正在安装依赖...
    echo [WARN] Streamlit not installed, installing dependencies...
    pip install -q streamlit pandas pyarrow pyyaml loguru
    if errorlevel 1 (
        echo [ERROR] 依赖安装失败
        echo [ERROR] Failed to install dependencies
        pause
        exit /b 1
    )
)

python -c "import pandas, pyarrow, yaml" >nul 2>&1
if errorlevel 1 (
    echo [WARN] 部分依赖缺失，正在安装...
    echo [WARN] Some dependencies missing, installing...
    pip install -q pandas pyarrow pyyaml loguru
)

echo [INFO] 依赖检查完成 ✓
echo [INFO] Dependencies check complete ✓
echo.

:: Check for existing runs
set RUNS_EXIST=0
if exist "runs" (
    for /d %%D in (runs\*) do (
        if exist "%%D\results\metrics.json" (
            set RUNS_EXIST=1
        )
    )
)

if %RUNS_EXIST%==0 (
    echo [WARN] 未检测到回测结果，建议先运行示例脚本生成数据
    echo [WARN] No backtest results detected. Consider running example scripts first.
    echo.
    echo 可用示例 Available examples:
    echo   - python examples/risk_constraints_demo.py
    echo.
)

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

if "%choice%"=="0" (
    echo 退出 Exit
    exit /b 0
)

if "%choice%"=="1" (
    set STREAMLIT_ENTRY=ui/pages/1_runs.py
    goto :launch
)

if "%choice%"=="2" (
    set STREAMLIT_ENTRY=ui/pages/3_chart_demo.py
    goto :launch
)

if "%choice%"=="3" (
    set STREAMLIT_ENTRY=ui/pages/2_run_detail.py
    goto :launch
)

if "%choice%"=="4" (
    set STREAMLIT_ENTRY=ui/app.py
    goto :launch
)

echo [ERROR] 无效选项 Invalid choice: %choice%
pause
exit /b 1

:launch
echo.
echo [INFO] 正在启动 Streamlit...
echo [INFO] Launching Streamlit...
echo [INFO] Entry: %STREAMLIT_ENTRY%
echo.
echo 浏览器将自动打开，请稍候...
echo Browser will open automatically, please wait...
echo.
echo 按 Ctrl+C 停止服务
echo Press Ctrl+C to stop the server
echo.

python -m streamlit run %STREAMLIT_ENTRY% --server.headless false

if errorlevel 1 (
    echo.
    echo [ERROR] Streamlit 启动失败
    echo [ERROR] Failed to start Streamlit
    echo.
    echo 请检查:
    echo - Python 环境是否正确
    echo - 依赖是否完整安装: pip install streamlit pandas pyarrow
    echo.
    pause
    exit /b 1
)

echo.
echo [INFO] Streamlit 已停止
echo [INFO] Streamlit stopped
pause
