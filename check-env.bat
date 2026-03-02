@echo off
chcp 65001 >nul
title QuantLab - Environment Check
echo.
echo ╔══════════════════════════════════════════════════════════════╗
echo ║              QuantLab 环境检查 Environment Check             ║
echo ╚══════════════════════════════════════════════════════════════╝
echo.

:: Check directory
echo [1/6] 检查目录结构 Checking directory structure...
if not exist "src\quantlab" (
    echo   [FAIL] 不在 quantlab 根目录下 Not in quantlab root directory
    exit /b 1
) else (
    echo   [PASS] 目录正确 Directory OK
)

:: Check Python
echo [2/6] 检查 Python Checking Python...
python --version >nul 2>&1
if errorlevel 1 (
    echo   [FAIL] Python 未安装 Python not installed
    exit /b 1
)
for /f "tokens=2" %%a in ('python --version 2^>^&1') do echo   [PASS] Python %%a

:: Check core dependencies
echo [3/6] 检查核心依赖 Checking core dependencies...

python -c "import pandas" >nul 2>&1
if errorlevel 1 (echo   [FAIL] pandas) else (echo   [PASS] pandas)

python -c "import numpy" >nul 2>&1
if errorlevel 1 (echo   [FAIL] numpy) else (echo   [PASS] numpy)

python -c "import pyarrow" >nul 2>&1
if errorlevel 1 (echo   [FAIL] pyarrow) else (echo   [PASS] pyarrow)

python -c "import streamlit" >nul 2>&1
if errorlevel 1 (echo   [FAIL] streamlit) else (
    for /f "tokens=2" %%a in ('python -c "import streamlit; print(streamlit.__version__)" 2^>^&1') do (
        echo   [PASS] streamlit %%a
    )
)

python -c "import yaml" >nul 2>&1
if errorlevel 1 (echo   [FAIL] pyyaml) else (echo   [PASS] pyyaml)

python -c "import loguru" >nul 2>&1
if errorlevel 1 (echo   [FAIL] loguru) else (echo   [PASS] loguru)

:: Check data
echo [4/6] 检查数据 Checking data...
set RUN_COUNT=0
if exist "runs" (
    for /d %%D in (runs\*) do (
        if exist "%%D\results\metrics.json" (
            set /a RUN_COUNT+=1
            echo   [DATA] %%~nD
        )
    )
)
if %RUN_COUNT%==0 (
    echo   [WARN] 未检测到回测结果 No backtest results found
    echo        运行 python examples/risk_constraints_demo.py 生成示例数据
) else (
    echo   [PASS] 发现 %RUN_COUNT% 个回测结果 Found %RUN_COUNT% runs
)

:: Check curated data
echo [5/6] 检查 curated 数据 Checking curated data...
if exist "data\curated\bars" (
    for /r "data\curated\bars" %%f in (*.csv *.parquet) do (
        echo   [DATA] %%~nxf
    )
) else (
    echo   [INFO] 无 curated 数据 No curated data (will use synthetic fallback)
)

:: Test imports
echo [6/6] 测试模块导入 Testing module imports...
python -c "import sys; sys.path.insert(0, 'src'); from quantlab.data.schema import validate_bars_df; print('  [PASS] data.schema')" >nul 2>&1
if errorlevel 1 (echo   [FAIL] data.schema) else (echo   [PASS] data.schema)

python -c "import sys; sys.path.insert(0, 'src'); from quantlab.execution.constraints import check_all_constraints; print('  [PASS] execution.constraints')" >nul 2>&1
if errorlevel 1 (echo   [FAIL] execution.constraints) else (echo   [PASS] execution.constraints)

python -c "import sys; sys.path.insert(0, 'src'); from quantlab.research.reconcile import quick_reconcile; print('  [PASS] research.reconcile')" >nul 2>&1
if errorlevel 1 (echo   [FAIL] research.reconcile) else (echo   [PASS] research.reconcile)

echo.
echo ════════════════════════════════════════════════════════════════
echo 环境检查完成 Environment check complete!
echo.
echo 启动命令 Start commands:
echo   start.bat       - 交互式启动 Interactive start
echo   start-quick.bat - 快速启动 Quick start (Chart Demo)
echo ════════════════════════════════════════════════════════════════
pause
