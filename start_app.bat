@echo off
setlocal
cd /d %~dp0
set PYTHONPATH=%CD%\src;%PYTHONPATH%
<<<<<<< codex/troubleshoot-start_app.bat-issues-ozj679
python -m streamlit run ui/app.py --server.headless false
=======
python -m streamlit run ui/app.py --server.port 8501 --server.headless false
>>>>>>> main
pause
