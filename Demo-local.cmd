@echo off
cd /d "%~dp0"
python -c "import starlette,uvicorn" >nul 2>&1
if errorlevel 1 (
  python -m pip install -r portal\requirements.txt
  if errorlevel 1 goto error
)
echo Abre http://127.0.0.1:8090/laboratory cuando el servidor indique que esta listo.
python -m portal.local_demo
:error
pause
