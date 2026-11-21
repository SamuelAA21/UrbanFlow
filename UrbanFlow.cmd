@echo off
cd /d "%~dp0"
start "" powershell.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File "%~dp0launcher\UrbanFlowLauncher.ps1" %*
if errorlevel 1 (
  echo.
  echo No se pudo abrir el lanzador de UrbanFlow.
  pause
)
