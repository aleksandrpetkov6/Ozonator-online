@echo off
setlocal
cd /d "%~dp0"
if exist "%~dp0release\win-unpacked\Ozonator.exe" (
  start "" "%~dp0release\win-unpacked\Ozonator.exe"
) else (
  echo Portable билд не найден: release\win-unpacked\Ozonator.exe
  echo Сначала соберите приложение: npm run dist
  pause
)
