@echo off
cd /d "%~dp0.."

:: port na kojem se ovaj worker vrti, mijenjaj ako pokreces vise workera
set WORKER_PORT=8001

:: adresa schedulera
set SCHEDULER_URL=http://localhost:8000

uvicorn worker.main:app --host 0.0.0.0 --port %WORKER_PORT% --reload
