@echo off
cd /d "%~dp0.."
uvicorn worker.main:app --host 0.0.0.0 --port 8001 --reload
