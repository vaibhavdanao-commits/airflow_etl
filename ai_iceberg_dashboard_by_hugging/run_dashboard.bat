@echo off
setlocal
cd /d "%~dp0"
if not exist ".venv\Scripts\python.exe" (
  echo Creating a project virtual environment...
  py -3.12 -m venv .venv
  if errorlevel 1 py -3.11 -m venv .venv
)
call .venv\Scripts\activate.bat
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
if not exist "dashboard\.env" copy "dashboard\.env.example" "dashboard\.env"
echo.
echo Make sure Ollama is running and the configured model is installed.
echo Starting Streamlit...
python -m streamlit run dashboard\app.py
pause
