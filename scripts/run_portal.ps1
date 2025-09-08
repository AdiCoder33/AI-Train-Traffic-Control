param(
  [Parameter(Position=0, Mandatory=$false)][string]$ApiHost = '127.0.0.1',
  [Parameter(Position=1, Mandatory=$false)][int]$ApiPort = 8000
)

$ErrorActionPreference = 'Stop'

# Prefer venv python if present
$RepoRoot = Split-Path $PSScriptRoot -Parent
$VenvPy = Join-Path $RepoRoot ".venv\Scripts\python.exe"
$Py = if (Test-Path $VenvPy) { $VenvPy } else { "python" }

Write-Host "[PORTAL] Starting API (FastAPI) and UI (Streamlit) using $Py"

Start-Process -NoNewWindow -FilePath $Py -ArgumentList "-m","uvicorn","src.api.server:app","--host",$ApiHost,"--port",$ApiPort,"--reload"

# Configure Streamlit to be headless and skip usage prompt
$env:STREAMLIT_SERVER_HEADLESS = "true"
$env:STREAMLIT_BROWSER_GATHER_USAGE_STATS = "false"
$env:STREAMLIT_SERVER_ADDRESS = $ApiHost
$env:STREAMLIT_SERVER_PORT = "8501"

# Use python -m streamlit for portability
Start-Process -NoNewWindow -FilePath $Py -ArgumentList "-m","streamlit","run","src/ui/app.py","--server.headless","true","--server.address",$ApiHost,"--server.port","8501","--browser.gatherUsageStats","false"

Write-Host ("[PORTAL] API on http://{0}:{1}, UI on Streamlit default port" -f $ApiHost, $ApiPort)
