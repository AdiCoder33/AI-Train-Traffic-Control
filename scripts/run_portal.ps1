param(
  [Parameter(Position=0, Mandatory=$false)][string]$ApiHost = '127.0.0.1',
  [Parameter(Position=1, Mandatory=$false)][int]$ApiPort = 8000,
  [Parameter(Position=2, Mandatory=$false)][int]$WebPort = 5173
)

$ErrorActionPreference = 'Stop'

# Prefer venv python if present
$RepoRoot = Split-Path $PSScriptRoot -Parent
$VenvPy = Join-Path $RepoRoot ".venv\Scripts\python.exe"
$Py = if (Test-Path $VenvPy) { $VenvPy } else { "python" }

Write-Host "[PORTAL] Starting API (FastAPI) using $Py"
Start-Process -NoNewWindow -FilePath $Py -ArgumentList "-m","uvicorn","src.api.server:app","--host",$ApiHost,"--port",$ApiPort,"--reload"

Write-Host ("[PORTAL] Starting Web UI on http://localhost:{0}" -f $WebPort)
Push-Location (Join-Path $RepoRoot 'web')
Write-Host "[PORTAL] Installing web dependencies (npm install)"
npm install
$env:VITE_API_BASE = "http://${ApiHost}:${ApiPort}"
Start-Process -NoNewWindow -FilePath "npm" -ArgumentList "run","dev","--","--port",$WebPort
Pop-Location

Write-Host ("[PORTAL] API on http://{0}:{1}, Web UI on http://localhost:{2}" -f $ApiHost, $ApiPort, $WebPort)
