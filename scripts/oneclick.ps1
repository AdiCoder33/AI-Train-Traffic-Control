param(
  [Parameter(Mandatory=$false)][switch]$TrainIL = $true,
  [Parameter(Mandatory=$false)][switch]$BuildRL = $false,
  [Parameter(Mandatory=$false)][switch]$TrainRL = $false,
  [Parameter(Mandatory=$false)][string]$ApiHost = '127.0.0.1',
  [Parameter(Mandatory=$false)][int]$ApiPort = 8000,
  [Parameter(Mandatory=$false)][int]$WebPort = 5173,
  [Parameter(Mandatory=$false)][double]$Alpha = 0.2
)

$ErrorActionPreference = 'Stop'

$RepoRoot = Split-Path $PSScriptRoot -Parent
$VenvPy = Join-Path $RepoRoot ".venv\Scripts\python.exe"
$Py = if (Test-Path $VenvPy) { $VenvPy } else { "python" }

Write-Host "[ONECLICK] Using Python: $Py"

if ($TrainIL) {
  Write-Host "[ONECLICK] Training Global IL model..."
  & $Py -m src.learn.train_corpus | Write-Host
}

if ($BuildRL) {
  Write-Host "[ONECLICK] Building Offline RL dataset (alpha=$Alpha)..."
  & $Py -m src.learn.offline_rl --alpha $Alpha | Write-Host
}

if ($TrainRL) {
  Write-Host "[ONECLICK] Training Offline RL policy..."
  & $Py -m src.learn.train_offrl | Write-Host
}

Write-Host ("[ONECLICK] Starting API (FastAPI) on http://{0}:{1}" -f $ApiHost, $ApiPort)
Start-Process -NoNewWindow -FilePath $Py -ArgumentList "-m","uvicorn","src.api.server:app","--host",$ApiHost,"--port",$ApiPort,"--reload"

Write-Host ("[ONECLICK] Starting Web UI (Vite) on http://localhost:{0}" -f $WebPort)
Push-Location (Join-Path $RepoRoot 'web')
Write-Host "[ONECLICK] Installing web dependencies (npm install)"
npm install
$env:VITE_API_BASE = "http://${ApiHost}:${ApiPort}"
Start-Process -NoNewWindow -FilePath "npm" -ArgumentList "run","dev","--","--port",$WebPort
Pop-Location

Write-Host ("[ONECLICK] Ready. API on http://{0}:{1}, Web UI on http://localhost:{2}" -f $ApiHost, $ApiPort, $WebPort)
