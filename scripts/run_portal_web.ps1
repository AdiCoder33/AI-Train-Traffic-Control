param(
  [string]$ApiHost = "127.0.0.1",
  [int]$ApiPort = 8000,
  [int]$WebPort = 5173
)

$ErrorActionPreference = "Stop"

$Py = (Get-Command python).Source
if (-not $Py) { throw "python not found" }

Write-Host ("[PORTAL] Starting API on http://{0}:{1}" -f $ApiHost, $ApiPort)
Start-Process -NoNewWindow -FilePath $Py -ArgumentList "-m","uvicorn","src.api.server:app","--host",$ApiHost,"--port",$ApiPort,"--reload"

Write-Host ("[PORTAL] Starting Web UI on http://localhost:{0}" -f $WebPort)
Push-Location web
Write-Host "[PORTAL] Installing web dependencies (npm install)"
npm install
$env:VITE_API_BASE = "http://${ApiHost}:${ApiPort}"
Start-Process -NoNewWindow -FilePath "npm" -ArgumentList "run","dev"
Pop-Location

Write-Host "[PORTAL] API and Web UI launched."
