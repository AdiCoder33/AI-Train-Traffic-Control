param(
  [Parameter(Position=0, Mandatory=$true)][string]$Scope,
  [Parameter(Position=1, Mandatory=$true)][string]$Date
)

$ErrorActionPreference = 'Stop'

python -m src.reports.aggregate $Scope $Date

Write-Host "[REPORT] Wrote kpi_reports.json"

