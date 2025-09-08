param(
  [Parameter(Position=0, Mandatory=$true)][string]$Scope,
  [Parameter(Position=1, Mandatory=$true)][string]$Date
)

$ErrorActionPreference = 'Stop'

python -m src.learn.train_delay $Scope $Date
python -m src.learn.update_risk $Scope $Date
python -m src.learn.collect_rl $Scope $Date

Write-Host "[RETRAIN] Reports written under artifacts/$Scope/$Date"

