param(
  [Parameter(Position=0, Mandatory=$true)][string]$Corridor,
  [Parameter(Position=1, Mandatory=$true)][string]$Date,
  [Parameter(Position=2, Mandatory=$false)][string]$CsvPattern = 'Train_details*.csv'
)

$ErrorActionPreference = 'Stop'
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ArtifactDir = "artifacts/$Corridor/$Date"

Write-Host "[ALL] Running Phase 1 + Block View for '$Corridor' on '$Date'"

# Step 1-6: Phase 1 pipeline
Write-Host "[ALL] Step 1/2: Phase 1 pipeline"
powershell -NoProfile -ExecutionPolicy Bypass -File "$ScriptDir/run_phase1.ps1" $Corridor $Date $CsvPattern
if ($LASTEXITCODE -ne 0) { throw "run_phase1.ps1 failed (exit $LASTEXITCODE)" }

# Step 7: Block-level view
Write-Host "[ALL] Step 2/2: Block-level view"
powershell -NoProfile -ExecutionPolicy Bypass -File "$ScriptDir/run_block_view.ps1" $Corridor $Date
if ($LASTEXITCODE -ne 0) { throw "run_block_view.ps1 failed (exit $LASTEXITCODE)" }

Write-Host "[ALL] Complete. Artifacts stored in $ArtifactDir"

