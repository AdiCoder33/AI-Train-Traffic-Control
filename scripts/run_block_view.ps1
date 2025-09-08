param(
  [Parameter(Position=0, Mandatory=$true)][string]$Corridor,
  [Parameter(Position=1, Mandatory=$true)][string]$Date
)

$ErrorActionPreference = 'Stop'

$ArtifactDir = "artifacts/$Corridor/$Date"

Write-Host "Running block-level view for corridor '$Corridor' on '$Date'"

if (-not (Test-Path "$ArtifactDir/events_clean.parquet")) {
  Write-Error "Missing events_clean.parquet in $ArtifactDir. Run phase1 pipeline first."
  exit 1
}
if (-not (Test-Path "$ArtifactDir/section_edges.parquet")) {
  Write-Error "Missing section_edges.parquet in $ArtifactDir. Run phase1 pipeline first."
  exit 1
}

python -c "import sys, pandas as pd; from src.data.block_view import build, save; artifact_dir,corridor,date=sys.argv[1],sys.argv[2],sys.argv[3]; df_slice=pd.read_parquet(fr'{artifact_dir}/events_clean.parquet'); edges_df=pd.read_parquet(fr'{artifact_dir}/section_edges.parquet'); res=build(df_slice, edges_df); save(res, corridor, date)" $ArtifactDir $Corridor $Date
if ($LASTEXITCODE -ne 0) { throw "Block-level view failed (exit $LASTEXITCODE)" }

Write-Host "Block-level artifacts written to $ArtifactDir"
