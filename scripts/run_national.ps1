param(
  [Parameter(Position=0, Mandatory=$true)][string]$ScopeId,
  [Parameter(Position=1, Mandatory=$true)][string]$Date
)

$ErrorActionPreference = 'Stop'

$ArtifactDir = "artifacts/$ScopeId/$Date"

if (-not (Test-Path "$ArtifactDir/events_clean.parquet")) {
  Write-Error "Missing $ArtifactDir/events_clean.parquet"
  exit 1
}
if (-not (Test-Path "$ArtifactDir/section_edges.parquet")) {
  Write-Error "Missing $ArtifactDir/section_edges.parquet"
  exit 1
}
if (-not (Test-Path "$ArtifactDir/section_nodes.parquet")) {
  Write-Error "Missing $ArtifactDir/section_nodes.parquet"
  exit 1
}

Write-Host "Running nationwide baseline replay for scope '$ScopeId' on '$Date'"

python -c "import sys, json; from pathlib import Path; import pandas as pd; from src.model.section_graph import load_graph; from src.sim.national_replay import run, save; artifact_dir=Path(sys.argv[1]); events=pd.read_parquet(artifact_dir/'events_clean.parquet'); edges=pd.read_parquet(artifact_dir/'section_edges.parquet'); nodes=pd.read_parquet(artifact_dir/'section_nodes.parquet'); graph=load_graph(nodes, edges); res=run(events, graph); save(res, artifact_dir); print(json.dumps(res.sim_kpis, indent=2))" $ArtifactDir

if ($LASTEXITCODE -ne 0) { throw "national replay failed (exit $LASTEXITCODE)" }

Write-Host "National replay artifacts written to $ArtifactDir"
