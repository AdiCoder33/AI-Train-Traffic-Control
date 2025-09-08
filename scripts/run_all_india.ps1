param(
  [Parameter(Position=0, Mandatory=$true)][string]$ScopeId,
  [Parameter(Position=1, Mandatory=$true)][string]$Date,
  [Parameter(Position=2, Mandatory=$false)][string]$CsvPattern = 'Train_details*.csv'
)

$ErrorActionPreference = 'Stop'
$ArtifactDir = "artifacts/$ScopeId/$Date"

Write-Host "[ALL-INDIA] Preparing artifacts for scope '$ScopeId' on '$Date' (pattern=$CsvPattern)"

New-Item -ItemType Directory -Force -Path $ArtifactDir | Out-Null

# 1) Load raw
Write-Host "[1/4] Loading raw CSVs"
python -c "import sys; from src.data.loader import load_raw; import pandas as pd; pattern=sys.argv[1]; out=sys.argv[2]; df=load_raw(pattern=pattern); df.to_parquet(out, index=False)" $CsvPattern "$ArtifactDir/raw.parquet"
if ($LASTEXITCODE -ne 0) { throw "Load raw failed (exit $LASTEXITCODE)" }

# 2) Normalize to events_clean
Write-Host "[2/4] Normalizing to events_clean"
python -c "import sys, pandas as pd; from src.data.normalize import to_train_events; raw_p, out_p, date = sys.argv[1], sys.argv[2], sys.argv[3]; df_raw=pd.read_parquet(raw_p); df_norm=to_train_events(df_raw, default_service_date=date); df_norm.to_parquet(out_p, index=False)" "$ArtifactDir/raw.parquet" "$ArtifactDir/events_clean.parquet" $Date
if ($LASTEXITCODE -ne 0) { throw "Normalize failed (exit $LASTEXITCODE)" }

# 3) Build national nodes/edges (set larger default platforms)
Write-Host "[3/4] Building national nodes/edges"
python -c "import sys, json, pandas as pd; from src.data.graph import build; events_p, edges_p, nodes_p = sys.argv[1], sys.argv[2], sys.argv[3]; DEFAULT_PLATFORMS=6; df = pd.read_parquet(events_p); stations = sorted([s for s in df['station_id'].dropna().unique().tolist()]); stations_dict = {sid:i for i,sid in enumerate(stations)}; edges_df, nodes_df = build(df, stations_dict); nodes_df['platforms'] = DEFAULT_PLATFORMS; edges_df.to_parquet(edges_p, index=False); nodes_df.to_parquet(nodes_p, index=False)" "$ArtifactDir/events_clean.parquet" "$ArtifactDir/section_edges.parquet" "$ArtifactDir/section_nodes.parquet"
if ($LASTEXITCODE -ne 0) { throw "Build national graph failed (exit $LASTEXITCODE)" }

# 4) Run national baseline replay
Write-Host "[4/4] Running national baseline replay"
powershell -NoProfile -ExecutionPolicy Bypass -File "$(Join-Path $PSScriptRoot 'run_national.ps1')" $ScopeId $Date
if ($LASTEXITCODE -ne 0) { throw "run_national.ps1 failed (exit $LASTEXITCODE)" }

Write-Host "[ALL-INDIA] Complete. Artifacts at $ArtifactDir"

# Print quick KPI glance if available
if (Test-Path "$ArtifactDir/national_sim_kpis.json") {
  Write-Host "[ALL-INDIA] KPIs:"
  Get-Content "$ArtifactDir/national_sim_kpis.json"
}
