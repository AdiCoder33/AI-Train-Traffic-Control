param(
  [Parameter(Position=0, Mandatory=$true)][string]$Corridor,
  [Parameter(Position=1, Mandatory=$true)][string]$Date
)

$ErrorActionPreference = 'Stop'

$ArtifactDir = "artifacts/$Corridor/$Date"
$StationsFile = "data/${Corridor}_stations.txt"

Write-Host "Running phase 1 pipeline for corridor '$Corridor' on '$Date'"

if (-not (Test-Path $StationsFile)) {
  Write-Error "Missing stations file: $StationsFile"
  exit 1
}

New-Item -ItemType Directory -Force -Path $ArtifactDir | Out-Null

# Helper to fail fast after external commands
function Assert-LastExit {
  param([string]$Step)
  if ($LASTEXITCODE -ne 0) { throw "Step failed: $Step (exit $LASTEXITCODE)" }
}

# [1/6] Load raw data
Write-Host "[1/6] Loading raw data"
python -c "from src.data.loader import load_raw; import pandas as pd; df=load_raw(); df.to_parquet(r'$ArtifactDir/raw.parquet', index=False)"
Assert-LastExit "Load raw data"

# [2/6] Normalize dataset
Write-Host "[2/6] Normalizing dataset"
python -c "import sys, pandas as pd; from src.data.normalize import to_train_events; artifact_dir=sys.argv[1]; date=sys.argv[2]; df_raw=pd.read_parquet(fr'{artifact_dir}/raw.parquet'); df_norm=to_train_events(df_raw, default_service_date=date); df_norm.to_parquet(fr'{artifact_dir}/events.parquet', index=False)" $ArtifactDir $Date
Assert-LastExit "Normalize dataset"

# [3/6] Slice corridor
Write-Host "[3/6] Slicing corridor"
python -c "import sys, json; from pathlib import Path; import pandas as pd; from src.data.corridor import slice as corridor_slice; import src.data.normalize as nm; artifact_dir,date,stations_path=sys.argv[1],sys.argv[2],sys.argv[3]; raw_stations=[s.strip() for s in open(stations_path, encoding='utf-8') if s.strip()]; looks_like_id=lambda s: s.upper().startswith('S') and s[1:].isdigit(); stations=list(raw_stations); 
sp=Path(nm.__file__).with_name('station_map.csv');
if not all(looks_like_id(s) for s in raw_stations) and sp.exists():
    sm=pd.read_csv(sp); name_to_id=dict(zip(sm['name'], sm['station_id'])); stations=[name_to_id.get(s, s) for s in raw_stations]
df_norm=pd.read_parquet(fr'{artifact_dir}/events.parquet'); df_slice, stations_dict=corridor_slice(df_norm, stations, date); df_slice.to_parquet(fr'{artifact_dir}/events_clean.parquet', index=False); json.dump(stations_dict, open(fr'{artifact_dir}/stations.json','w'))" $ArtifactDir $Date $StationsFile
Assert-LastExit "Slice corridor"

# [4/6] Build graph
Write-Host "[4/6] Building graph"
python -c "import sys, json, pandas as pd; from src.data.graph import build, save as save_graph; artifact_dir,corridor,date=sys.argv[1],sys.argv[2],sys.argv[3]; df_slice=pd.read_parquet(fr'{artifact_dir}/events_clean.parquet'); stations_dict=json.load(open(fr'{artifact_dir}/stations.json')); edges_df,nodes_df=build(df_slice, stations_dict); save_graph(edges_df, nodes_df, corridor, date)" $ArtifactDir $Corridor $Date
Assert-LastExit "Build graph"

# [5/6] Baseline replay
Write-Host "[5/6] Running baseline replay"
python -c "import sys, pandas as pd; from src.data.baseline import save as save_baseline; artifact_dir,corridor,date=sys.argv[1],sys.argv[2],sys.argv[3]; df_slice=pd.read_parquet(fr'{artifact_dir}/events_clean.parquet'); edges_df=pd.read_parquet(fr'{artifact_dir}/section_edges.parquet'); save_baseline(df_slice, edges_df, corridor, date)" $ArtifactDir $Corridor $Date
Assert-LastExit "Baseline replay"

# [6/6] Data quality checks
Write-Host "[6/6] Performing data quality checks"
python -c "import sys, json, pandas as pd; from src.data.dq_checks import run_all; artifact_dir=sys.argv[1]; df_slice=pd.read_parquet(fr'{artifact_dir}/events_clean.parquet'); edges_df=pd.read_parquet(fr'{artifact_dir}/section_edges.parquet'); stations_dict=json.load(open(fr'{artifact_dir}/stations.json')); report_path=fr'{artifact_dir}/dq_report.md'; run_all(df_slice, edges_df, stations_dict, report_path=report_path)" $ArtifactDir
Assert-LastExit "Data quality checks"

Write-Host "Phase 1 pipeline complete. Artifacts stored in $ArtifactDir"
