param(
  [Parameter(Position=0, Mandatory=$true)][string]$ScopeId,
  [Parameter(Position=1, Mandatory=$true)][string]$Date,
  [Parameter(Position=2, Mandatory=$false)][int]$HorizonMin = 60,
  [Parameter(Position=3, Mandatory=$false)][string]$T0 = ''
)

$ErrorActionPreference = 'Stop'
$ArtifactDir = "artifacts/$ScopeId/$Date"

if (-not (Test-Path "$ArtifactDir/section_edges.parquet")) { throw "Missing section_edges.parquet" }
if (-not (Test-Path "$ArtifactDir/section_nodes.parquet")) { throw "Missing section_nodes.parquet" }

# Determine occupancy sources (national or corridor)
$BlockPath = Join-Path $ArtifactDir 'national_block_occupancy.parquet'
if (-not (Test-Path $BlockPath)) { $BlockPath = Join-Path $ArtifactDir 'block_occupancy.parquet' }
if (-not (Test-Path $BlockPath)) { throw "Missing block occupancy parquet in $ArtifactDir" }

$PlatPath = Join-Path $ArtifactDir 'national_platform_occupancy.parquet'
$WaitPath = Join-Path $ArtifactDir 'national_waiting_ledger.parquet'

Write-Host "[RISK] Running risk analysis for '$ScopeId' on '$Date' (H=$HorizonMin min)"

python -c "import sys,json,os; import pandas as pd; from src.sim.risk import analyze, save; argv=sys.argv[1:];
art,edges_p,nodes_p,block_p = argv[:4]; plat_p = argv[4] if len(argv)>4 else ''; wait_p = argv[5] if len(argv)>5 else ''; h = argv[6] if len(argv)>6 else '60'; t0 = argv[7] if len(argv)>7 else '';
edges=pd.read_parquet(edges_p); nodes=pd.read_parquet(nodes_p); block=pd.read_parquet(block_p);
plat=pd.read_parquet(plat_p) if plat_p and os.path.exists(plat_p) else None; wait=pd.read_parquet(wait_p) if wait_p and os.path.exists(wait_p) else None;
horizon=int(h); t0_val = t0 or None; risks, timeline, previews, kpis = analyze(edges, nodes, block, platform_occ_df=plat, waiting_df=wait, t0=t0_val, horizon_min=horizon); save(risks, timeline, previews, kpis, art); print(json.dumps(kpis, indent=2))" $ArtifactDir "$ArtifactDir/section_edges.parquet" "$ArtifactDir/section_nodes.parquet" $BlockPath $(if (Test-Path $PlatPath) {"$PlatPath"} else {""}) $(if (Test-Path $WaitPath) {"$WaitPath"} else {""}) $HorizonMin $T0

if ($LASTEXITCODE -ne 0) { throw "Risk analysis failed (exit $LASTEXITCODE)" }

Write-Host "[RISK] Artifacts written to $ArtifactDir (conflict_radar.json, risk_timeline.parquet, mitigation_preview.json, risk_kpis.json)"
