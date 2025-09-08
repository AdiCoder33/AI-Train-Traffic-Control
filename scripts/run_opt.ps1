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

$BlockPath = Join-Path $ArtifactDir 'national_block_occupancy.parquet'
if (-not (Test-Path $BlockPath)) { $BlockPath = Join-Path $ArtifactDir 'block_occupancy.parquet' }
if (-not (Test-Path $BlockPath)) { throw "Missing block occupancy parquet in $ArtifactDir" }

$RadarPath = Join-Path $ArtifactDir 'conflict_radar.json'
if (-not (Test-Path $RadarPath)) { throw "Missing conflict_radar.json. Run risk first." }

Write-Host "[OPT] Proposing actions for '$ScopeId' on '$Date' (H=$HorizonMin min)"

python -c "import sys,json,os; import pandas as pd; from src.opt.engine import propose, save; argv=sys.argv[1:];
art=argv[0]; edges_p=argv[1]; nodes_p=argv[2]; block_p=argv[3]; radar_p=argv[4]; h=argv[5]; t0=argv[6] if len(argv)>6 else '';
edges=pd.read_parquet(edges_p); nodes=pd.read_parquet(nodes_p); block=pd.read_parquet(block_p); risks=json.load(open(radar_p));
rec, alts, metrics, audit = propose(edges, nodes, block, risks, t0=(t0 or None), horizon_min=int(h));
save(rec, alts, metrics, audit, art);
out=dict(metrics); out['runtime_sec']=audit.get('runtime_sec', 0.0); print(json.dumps(out, indent=2))" $ArtifactDir "$ArtifactDir/section_edges.parquet" "$ArtifactDir/section_nodes.parquet" $BlockPath $RadarPath $HorizonMin $T0

if ($LASTEXITCODE -ne 0) { throw "Optimization failed (exit $LASTEXITCODE)" }

Write-Host "[OPT] Plan written: rec_plan.json, alt_options.json, plan_metrics.json, audit_log.json"
