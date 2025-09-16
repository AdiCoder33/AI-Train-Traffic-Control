param(
  [Parameter(Position=0, Mandatory=$true)][string]$ScopeId,
  [Parameter(Position=1, Mandatory=$true)][string]$Date,
  [Parameter(Position=2, Mandatory=$false)][int]$HorizonMin = 60,
  [Parameter(Position=3, Mandatory=$false)][string]$T0 = '',
  [Parameter(Position=4, Mandatory=$false)][int]$MaxHoldMin = 3,
  [Parameter(Position=5, Mandatory=$false)][int]$MaxHoldsPerTrain = 2
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

# Optional priorities CSV
$PriorityPath = "data/train_priority.csv"

Write-Host "[OPT] Proposing actions for '$ScopeId' on '$Date' (H=$HorizonMin min)"

python -c "import sys,json,os; import pandas as pd; from src.opt.engine import propose, save; argv=sys.argv[1:];
art,edges_p,nodes_p,block_p,radar_p = argv[:5];
rest = argv[5:]
if len(rest) == 4:
    h, max_hold, max_hpt, prio_p = rest
    t0 = ''
elif len(rest) >= 5:
    h, t0, max_hold, max_hpt, prio_p = rest[:5]
else:
    h = rest[0] if len(rest) > 0 else '60'
    t0 = rest[1] if len(rest) > 1 else ''
    max_hold = rest[2] if len(rest) > 2 else '3'
    max_hpt = rest[3] if len(rest) > 3 else '2'
    prio_p = rest[4] if len(rest) > 4 else ''
edges=pd.read_parquet(edges_p); nodes=pd.read_parquet(nodes_p); block=pd.read_parquet(block_p); risks=json.load(open(radar_p));
priorities=None
if prio_p and os.path.exists(prio_p):
    try:
        dfp=pd.read_csv(prio_p, dtype=str)
        if 'train_id' in dfp.columns and 'priority' in dfp.columns:
            priorities={str(k):int(v) for k,v in zip(dfp['train_id'], dfp['priority'])}
    except Exception:
        priorities=None
rec, alts, metrics, audit = propose(edges, nodes, block, risks, t0=(t0 or None) if t0 else None, horizon_min=int(h), max_hold_min=int(max_hold), max_holds_per_train=int(max_hpt), priorities=priorities);
save(rec, alts, metrics, audit, art);
out=dict(metrics); out['runtime_sec']=audit.get('runtime_sec', 0.0); out['max_hold_min']=int(max_hold); out['max_holds_per_train']=int(max_hpt); print(json.dumps(out, indent=2))" $ArtifactDir "$ArtifactDir/section_edges.parquet" "$ArtifactDir/section_nodes.parquet" $BlockPath $RadarPath $HorizonMin $T0 $MaxHoldMin $MaxHoldsPerTrain $PriorityPath

if ($LASTEXITCODE -ne 0) { throw "Optimization failed (exit $LASTEXITCODE)" }

Write-Host "[OPT] Plan written: rec_plan.json, alt_options.json, plan_metrics.json, audit_log.json"
