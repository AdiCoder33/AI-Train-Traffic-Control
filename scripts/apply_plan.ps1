param(
  [Parameter(Position=0, Mandatory=$true)][string]$ScopeId,
  [Parameter(Position=1, Mandatory=$true)][string]$Date,
  [Parameter(Position=2, Mandatory=$false)][int]$HorizonMin = 60,
  [Parameter(Position=3, Mandatory=$false)][string]$T0 = ''
)

$ErrorActionPreference = 'Stop'
$ArtifactDir = "artifacts/$ScopeId/$Date"

if (-not (Test-Path "$ArtifactDir/events_clean.parquet")) { throw "Missing events_clean.parquet" }
if (-not (Test-Path "$ArtifactDir/section_edges.parquet")) { throw "Missing section_edges.parquet" }
if (-not (Test-Path "$ArtifactDir/section_nodes.parquet")) { throw "Missing section_nodes.parquet" }
if (-not (Test-Path "$ArtifactDir/rec_plan.json")) { throw "Missing rec_plan.json. Run optimizer first." }

Write-Host "[APPLY] Applying plan and validating '$ScopeId' on '$Date' (H=$HorizonMin min)"

python -c "import sys,json,os; import pandas as pd; from src.sim.apply_plan import apply_and_validate, save as save_apply; from src.sim.risk import analyze as risk_analyze; argv=sys.argv[1:]; art=argv[0]; h=argv[1]; t0=argv[2] if len(argv)>2 else ''; events=pd.read_parquet(f'{art}/events_clean.parquet'); edges=pd.read_parquet(f'{art}/section_edges.parquet'); nodes=pd.read_parquet(f'{art}/section_nodes.parquet'); rec=json.load(open(f'{art}/rec_plan.json'));
# baseline risk from baseline occupancy if available
baseline_block = f'{art}/national_block_occupancy.parquet'
b_risks = None
if os.path.exists(baseline_block):
    bo = pd.read_parquet(baseline_block)
    risks,_,_,_ = risk_analyze(edges, nodes, bo, t0=(t0 or None), horizon_min=int(h))
    b_risks = len(risks)
res = apply_and_validate(events, edges, nodes, rec, t0=(t0 or None), horizon_min=int(h))
if b_risks is not None:
    res['baseline_risks'] = int(b_risks)
    res['risk_reduction'] = int(b_risks) - int(res.get('applied_risks', 0))
save_apply(art, res) 
print(json.dumps(res, indent=2))" $ArtifactDir $HorizonMin $T0

if ($LASTEXITCODE -ne 0) { throw "Apply-and-validate failed (exit $LASTEXITCODE)" }

Write-Host "[APPLY] Wrote plan_apply_report.json (and applied_block_occupancy.parquet if enabled)"
