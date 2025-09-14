param(
  [Parameter(Position=0, Mandatory=$false)][string]$Scope = 'demo_section',
  [Parameter(Position=1, Mandatory=$false)][string]$Date = '2024-01-01',
  [Parameter(Position=2, Mandatory=$false)][int]$Trains = 8
)

$ErrorActionPreference = 'Stop'

Write-Host "[DEMO] Generating synthetic corridor: $Scope on $Date with $Trains trains"

python - << 'PY'
import sys
from src.sim.synthetic import CorridorSpec, build_and_save

scope, date, trains = sys.argv[1], sys.argv[2], int(sys.argv[3])
spec = CorridorSpec(scope=scope, date=date, stations=("STN-A","STN-B","STN-C"), trains=trains)
out = build_and_save(spec)
print(str(out))
PY
$LASTEXITCODE | Out-Null

Write-Host "[DEMO] Running risk analysis"
./scripts/run_risk.ps1 $Scope $Date 60

Write-Host "[DEMO] Running optimization (heuristic)"
./scripts/run_opt.ps1 $Scope $Date 60 '' 3 2

Write-Host "[DEMO] Injecting disruption: +8 min to T00001 at STN-B"
python - << 'PY'
import sys, pandas as pd
from pathlib import Path
from src.api.server import _art_dir
from src.model.section_graph import load_graph
from src.sim.national_replay import run as replay_run, save as replay_save
from src.sim.risk import analyze as risk_analyze, save as risk_save
from src.opt.engine import propose, save as opt_save

scope, date = sys.argv[1], sys.argv[2]
base = _art_dir(scope, date)
events_p = base / 'events_clean.parquet'
edges_p = base / 'section_edges.parquet'
nodes_p = base / 'section_nodes.parquet'
ev = pd.read_parquet(events_p)
ev['act_dep'] = pd.to_datetime(ev.get('act_dep'), utc=True, errors='coerce')
m = (ev['train_id'].astype(str) == 'T00001') & (ev['station_id'].astype(str) == 'STN-B')
sel = ev.loc[m].index
if len(sel):
    base_dep = ev.loc[sel, 'act_dep']
    base_dep = base_dep.where(base_dep.notna(), ev.loc[sel, 'sched_dep'])
    ev.loc[sel, 'act_dep'] = base_dep + pd.to_timedelta(8, unit='m')
    ev.to_parquet(events_p, index=False)
edges = pd.read_parquet(edges_p); nodes = pd.read_parquet(nodes_p)
graph = load_graph(nodes, edges)
sim = replay_run(ev, graph)
replay_save(sim, base)
risks, tl, prev, kpi = risk_analyze(edges, nodes, sim.block_occupancy, platform_occ_df=sim.platform_occupancy, waiting_df=sim.waiting_ledger, t0=None, horizon_min=60)
risk_save(risks, tl, prev, kpi, base)
rec, alts, metrics, audit = propose(edges, nodes, sim.block_occupancy, risks, max_hold_min=3)
opt_save(rec, alts, metrics, audit, base)
print('OK')
PY
$LASTEXITCODE | Out-Null

Write-Host "[DEMO] Done. Open portal and inspect demo_section on $Date"

