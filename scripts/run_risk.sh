#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 || $# -gt 4 ]]; then
  echo "Usage: $0 <scope_id> <date YYYY-MM-DD> [horizon_min] [t0_iso]" >&2
  exit 1
fi

SCOPE="$1"
DATE="$2"
HORIZON="${3:-60}"
T0="${4:-}"

ARTIFACT_DIR="artifacts/$SCOPE/$DATE"
EDGES_P="$ARTIFACT_DIR/section_edges.parquet"
NODES_P="$ARTIFACT_DIR/section_nodes.parquet"
BLOCK_P="$ARTIFACT_DIR/national_block_occupancy.parquet"
[[ -f "$BLOCK_P" ]] || BLOCK_P="$ARTIFACT_DIR/block_occupancy.parquet"
PLAT_P="$ARTIFACT_DIR/national_platform_occupancy.parquet"
WAIT_P="$ARTIFACT_DIR/national_waiting_ledger.parquet"

[[ -f "$EDGES_P" && -f "$NODES_P" && -f "$BLOCK_P" ]] || { echo "Missing required inputs in $ARTIFACT_DIR" >&2; exit 1; }

python - "$ARTIFACT_DIR" "$EDGES_P" "$NODES_P" "$BLOCK_P" "$PLAT_P" "$WAIT_P" "$HORIZON" "$T0" <<'PY'
import sys, os, json
import pandas as pd
from src.sim.risk import analyze, save

art, edges_p, nodes_p, block_p, plat_p, wait_p, h, t0 = sys.argv[1:9]
edges = pd.read_parquet(edges_p)
nodes = pd.read_parquet(nodes_p)
block = pd.read_parquet(block_p)
plat = pd.read_parquet(plat_p) if plat_p and os.path.exists(plat_p) else None
wait = pd.read_parquet(wait_p) if wait_p and os.path.exists(wait_p) else None
risks, timeline, previews, kpis = analyze(edges, nodes, block, platform_occ_df=plat, waiting_df=wait, t0=(t0 or None), horizon_min=int(h))
save(risks, timeline, previews, kpis, art)
print(json.dumps(kpis, indent=2))
PY

echo "[RISK] Artifacts written to $ARTIFACT_DIR (conflict_radar.json, risk_timeline.parquet, mitigation_preview.json, risk_kpis.json)"

