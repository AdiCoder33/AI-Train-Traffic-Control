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
RADAR_P="$ARTIFACT_DIR/conflict_radar.json"

[[ -f "$EDGES_P" && -f "$NODES_P" && -f "$BLOCK_P" && -f "$RADAR_P" ]] || { echo "Missing required inputs. Run risk first." >&2; exit 1; }

python - "$ARTIFACT_DIR" "$EDGES_P" "$NODES_P" "$BLOCK_P" "$RADAR_P" "$HORIZON" "$T0" <<'PY'
import sys, json
import pandas as pd
from src.opt.engine import propose, save

art, edges_p, nodes_p, block_p, radar_p, h, t0 = sys.argv[1:8]
edges = pd.read_parquet(edges_p)
nodes = pd.read_parquet(nodes_p)
block = pd.read_parquet(block_p)
risks = json.load(open(radar_p))
rec, alts, metrics, audit = propose(edges, nodes, block, risks, t0=(t0 or None), horizon_min=int(h))
save(rec, alts, metrics, audit, art)
print(json.dumps({**metrics, **{"runtime_sec": audit['runtime_sec']}}, indent=2))
PY

echo "[OPT] Plan written: rec_plan.json, alt_options.json, plan_metrics.json, audit_log.json"

