#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 || $# -gt 6 ]]; then
  echo "Usage: $0 <scope_id> <date YYYY-MM-DD> [horizon_min] [t0_iso] [max_hold_min] [max_holds_per_train]" >&2
  exit 1
fi

SCOPE="$1"
DATE="$2"
HORIZON="${3:-60}"
T0="${4:-}"
MAX_HOLD="${5:-3}"
MAX_HPT="${6:-2}"

ARTIFACT_DIR="artifacts/$SCOPE/$DATE"
EDGES_P="$ARTIFACT_DIR/section_edges.parquet"
NODES_P="$ARTIFACT_DIR/section_nodes.parquet"
BLOCK_P="$ARTIFACT_DIR/national_block_occupancy.parquet"
[[ -f "$BLOCK_P" ]] || BLOCK_P="$ARTIFACT_DIR/block_occupancy.parquet"
RADAR_P="$ARTIFACT_DIR/conflict_radar.json"
PRIO_P="data/train_priority.csv"

[[ -f "$EDGES_P" && -f "$NODES_P" && -f "$BLOCK_P" && -f "$RADAR_P" ]] || { echo "Missing required inputs. Run risk first." >&2; exit 1; }

python - "$ARTIFACT_DIR" "$EDGES_P" "$NODES_P" "$BLOCK_P" "$RADAR_P" "$HORIZON" "$T0" "$MAX_HOLD" "$MAX_HPT" "$PRIO_P" <<'PY'
import sys, json
import pandas as pd
from src.opt.engine import propose, save

art, edges_p, nodes_p, block_p, radar_p, h, t0, max_hold, max_hpt, prio_p = sys.argv[1:11]
edges = pd.read_parquet(edges_p)
nodes = pd.read_parquet(nodes_p)
block = pd.read_parquet(block_p)
risks = json.load(open(radar_p))
priorities = None
import os
if prio_p and os.path.exists(prio_p):
    try:
        dfp = pd.read_csv(prio_p, dtype=str)
        if 'train_id' in dfp.columns and 'priority' in dfp.columns:
            priorities = {str(k): int(v) for k, v in zip(dfp['train_id'], dfp['priority'])}
    except Exception:
        priorities = None
rec, alts, metrics, audit = propose(edges, nodes, block, risks, t0=(t0 or None), horizon_min=int(h), max_hold_min=int(max_hold), max_holds_per_train=int(max_hpt), priorities=priorities)
save(rec, alts, metrics, audit, art)
print(json.dumps({**metrics, **{"runtime_sec": audit['runtime_sec'], "max_hold_min": int(max_hold), "max_holds_per_train": int(max_hpt)}}, indent=2))
PY

echo "[OPT] Plan written: rec_plan.json, alt_options.json, plan_metrics.json, audit_log.json"
