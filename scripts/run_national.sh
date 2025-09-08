#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "Usage: $0 <scope_id> <service_date YYYY-MM-DD>" >&2
  exit 1
fi

SCOPE="$1"
DATE="$2"
ARTIFACT_DIR="artifacts/$SCOPE/$DATE"

if [[ ! -f "$ARTIFACT_DIR/events_clean.parquet" ]]; then echo "Missing $ARTIFACT_DIR/events_clean.parquet" >&2; exit 1; fi
if [[ ! -f "$ARTIFACT_DIR/section_edges.parquet" ]]; then echo "Missing $ARTIFACT_DIR/section_edges.parquet" >&2; exit 1; fi
if [[ ! -f "$ARTIFACT_DIR/section_nodes.parquet" ]]; then echo "Missing $ARTIFACT_DIR/section_nodes.parquet" >&2; exit 1; fi

python - "$ARTIFACT_DIR" <<'PY'
import sys, json
from pathlib import Path
import pandas as pd
from src.model.section_graph import load_graph
from src.sim.national_replay import run, save

artifact_dir = Path(sys.argv[1])
events = pd.read_parquet(artifact_dir / 'events_clean.parquet')
edges = pd.read_parquet(artifact_dir / 'section_edges.parquet')
nodes = pd.read_parquet(artifact_dir / 'section_nodes.parquet')
graph = load_graph(nodes, edges)
res = run(events, graph)
save(res, artifact_dir)
print(json.dumps(res.sim_kpis, indent=2))
PY

echo "National replay artifacts written to $ARTIFACT_DIR"

