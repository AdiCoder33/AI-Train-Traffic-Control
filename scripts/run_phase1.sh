#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "Usage: $0 <corridor_id> <service_date YYYY-MM-DD>" >&2
  exit 1
fi

CORRIDOR="$1"
DATE="$2"
ARTIFACT_DIR="artifacts/$CORRIDOR/$DATE"
STATIONS_FILE="data/${CORRIDOR}_stations.txt"

echo "Running phase 1 pipeline for corridor '$CORRIDOR' on '$DATE'"

mkdir -p "$ARTIFACT_DIR"

if [[ ! -f "$STATIONS_FILE" ]]; then
  echo "Missing stations file: $STATIONS_FILE" >&2
  exit 1
fi

echo "Using stations from $STATIONS_FILE"

echo "[1/6] Loading raw data"
python - "$ARTIFACT_DIR" <<'PYTHON'
import sys
from src.data.loader import load_raw
import pandas as pd
out_dir = sys.argv[1]
df = load_raw()
df.to_parquet(f"{out_dir}/raw.parquet", index=False)
PYTHON

echo "[2/6] Normalizing dataset"
python - "$ARTIFACT_DIR" <<'PYTHON'
import sys
import pandas as pd
from src.data.normalize import to_train_events
artifact_dir = sys.argv[1]
df_raw = pd.read_parquet(f"{artifact_dir}/raw.parquet")
df_norm = to_train_events(df_raw)
df_norm.to_parquet(f"{artifact_dir}/events.parquet", index=False)
PYTHON

echo "[3/6] Slicing corridor"
python - "$ARTIFACT_DIR" "$DATE" "$STATIONS_FILE" <<'PYTHON'
import sys, json
import pandas as pd
from src.data.corridor import slice as corridor_slice
artifact_dir, date, stations_path = sys.argv[1], sys.argv[2], sys.argv[3]
stations = [s.strip() for s in open(stations_path) if s.strip()]
df_norm = pd.read_parquet(f"{artifact_dir}/events.parquet")
df_slice, stations_dict = corridor_slice(df_norm, stations, date)
df_slice.to_parquet(f"{artifact_dir}/events_clean.parquet", index=False)
with open(f"{artifact_dir}/stations.json", "w") as f:
    json.dump(stations_dict, f)
PYTHON

echo "[4/6] Building graph"
python - "$ARTIFACT_DIR" "$CORRIDOR" "$DATE" <<'PYTHON'
import sys, json
import pandas as pd
from src.data.graph import build, save as save_graph
artifact_dir, corridor, date = sys.argv[1], sys.argv[2], sys.argv[3]
df_slice = pd.read_parquet(f"{artifact_dir}/events_clean.parquet")
with open(f"{artifact_dir}/stations.json") as f:
    stations_dict = json.load(f)
edges_df, nodes_df = build(df_slice, stations_dict)
save_graph(edges_df, nodes_df, corridor, date)
PYTHON

echo "[5/6] Running baseline replay"
python - "$ARTIFACT_DIR" "$CORRIDOR" "$DATE" <<'PYTHON'
import sys
import pandas as pd
from src.data.baseline import save as save_baseline
artifact_dir, corridor, date = sys.argv[1], sys.argv[2], sys.argv[3]
df_slice = pd.read_parquet(f"{artifact_dir}/events_clean.parquet")
edges_df = pd.read_parquet(f"{artifact_dir}/section_edges.parquet")
save_baseline(df_slice, edges_df, corridor, date)
PYTHON

echo "[6/6] Performing data quality checks"
python - "$ARTIFACT_DIR" "$CORRIDOR" "$DATE" <<'PYTHON'
import sys, json
import pandas as pd
from src.data.dq_checks import run_all
artifact_dir, corridor, date = sys.argv[1], sys.argv[2], sys.argv[3]
df_slice = pd.read_parquet(f"{artifact_dir}/events_clean.parquet")
edges_df = pd.read_parquet(f"{artifact_dir}/section_edges.parquet")
with open(f"{artifact_dir}/stations.json") as f:
    stations_dict = json.load(f)
report_path = f"{artifact_dir}/dq_report.md"
run_all(df_slice, edges_df, stations_dict, report_path=report_path)
PYTHON

echo "Phase 1 pipeline complete. Artifacts stored in $ARTIFACT_DIR"
