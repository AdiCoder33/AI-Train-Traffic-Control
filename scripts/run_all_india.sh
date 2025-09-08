#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 || $# -gt 3 ]]; then
  echo "Usage: $0 <scope_id> <service_date YYYY-MM-DD> [csv_glob_pattern]" >&2
  exit 1
fi

SCOPE="$1"
DATE="$2"
CSV_PATTERN="${3:-Train_details*.csv}"
ARTIFACT_DIR="artifacts/$SCOPE/$DATE"

mkdir -p "$ARTIFACT_DIR"

echo "[ALL-INDIA] Preparing artifacts for '$SCOPE' on '$DATE' (pattern=$CSV_PATTERN)"

echo "[1/4] Loading raw CSVs"
python - "$CSV_PATTERN" "$ARTIFACT_DIR/raw.parquet" <<'PY'
import sys
from src.data.loader import load_raw
import pandas as pd
pattern, out = sys.argv[1], sys.argv[2]
df = load_raw(pattern=pattern)
df.to_parquet(out, index=False)
PY

echo "[2/4] Normalizing to events_clean"
python - "$ARTIFACT_DIR/raw.parquet" "$ARTIFACT_DIR/events_clean.parquet" "$DATE" <<'PY'
import sys, pandas as pd
from src.data.normalize import to_train_events
raw_p, out_p, date = sys.argv[1], sys.argv[2], sys.argv[3]
df_raw = pd.read_parquet(raw_p)
df_norm = to_train_events(df_raw, default_service_date=date)
df_norm.to_parquet(out_p, index=False)
PY

echo "[3/4] Building national nodes/edges"
python - "$ARTIFACT_DIR/events_clean.parquet" "$ARTIFACT_DIR/section_edges.parquet" "$ARTIFACT_DIR/section_nodes.parquet" <<'PY'
import sys, pandas as pd
from src.data.graph import build
events_p, edges_p, nodes_p = sys.argv[1], sys.argv[2], sys.argv[3]
df = pd.read_parquet(events_p)
stations = sorted([s for s in df['station_id'].dropna().unique().tolist()])
stations_dict = {sid:i for i,sid in enumerate(stations)}
edges_df, nodes_df = build(df, stations_dict)
edges_df.to_parquet(edges_p, index=False)
nodes_df.to_parquet(nodes_p, index=False)
PY

echo "[4/4] Running national baseline replay"
"$(dirname "$0")/run_national.sh" "$SCOPE" "$DATE"

echo "[ALL-INDIA] Complete. Artifacts at $ARTIFACT_DIR"

