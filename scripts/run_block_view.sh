#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "Usage: $0 <corridor_id> <service_date YYYY-MM-DD>" >&2
  exit 1
fi

CORRIDOR="$1"
DATE="$2"
ARTIFACT_DIR="artifacts/$CORRIDOR/$DATE"

if [[ ! -f "$ARTIFACT_DIR/events_clean.parquet" ]]; then
  echo "Missing $ARTIFACT_DIR/events_clean.parquet. Run phase1 first." >&2
  exit 1
fi
if [[ ! -f "$ARTIFACT_DIR/section_edges.parquet" ]]; then
  echo "Missing $ARTIFACT_DIR/section_edges.parquet. Run phase1 first." >&2
  exit 1
fi

python - "$ARTIFACT_DIR" "$CORRIDOR" "$DATE" <<'PY'
import sys
import pandas as pd
from src.data.block_view import build, save

artifact_dir, corridor, date = sys.argv[1], sys.argv[2], sys.argv[3]
df_slice = pd.read_parquet(f"{artifact_dir}/events_clean.parquet")
edges_df = pd.read_parquet(f"{artifact_dir}/section_edges.parquet")
res = build(df_slice, edges_df)
save(res, corridor, date)
PY

echo "Block-level artifacts written to $ARTIFACT_DIR"

