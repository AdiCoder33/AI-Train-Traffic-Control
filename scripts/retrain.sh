#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "Usage: $0 <scope_id> <date YYYY-MM-DD>" >&2
  exit 1
fi

SCOPE="$1"
DATE="$2"

python -m src.learn.train_delay "$SCOPE" "$DATE"
python -m src.learn.update_risk "$SCOPE" "$DATE"
python -m src.learn.collect_rl "$SCOPE" "$DATE"

echo "[RETRAIN] Reports written under artifacts/$SCOPE/$DATE"

