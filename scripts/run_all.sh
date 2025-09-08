#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 || $# -gt 3 ]]; then
  echo "Usage: $0 <corridor_id> <service_date YYYY-MM-DD> [csv_glob_pattern]" >&2
  exit 1
fi

CORRIDOR="$1"
DATE="$2"
CSV_PATTERN="${3:-Train_details*.csv}"

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ARTIFACT_DIR="artifacts/$CORRIDOR/$DATE"

echo "[ALL] Running Phase 1 + Block View for '$CORRIDOR' on '$DATE'"

echo "[ALL] Step 1/2: Phase 1 pipeline"
"$DIR/run_phase1.sh" "$CORRIDOR" "$DATE" "$CSV_PATTERN"

echo "[ALL] Step 2/2: Block-level view"
"$DIR/run_block_view.sh" "$CORRIDOR" "$DATE"

echo "[ALL] Complete. Artifacts stored in $ARTIFACT_DIR"

