#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "Usage: $0 <scope_id> <date YYYY-MM-DD>" >&2
  exit 1
fi

python -m src.reports.aggregate "$1" "$2"
echo "[REPORT] Wrote kpi_reports.json"

