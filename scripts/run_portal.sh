#!/usr/bin/env bash
set -euo pipefail

HOST="${1:-127.0.0.1}"
PORT="${2:-8000}"

# Prefer venv python if present
if [[ -x ".venv/bin/python" ]]; then
  PY=".venv/bin/python"
else
  PY="python3"
fi

echo "[PORTAL] Starting API (FastAPI) and UI (Streamlit) using $PY"
"$PY" -m uvicorn src.api.server:app --host "$HOST" --port "$PORT" --reload &
STREAMLIT_SERVER_HEADLESS=true STREAMLIT_BROWSER_GATHER_USAGE_STATS=false STREAMLIT_SERVER_ADDRESS="$HOST" STREAMLIT_SERVER_PORT=8501 \
"$PY" -m streamlit run src/ui/app.py --server.headless true --server.address "$HOST" --server.port 8501 --browser.gatherUsageStats false
