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

echo "[PORTAL] Starting API (FastAPI) and Web UI (Vite) using $PY"
"$PY" -m uvicorn src.api.server:app --host "$HOST" --port "$PORT" --reload &
cd web
echo "[PORTAL] Installing web dependencies (npm install)"
npm install
VITE_API_BASE="http://$HOST:$PORT" npm run dev
