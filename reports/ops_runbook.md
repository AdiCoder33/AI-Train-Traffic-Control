# Ops Runbook (Pilot)

## Services
- API: `python -m uvicorn src.api.server:app --host 127.0.0.1 --port 8000`
- Web UI: `cd web && npm install && npm run dev`
- Combined (Windows): `./scripts/run_portal_web.ps1 <ApiHost> <ApiPort> <WebPort>`

## Health
- Liveness: `GET /healthz`
- Readiness: `GET /readiness`
- Metrics: `GET /metrics` (Prometheus text)

## Common Alerts and Actions
- data_stale: check adapters (file_drop path, external polling); inspect `events_live.jsonl` growth.
- solver_sla_breach: inspect `plan_metrics.json` and API logs; fallback to heuristic is automatic.
- risk_spike: `GET /radar` and validate feed; consider raising headway policy temporarily.
- audit_gap: `GET /audit/completeness` and ensure controllers are submitting decisions.

## Rollback
- Policy: revert `policy_state.json` or `PUT /policy` with prior version.
- Feature flags: edit `config/feature_flags.yaml` and reload API.

## Notes
- Sandbox is default (no live emissions). To enable live, set `EngineConfig.sandbox = False` in code or derive from env var.
