 **“Maximizing Section Throughput Using AI-Powered Precise Train Traffic Control”**

## Scope
- Pure software (no hardware), offline or file-based inputs.
- Dataset: “A high-speed railway network dataset from train operation records and weather data” (Figshare, CC0).
- Goal: given a section’s trains + constraints, recommend real-time precedence/holding/crossing decisions that minimize total delay and maximize throughput; support what-if simulation.

## Tech stack
- Python 3.11, Poetry or pip.
- Core: pandas, numpy, pydantic, orjson, networkx, pulp **or** OR-Tools, scikit-learn (optional), fastapi, uvicorn, streamlit (UI), plotly.
- Tests: pytest.
- Style: type hints, black, ruff.

## Deliverables
1) Data prep: loaders that unify timetable, actuals, station graph, delays, weather → normalized parquet.
2) Section Digital Twin: graph model (stations as nodes, track sections/blocks as edges; capacities, headways, min dwell, gradients if available).
3) Optimizer:
   - Rolling-horizon MILP (or OR-Tools CP-SAT) that decides:
     * per-train departure/arrival times at stations in section
     * holds and overtakings
     * platform and block occupancy respecting headway/clearance
     * priorities (express > passenger > freight) via weights/penalties
   - Objective: minimize ∑(arrival_delay) + λ*max_delay + μ*meets_conflicts + ν*platform_conflicts; maximize served trains within horizon.
   - Constraints: block occupancy (no overlap), headway, single-platform per train, dwell ≥ min, speed/capacity implied by edge time, precedence decisions binary.
   - Fast heuristic fallback: greedy dispatch with look-ahead & priority queue when MILP times out.
4) Simulator:
   - Replays historical day; injects disruptions (late trains, weather-induced slowdowns).
   - Calls optimizer each 5 minutes (rolling horizon).
   - Produces action plan (hold X at Y for Z min; route via platform P; let A overtake B).
5) UI (Streamlit):
   - Upload/select subset (date/region/section).
   - “Run baseline” vs “Run optimized”.
   - Gantt of block/platform occupancy; delay waterfall; KPIs: throughput, avg delay, 90p delay, platform utilization, conflicts avoided.
   - What-if panel (late train slider, blocked platform, weather slowdown factor).
   - Explainability: per decision, show binding constraints and marginal penalties.
6) API (FastAPI): /optimize, /simulate, /kpis for programmatic tests.
7) Tests + fixtures; small demo dataset slice.

## File tree (generate all files)
.
├─ pyproject.toml (or requirements.txt)
├─ src/
│  ├─ data/
│  │  ├─ loader.py
│  │  ├─ schemas.py
│  │  └─ preprocess.py
│  ├─ model/
│  │  ├─ section_graph.py
│  │  ├─ constraints.py
│  │  ├─ optimizer_milp.py
│  │  └─ heuristic.py
│  ├─ sim/
│  │  ├─ rolling_horizon.py
│  │  └─ scenarios.py
│  ├─ api/
│  │  └─ server.py
│  └─ ui/
│     └─ app.py
├─ tests/
│  ├─ test_loader.py
│  ├─ test_constraints.py
│  └─ test_optimizer.py
└─ README.md

## Data contracts (pydantic)
- TrainEvent { train_id:str, station_id:str, sched_arr:ts, sched_dep:ts, act_arr:ts|None, act_dep:ts|None, day:int, priority:int }
- Station { station_id:str, name:str }
- Edge { u:str, v:str, min_run_time:float, headway:float, capacity:int=1, block_id:str, platform_cap:int }
- WeatherTick { ts:ts, station_id:str, temp:float, wind:float, precip:float, holiday:int }
Normalize into parquet partitions by date/section.

## Loading & preprocessing (loader.py / preprocess.py)
- Implement `load_figshare(path)` reading CSVs; map to TrainEvent/Station/Edge/WeatherTick.
- Build section subset: choose a contiguous corridor of ~8–20 stations with dense traffic.
- Derive delays: arr_delay = (act_arr - sched_arr).minutes, dep_delay likewise.
- Compute baseline run-times per edge from schedule medians.
- Persist to `./artifacts/{section}/{date}/*.parquet`.

## Section graph (section_graph.py)
- Build networkx DiGraph:
  - nodes=stations; edges=(station_i → station_j) with attributes {run_time, headway, block_id, capacity}.
  - platform availability per station; optional parallel tracks via multi-edges or capacity>1.
- Helper: `compute_feasible_window(train_id, node)` returns earliest/latest times with dwell.

## Optimization (optimizer_milp.py)
- Function: `optimize_window(events: list[TrainEvent], graph, horizon_start, horizon_end, params) -> list[Action]`
- Variables:
  - t_arr[t,s], t_dep[t,s] (continuous, minutes from horizon_start)
  - y_over[t1,t2,s] (binary: t1 precedes t2 at s)
  - z_block[t,e] (binary: train t uses edge e within horizon)
- Core constraints:
  - Dwell: t_dep ≥ t_arr + dwell_min(t,s)
  - Edge timing: t_arr[next] ≥ t_dep[this] + run_time(e) + slowdown_factor(weather)
  - Headway: for any pair (t1,t2) using same block/platform, enforce ≥ headway via y_over binaries (Big-M or indicator constraints)
  - Platform capacity: at most one train per platform per time; or cumulative with capacity
  - Fix scheduleds: soft penalties for deviation from sched where needed
- Objective:
  minimize Σ delay(t) + λ Σ conflicts + μ Σ holds + ζ Σ overtakes_penalty − κ * trains_served
- Time limits & gap tolerance; if solver times out → call `heuristic.dispatch()`.

## Heuristic (heuristic.py)
- Priority queue by (priority, lateness, slack). Greedy assign next feasible departure given headways; allow limited overtakes if it reduces global delay.

## Rolling horizon (rolling_horizon.py)
- Loop every Δ=5 min:
  - Observe state (actuals so far, disruptions)
  - Optimize next H=45–60 min window
  - Emit actionable plan: [{train_id, action: "HOLD", at_station, minutes, reason}...]

## UI (ui/app.py – Streamlit)
- Sidebar: select section/date; sliders: disruption (late N min for Train X), block outage, weather slowdown factor.
- Buttons: Run Baseline / Run Optimized.
- Charts:
  - Gantt: per train over sections; color code delays.
  - Block/platform occupancy timelines.
  - KPIs table: throughput, avg/90p delay, utilization, total holds, overtakes.
- Panel: Explain decisions (show top 3 binding constraints for each action).

## API (api/server.py – FastAPI)
- POST /optimize {section, horizon_start, horizon_end, params, disruptions[]} → ActionPlan
- POST /simulate {date, section, scenario} → KPIs + timelines

## README.md
- Quickstart:
  - `python -m venv .venv && source .venv/bin/activate`
  - `pip install -r requirements.txt` (or `poetry install`)
  - Place raw figshare CSVs under `./data/raw/`
  - `python -m src.data.preprocess --section "BEIJING-SHANGHAI" --date 2019-12-10`
  - UI: `streamlit run src/ui/app.py`
  - API: `uvicorn src.api.server:app --reload`

## Acceptance criteria
- Load & normalize ≥1 day for a corridor (≥500 train events).
- Optimizer returns a feasible plan in ≤30s per window on laptop; heuristic in ≤2s.
- Demonstrated reduction in avg arrival delay vs baseline by ≥10% on sample day.
- UI shows clear recommendations + KPIs + explainability.

## Nice-to-have (if time permits)
- Learning policy to tune weights λ, μ, ζ, κ via Bayesian optimization.
- Cached warm starts; cut generation for pairwise headways; CP-SAT alternative.
- Export action plan to CSV/PDF.

Generate the full codebase with stubs, docstrings, and a minimal demo run that works out-of-the-box on a 2000-row slice. Prefer readability and modularity over micro-optimizations.

## Phase 1 Pipeline (Windows)
- Create venv: `python -m venv .venv` then `.\.venv\Scripts\Activate.ps1`
- Install deps: `pip install -r requirements.txt`
- Place raw CSVs under `data/raw/`
- Prepare a stations file like `data/demo_corridor_stations.txt` (one station name or ID per line)
 - Run: `./scripts/run_phase1.ps1 demo_corridor 2024-01-01`
 - Use real dataset only (optional third arg):
   - `./scripts/run_phase1.ps1 konkan_corridor 2024-01-01 'Train_details*.csv'`
- Artifacts appear under `artifacts/<corridor>/<date>/` including `events.parquet`, `section_edges.parquet`, `kpis.json`, and `baseline_gantt.png`.

## Phase 1 Status
- Pipeline: `scripts/run_phase1.(sh|ps1)` runs load → normalize → slice → graph → baseline → DQ and writes artifacts per corridor/date.
- Normalization: `src/data/normalize.py` now
  - accepts `default_service_date` and always outputs `service_date`;
  - parses time-of-day with the date to UTC timestamps (no parser warnings);
  - maps station names/codes to stable `station_id` via `station_map.csv`;
  - handles duplicate station columns safely; extended column aliases.
- Loader: `src/data/loader.py` reads CSVs as strings with `utf-8-sig` and trims headers to avoid Parquet dtype issues.
- Windows: added `scripts/run_phase1.ps1` with fail-fast checks after each step.
- Artifacts: edges/nodes parquet, events/events_clean parquet, `kpis.json`, `baseline_gantt.png`, `dq_report.md`, and `stations.json` under `artifacts/<corridor>/<date>/`.
- Tests: `pytest -q` runs `tests/test_normalize.py` (column mapping + delay calc).
- Demo: `data/konkan_corridor_stations.txt` matches the included Indian Railways sample.

### How To Validate
- Run (Windows): `./scripts/run_phase1.ps1 konkan_corridor 2024-01-01`
- Inspect: `artifacts/konkan_corridor/2024-01-01/kpis.json`, `section_edges.parquet`, `baseline_gantt.png`, `dq_report.md`.

### Known Limitations (Phase 1)
- Graph uses single-track/platform defaults; overtakes and capacities simplified.
- Baseline replays from actuals when available, otherwise scheduled + medians.
- Station lists must match the normalized data; use `src/data/station_map.csv` for reference.
