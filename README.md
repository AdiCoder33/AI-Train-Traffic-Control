<!-- PROJECT HEADER -->
<h1 align="center">🚄 AI Train Traffic Control</h1>
<p align="center">
  <b>Maximizing Section Throughput Using AI-Powered Precise Train Traffic Control</b>
</p>
<p align="center">
  <img src="https://img.shields.io/badge/python-3.11-blue.svg" />
  <img src="https://img.shields.io/badge/build-passing-brightgreen.svg" />
  <img src="https://img.shields.io/badge/license-MIT-blue.svg" />
  <img src="https://img.shields.io/badge/react-vite-orange.svg" />
</p>

---

## 🚦 Project Overview

> **AI-Train-Traffic-Control** is an advanced software system for real-time, AI-driven optimization of train operations, minimizing delays and maximizing throughput on busy rail sections.  
> **Features:** digital twin, MILP optimization, rolling horizon, web dashboard, simulation & what-if, explainable decisions, and programmatic API.

---

## ✨ Quick Start

```bash
# 1. Create and activate a virtual environment
python -m venv .venv && source .venv/bin/activate

# 2. Install dependencies
pip install -r requirements.txt  # or: poetry install

# 3. Place raw Figshare CSVs
mkdir -p ./data/raw/
# (add your dataset files here)

# 4. Prepare data
python -m src.data.preprocess --section "BEIJING-SHANGHAI" --date 2019-12-10

# 5. Run the API
uvicorn src.api.server:app --reload

# 6. Run the Web UI
cd web && npm install && npm run dev
# Open http://localhost:5173
```

---

## 📊 Tech Stack

| Layer          | Tech/Libs                                             |
| -------------- | ----------------------------------------------------- |
| Backend        | Python 3.11, FastAPI, pandas, numpy, networkx, pulp/OR-Tools, scikit-learn |
| Optimization   | MILP (PuLP/OR-Tools CP-SAT), rolling horizon, heuristics |
| Frontend       | React (Vite), Plotly                                  |
| API            | REST (FastAPI)                                        |
| Testing        | pytest                                                |
| Style          | type hints, black, ruff                               |

---

## 🧩 Core Features

- **Section Digital Twin:** Graph model of stations, tracks, blocks, and operational constraints.
- **Optimizer:** Rolling-horizon MILP/CP-SAT for train scheduling, holding, and platform assignment. Fast greedy heuristic fallback.
- **Simulator:** Replay past days, inject disruptions (weather, delays), and simulate controller decisions.
- **Web UI:** Upload datasets, run baseline/optimized scenarios, visualize KPIs, delays, occupancy, and run what-if analysis.
- **API:** Programmatic endpoints for optimization, simulation, and KPI fetching.
- **Explainability:** Every decision shows binding constraints and marginal penalties.
- **AI Assistant & Learning:** Global imitation learning (IL), offline RL, safe suggestions, and human-in-the-loop improvements.

---

## 🗂️ Project Structure

```plaintext
.
├─ pyproject.toml (or requirements.txt)
├─ src/
│  ├─ data/         # Data loaders & preprocessing
│  ├─ model/        # Digital twin, constraints, optimizer, heuristic
│  ├─ sim/          # Rolling horizon, scenarios
│  ├─ api/          # FastAPI server
│  └─ web/          # React SPA (frontend)
├─ tests/           # Pytest test cases
├─ scripts/         # One-click wrappers for Windows/Linux/macOS
├─ artifacts/       # Output: parquet, KPIs, PNGs
└─ README.md
```

---

## 🚀 Demo

> _Add screenshots or GIFs here to show the UI, Gantt charts, KPIs, or API usage!_

![Demo Screenshot Placeholder](https://via.placeholder.com/800x300?text=Demo+Screenshot)

---

## 📖 Details

<details>
<summary><b>Scope & Deliverables</b></summary>

- Software-only, offline/file-based inputs.
- Dataset: [Figshare high-speed railway](https://doi.org/10.6084/m9.figshare.16858218).
- Recommend real-time precedence/holding/crossing to minimize delay & maximize throughput.
- Deliverables: Data prep, digital twin, optimizer, simulator, web UI, API, tests, demo dataset.
</details>

<details>
<summary><b>Section Graph & Optimization Details</b></summary>

- Graph: stations as nodes, track sections as edges; capacity, headways, dwell, gradients.
- MILP/CP-SAT: Departure/arrival times, holds, overtakings, platform/block occupancy, priorities.
- Objective: Minimize arrival delay + penalties, maximize trains served.
- Constraints: Block/platform occupancy, headway, dwell, speed, binary precedence.
- Heuristic: Greedy dispatch by priority, lateness, slack.
</details>

<details>
<summary><b>Simulator & Rolling Horizon</b></summary>

- Replay historical days, inject disruptions.
- Call optimizer every 5 minutes over a 45–60 min rolling window.
- Output: Hold/route/overtake plans, action plans, simulated KPIs.
</details>

<details>
<summary><b>Web UI & API</b></summary>

- React SPA under `/web`
- Pages: Overview, Radar, Recommendations, What-If
- API: `/optimize`, `/simulate`, `/kpis`
- Auth: Bearer token or dev headers
</details>

<details>
<summary><b>AI Assistant & Learning</b></summary>

- Global Imitation Learning (IL), Offline RL, safety filter, human-in-the-loop.
- Endpoints: `/ai/ask`, `/ai/suggest`, `/admin/train_global`, etc.
</details>

---

## 🧑‍💻 Development & Scripts

| Task                  | Windows Script         | Linux/macOS Script     |
|-----------------------|-----------------------|-----------------------|
| Phase 1 Pipeline      | run_phase1.ps1        | run_phase1.sh         |
| Block-level View      | run_block_view.ps1    | run_block_view.sh     |
| One-click End-to-End  | run_all.ps1           | run_all.sh            |
| National Replay       | run_national.ps1      | run_national.sh       |

---

## ✅ Acceptance Criteria

- Load & normalize ≥1 corridor day (≥500 events)
- Optimizer: plan in ≤30s/window, heuristic in ≤2s
- ≥10% reduction in avg arrival delay vs baseline (sample day)
- UI: clear KPIs, recommendations, explainability

---

## ⚡ Nice-to-Have

- Automatic weight tuning (Bayesian optimization)
- Warm starts, pairwise headways, CP-SAT alt
- Export action plan to CSV/PDF

---

## 📄 License

This project is licensed under the MIT License.

---

## 🤝 Contributors

Thanks to all contributors! PRs and suggestions welcome.

---

## 📬 Contact

For questions, open an issue or reach out to [AdiCoder33](https://github.com/AdiCoder33).
