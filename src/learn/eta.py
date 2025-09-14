from __future__ import annotations

"""Train and serve a Gradient Boosted ETA model (next-station arrival delay).

Artifacts are stored under artifacts/<scope>/<date>/eta_model.joblib and
predicted risk heat under incident_heat.json is handled by a separate module.
"""

from pathlib import Path
from typing import Dict, List, Tuple

import joblib  # type: ignore
import numpy as np
import pandas as pd
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.metrics import mean_absolute_error
from sklearn.model_selection import train_test_split


def _base(scope: str, date: str) -> Path:
    return Path("artifacts") / scope / date


def _to_utc(s: pd.Series | None) -> pd.Series:
    if s is None:
        return pd.Series(dtype="datetime64[ns, UTC]")
    return pd.to_datetime(s, utc=True, errors="coerce")


def _pair_key(u: str, v: str) -> str:
    return f"{u}__{v}"


def build_training_frame(scope: str, date: str) -> pd.DataFrame:
    base = _base(scope, date)
    events = pd.read_parquet(base / "events_clean.parquet")
    edges = pd.read_parquet(base / "section_edges.parquet") if (base / "section_edges.parquet").exists() else pd.DataFrame()

    df = events.copy()
    for c in ("sched_arr", "sched_dep", "act_arr", "act_dep"):
        if c in df.columns:
            df[c] = _to_utc(df[c])

    # Compute per-train next stop records
    df = df.sort_values(["train_id", "sched_dep", "sched_arr"]).copy()
    df["next_station"] = df.groupby("train_id")["station_id"].shift(-1)
    df["next_sched_arr"] = df.groupby("train_id")["sched_arr"].shift(-1)
    df["next_act_arr"] = df.groupby("train_id")["act_arr"].shift(-1)

    # Features at current stop
    df["arr_delay_here"] = (df["act_arr"] - df["sched_arr"]).dt.total_seconds() / 60
    df["dep_delay_here"] = (df["act_dep"] - df["sched_dep"]).dt.total_seconds() / 60
    df["hour"] = (df["sched_dep"].dt.hour).astype(float)
    df["dow"] = (df["sched_dep"].dt.dayofweek).astype(float)

    # Join edge attributes (u = station_id, v = next_station)
    if not edges.empty:
        edges_k = edges.copy()
        edges_k["k"] = edges_k.apply(lambda r: _pair_key(str(r["u"]), str(r["v"])), axis=1)
        e_map = edges_k.set_index("k")[ ["min_run_time","headway","capacity"] ]
        kser = df.apply(lambda r: _pair_key(str(r["station_id"]), str(r["next_station"])) if pd.notna(r["next_station"]) else None, axis=1)
        df["min_run_time"] = kser.map(e_map["min_run_time"]) if "min_run_time" in e_map.columns else np.nan
        df["headway"] = kser.map(e_map["headway"]) if "headway" in e_map.columns else np.nan
        df["capacity"] = kser.map(e_map["capacity"]) if "capacity" in e_map.columns else 1
    else:
        df["min_run_time"] = np.nan
        df["headway"] = np.nan
        df["capacity"] = 1

    # Target: arrival delay at next station
    df["y_arr_delay_next"] = (df["next_act_arr"] - df["next_sched_arr"]).dt.total_seconds() / 60
    out = df.dropna(subset=["next_station", "next_sched_arr"]).copy()
    return out


def train_eta(scope: str, date: str) -> Dict[str, float]:
    base = _base(scope, date)
    df = build_training_frame(scope, date)
    if df.empty:
        rpt = {"status": "no_data"}
        (base / "eta_report.json").write_text(pd.Series(rpt).to_json())
        return rpt
    # Encode IDs
    df["train_code"] = df["train_id"].astype("category").cat.codes
    df["u_code"] = df["station_id"].astype("category").cat.codes
    df["v_code"] = df["next_station"].astype("category").cat.codes
    features = [
        "train_code","u_code","v_code","arr_delay_here","dep_delay_here","hour","dow","min_run_time","headway","capacity"
    ]
    X = df[features].fillna(0.0).astype(float)
    y = df["y_arr_delay_next"].fillna(0.0).astype(float)
    Xtr, Xte, ytr, yte = train_test_split(X, y, test_size=0.2, random_state=42)
    model = GradientBoostingRegressor(random_state=42)
    model.fit(Xtr, ytr)
    pred = model.predict(Xte)
    mae = float(mean_absolute_error(yte, pred))
    payload = {"model": model, "features": features}
    out_p = base / "eta_model.joblib"
    joblib.dump(payload, out_p)
    rpt = {"status": "ok", "mae_min": mae, "n": int(len(df))}
    (base / "eta_report.json").write_text(pd.Series(rpt).to_json())
    return rpt


def predict_next_eta(scope: str, date: str, train_id: str) -> Dict[str, object]:
    base = _base(scope, date)
    p = base / "eta_model.joblib"
    if not p.exists():
        return {"status": "unavailable"}
    payload = joblib.load(p)
    model = payload.get("model")
    features: List[str] = payload.get("features") or []
    # Build last known record for this train
    df = build_training_frame(scope, date)
    df = df[df["train_id"].astype(str) == str(train_id)]
    if df.empty:
        return {"status": "no_data"}
    row = df.sort_values("sched_dep").tail(1).iloc[0]
    # Recompute encoding aligned with training
    df_all = build_training_frame(scope, date)
    df_all["train_code"] = df_all["train_id"].astype("category").cat.codes
    df_all["u_code"] = df_all["station_id"].astype("category").cat.codes
    df_all["v_code"] = df_all["next_station"].astype("category").cat.codes
    # Find codes for this row using category maps
    train_code = int(df_all[df_all["train_id"] == row["train_id"]]["train_code"].iloc[0]) if not df_all.empty else 0
    u_code = int(df_all[df_all["station_id"] == row["station_id"]]["u_code"].iloc[0]) if not df_all.empty else 0
    v_code = int(df_all[df_all["next_station"] == row["next_station"]]["v_code"].iloc[0]) if not df_all.empty else 0
    rec = {
        "train_code": train_code,
        "u_code": u_code,
        "v_code": v_code,
        "arr_delay_here": float(row.get("arr_delay_here", 0.0) or 0.0),
        "dep_delay_here": float(row.get("dep_delay_here", 0.0) or 0.0),
        "hour": float(pd.to_datetime(row.get("sched_dep")).hour if pd.notna(row.get("sched_dep")) else 0.0),
        "dow": float(pd.to_datetime(row.get("sched_dep")).dayofweek if pd.notna(row.get("sched_dep")) else 0.0),
        "min_run_time": float(row.get("min_run_time", 0.0) or 0.0),
        "headway": float(row.get("headway", 0.0) or 0.0),
        "capacity": float(row.get("capacity", 1) or 1),
    }
    x = pd.DataFrame([[rec.get(c, 0.0) for c in features]], columns=features)
    yhat = float(model.predict(x)[0])
    return {
        "status": "ok",
        "train_id": str(train_id),
        "from_station": row.get("station_id"),
        "to_station": row.get("next_station"),
        "pred_arr_delay_min": yhat,
        "features": {k: rec.get(k) for k in features},
    }

