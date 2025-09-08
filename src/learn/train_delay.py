from __future__ import annotations

from pathlib import Path
import json
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error


def _art(scope: str, date: str) -> Path:
    return Path("artifacts") / scope / date


def main(scope: str, date: str) -> None:
    base = _art(scope, date)
    events = pd.read_parquet(base / "events_clean.parquet")
    # Baseline features: scheduled run/dwell minutes; station and train ids as codes
    df = events.copy()
    for c in ["sched_arr", "sched_dep", "act_arr", "act_dep"]:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], utc=True, errors="coerce")
    df["arr_delay"] = (df["act_arr"] - df["sched_arr"]).dt.total_seconds() / 60
    df = df.dropna(subset=["arr_delay"]).copy()
    if df.empty:
        report = {"status": "no_data"}
        (base / "model_update_report.md").write_text(json.dumps(report, indent=2))
        return
    df["train_code"] = df["train_id"].astype("category").cat.codes
    df["station_code"] = df["station_id"].astype("category").cat.codes
    X = df[["train_code", "station_code"]]
    y = df["arr_delay"].values
    model = LinearRegression().fit(X, y)
    pred = model.predict(X)
    mae = float(mean_absolute_error(y, pred))
    report = {"status": "ok", "mae_min": mae, "n": int(len(df))}
    (base / "model_update_report.md").write_text(json.dumps(report, indent=2))


if __name__ == "__main__":  # pragma: no cover
    import sys

    main(sys.argv[1], sys.argv[2])

