from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

import pandas as pd
import streamlit as st


def art_dir(scope: str, date: str) -> Path:
    return Path("artifacts") / scope / date


def read_json(p: Path) -> Any:
    if not p.exists():
        return None
    return json.loads(p.read_text())


def load_parquet(p: Path) -> pd.DataFrame:
    if not p.exists():
        return pd.DataFrame()
    return pd.read_parquet(p)


st.set_page_config(page_title="Train Control Portal", layout="wide")
st.title("Decision Support Portal")

scope = st.sidebar.text_input("Scope", value="all_india")
date = st.sidebar.text_input("Date (YYYY-MM-DD)", value="2024-01-01")
base = art_dir(scope, date)

col1, col2 = st.columns([2, 1])

with col1:
    st.subheader("Live Gantt (Platform Occupancy)")
    df_plat = load_parquet(base / "national_platform_occupancy.parquet")
    if df_plat.empty:
        df_plat = load_parquet(base / "platform_occupancy.parquet")
    if df_plat.empty:
        st.info("No platform occupancy available.")
    else:
        dfp = df_plat.copy()
        dfp = dfp.sort_values("arr_platform").head(1000)
        dfp["y"] = dfp["station_id"].astype(str)
        dfp["x0"] = pd.to_datetime(dfp["arr_platform"])
        dfp["x1"] = pd.to_datetime(dfp["dep_platform"])
        for tid, g in dfp.groupby("train_id"):
            st.write(f"Train {tid}")
            st.progress(0)
        st.dataframe(dfp[["train_id", "station_id", "arr_platform", "dep_platform", "platform_slot"]] if "platform_slot" in dfp.columns else dfp[["train_id", "station_id", "arr_platform", "dep_platform"]])

with col2:
    st.subheader("Conflict Radar")
    radar = read_json(base / "conflict_radar.json") or []
    if not radar:
        st.info("No risks found in horizon.")
    else:
        st.json(radar[:20])

st.subheader("Recommendations")
rec_plan = read_json(base / "rec_plan.json") or []
alts = read_json(base / "alt_options.json") or []
if not rec_plan:
    st.info("No recommendations found.")
else:
    for i, rec in enumerate(rec_plan[:100]):
        with st.expander(f"#{i+1} {rec.get('type')} for train {rec.get('train_id')} at {rec.get('at_station') or rec.get('station_id')}"):
            st.write("Why:", rec.get("why"))
            st.write("Reason:", rec.get("reason"))
            st.json(rec)
            c1, c2, c3 = st.columns(3)
            with c1:
                if st.button("APPLY", key=f"apply_{i}"):
                    log_feedback(scope, date, rec, "APPLY")
                    st.success("Applied feedback logged")
            with c2:
                if st.button("DISMISS", key=f"dismiss_{i}"):
                    log_feedback(scope, date, rec, "DISMISS")
                    st.success("Dismiss feedback logged")
            with c3:
                mins = st.number_input("Modify hold (min)", min_value=0.0, max_value=10.0, value=float(rec.get("minutes", 0.0)), key=f"mins_{i}")
                plat = st.text_input("Platform slot", value=str(rec.get("platform", "")), key=f"plat_{i}")
                if st.button("MODIFY", key=f"modify_{i}"):
                    mod = dict(rec)
                    if "minutes" in rec:
                        mod["minutes"] = mins
                    if rec.get("type") == "PLATFORM_REASSIGN":
                        try:
                            mod["platform"] = int(plat)
                        except Exception:
                            mod["platform"] = plat
                    log_feedback(scope, date, rec, "MODIFY", modified=mod)
                    st.success("Modify feedback logged")


def log_feedback(scope: str, date: str, action: Dict[str, Any], decision: str, modified: Dict[str, Any] | None = None) -> None:
    # Write feedback locally under artifacts
    base = art_dir(scope, date)
    base.mkdir(parents=True, exist_ok=True)
    trail_path = base / "audit_trail.json"
    trail = read_json(trail_path) or []
    entry = {"action": action, "decision": decision, "modified": modified}
    trail.append(entry)
    trail_path.write_text(json.dumps(trail, indent=2))
    df_new = pd.DataFrame([
        {
            "decision": decision,
            "action": json.dumps(action),
            "modified": json.dumps(modified) if modified else None,
        }
    ])
    fb_path = base / "feedback.parquet"
    if fb_path.exists():
        df_all = pd.read_parquet(fb_path)
        df_all = pd.concat([df_all, df_new], ignore_index=True)
    else:
        df_all = df_new
    df_all.to_parquet(fb_path, index=False)

