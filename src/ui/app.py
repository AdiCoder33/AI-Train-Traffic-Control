from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
import streamlit as st
import altair as alt
import plotly.express as px
import requests


def art_dir(scope: str, date: str) -> Path:
    return Path("artifacts") / scope / date


def read_json(p: Path) -> Any:
    if not p.exists():
        return None
    return json.loads(p.read_text())


def write_json(p: Path, payload: Any) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(payload, indent=2))


def load_parquet(p: Path) -> pd.DataFrame:
    if not p.exists():
        return pd.DataFrame()
    return pd.read_parquet(p)


def sha1_dict(obj: Any) -> str:
    s = json.dumps(obj, sort_keys=True, separators=(",", ":"))
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


# ---- UI helpers (modernized rendering) ----
def _df_from(records: List[Dict[str, Any]] | Dict[str, Any], columns: List[str] | None = None) -> pd.DataFrame:
    if isinstance(records, dict):
        try:
            return pd.json_normalize(records)
        except Exception:
            return pd.DataFrame([records])
    try:
        df = pd.DataFrame(records)
    except Exception:
        return pd.DataFrame()
    if columns:
        cols = [c for c in columns if c in df.columns]
        if cols:
            df = df[cols]
    return df


def _render_kv_table(d: Dict[str, Any]) -> None:
    try:
        df = pd.DataFrame({"key": list(d.keys()), "value": [d[k] for k in d.keys()]})
        st.dataframe(df, hide_index=True, use_container_width=True)
    except Exception:
        st.write(d)


# ---- Badges / chips (emoji-based, table-friendly) ----
def _sev_badge(sev: str | None) -> str:
    s = (sev or "").strip().title()
    return {
        "Critical": "🔴 Critical",
        "High": "🟠 High",
        "Medium": "🟡 Medium",
        "Low": "🟢 Low",
    }.get(s, s)


def _type_badge(t: str | None) -> str:
    x = (t or "").strip()
    return {
        "headway": "⏱ headway",
        "block_capacity": "🚦 capacity",
        "platform_overflow": "🛤 platform",
    }.get(x, x)


def _action_badge(t: str | None) -> str:
    x = (t or "").strip().upper()
    return {
        "HOLD": "⏱ HOLD",
        "PLATFORM_REASSIGN": "🛤 PLATFORM",
        "SPEED_TUNE": "⚡ SPEED",
        "OVERTAKE": "🔁 OVERTAKE",
        "SKIP_STOP": "⛔ SKIP",
    }.get(x, x)


# ---- Styling helpers ----
def _style_hide_index(styler: Any) -> Any:
    try:
        return styler.hide(axis="index")  # pandas >= 1.4
    except Exception:
        try:
            return styler.hide_index()  # older pandas
        except Exception:
            return styler


def _style_risks(df: pd.DataFrame, sev_col: str = "Severity") -> Any:
    """Return a pandas Styler with row background by severity labels.

    Expects values like '🔴 Critical', '🟠 High', '🟡 Medium', '🟢 Low'.
    """
    def _row_style(row: pd.Series) -> list[str]:
        s = str(row.get(sev_col, "")).lower()
        if "critical" in s:
            color = "#ffe5e5"  # light red
        elif "high" in s:
            color = "#fff0e0"  # light orange
        elif "medium" in s:
            color = "#fff8e1"  # light yellow
        elif "low" in s:
            color = "#eaffea"  # light green
        else:
            color = "#ffffff"
        return [f"background-color: {color}"] * len(row)

    try:
        sty = df.style.apply(_row_style, axis=1)
        return _style_hide_index(sty)
    except Exception:
        return df


def _style_minutes(df: pd.DataFrame, col: str = "Minutes") -> Any:
    """Color minutes column with gentle thresholds (<=2 green, <=3 yellow, else orange/red)."""
    if col not in df.columns:
        return df
    def _cell(v: Any) -> str:
        try:
            x = float(v)
        except Exception:
            return ""
        if x <= 2.0:
            return "background-color: #eaffea"  # light green
        if x <= 3.0:
            return "background-color: #fff8e1"  # light yellow
        if x <= 4.0:
            return "background-color: #fff0e0"  # light orange
        return "background-color: #ffe5e5"      # light red
    try:
        sty = df.style.applymap(_cell, subset=[col])
        return _style_hide_index(sty)
    except Exception:
        return df


def _style_audit(df: pd.DataFrame, col: str = "Decision") -> Any:
    """Color decision column (APPLY green, DISMISS red, MODIFY blue, ACK gray)."""
    if col not in df.columns:
        return df
    def _cell(v: Any) -> str:
        s = str(v).upper()
        if s == "APPLY":
            return "background-color: #eaffea"
        if s == "DISMISS":
            return "background-color: #ffe5e5"
        if s == "MODIFY":
            return "background-color: #e6f0ff"
        if s == "ACK":
            return "background-color: #f2f2f2"
        return ""
    try:
        sty = df.style.applymap(_cell, subset=[col])
        return _style_hide_index(sty)
    except Exception:
        return df


def _crew_summary(rec: Dict[str, Any]) -> str:
    t = rec.get("type")
    if t == "HOLD":
        return f"Train {rec.get('train_id')}: Hold at {rec.get('at_station')} for {rec.get('minutes')} min"
    if t == "PLATFORM_REASSIGN":
        return f"Train {rec.get('train_id')}: Use platform {rec.get('platform')} at {rec.get('station_id')}"
    if t == "SPEED_TUNE":
        return f"Train {rec.get('train_id')}: Block {rec.get('block_id')} speed x{rec.get('speed_factor')}"
    return str(rec)


def log_feedback(
    scope: str,
    date: str,
    action: Dict[str, Any],
    decision: str,
    *,
    modified: Dict[str, Any] | None = None,
    user: str,
    role: str,
    plan_version: str,
    action_id: str,
    api_host: str | None = None,
    api_port: int | None = None,
) -> None:
    token = st.session_state.get("token")
    if token and api_host and api_port is not None:
        try:
            payload = {
                "scope": scope,
                "date": date,
                "action": action,
                "decision": decision,
                "reason": None,
                "modified": modified,
            }
            resp = requests.post(
                f"http://{api_host}:{api_port}/feedback",
                json=payload,
                headers={"Authorization": f"Bearer {token}"},
                timeout=5,
            )
            if resp.status_code == 200:
                return
            else:
                st.warning(f"API feedback failed, falling back to local: {resp.text}")
        except Exception as e:
            st.warning(f"API feedback error, falling back: {e}")

    # Local fallback
    base = art_dir(scope, date)
    base.mkdir(parents=True, exist_ok=True)
    trail_path = base / "audit_trail.json"
    trail = read_json(trail_path) or []
    entry = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "who": user,
        "role": role,
        "action_id": action_id,
        "decision": decision,
        "details": modified or {},
        "plan_version": plan_version,
        "action": action,
    }
    trail.append(entry)
    write_json(trail_path, trail)
    df_new = pd.DataFrame([
        {
            "ts": entry["ts"],
            "user": user,
            "role": role,
            "decision": decision,
            "plan_version": plan_version,
            "action_id": action_id,
            "modified": json.dumps(modified) if modified else None,
            "action": json.dumps(action),
        }
    ])
    fb_path = base / "feedback.parquet"
    if fb_path.exists():
        df_all = pd.read_parquet(fb_path)
        df_all = pd.concat([df_all, df_new], ignore_index=True)
    else:
        df_all = df_new
    df_all.to_parquet(fb_path, index=False)


st.set_page_config(page_title="Train Control Portal", layout="wide")
st.title("Decision Support Portal")

# Minimal CSS polish for compact tables/metrics
st.markdown(
    """
    <style>
    .stMetric { gap: 0.25rem; }
    .stDataFrame table { font-size: 0.9rem; }
    </style>
    """,
    unsafe_allow_html=True,
)

# Safe rerun helper for different Streamlit versions
def _safe_rerun() -> None:
    try:
        # Streamlit >= 1.25
        st.rerun()  # type: ignore[attr-defined]
    except Exception:
        try:
            # Older versions
            st.experimental_rerun()  # type: ignore[attr-defined]
        except Exception:
            pass

# Login-first guard: render a dedicated login page until authenticated
if "token" not in st.session_state:
    st.session_state["token"] = None
if "principal" not in st.session_state:
    st.session_state["principal"] = None
if not st.session_state.get("token") or not st.session_state.get("principal"):
    st.subheader("Sign In")
    colA, colB = st.columns(2)
    with colA:
        api_host_login = st.text_input("API Host", value=str(st.session_state.get("api_host", "127.0.0.1")))
        api_port_login = st.number_input("API Port", value=int(st.session_state.get("api_port", 8000)), step=1)
        user_login = st.text_input("Username")
        pass_login = st.text_input("Password", type="password")
        if st.button("Sign In", key="page_login"):
            try:
                import requests as _req
                resp = _req.post(
                    f"http://{api_host_login}:{api_port_login}/login",
                    json={"username": user_login, "password": pass_login},
                    timeout=5,
                )
                if resp.status_code == 200:
                    data = resp.json()
                    st.session_state["token"] = data.get("token")
                    st.session_state["principal"] = {"user": data.get("username"), "role": data.get("role")}
                    st.session_state["api_host"] = api_host_login
                    st.session_state["api_port"] = int(api_port_login)
                    _safe_rerun()
                else:
                    st.error(f"Login failed: {resp.text}")
            except Exception as e:
                st.error(f"Login error: {e}")
    with colB:
        st.info("Please sign in to access role-based dashboards.")
    st.stop()

# Login-first guard: render a dedicated login page until authenticated
if "token" not in st.session_state:
    st.session_state["token"] = None
if "principal" not in st.session_state:
    st.session_state["principal"] = None

if not st.session_state.get("token") or not st.session_state.get("principal"):
    st.subheader("Sign In")
    lcol1, lcol2 = st.columns(2)
    with lcol1:
        api_host_login = st.text_input("API Host", value=st.session_state.get("api_host", "127.0.0.1"))
        api_port_login = st.number_input("API Port", value=int(st.session_state.get("api_port", 8000)), step=1)
        user_login = st.text_input("Username")
        pass_login = st.text_input("Password", type="password")
        if st.button("Sign In"):
            try:
                import requests as _req
                resp = _req.post(f"http://{api_host_login}:{api_port_login}/login", json={"username": user_login, "password": pass_login}, timeout=5)
                if resp.status_code == 200:
                    data = resp.json()
                    st.session_state["token"] = data.get("token")
                    st.session_state["principal"] = {"user": data.get("username"), "role": data.get("role")}
                    st.session_state["api_host"] = api_host_login
                    st.session_state["api_port"] = int(api_port_login)
                    st.success(f"Signed in as {data.get('username')} ({data.get('role')})")
                    st.experimental_rerun()
                else:
                    st.error(f"Login failed: {resp.text}")
            except Exception as e:
                st.error(f"Login error: {e}")
    with lcol2:
        st.info("Use your credentials to access role-based dashboards. Admins can create users via API.")
    st.stop()

scope = st.sidebar.text_input("Scope", value="all_india")
date = st.sidebar.text_input("Date (YYYY-MM-DD)", value="2024-01-01")
api_host = st.sidebar.text_input("API Host", value=str(st.session_state.get("api_host", "127.0.0.1")))
api_port = st.sidebar.number_input("API Port", value=int(st.session_state.get("api_port", 8000)), step=1)
role = st.session_state["principal"]["role"]
user = st.session_state["principal"]["user"]
if st.sidebar.button("Sign Out"):
    st.session_state["token"] = None
    st.session_state["principal"] = None
    st.experimental_rerun()

refresh = st.sidebar.slider("Auto-refresh (sec)", 0, 60, 0)
base = art_dir(scope, date)

# Read common artifacts
df_plat = load_parquet(base / "national_platform_occupancy.parquet")
if df_plat.empty:
    df_plat = load_parquet(base / "platform_occupancy.parquet")
df_block = load_parquet(base / "national_block_occupancy.parquet")
if df_block.empty:
    df_block = load_parquet(base / "block_occupancy.parquet")
radar = read_json(base / "conflict_radar.json") or []
risk_timeline = load_parquet(base / "risk_timeline.parquet")
sim_kpis = read_json(base / "national_sim_kpis.json") or {}
plan_kpis = read_json(base / "plan_metrics.json") or {}
rec_plan: List[dict] = read_json(base / "rec_plan.json") or []
alts: List[dict] = read_json(base / "alt_options.json") or []
plan_version = sha1_dict(rec_plan) if rec_plan else ""

# Station scoping for Station Controller role
sc_station: str | None = None
if role == "SC":
    try:
        nodes_all = load_parquet(base / "section_nodes.parquet")
        opts_ids = nodes_all["station_id"].dropna().astype(str).unique().tolist() if not nodes_all.empty and "station_id" in nodes_all.columns else sorted(set((df_plat.get("station_id").dropna().astype(str).tolist() if not df_plat.empty else [])))
    except Exception:
        opts_ids = []
    if opts_ids:
        labels = [label_station(x) for x in sorted(opts_ids)]
        sel = st.sidebar.selectbox("Your Station (SC)", labels)
        # map back to id
        idx = labels.index(sel)
        sc_station = sorted(opts_ids)[idx]

# Apply station scoping to views
df_plat_view = df_plat.copy()
df_block_view = df_block.copy()
radar_view = list(radar)
rec_plan_view = list(rec_plan)
if role == "SC" and sc_station:
    sid = str(sc_station)
    if not df_plat_view.empty and "station_id" in df_plat_view.columns:
        df_plat_view = df_plat_view[df_plat_view["station_id"].astype(str) == sid]
    if not df_block_view.empty and {"u","v"}.issubset(df_block_view.columns):
        df_block_view = df_block_view[(df_block_view["u"].astype(str) == sid) | (df_block_view["v"].astype(str) == sid)]
    # radar filter
    radar_view = [r for r in radar_view if str(r.get("station_id","")) == sid or str(r.get("u","")) == sid or str(r.get("v","")) == sid]
    # rec_plan filter: match station or at_station or blocks touching
    blocks_touching = set()
    try:
        if not df_block.empty:
            sub = df_block[(df_block["u"].astype(str) == sid) | (df_block["v"].astype(str) == sid)]
            blocks_touching = set(sub["block_id"].astype(str).unique().tolist())
    except Exception:
        pass
    tmp = []
    for r in rec_plan_view:
        st1 = r.get("station_id")
        at = r.get("at_station")
        bid = r.get("block_id")
        if (st1 is not None and str(st1) == sid) or (at is not None and str(at) == sid) or (bid is not None and str(bid) in blocks_touching):
            tmp.append(r)
    rec_plan_view = tmp

tab_names = ["Overview", "Board", "Radar", "Recommendations", "Audit", "Policy", "Crew", "Assistant", "Lab", "Admin"]
t_over, t_board, t_radar, t_reco, t_audit, t_policy, t_crew, t_asst, t_lab, t_admin = st.tabs(tab_names)

# Sticky banners for critical/high risks
    if radar_view:
        sev_counts: Dict[str, int] = {}
        for r in radar_view:
            sev_counts[r.get("severity", "")] = sev_counts.get(r.get("severity", ""), 0) + 1
        if sev_counts.get("Critical"):
            st.error(f"Critical risks in horizon: {sev_counts['Critical']}")
        if sev_counts.get("High"):
            st.warning(f"High risks in horizon: {sev_counts['High']}")

with t_over:
    st.subheader("KPIs & Summary")
    c1, c2, c3, c4 = st.columns(4)
    otp = float(sim_kpis.get("otp_pct", sim_kpis.get("otp", 0.0)))
    avg_delay = float(sim_kpis.get("avg_delay", sim_kpis.get("avg_delay_min", 0.0)))
    conflicts = int((read_json(base / "risk_kpis.json") or {}).get("total_risks", len(radar)))
    actions = int(plan_kpis.get("actions", 0))
    c1.metric("OTP (%)", f"{otp:.1f}")
    c2.metric("Avg Delay (min)", f"{avg_delay:.1f}")
    c3.metric("Risks in Horizon", f"{conflicts}")
    c4.metric("Planned Actions", f"{actions}")

    # Severity distribution
    st.caption("Risk severity mix")
    if radar_view:
        sev_counts: Dict[str, int] = {}
        for r in radar_view:
            sev_counts[r.get("severity", "Unknown")] = sev_counts.get(r.get("severity", "Unknown"), 0) + 1
        df_sev = pd.DataFrame({"severity": list(sev_counts.keys()), "count": list(sev_counts.values())})
        chart = alt.Chart(df_sev).mark_bar().encode(x="severity:N", y="count:Q", color="severity:N")
        st.altair_chart(chart, use_container_width=True)
    else:
        st.info("No radar loaded.")

with t_board:
    st.subheader("Live Section Board")
    if role in ("CREW",):
        st.info("Board is not available for Crew role.")
    
    tb_plat, tb_block, tb_heat = st.tabs(["Platforms", "Blocks", "Platform Heatmap"])

    with tb_plat:
        if df_plat_view.empty:
            st.info("No platform occupancy available.")
        else:
            dfp = df_plat_view.copy().sort_values("arr_platform").head(2000)
            dfp["arr_platform"] = pd.to_datetime(dfp["arr_platform"])  # type: ignore
            dfp["dep_platform"] = pd.to_datetime(dfp["dep_platform"])  # type: ignore
            # Plotly Gantt timeline
            try:
                dfp["station_label"] = dfp["station_id"].map(lambda x: label_station(x))
                fig = px.timeline(
                    dfp,
                    x_start="arr_platform",
                    x_end="dep_platform",
                    y="station_label",
                    color="train_id",
                    hover_data=["train_id", "station_id", "arr_platform", "dep_platform"],
                )
                fig.update_yaxes(autorange="reversed")
                fig.update_layout(height=500, margin=dict(l=10, r=10, t=10, b=10))
                st.plotly_chart(fig, use_container_width=True)
            except Exception:
                st.dataframe(
                    dfp[["train_id", "station_id", "arr_platform", "dep_platform", "platform_slot"]]
                    if "platform_slot" in dfp.columns
                    else dfp[["train_id", "station_id", "arr_platform", "dep_platform"]]
                )
            # Optional station map
            try:
                nodes = load_parquet(base / "section_nodes.parquet")
                if not nodes.empty and set(["lat", "lon"]).issubset(set(nodes.columns)):
                    st.caption("Station map")
                    st.map(nodes.rename(columns={"lat": "latitude", "lon": "longitude"})[["latitude", "longitude"]])
            except Exception:
                pass

    with tb_block:
        if df_block_view.empty:
            st.info("No block occupancy available.")
        else:
            dfb = df_block_view.copy().sort_values("entry_time").head(3000)
            dfb["entry_time"] = pd.to_datetime(dfb["entry_time"])  # type: ignore
            dfb["exit_time"] = pd.to_datetime(dfb["exit_time"])  # type: ignore
            try:
                figb = px.timeline(
                    dfb,
                    x_start="entry_time",
                    x_end="exit_time",
                    y="block_id",
                    color="train_id",
                    hover_data=["train_id", "block_id", "u", "v", "entry_time", "exit_time"],
                )
                figb.update_yaxes(autorange="reversed")
                figb.update_layout(height=500, margin=dict(l=10, r=10, t=10, b=10))
                st.plotly_chart(figb, use_container_width=True)
            except Exception:
                st.dataframe(dfb[["train_id", "block_id", "u", "v", "entry_time", "exit_time"]])

    with tb_heat:
        st.caption("Platform utilization (trains per 15‑min bucket)")
        if df_plat.empty:
            st.info("No platform occupancy available.")
        else:
            try:
                dfx = df_plat.copy().head(5000)  # cap to keep UI responsive
                dfx["arr_platform"] = pd.to_datetime(dfx["arr_platform"])  # type: ignore
                dfx["dep_platform"] = pd.to_datetime(dfx["dep_platform"])  # type: ignore
                # Build bucketed rows
                bucket = "15min"
                dfx["start_b"] = dfx["arr_platform"].dt.floor(bucket)
                dfx["end_b"] = dfx["dep_platform"].dt.ceil(bucket)
                dfx = dfx[dfx["start_b"].notna() & dfx["end_b"].notna()]
                dfx["buckets"] = dfx.apply(lambda r: pd.date_range(r["start_b"], r["end_b"], freq=bucket), axis=1)
                dfe = dfx[["station_id", "buckets"]].explode("buckets").rename(columns={"buckets": "ts"})
                grp = dfe.groupby(["station_id", "ts"]).size().reset_index(name="count")
                grp["ts"] = pd.to_datetime(grp["ts"])  # type: ignore
                heat = (
                    alt.Chart(grp)
                    .mark_rect()
                    .encode(
                        x=alt.X("ts:T", title="Time"),
                        y=alt.Y("station_id:N", title="Station"),
                        color=alt.Color("count:Q", scale=alt.Scale(scheme="viridis")),
                        tooltip=["station_id", "ts", "count"],
                    )
                    .properties(height=500)
                )
                st.altair_chart(heat, use_container_width=True)
            except Exception as e:
                st.error(f"Failed heatmap: {e}")

with t_radar:
    st.subheader("Conflict Radar (0–60 min)")
    if role in ("CREW",):
        st.info("Radar is not available for Crew role.")
    
    if not radar:
        st.info("No risks found in horizon.")
    else:
        sev_counts: Dict[str, int] = {}
        for r in radar:
            sev_counts[r.get("severity", "")] = sev_counts.get(r.get("severity", ""), 0) + 1
        met1, met2 = st.columns(2)
        if sev_counts.get("Critical"):
            met1.error(f"Critical: {sev_counts['Critical']}")
        if sev_counts.get("High"):
            met2.warning(f"High: {sev_counts['High']}")
        # Risk timeline
        if not risk_timeline.empty and "ts_bucket" in risk_timeline.columns:
            rtime = risk_timeline.copy()
            rtime["ts_bucket"] = pd.to_datetime(rtime["ts_bucket"])  # type: ignore
            tl = (
                alt.Chart(rtime)
                .mark_area()
                .encode(
                    x="ts_bucket:T",
                    y="risk_count:Q",
                    color="resource_type:N",
                    tooltip=["ts_bucket", "resource_type", "risk_count"],
                )
                .properties(height=300)
            )
            st.altair_chart(tl, use_container_width=True)
        # Mini-map of risk by station
        try:
            df_radar = pd.DataFrame(radar)
            nodes = load_parquet(base / "section_nodes.parquet")
            if not df_radar.empty and not nodes.empty and set(["station_id", "lat", "lon"]).issubset(set(nodes.columns).union(df_radar.columns)):
                st.caption("Risk mini-map (bubble size = risk count)")
                bys = df_radar.dropna(subset=["station_id"]).groupby("station_id").size().reset_index(name="count") if "station_id" in df_radar.columns else pd.DataFrame()
                if not bys.empty:
                    nn = nodes[["station_id", "lat", "lon"]].drop_duplicates()
                    m = bys.merge(nn, on="station_id", how="inner")
                    figm = px.scatter_geo(m, lat="lat", lon="lon", size="count", color="count", hover_name="station_id", projection="natural earth")
                    figm.update_layout(height=350, margin=dict(l=10, r=10, t=10, b=10))
                    st.plotly_chart(figm, use_container_width=True)
        except Exception:
            pass
        st.caption("Top risks")
        df_radar = pd.DataFrame(radar_view)
        cols = [c for c in ["severity", "type", "block_id", "station_id", "time_window", "train_ids"] if c in df_radar.columns]
        if not df_radar.empty and cols:
            dfv = df_radar[cols].head(100).copy()
            if "severity" in dfv.columns:
                dfv["severity"] = dfv["severity"].map(_sev_badge)
            if "type" in dfv.columns:
                dfv["type"] = dfv["type"].map(_type_badge)
            dfv = dfv.rename(columns={"severity": "Severity", "type": "Type", "block_id": "Block", "station_id": "Station", "time_window": "Window"})
            styled = _style_risks(dfv, sev_col="Severity")
            st.dataframe(styled, use_container_width=True)

with t_reco:
    st.subheader("Recommendations Panel")
    if role in ("CREW",):
        st.info("Recommendation controls are not available for Crew role.")
    elif not rec_plan_view:
        st.info("No recommendations found.")
    else:
        st.caption(f"Plan version: {plan_version}")
        # Summary by type chart
        df_types = pd.DataFrame(rec_plan_view)
        if not df_types.empty and "type" in df_types.columns:
            agg = df_types.groupby("type").size().reset_index(name="count")
            st.altair_chart(alt.Chart(agg).mark_bar().encode(x="type:N", y="count:Q", color="type:N"), use_container_width=True)
        for i, rec in enumerate(rec_plan_view[:100]):
            aid = rec.get("action_id") or sha1_dict(rec)
            title = f"#{i+1} {rec.get('type')} for train {rec.get('train_id')} at {rec.get('at_station') or rec.get('station_id')}"
            with st.expander(title):
                st.write("Why:", rec.get("why"))
                st.write("Reason:", rec.get("reason"))
                # show tradeoffs if available
                if alts:
                    st.caption("Trade-offs available")
                rec_view = dict(rec)
                rec_view["type"] = _action_badge(str(rec_view.get("type")))
                df_one = _df_from(rec_view, columns=["train_id","type","at_station","station_id","block_id","minutes","reason","why"]) 
                # add name labels
                if "station_id" in df_one.columns:
                    df_one["Station"] = df_one["station_id"].map(label_station)
                if "at_station" in df_one.columns:
                    df_one["At"] = df_one["at_station"].map(label_station)
                if "train_id" in df_one.columns:
                    df_one["Train"] = df_one["train_id"].map(label_train)
                df_one = df_one.rename(columns={"type":"Action","block_id":"Block","minutes":"Minutes","reason":"Reason","why":"Why"})
                df_one = df_one[[c for c in ["Train","Action","At","Station","Block","Minutes","Reason","Why"] if c in df_one.columns]]
                st.dataframe(_style_minutes(df_one, col="Minutes"), use_container_width=True)
                c1, c2, c3, c4 = st.columns(4)
                with c1:
                    if role in ("SC", "ADM") and st.button("APPLY", key=f"apply_{i}"):
                        log_feedback(scope, date, rec, "APPLY", user=user, role=role, plan_version=plan_version, action_id=aid, api_host=api_host, api_port=api_port)
                        st.success("Applied feedback logged")
                with c2:
                    if role in ("SC", "ADM") and st.button("DISMISS", key=f"dismiss_{i}"):
                        log_feedback(scope, date, rec, "DISMISS", user=user, role=role, plan_version=plan_version, action_id=aid, api_host=api_host, api_port=api_port)
                        st.success("Dismiss feedback logged")
                with c3:
                    mins = st.number_input("Modify hold (min)", min_value=0.0, max_value=10.0, value=float(rec.get("minutes", 0.0)), key=f"mins_{i}")
                    plat = st.text_input("Platform slot", value=str(rec.get("platform", "")), key=f"plat_{i}")
                    if role in ("SC", "ADM") and st.button("MODIFY", key=f"modify_{i}"):
                        mod = dict(rec)
                        if "minutes" in rec:
                            mod["minutes"] = mins
                        if rec.get("type") == "PLATFORM_REASSIGN":
                            try:
                                mod["platform"] = int(plat)
                            except Exception:
                                mod["platform"] = plat
                        log_feedback(scope, date, rec, "MODIFY", modified=mod, user=user, role=role, plan_version=plan_version, action_id=aid, api_host=api_host, api_port=api_port)
                        st.success("Modify feedback logged")
                with c4:
                    if st.button("DO NOTHING", key=f"noop_{i}"):
                        log_feedback(scope, date, rec, "ACK", user=user, role=role, plan_version=plan_version, action_id=aid, api_host=api_host, api_port=api_port)
                        st.info("No-op acknowledged")

with t_audit:
    st.subheader("Decision Log & Completeness")
    if role in ("CREW",):
        st.info("Audit is not available for Crew role.")
    trail = read_json(base / "audit_trail.json") or []
    st.write(f"Logged decisions: {len(trail)} for plan {plan_version}")
    # Completeness
    completeness = 0.0
    if rec_plan:
        acted = len([e for e in trail if e.get("decision") in ("APPLY", "DISMISS", "MODIFY", "ACK")])
        completeness = acted / len(rec_plan) * 100.0
    st.metric("Feedback completeness (%)", f"{completeness:.1f}")
    if trail:
        df_trail = _df_from(trail[-50:], ["ts","user","role","decision","plan_version","action_id"])  # type: ignore
        df_trail = df_trail.rename(columns={"ts":"Time","user":"User","role":"Role","decision":"Decision","plan_version":"Plan","action_id":"Action"})
        st.dataframe(_style_audit(df_trail, col="Decision"), use_container_width=True)

with t_policy:
    st.subheader("Policy Console (OM/DH)")
    pol = read_json(base / "policy_state.json") or {}
    colp1, colp2 = st.columns(2)
    with colp1:
        weights = st.text_area("priority_weights (json)", value=json.dumps(pol.get("priority_weights", {}), indent=2))
        budgets = st.text_area("hold_budgets (json)", value=json.dumps(pol.get("hold_budgets", {}), indent=2))
        fairness = st.text_area("fairness_limits (json)", value=json.dumps(pol.get("fairness_limits", {}), indent=2))
    with colp2:
        flags = st.text_area("flags (json)", value=json.dumps(pol.get("flags", {}), indent=2))
        sla = st.text_area("solver_SLA (json)", value=json.dumps(pol.get("solver_SLA", {}), indent=2))
        if role in ("OM", "DH", "ADM") and st.button("Save Policy"):
            new_pol = {
                "priority_weights": json.loads(weights or "{}"),
                "hold_budgets": json.loads(budgets or "{}"),
                "fairness_limits": json.loads(fairness or "{}"),
                "flags": json.loads(flags or "{}"),
                "solver_SLA": json.loads(sla or "{}"),
            }
            if st.session_state.get("token"):
                try:
                    resp = requests.put(
                        f"http://{api_host}:{api_port}/policy",
                        params={"scope": scope, "date": date},
                        json=new_pol,
                        headers={"Authorization": f"Bearer {st.session_state['token']}"},
                        timeout=5,
                    )
                    if resp.status_code == 200:
                        st.success("Policy saved via API")
                    else:
                        st.error(f"API policy save failed: {resp.text}")
                except Exception as e:
                    st.error(f"API error: {e}")
            else:
                write_json(base / "policy_state.json", new_pol)
                st.success("Policy saved locally (no auth)")

        st.divider()
        st.subheader("Model Training (Admin/OM/DH)")
        cta1, cta2 = st.columns(2)
        with cta1:
            if st.button("Train Global IL Model"):
                try:
                    resp = requests.post(
                        f"http://{api_host}:{api_port}/admin/train_global",
                        headers={"Authorization": f"Bearer {st.session_state['token']}"} if st.session_state.get("token") else {},
                        timeout=30,
                    )
                    if resp.status_code == 200:
                        st.success("Global model trained")
                        st.dataframe(_df_from(resp.json()), hide_index=True, use_container_width=True)
                    else:
                        st.error(f"Train failed: {resp.text}")
                except Exception as e:
                    st.error(f"API error: {e}")
        with cta2:
            alpha = st.slider("RL penalty alpha", 0.0, 1.0, 0.2, 0.05, key="rl_alpha")
            if st.button("Build Offline RL Dataset"):
                try:
                    resp = requests.post(
                        f"http://{api_host}:{api_port}/admin/build_offline_rl",
                        params={"alpha": float(alpha)},
                        headers={"Authorization": f"Bearer {st.session_state['token']}"} if st.session_state.get("token") else {},
                        timeout=60,
                    )
                    if resp.status_code == 200:
                        st.success("Offline RL dataset built")
                        st.dataframe(_df_from(resp.json()), hide_index=True, use_container_width=True)
                    else:
                        st.error(f"Build failed: {resp.text}")
                except Exception as e:
                    st.error(f"API error: {e}")
        st.markdown("")
        cta3 = st.columns(1)[0]
        with cta3:
            if st.button("Train Offline RL Policy"):
                try:
                    resp = requests.post(
                        f"http://{api_host}:{api_port}/admin/train_offrl",
                        headers={"Authorization": f"Bearer {st.session_state['token']}"} if st.session_state.get("token") else {},
                        timeout=30,
                    )
                    if resp.status_code == 200:
                        st.success("Offline RL policy trained")
                        st.dataframe(_df_from(resp.json()), hide_index=True, use_container_width=True)
                    else:
                        st.error(f"Train failed: {resp.text}")
                except Exception as e:
                    st.error(f"API error: {e}")
        cta5 = st.columns(1)[0]
        with cta5:
            if st.button("Train Torch IL Model"):
                try:
                    resp = requests.post(
                        f"http://{api_host}:{api_port}/admin/train_il_torch",
                        headers={"Authorization": f"Bearer {st.session_state['token']}"} if st.session_state.get("token") else {},
                        timeout=60,
                    )
                    if resp.status_code == 200:
                        st.success("Torch IL model trained")
                        st.json(resp.json())
                    else:
                        st.error(f"Train failed: {resp.text}")
                except Exception as e:
                    st.error(f"API error: {e}")
        st.markdown("")
        cta4 = st.columns(1)[0]
        with cta4:
            if st.button("Run Offline Evaluation"):
                try:
                    resp = requests.get(
                        f"http://{api_host}:{api_port}/admin/eval_offline",
                        headers={"Authorization": f"Bearer {st.session_state['token']}"} if st.session_state.get("token") else {},
                        timeout=30,
                    )
                    if resp.status_code == 200:
                        payload = resp.json()
                        res = payload.get("result", {}) if isinstance(payload, dict) else {}
                        st.success("Offline evaluation complete")
                        # Render leaderboard if available
                        lb = res.get("leaderboard", []) if isinstance(res, dict) else []
                        if lb:
                            st.caption("Leaderboard (by mean_q)")
                            st.dataframe(_df_from(lb), hide_index=True, use_container_width=True)
                        # Render summary
                        st.dataframe(_df_from({k: res.get(k) for k in ("status","n","models")} ), hide_index=True, use_container_width=True)
                    else:
                        st.error(f"Eval failed: {resp.text}")
                except Exception as e:
                    st.error(f"API error: {e}")
        st.divider()
        st.subheader("Recompute plan (what-if)")
        hr = st.number_input("Horizon (min)", min_value=15, max_value=240, value=60, key="policy_horizon")
        mh = st.number_input("Max hold per action (min)", min_value=1, max_value=10, value=3, key="policy_max_hold")
        mhpt = st.number_input("Max holds per train", min_value=1, max_value=5, value=2, key="policy_max_hpt")
        if st.button("Recompute using current policy"):
            try:
                from src.opt.engine import propose
                edges = pd.read_parquet(base / "section_edges.parquet")
                nodes = pd.read_parquet(base / "section_nodes.parquet")
                block = pd.read_parquet(base / "national_block_occupancy.parquet") if (base / "national_block_occupancy.parquet").exists() else pd.read_parquet(base / "block_occupancy.parquet")
                risks = read_json(base / "conflict_radar.json") or []
                prio_map = (json.loads(weights) if weights else {}) if isinstance(weights, str) else {}
                rec, alt, metrics, audit = propose(edges, nodes, block, risks, horizon_min=int(hr), priorities=prio_map, max_hold_min=int(mh), max_holds_per_train=int(mhpt))
                mcols = st.columns(3)
                mcols[0].metric("Actions", f"{int(metrics.get('actions',0))}")
                mcols[1].metric("Conflicts targeted", f"{int(metrics.get('conflicts_targeted',0))}")
                mcols[2].metric("Expected reduction", f"{int(metrics.get('expected_conflict_reduction',0))}")
                st.caption("Preview actions (first 20)")
                # Badge action type
                preview = []
                for r in rec[:20]:
                    x = dict(r)
                    x["type"] = _action_badge(str(x.get("type")))
                    preview.append(x)
                df_prev = _df_from(preview, columns=["train_id","type","at_station","station_id","block_id","minutes","reason"]) 
                if "station_id" in df_prev.columns:
                    df_prev["Station"] = df_prev["station_id"].map(label_station)
                if "at_station" in df_prev.columns:
                    df_prev["At"] = df_prev["at_station"].map(label_station)
                if "train_id" in df_prev.columns:
                    df_prev["Train"] = df_prev["train_id"].map(label_train)
                df_prev = df_prev.rename(columns={"type":"Action","block_id":"Block","minutes":"Minutes","reason":"Reason"})
                df_prev = df_prev[[c for c in ["Train","Action","At","Station","Block","Minutes","Reason"] if c in df_prev.columns]]
                st.dataframe(_style_minutes(df_prev, col="Minutes"), use_container_width=True)
            except Exception as e:
                st.error(f"Failed to recompute: {e}")

with t_crew:
    st.subheader("Crew Instruction Feed")
    # Next-2-stations ETA card
    my_train = st.text_input("Your Train ID (optional)", value="")
    try:
        if my_train and not df_plat.empty:
            dfc = df_plat.copy()
            dfc["arr_platform"] = pd.to_datetime(dfc["arr_platform"])  # type: ignore
            dfc["dep_platform"] = pd.to_datetime(dfc["dep_platform"])  # type: ignore
            now = pd.Timestamp.utcnow().tz_localize("UTC")
            nxt = dfc[(dfc["train_id"].astype(str) == str(my_train)) & (dfc["dep_platform"] >= now)].sort_values("arr_platform").head(2)
            if not nxt.empty:
                st.caption("Next stations (ETA / ETD)")
                nxt = nxt.assign(Station=nxt["station_id"].map(lambda x: label_station(x)))
                st.dataframe(nxt[["Station", "arr_platform", "dep_platform"]], hide_index=True, use_container_width=True)
    except Exception:
        pass
    if not rec_plan:
        st.info("No instructions.")
    else:
        for rec in [r for r in rec_plan if r.get("type") in ("HOLD", "PLATFORM_REASSIGN", "SPEED_TUNE")][:50]:
            aid = rec.get("action_id") or sha1_dict(rec)
            line = _crew_summary(rec)
            c1, c2 = st.columns([3, 1])
            with c1:
                st.write(line)
            with c2:
                if role in ("CREW", "SC", "ADM") and st.button("ACK", key=f"ack_{aid}"):
                    log_feedback(scope, date, rec, "ACK", user=user, role=role, plan_version=plan_version, action_id=aid, api_host=api_host, api_port=api_port)
                    st.success("Acknowledged")

with t_asst:
    st.subheader("Assistant – Ask & Suggest")
    with st.expander("Ask a question", expanded=True):
        q = st.text_input("Question", value="What are today’s risks and OTP?", key="asst_q")
        train_q = st.text_input("Train ID (optional)", value="", key="asst_train_q")
        if st.button("Ask", key="asst_btn_ask"):
            try:
                payload = {"scope": scope, "date": date, "query": q, "train_id": (train_q or None)}
                if role == "SC" and sc_station:
                    payload["station_id"] = sc_station
                headers = {"Authorization": f"Bearer {st.session_state['token']}"} if st.session_state.get("token") else {}
                resp = requests.post(f"http://{api_host}:{api_port}/ai/ask", json=payload, headers=headers, timeout=8)
                if resp.status_code == 200:
                    data = resp.json().get("result", {})
                    st.write(data.get("answer", ""))
                    details = data.get("details")
                    if isinstance(details, list) and details and isinstance(details[0], dict):
                        st.dataframe(_df_from(details), hide_index=True, use_container_width=True)
                    elif isinstance(details, list) and details and isinstance(details[0], str):
                        for item in details:
                            st.write(f"• {item}")
                    elif isinstance(details, dict):
                        _render_kv_table(details)
                else:
                    st.error(f"Ask failed: {resp.text}")
            except Exception as e:
                st.error(f"Ask error: {e}")

    with st.expander("Get suggestions", expanded=True):
        train_s = st.text_input("Train ID (optional; required for CREW)", value="", key="asst_train_s")
        max_hold = st.slider("Max hold (min)", 1, 5, 3, key="asst_max_hold")
        col1, col2 = st.columns(2)
        with col1:
            if st.button("Suggest", key="asst_btn_suggest"):
                try:
                    payload = {"scope": scope, "date": date, "train_id": (train_s or None), "max_hold_min": int(max_hold)}
                    if role == "SC" and sc_station:
                        payload["station_id"] = sc_station
                    headers = {"Authorization": f"Bearer {st.session_state['token']}"} if st.session_state.get("token") else {}
                    resp = requests.post(f"http://{api_host}:{api_port}/ai/suggest", json=payload, headers=headers, timeout=12)
                    if resp.status_code == 200:
                        data = resp.json().get("result", {})
                        src = data.get("source")
                        st.caption(f"Source: {src}")
                        sugg = data.get("suggestions", [])
                        if sugg:
                            view = []
                            for r in sugg[:20]:
                                x = dict(r)
                                x["type"] = _action_badge(str(x.get("type")))
                                view.append(x)
                            df_s = _df_from(view, columns=["train_id","type","at_station","station_id","block_id","minutes","reason","why"]) 
                            if "station_id" in df_s.columns:
                                df_s["Station"] = df_s["station_id"].map(label_station)
                            if "at_station" in df_s.columns:
                                df_s["At"] = df_s["at_station"].map(label_station)
                            if "train_id" in df_s.columns:
                                df_s["Train"] = df_s["train_id"].map(label_train)
                            df_s = df_s.rename(columns={"type":"Action","block_id":"Block","minutes":"Minutes","reason":"Reason","why":"Why"})
                            df_s = df_s[[c for c in ["Train","Action","At","Station","Block","Minutes","Reason","Why"] if c in df_s.columns]]
                            st.dataframe(_style_minutes(df_s, col="Minutes"), use_container_width=True)
                        else:
                            st.info("No suggestions.")
                else:
                    st.error(f"Suggest failed: {resp.text}")
                except Exception as e:
                    st.error(f"Suggest error: {e}")
        with col2:
            st.write("Role:", role)

with t_lab:
    st.subheader("Analyst Lab – Apply & Validate")
    t0 = st.text_input("T0 (ISO UTC, optional)", value="")
    horizon = st.number_input("Horizon (min)", min_value=15, max_value=240, value=60, key="lab_horizon")
    if st.button("Run apply-and-validate on current rec_plan"):
        try:
            from src.sim.apply_plan import apply_and_validate
            events = pd.read_parquet(base / "events_clean.parquet")
            edges = pd.read_parquet(base / "section_edges.parquet")
            nodes = pd.read_parquet(base / "section_nodes.parquet")
            res = apply_and_validate(events, edges, nodes, rec_plan, t0=(t0 or None), horizon_min=int(horizon))
            # KPIs snapshot
            k1, k2, k3 = st.columns(3)
            k1.metric("Risks before", f"{int(res.get('baseline_risks',0))}")
            k2.metric("Risks after", f"{int(res.get('applied_risks',0))}")
            k3.metric("Reduction", f"{int(res.get('risk_reduction',0))}")
            kb, ka = res.get("kpi_before", {}), res.get("kpi_after", {})
            c1, c2 = st.columns(2)
            with c1:
                st.caption("KPI before")
                _render_kv_table(kb)
            with c2:
                st.caption("KPI after")
                _render_kv_table(ka)
            st.success("Validation complete (not saved)")
        except Exception as e:
            st.error(f"Failed: {e}")

with t_admin:
    st.subheader("Admin – User Management")
    if role != "ADM":
        st.info("Admin area is available to ADM role only.")
    else:
        if not st.session_state.get("token"):
            st.warning("Sign in as admin to manage users.")
        else:
            try:
                resp = requests.get(
                    f"http://{api_host}:{api_port}/admin/users",
                    headers={"Authorization": f"Bearer {st.session_state['token']}"},
                    timeout=5,
                )
                if resp.status_code == 200:
                    users = resp.json().get("users", [])
                    st.table(users)
                else:
                    st.error(f"List users failed: {resp.text}")
            except Exception as e:
                st.error(f"API error: {e}")
            st.divider()
            st.write("Create user")
            cu1, cu2, cu3 = st.columns(3)
            with cu1:
                nu = st.text_input("Username", key="nu")
            with cu2:
                npw = st.text_input("Password", key="npw")
            with cu3:
                nrole = st.selectbox("Role", ["SC", "CREW", "OM", "DH", "AN", "ADM"], key="nr")
            if st.button("Add user"):
                try:
                    resp = requests.post(
                        f"http://{api_host}:{api_port}/admin/users",
                        json={"username": nu, "password": npw, "role": nrole},
                        headers={"Authorization": f"Bearer {st.session_state['token']}"},
                        timeout=5,
                    )
                    if resp.status_code == 200:
                        st.success("User created")
                    else:
                        st.error(f"Create failed: {resp.text}")
                except Exception as e:
                    st.error(f"API error: {e}")
            st.divider()
            st.write("Change role")
            cr1, cr2 = st.columns(2)
            with cr1:
                cr_user = st.text_input("Username", key="cr_u")
            with cr2:
                cr_role = st.selectbox("New role", ["SC", "CREW", "OM", "DH", "AN", "ADM"], key="cr_r")
            if st.button("Update role"):
                try:
                    resp = requests.put(
                        f"http://{api_host}:{api_port}/admin/users/{cr_user}/role",
                        json={"role": cr_role},
                        headers={"Authorization": f"Bearer {st.session_state['token']}"},
                        timeout=5,
                    )
                    if resp.status_code == 200:
                        st.success("Role updated")
                    else:
                        st.error(f"Update failed: {resp.text}")
                except Exception as e:
                    st.error(f"API error: {e}")

# Auto-refresh if enabled
if refresh and refresh > 0:
    import time
    time.sleep(refresh)
    _safe_rerun()
# Name lookups (stations/trains)
def _station_name_map() -> dict[str, str]:
    m: dict[str, str] = {}
    try:
        # Prefer artifacts stations.json
        sj = read_json(base / "stations.json")
        if isinstance(sj, list):
            for x in sj:
                sid = str(x.get("station_id")) if x.get("station_id") is not None else None
                nm = x.get("name") or x.get("station_name")
                if sid and nm:
                    m[sid] = str(nm)
        elif isinstance(sj, dict):
            for sid, nm in sj.items():
                m[str(sid)] = str(nm)
    except Exception:
        pass
    # Fallback to repo station_map.csv
    try:
        import os
        from pathlib import Path as _P
        smp = _P("src/data/station_map.csv")
        if smp.exists():
            import pandas as _pd
            dfm = _pd.read_csv(smp)
            if not dfm.empty and {"station_id","name"}.issubset(dfm.columns):
                for _, r in dfm.iterrows():
                    m[str(r["station_id"])]= str(r["name"]) 
    except Exception:
        pass
    return m


def _train_name_map() -> dict[str, str]:
    m: dict[str, str] = {}
    # Try events_clean for train_name
    try:
        ev = load_parquet(base / "events_clean.parquet")
        if not ev.empty and "train_id" in ev.columns and any(c in ev.columns for c in ["train_name","Train Name","name"]):
            name_col = next(c for c in ["train_name","Train Name","name"] if c in ev.columns)
            g = ev.dropna(subset=["train_id"]).drop_duplicates(subset=["train_id"]) [["train_id", name_col]]
            for _, r in g.iterrows():
                m[str(r["train_id"])]= str(r[name_col])
    except Exception:
        pass
    # Fallback raw CSV
    try:
        import pandas as _pd
        raw = _pd.read_csv("data/raw/Train_details.csv")
        if not raw.empty and set(["Train No","Train Name"]).issubset(raw.columns):
            for _, r in raw.drop_duplicates(subset=["Train No"]).iterrows():
                m[str(r["Train No"])]= str(r["Train Name"]) 
    except Exception:
        pass
    return m


STATION_NAME = _station_name_map()
TRAIN_NAME = _train_name_map()

def label_station(sid: Any) -> str:
    s = str(sid)
    nm = STATION_NAME.get(s)
    return f"{nm} ({s})" if nm else s

def label_train(tid: Any) -> str:
    t = str(tid)
    nm = TRAIN_NAME.get(t)
    return f"{nm} ({t})" if nm else t
