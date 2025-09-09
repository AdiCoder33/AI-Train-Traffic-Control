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

tab_names = ["Overview", "Board", "Radar", "Recommendations", "Audit", "Policy", "Crew", "Lab", "Admin"]
t_over, t_board, t_radar, t_reco, t_audit, t_policy, t_crew, t_lab, t_admin = st.tabs(tab_names)

# Sticky banners for critical/high risks
if radar:
    sev_counts: Dict[str, int] = {}
    for r in radar:
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
    if radar:
        sev_counts: Dict[str, int] = {}
        for r in radar:
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
        if df_plat.empty:
            st.info("No platform occupancy available.")
        else:
            dfp = df_plat.copy().sort_values("arr_platform").head(2000)
            dfp["arr_platform"] = pd.to_datetime(dfp["arr_platform"])  # type: ignore
            dfp["dep_platform"] = pd.to_datetime(dfp["dep_platform"])  # type: ignore
            # Plotly Gantt timeline
            try:
                fig = px.timeline(
                    dfp,
                    x_start="arr_platform",
                    x_end="dep_platform",
                    y="station_id",
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
        if df_block.empty:
            st.info("No block occupancy available.")
        else:
            dfb = df_block.copy().sort_values("entry_time").head(3000)
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
        df_radar = pd.DataFrame(radar)
        cols = [c for c in ["severity", "type", "block_id", "station_id", "time_window", "train_ids"] if c in df_radar.columns]
        if not df_radar.empty and cols:
            st.dataframe(df_radar[cols].head(100))

with t_reco:
    st.subheader("Recommendations Panel")
    if role in ("CREW",):
        st.info("Recommendation controls are not available for Crew role.")
    elif not rec_plan:
        st.info("No recommendations found.")
    else:
        st.caption(f"Plan version: {plan_version}")
        # Summary by type chart
        df_types = pd.DataFrame(rec_plan)
        if not df_types.empty and "type" in df_types.columns:
            agg = df_types.groupby("type").size().reset_index(name="count")
            st.altair_chart(alt.Chart(agg).mark_bar().encode(x="type:N", y="count:Q", color="type:N"), use_container_width=True)
        for i, rec in enumerate(rec_plan[:100]):
            aid = rec.get("action_id") or sha1_dict(rec)
            title = f"#{i+1} {rec.get('type')} for train {rec.get('train_id')} at {rec.get('at_station') or rec.get('station_id')}"
            with st.expander(title):
                st.write("Why:", rec.get("why"))
                st.write("Reason:", rec.get("reason"))
                # show tradeoffs if available
                if alts:
                    st.caption("Trade-offs available")
                st.json(rec)
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
        st.json(trail[-50:])

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
                st.json({"metrics": metrics, "audit": audit})
                st.write("Preview actions (first 20):")
                st.json(rec[:20])
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
                st.dataframe(nxt[["station_id", "arr_platform", "dep_platform"]])
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
            st.json(res)
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
