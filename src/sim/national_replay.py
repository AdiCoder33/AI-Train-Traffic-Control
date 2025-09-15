"""Nationwide event-driven replay simulator (baseline safety).

Enforces:
- Block capacity and headway per block (one-train-per-track with headway).
- Platform capacity per station and minimum dwell time.

Inputs:
- events_df: all-India events (train x station) with sched/actual times, stop_seq.
- graph: SectionGraph with nodes (platforms, min_dwell) and edges (min_run_time, headway, capacity).

Outputs:
- block_occupancy: per train x block entry/exit with holds applied.
- platform_occupancy: per train x station dwell windows.
- waiting_ledger: holds with reason (block/headway/platform) and durations.
- sim_kpis: OTP%, avg/p90 delays at last station; trains_served; total wait minutes by reason.

Note: This is a baseline replay (no optimization). It respects actual times
when present (never departs/arrives earlier than actual).
"""

from __future__ import annotations

from dataclasses import dataclass
import heapq
from typing import Dict, List, Tuple, Optional

import pandas as pd

from src.model.section_graph import SectionGraph

__all__ = ["SimResult", "run", "save"]


@dataclass
class SimResult:
    block_occupancy: pd.DataFrame
    platform_occupancy: pd.DataFrame
    waiting_ledger: pd.DataFrame
    sim_kpis: Dict[str, float]


def _to_utc(s: pd.Series | None) -> pd.Series:
    if s is None:
        return pd.Series(dtype="datetime64[ns, UTC]")
    return pd.to_datetime(s, utc=True, errors="coerce")


def _sort_group(grp: pd.DataFrame) -> pd.DataFrame:
    # Prefer stop_seq; else by best available time
    if "stop_seq" in grp.columns and grp["stop_seq"].notna().any():
        try:
            return grp.sort_values("stop_seq")
        except Exception:
            pass
    # Fallback: by departure then arrival
    dep = _to_utc(grp.get("act_dep")).fillna(_to_utc(grp.get("sched_dep")))
    arr = _to_utc(grp.get("act_arr")).fillna(_to_utc(grp.get("sched_arr")))
    tmp = grp.assign(__dep__=dep, __arr__=arr)
    tmp = tmp.sort_values(["__dep__", "__arr__"]).drop(columns=["__dep__", "__arr__"])
    return tmp


def _itinerary_from_events(events_df: pd.DataFrame, graph: SectionGraph) -> Dict[str, List[Tuple[str, str]]]:
    itins: Dict[str, List[Tuple[str, str]]] = {}
    for tid, grp0 in events_df.groupby("train_id", sort=False):
        grp = _sort_group(grp0)
        stops = grp["station_id"].tolist()
        hops: List[Tuple[str, str]] = []
        for i in range(len(stops) - 1):
            u, v = stops[i], stops[i + 1]
            if (u, v) in graph.pair_to_block:
                hops.append((u, v))
        if hops:
            itins[tid] = hops
    return itins


def run(
    events_df: pd.DataFrame,
    graph: SectionGraph,
    *,
    per_train_speed: Dict[tuple, float] | None = None,
    platform_override: Dict[tuple, int] | None = None,
) -> SimResult:
    # Normalize time columns
    df = events_df.copy()
    for c in ("sched_arr", "sched_dep", "act_arr", "act_dep"):
        if c in df.columns:
            df[c] = _to_utc(df[c])

    itins = _itinerary_from_events(df, graph)

    # Resource heaps: block availability times per track; platform availability per slot
    block_heap: Dict[str, List[pd.Timestamp]] = {}
    for bid, (_, _, cap) in graph.block_attr.items():
        block_heap[bid] = [pd.Timestamp.min.tz_localize("UTC")] * max(1, cap)

    # Platform availability per slot (track slot index for reassignment)
    plat_avail: Dict[str, List[pd.Timestamp]] = {}
    for sid, (cap, _, _) in graph.station_attr.items():
        plat_avail[sid] = [pd.Timestamp.min.tz_localize("UTC")] * max(1, cap)

    block_records: List[dict] = []
    platform_records: List[dict] = []
    waits: List[dict] = []

    def _alloc_heap(heap: List[pd.Timestamp], t_req: pd.Timestamp) -> Tuple[pd.Timestamp, float]:
        t_avail = heapq.heappop(heap)
        start = max(t_req, t_avail)
        wait_min = max(0.0, (start - t_req).total_seconds() / 60.0)
        return start, wait_min

    def _alloc_platform(station_id: str, t_req: pd.Timestamp, dwell_min: float, *, slot_idx: Optional[int] = None) -> Tuple[pd.Timestamp, pd.Timestamp, int, float]:
        slots = plat_avail[station_id]
        if slot_idx is None or not (0 <= slot_idx < len(slots)):
            slot_idx = min(range(len(slots)), key=lambda i: slots[i])
        start = max(t_req, slots[slot_idx])
        wait_min = max(0.0, (start - t_req).total_seconds() / 60.0)
        dep = start + pd.Timedelta(minutes=dwell_min)
        slots[slot_idx] = dep
        plat_avail[station_id] = slots
        return start, dep, slot_idx, wait_min

    # Iterate trains in chronological order of their initial departure
    # Robust initial time per train: earliest of any known time columns
    t0 = _to_utc(df.get("act_dep"))
    for c in ("sched_dep", "act_arr", "sched_arr"):
        ser = _to_utc(df.get(c))
        if len(t0) == 0:
            t0 = ser
        else:
            t0 = t0.fillna(ser)
    dep0 = t0.groupby(df["train_id"]).min()
    for train_id in dep0.sort_values().index:
        hops = itins.get(train_id)
        if not hops:
            continue
        grp = _sort_group(df[df["train_id"] == train_id])
        # Deduplicate by station to ensure scalar lookups per station_id
        grp_unique = grp.drop_duplicates(subset=["station_id"], keep="first")
        # Station-wise schedule/actual for departures and arrivals (scalar maps)
        sched_dep_map = _to_utc(grp_unique.set_index("station_id")["sched_dep"]) if "sched_dep" in grp_unique.columns else pd.Series(dtype="datetime64[ns, UTC]")
        act_dep_map = _to_utc(grp_unique.set_index("station_id")["act_dep"]) if "act_dep" in grp_unique.columns else pd.Series(dtype="datetime64[ns, UTC]")
        sched_arr_map = _to_utc(grp_unique.set_index("station_id")["sched_arr"]) if "sched_arr" in grp_unique.columns else pd.Series(dtype="datetime64[ns, UTC]")
        act_arr_map = _to_utc(grp_unique.set_index("station_id")["act_arr"]) if "act_arr" in grp_unique.columns else pd.Series(dtype="datetime64[ns, UTC]")

        # Initialize at first station u
        u0 = hops[0][0]
        # Station attributes tuple: (platforms, min_dwell, route_setup)
        platforms_u, dwell_u, _route_setup = graph.station_attr.get(u0, (1, 2.0, 0.5))
        # Arrival at origin (use actual or sched); departure request (actual/sched)
        arr0 = act_arr_map.get(u0, pd.NaT)
        if pd.isna(arr0):
            arr0 = sched_arr_map.get(u0, pd.NaT)
        dep_sched0 = act_dep_map.get(u0, pd.NaT)
        if pd.isna(dep_sched0):
            dep_sched0 = sched_dep_map.get(u0, pd.NaT)
        if pd.isna(arr0) and pd.notna(dep_sched0):
            arr0 = dep_sched0 - pd.Timedelta(minutes=dwell_u)
        if pd.isna(dep_sched0) and pd.notna(arr0):
            dep_sched0 = arr0 + pd.Timedelta(minutes=dwell_u)

        # Allocate platform dwell at origin using per-slot availability (override supported)
        t_origin_req = arr0 if pd.notna(arr0) else dep_sched0
        ov_slot_u = None
        if platform_override is not None:
            ov_slot_u = platform_override.get((str(train_id), str(u0)))
        start_plat_u, dwell_end_u, slot_u, wait_plat0 = _alloc_platform(u0, t_origin_req, float(dwell_u), slot_idx=ov_slot_u)
        # Requested departure time respects dwell and schedule
        t_req_dep = max(dep_sched0, dwell_end_u) if pd.notna(dep_sched0) else dwell_end_u
        if wait_plat0 > 0:
            waits.append({"train_id": train_id, "resource": "platform", "id": u0, "start_time": str(arr0), "end_time": str(start_plat_u), "minutes": wait_plat0, "reason": "platform_busy"})
        # Record platform occupancy window at origin
        platform_records.append({"train_id": train_id, "station_id": u0, "arr_platform": start_plat_u, "dep_platform": t_req_dep, "platform_slot": int(slot_u)})

        current_time = t_req_dep
        for (u, v) in hops:
            bid = graph.pair_to_block[(u, v)]
            min_run, headway, capacity = graph.block_attr[bid]
            # Prefer observed run if both endpoints have actual times
            arr_v_act = act_arr_map.get(v, pd.NaT)
            dep_u_act = act_dep_map.get(u, pd.NaT)
            run_min = None
            if pd.notna(arr_v_act) and pd.notna(dep_u_act):
                run_min = max(0.0, (arr_v_act - dep_u_act).total_seconds() / 60.0)
            if run_min is None:
                run_min = float(min_run)
            # Apply optional speed tuning (factor < 1.0 reduces run time)
            if per_train_speed is not None:
                key = (str(train_id), str(bid))
                if key in per_train_speed:
                    fac = float(per_train_speed[key])
                    fac = max(0.8, min(1.0, fac))
                    run_min = run_min * fac

            # Request block at current_time
            entry, wait_block = _alloc_heap(block_heap[bid], current_time)
            if wait_block > 0:
                waits.append({"train_id": train_id, "resource": "block", "id": bid, "start_time": str(current_time), "end_time": str(entry), "minutes": wait_block, "reason": "block_or_headway"})

            exit_time = entry + pd.Timedelta(minutes=run_min)
            # After exiting block, the track is available only after headway
            heapq.heappush(block_heap[bid], exit_time + pd.Timedelta(minutes=float(headway)))
            # Record block occupancy
            block_records.append({
                "train_id": train_id,
                "u": u,
                "v": v,
                "block_id": bid,
                "entry_time": entry,
                "exit_time": exit_time,
                "headway_applied_min": wait_block,
            })

            # Arrival at v cannot be earlier than actual arrival if present
            if pd.notna(arr_v_act) and exit_time < arr_v_act:
                exit_time = arr_v_act

            # Platform at v with route setup time
            platforms_v, dwell_v, route_setup = graph.station_attr.get(v, (1, 2.0, 0.5))
            arr_sched_v = sched_arr_map.get(v, pd.NaT)
            dep_sched_v = act_dep_map.get(v, pd.NaT)
            if pd.isna(dep_sched_v):
                dep_sched_v = sched_dep_map.get(v, pd.NaT)

            # Allocate platform at v with override slot if provided
            ov_slot_v = None
            if platform_override is not None:
                ov_slot_v = platform_override.get((str(train_id), str(v)))
            # Route setup approximated as additional readiness lag before platform entry
            start_req = exit_time + pd.Timedelta(minutes=float(route_setup))
            start_plat_v, dwell_end_v, slot_v, wait_plat_v = _alloc_platform(v, start_req, float(dwell_v), slot_idx=ov_slot_v)
            if wait_plat_v > 0:
                waits.append({"train_id": train_id, "resource": "platform", "id": v, "start_time": str(exit_time), "end_time": str(start_plat_v), "minutes": wait_plat_v, "reason": "platform_busy_or_route"})
            # Respect scheduled/actual departure at v if exists
            next_dep_req = dwell_end_v
            if pd.notna(dep_sched_v):
                if next_dep_req < dep_sched_v:
                    next_dep_req = dep_sched_v

            # Record platform occupancy at v
            platform_records.append({"train_id": train_id, "station_id": v, "arr_platform": start_plat_v, "dep_platform": next_dep_req, "platform_slot": int(slot_v)})

            current_time = next_dep_req

    # Build DataFrames
    df_blocks = pd.DataFrame(block_records)
    df_plats = pd.DataFrame(platform_records)
    df_waits = pd.DataFrame(waits)

    # KPIs: compute delay at last station per train
    kpi = {
        "trains_served": int(df_blocks["train_id"].nunique()) if not df_blocks.empty else 0,
    }
    if not df_blocks.empty and not df_plats.empty:
        last_dep = df_plats.sort_values(["train_id", "dep_platform"]).groupby("train_id").tail(1)
        # Use a de-duplicated (train_id, station_id) mapping for scheduled arrival
        df_unique = df.drop_duplicates(subset=["train_id", "station_id"], keep="first")
        last_station = last_dep.set_index("train_id")["station_id"]
        # Build a unique multi-index Series for scheduled arrival
        sched_arr_map_all = _to_utc(df_unique.set_index(["train_id", "station_id"]) ["sched_arr"])  # type: ignore
        # Align indices to (train_id, last_station)
        idx = pd.MultiIndex.from_arrays([
            last_station.index,  # train_id
            last_station.values,  # station_id
        ], names=["train_id", "station_id"])  # type: ignore
        sched_arr_last = sched_arr_map_all.reindex(idx)
        delay = None
        if sched_arr_last is not None:
            delay = (last_dep.set_index("train_id")["dep_platform"] - sched_arr_last).dt.total_seconds() / 60
        if delay is not None:
            kpi.update({
                "otp_exit_pct": float((delay.le(5).mean() * 100.0) if len(delay) else 0.0),
                "avg_exit_delay_min": float(delay.mean(skipna=True) if len(delay) else 0.0),
                "p90_exit_delay_min": float(delay.quantile(0.9) if len(delay) else 0.0),
            })
        else:
            kpi.update({"otp_exit_pct": 0.0, "avg_exit_delay_min": 0.0, "p90_exit_delay_min": 0.0})

    # Totals by reason
    if not df_waits.empty:
        total_wait = float(df_waits["minutes"].sum())
        kpi["total_wait_min"] = total_wait
        for reason, grp in df_waits.groupby("reason"):
            kpi[f"wait_min_{reason}"] = float(grp["minutes"].sum())
    else:
        kpi["total_wait_min"] = 0.0

    return SimResult(df_blocks, df_plats, df_waits, kpi)


def save(result: SimResult, out_dir: str | "PathLike[str]") -> None:
    from pathlib import Path

    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    result.block_occupancy.to_parquet(out / "national_block_occupancy.parquet", index=False)
    result.platform_occupancy.to_parquet(out / "national_platform_occupancy.parquet", index=False)
    result.waiting_ledger.to_parquet(out / "national_waiting_ledger.parquet", index=False)
    import json
    (out / "national_sim_kpis.json").write_text(json.dumps(result.sim_kpis, indent=2))
