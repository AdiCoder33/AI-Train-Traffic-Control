"""Block-level occupancy, safety, and KPI utilities.

This module turns station-level train events into a block-level view
for a chosen corridor and day. A "block" is treated as a directed hop
between consecutive corridor stations (u->v) with a stable ``block_id``.

Primary functions
-----------------
- build: create per-train block occupancy windows, detect pre-headway
  conflicts, and enforce headway safety.
- snapshot: generate a virtual nowcast snapshot at a timestamp ``t``.
- save: persist artifacts under the standard artifact hierarchy.

Assumptions
-----------
- Single-track safety: one train per block at a time.
- Successor must respect headway_min after predecessor clears.
- Time is UTC and tz-aware. If actual is missing, fall back to schedule.
  If one endpoint is missing, infer the other using ``min_run_time``.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple, Optional

import json
import pandas as pd

__all__ = [
    "build",
    "snapshot",
    "save",
]


@dataclass
class BuildResult:
    occupancy: pd.DataFrame  # post-headway times
    occupancy_raw: pd.DataFrame  # pre-headway times
    conflicts_pre: Dict[str, object]
    kpis: Dict[str, float]
    log: Dict[str, object]


def _to_utc(series: pd.Series | None) -> pd.Series:
    if series is None:
        return pd.Series(dtype="datetime64[ns, UTC]")
    return pd.to_datetime(series, utc=True, errors="coerce")


def _preferred(series_list: List[Optional[pd.Series]], *, index: Optional[pd.Index] = None) -> pd.Series:
    """Return first-non-null across a list of timestamp series.

    Ensures tz-aware dtype.
    """
    if not series_list:
        # Default empty series; caller should pass index where needed
        return pd.Series(dtype="datetime64[ns, UTC]")

    # Determine target index: explicit index param, or first non-None series
    if index is None:
        for s in series_list:
            if s is not None:
                index = s.index
                break
    if index is None:
        # Fall back to empty series
        return pd.Series(dtype="datetime64[ns, UTC]")

    out = pd.Series(pd.NaT, index=index, dtype="datetime64[ns, UTC]")
    for s in series_list:
        if s is None:
            continue
        out = out.fillna(_to_utc(s))
    return out


def _sort_group(grp: pd.DataFrame) -> pd.DataFrame:
    # Prefer numeric stop sequence if available; else by time reference
    if "stop_seq" in grp.columns and grp["stop_seq"].notna().any():
        try:
            seq = pd.to_numeric(grp["stop_seq"], errors="coerce")
            return grp.sort_values(["stop_seq", "station_id"])  # stable tie-breaker
        except Exception:
            pass
    dep_ref = _preferred([grp.get("act_dep"), grp.get("sched_dep"), grp.get("act_arr"), grp.get("sched_arr")], index=grp.index)
    arr_ref = _preferred([grp.get("act_arr"), grp.get("sched_arr"), grp.get("act_dep"), grp.get("sched_dep")], index=grp.index)
    tmp = grp.assign(__dep_ref__=dep_ref, __arr_ref__=arr_ref)
    tmp = tmp.sort_values(["__dep_ref__", "__arr_ref__", "station_id"])  # deterministic
    return tmp.drop(columns=["__dep_ref__", "__arr_ref__"])  # clean up


def build(
    df_slice: pd.DataFrame, edges_df: pd.DataFrame
) -> BuildResult:
    """Build block occupancy and KPIs from station-level events.

    Parameters
    ----------
    df_slice:
        Per train×station events with columns:
        ``train_id``, ``station_id``, ``sched_arr``, ``sched_dep``,
        ``act_arr``, ``act_dep`` (tz-aware) and optionally ``stop_seq``.
    edges_df:
        Section edges with columns ``u``, ``v``, ``block_id``, ``min_run_time`` (min),
        and ``headway`` (min).

    Returns
    -------
    BuildResult
        Occupancy pre/post headway, pre-headway conflicts summary, KPIs, and a log summary.
    """

    if df_slice.empty or edges_df.empty:
        empty = pd.DataFrame(
            columns=[
                "train_id",
                "u",
                "v",
                "block_id",
                "entry_time",
                "exit_time",
                "entry_time_raw",
                "exit_time_raw",
                "delay_min",
                "headway_applied_min",
                "source",
            ]
        )
        return BuildResult(
            occupancy=empty.copy(),
            occupancy_raw=empty.copy(),
            conflicts_pre={"count": 0, "samples": []},
            kpis={
                "otp_exit_pct": 0.0,
                "avg_exit_delay_min": 0.0,
                "p90_exit_delay_min": 0.0,
                "conflicts_pre_headway": 0,
                "trains_served": 0,
            },
            log={
                "hops_total": 0,
                "hops_kept": 0,
                "hops_skipped_missing_edge": 0,
                "windows_built": 0,
                "windows_inferred": 0,
                "source_counts": {},
                "headway_shifts_applied": 0,
                "headway_total_min": 0.0,
            },
        )

    # Edge lookups
    edges = edges_df[["u", "v", "block_id", "min_run_time", "headway"]].copy()
    edges["min_run_time"] = pd.to_numeric(edges["min_run_time"], errors="coerce").fillna(0.0)
    edges["headway"] = pd.to_numeric(edges["headway"], errors="coerce").fillna(0.0)
    key_to_edge = {
        (row["u"], row["v"]): (row["block_id"], float(row["min_run_time"]), float(row["headway"]))
        for _, row in edges.iterrows()
    }

    # Iterate per train and build hops
    hops: List[dict] = []
    hops_total = 0
    hops_skipped_missing_edge = 0
    windows_inferred = 0
    source_counter: Dict[str, int] = {"actual": 0, "scheduled": 0, "hybrid": 0, "inferred": 0}

    # Ensure datetime dtype present for consistent operations
    df = df_slice.copy()
    for c in ("sched_arr", "sched_dep", "act_arr", "act_dep"):
        if c in df.columns:
            df[c] = _to_utc(df[c])

    for train_id, grp0 in df.groupby("train_id", sort=False):
        grp = _sort_group(grp0)
        if len(grp) < 2:
            continue
        for i in range(len(grp) - 1):
            hops_total += 1
            u_row = grp.iloc[i]
            v_row = grp.iloc[i + 1]
            u = u_row.get("station_id")
            v = v_row.get("station_id")
            edge = key_to_edge.get((u, v))
            if edge is None:
                hops_skipped_missing_edge += 1
                continue
            block_id, min_rt, hw = edge

            # Build entry/exit with fallbacks
            dep_act = u_row.get("act_dep")
            dep_sch = u_row.get("sched_dep")
            arr_act = v_row.get("act_arr")
            arr_sch = v_row.get("sched_arr")

            entry = dep_act if pd.notna(dep_act) else dep_sch
            exit_ = arr_act if pd.notna(arr_act) else arr_sch

            dep_src = "actual" if pd.notna(dep_act) else ("scheduled" if pd.notna(dep_sch) else None)
            arr_src = "actual" if pd.notna(arr_act) else ("scheduled" if pd.notna(arr_sch) else None)

            inferred = False
            if pd.isna(entry) and pd.notna(exit_):
                entry = exit_ - pd.Timedelta(minutes=min_rt)
                inferred = True
            elif pd.isna(exit_) and pd.notna(entry):
                exit_ = entry + pd.Timedelta(minutes=min_rt)
                inferred = True
            elif pd.isna(entry) and pd.isna(exit_):
                # Cannot build a window without any endpoint
                continue

            if inferred:
                windows_inferred += 1
                source = "inferred"
            else:
                if dep_src == "actual" and arr_src == "actual":
                    source = "actual"
                elif dep_src == "scheduled" and arr_src == "scheduled":
                    source = "scheduled"
                else:
                    source = "hybrid"

            source_counter[source] = source_counter.get(source, 0) + 1

            # Compute delay vs scheduled arrival at v when available
            delay_min = None
            if pd.notna(arr_sch) and pd.notna(exit_):
                delay_min = (exit_ - arr_sch).total_seconds() / 60.0

            observed_run_min = None
            if pd.notna(entry) and pd.notna(exit_):
                observed_run_min = (exit_ - entry).total_seconds() / 60.0

            hops.append(
                {
                    "train_id": train_id,
                    "u": u,
                    "v": v,
                    "block_id": block_id,
                    "entry_time_raw": entry,
                    "exit_time_raw": exit_,
                    "headway_min": hw,
                    "min_run_time_min": min_rt,
                    "observed_run_min": observed_run_min,
                    "sched_arr_v": arr_sch,
                    "source": source,
                    "delay_min_raw": delay_min,
                    "exit_src": arr_src,
                }
            )

    if not hops:
        empty_df = pd.DataFrame(
            columns=[
                "train_id",
                "u",
                "v",
                "block_id",
                "entry_time",
                "exit_time",
                "entry_time_raw",
                "exit_time_raw",
                "delay_min",
                "headway_applied_min",
                "source",
            ]
        )
        conflicts = {"count": 0, "samples": []}
        kpis = {
            "otp_exit_pct": 0.0,
            "avg_exit_delay_min": 0.0,
            "p90_exit_delay_min": 0.0,
            "conflicts_pre_headway": 0,
            "trains_served": 0,
        }
        log = {
            "hops_total": hops_total,
            "hops_kept": 0,
            "hops_skipped_missing_edge": hops_skipped_missing_edge,
            "windows_built": 0,
            "windows_inferred": windows_inferred,
            "source_counts": source_counter,
            "headway_shifts_applied": 0,
            "headway_total_min": 0.0,
        }
        return BuildResult(empty_df, empty_df.copy(), conflicts, kpis, log)

    occ_raw = pd.DataFrame(hops)

    # Filter invalid rows with missing endpoints
    occ_raw = occ_raw.dropna(subset=["entry_time_raw", "exit_time_raw"]).copy()

    # Detect pre-headway conflicts: overlapping [entry, exit)
    conflicts_samples: List[dict] = []
    conflict_count = 0
    for bid, grp in occ_raw.groupby("block_id"):
        grp_sorted = grp.sort_values("entry_time_raw")
        prev_exit = None
        prev_row = None
        for _, row in grp_sorted.iterrows():
            entry = row["entry_time_raw"]
            exit_ = row["exit_time_raw"]
            if pd.isna(entry) or pd.isna(exit_):
                continue
            if prev_exit is not None and entry < prev_exit:
                conflict_count += 1
                if len(conflicts_samples) < 25:
                    overlap_start = entry
                    overlap_end = min(prev_exit, exit_)
                    conflicts_samples.append(
                        {
                            "block_id": bid,
                            "u": row["u"],
                            "v": row["v"],
                            "train_prev": prev_row["train_id"],
                            "train_next": row["train_id"],
                            "overlap_start": str(overlap_start),
                            "overlap_end": str(overlap_end),
                            "overlap_min": max(
                                0.0,
                                (pd.to_datetime(overlap_end) - pd.to_datetime(overlap_start)).total_seconds() / 60.0,
                            ),
                        }
                    )
            if pd.notna(exit_):
                prev_exit = exit_
                prev_row = row

    conflicts = {"count": int(conflict_count), "samples": conflicts_samples}

    # Enforce headway: successor.entry >= predecessor.exit + headway
    def _run_duration(row: pd.Series) -> float:
        if pd.notna(row.get("observed_run_min")):
            return float(row["observed_run_min"])  # prefer observed
        return float(row.get("min_run_time_min", 0.0))

    occ_post = occ_raw.copy()
    occ_post["entry_time"] = occ_post["entry_time_raw"]
    occ_post["exit_time"] = occ_post["exit_time_raw"]
    occ_post["headway_applied_min"] = 0.0

    total_headway_min = 0.0
    shifts_applied = 0
    # process per block in entry order
    for bid, grp in occ_post.groupby("block_id"):
        order = grp.sort_values("entry_time")
        prev_clear = None
        idxs = order.index.tolist()
        for idx in idxs:
            row = occ_post.loc[idx]
            run_min = _run_duration(row)
            if pd.isna(row["entry_time"]):
                continue
            if prev_clear is not None:
                required_entry = prev_clear + pd.Timedelta(minutes=float(row["headway_min"]))
                if row["entry_time"] < required_entry:
                    new_entry = required_entry
                    delta_min = (new_entry - row["entry_time"]).total_seconds() / 60.0
                    occ_post.at[idx, "entry_time"] = new_entry
                    occ_post.at[idx, "exit_time"] = new_entry + pd.Timedelta(minutes=run_min)
                    occ_post.at[idx, "headway_applied_min"] = delta_min
                    total_headway_min += max(0.0, float(delta_min))
                    shifts_applied += 1
                else:
                    # keep exit consistent with (potential) observed duration
                    occ_post.at[idx, "exit_time"] = row["entry_time"] + pd.Timedelta(minutes=run_min)
            else:
                # first in block: recompute exit using run duration for consistency
                occ_post.at[idx, "exit_time"] = row["entry_time"] + pd.Timedelta(minutes=run_min)

            prev_clear = occ_post.at[idx, "exit_time"]

    # Validate: after headway enforcement there should be no overlaps per block
    for bid, grp in occ_post.groupby("block_id"):
        grp_sorted = grp.sort_values("entry_time")
        prev_exit = None
        for _, row in grp_sorted.iterrows():
            entry = row["entry_time"]
            exit_ = row["exit_time"]
            if pd.isna(entry) or pd.isna(exit_):
                continue
            if prev_exit is not None and entry < prev_exit:
                raise ValueError(f"Headway enforcement failed for block {bid}")
            prev_exit = exit_

    # Compute post-headway delay vs sched at corridor exit (per train last hop)
    occ_last = (
        occ_post.sort_values(["train_id", "entry_time"]).groupby("train_id").tail(1)
    )
    # Prefer actual exit if exit source is actual; else use enforced exit
    use_raw = occ_last.get("exit_src").eq("actual") if "exit_src" in occ_last.columns else pd.Series(False, index=occ_last.index)
    exit_used = occ_last["exit_time_raw"].where(use_raw, occ_last["exit_time"])  # type: ignore
    exit_delay = None
    if "sched_arr_v" in occ_last.columns:
        exit_delay = (exit_used - occ_last["sched_arr_v"]).dt.total_seconds() / 60
    else:
        exit_delay = pd.Series([], dtype=float)

    trains_served = int(occ_post["train_id"].nunique())
    avg_delay = float(exit_delay.mean(skipna=True)) if len(exit_delay) else 0.0
    p90_delay = float(exit_delay.quantile(0.9)) if len(exit_delay) else 0.0
    otp = float((exit_delay.le(5).mean()) * 100.0) if len(exit_delay) else 0.0

    # Finalize frames and outputs
    occ_post = occ_post.assign(
        delay_min=(occ_post["exit_time"] - occ_post["sched_arr_v"]).dt.total_seconds() / 60
        if "sched_arr_v" in occ_post.columns
        else pd.Series(dtype=float)
    )

    kpis = {
        "otp_exit_pct": otp,
        "avg_exit_delay_min": avg_delay,
        "p90_exit_delay_min": p90_delay,
        "conflicts_pre_headway": int(conflict_count),
        "trains_served": trains_served,
    }

    log = {
        "hops_total": int(hops_total),
        "hops_kept": int(len(occ_raw)),
        "hops_skipped_missing_edge": int(hops_skipped_missing_edge),
        "windows_built": int(len(occ_raw)),
        "windows_inferred": int(windows_inferred),
        "source_counts": source_counter,
        "headway_shifts_applied": int(shifts_applied),
        "headway_total_min": float(total_headway_min),
    }

    # Make a light raw copy with a consistent schema for saving
    occ_raw_out = occ_post[[
        "train_id",
        "u",
        "v",
        "block_id",
        "entry_time_raw",
        "exit_time_raw",
        "source",
        "delay_min_raw",
    ]].copy()

    occ_out = occ_post[[
        "train_id",
        "u",
        "v",
        "block_id",
        "entry_time",
        "exit_time",
        "headway_applied_min",
        "source",
        "delay_min",
    ]].copy()

    return BuildResult(occupancy=occ_out, occupancy_raw=occ_raw_out, conflicts_pre=conflicts, kpis=kpis, log=log)


def snapshot(occupancy: pd.DataFrame, t: str | pd.Timestamp) -> pd.DataFrame:
    """Compute a virtual nowcast snapshot at timestamp ``t``.

    Returns a DataFrame with ``train_id``, ``block_id``, ``u``, ``v``,
    ``progress_pct`` and ``ETA_to_v``.
    """
    if occupancy.empty:
        return pd.DataFrame(columns=["train_id", "block_id", "u", "v", "progress_pct", "ETA_to_v"])  # noqa: E501

    t = pd.to_datetime(t, utc=True)
    dur = (occupancy["exit_time"] - occupancy["entry_time"]).dt.total_seconds() / 60
    # Avoid zero/negative with safe divisor
    safe_dur = dur.where(dur > 0, other=1.0)
    progress = (t - occupancy["entry_time"]).dt.total_seconds() / 60 / safe_dur
    progress = progress.clip(lower=0.0, upper=1.0)

    out = occupancy[["train_id", "block_id", "u", "v"]].copy()
    out["progress_pct"] = progress
    out["ETA_to_v"] = occupancy["exit_time"]
    return out


def save(
    result: BuildResult,
    corridor: str,
    date: str | pd.Timestamp,
    base_dir: str | Path = "artifacts",
) -> None:
    """Persist block-level outputs under artifacts.

    Writes:
    - block_occupancy.parquet (post-headway)
    - block_occupancy_raw.parquet (pre-headway)
    - conflicts_pre_headway.json
    - kpis_block.json
    - block_log.json
    """
    out_dir = Path(base_dir) / corridor / pd.to_datetime(date).date().isoformat()
    out_dir.mkdir(parents=True, exist_ok=True)
    result.occupancy.to_parquet(out_dir / "block_occupancy.parquet", index=False)
    result.occupancy_raw.to_parquet(out_dir / "block_occupancy_raw.parquet", index=False)
    (out_dir / "conflicts_pre_headway.json").write_text(json.dumps(result.conflicts_pre, indent=2))
    (out_dir / "kpis_block.json").write_text(json.dumps(result.kpis, indent=2))
    (out_dir / "block_log.json").write_text(json.dumps(result.log, indent=2))
