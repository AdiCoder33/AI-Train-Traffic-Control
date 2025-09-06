"""Corridor data slicing utilities.

This module exposes :func:`slice` which extracts events for a given
corridor and service date. Only trains that traverse at least two
consecutive stations within the corridor are retained. The function is
careful to use the provided ``service_date`` column so that trains
crossing midnight remain associated with their original operating day.
"""

from __future__ import annotations

from typing import Sequence
import pandas as pd

__all__ = ["slice"]


def slice(
    df_events: pd.DataFrame, stations_list: Sequence[str], date: str | pd.Timestamp
) -> tuple[pd.DataFrame, dict[str, int]]:
    """Return corridor-specific events and station mapping.

    Parameters
    ----------
    df_events:
        DataFrame containing train events with at least ``train_id``,
        ``station_id`` and ``service_date`` columns.
    stations_list:
        Ordered sequence of station identifiers defining the corridor.
    date:
        Service date to filter on. Strings are parsed using
        :func:`pandas.to_datetime`.

    Returns
    -------
    df_slice, stations_dict:
        ``df_slice`` is the subset of ``df_events`` containing only trains
        that visit at least two consecutive stations from ``stations_list``
        on the specified ``service_date``. ``stations_dict`` maps station id
        to its order in ``stations_list``.
    """

    if "service_date" not in df_events.columns:
        raise KeyError("df_events must contain a 'service_date' column")

    stations_dict: dict[str, int] = {sid: i for i, sid in enumerate(stations_list)}
    service_date = pd.to_datetime(date).date()

    df = df_events.copy()
    df["service_date"] = pd.to_datetime(df["service_date"]).dt.date
    df = df[df["service_date"] == service_date]
    df = df[df["station_id"].isin(stations_dict)]

    if df.empty:
        return df.drop(columns=[]), stations_dict

    df["__order__"] = df["station_id"].map(stations_dict)

    time_col = None
    for col in ("sched_arr", "sched_dep", "act_arr", "act_dep"):
        if col in df.columns:
            time_col = col
            break

    valid_groups: list[pd.DataFrame] = []
    for _, grp in df.groupby("train_id"):
        if time_col:
            grp = grp.sort_values(time_col)
        grp = grp.sort_values("__order__") if time_col is None else grp
        if grp["__order__"].diff().abs().eq(1).any():
            valid_groups.append(grp)

    if valid_groups:
        df_slice = pd.concat(valid_groups, ignore_index=True).drop(columns="__order__")
    else:
        df_slice = df.iloc[0:0].drop(columns="__order__")

    return df_slice, stations_dict