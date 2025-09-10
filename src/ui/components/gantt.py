from __future__ import annotations

from typing import Optional
import pandas as pd
import plotly.express as px
import streamlit as st


def gantt_platform(df_platform: pd.DataFrame, *, label_station) -> None:
    if df_platform.empty:
        st.info("No platform occupancy available.")
        return
    dfp = df_platform.copy().sort_values("arr_platform").head(3000)
    dfp["arr_platform"] = pd.to_datetime(dfp["arr_platform"], utc=True, errors="coerce")
    dfp["dep_platform"] = pd.to_datetime(dfp["dep_platform"], utc=True, errors="coerce")
    dfp["station_label"] = dfp["station_id"].map(lambda x: label_station(x))
    try:
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
        st.dataframe(dfp[["train_id", "station_id", "arr_platform", "dep_platform"]], hide_index=True, use_container_width=True)

