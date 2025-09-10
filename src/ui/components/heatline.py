from __future__ import annotations

from typing import List, Dict
import pandas as pd
import altair as alt
import streamlit as st


def risk_heatline(radar: List[Dict]) -> None:
    if not radar:
        st.info("No risks.")
        return
    df = pd.DataFrame(radar)
    if df.empty:
        st.info("No risks.")
        return
    # Aggregate by type and lead bucket (0-30 min)
    if "lead_bucket" in df.columns and "type" in df.columns:
        agg = df.groupby(["lead_bucket","type"]).size().reset_index(name="count")
        chart = (
            alt.Chart(agg)
            .mark_bar()
            .encode(x="lead_bucket:N", y="count:Q", color="type:N", tooltip=["lead_bucket","type","count"])
            .properties(height=250)
        )
        st.altair_chart(chart, use_container_width=True)
    else:
        st.dataframe(df[[c for c in ["severity","lead_min","type"] if c in df.columns]].head(20), hide_index=True)

