from __future__ import annotations

from typing import List, Dict, Any
import streamlit as st
from ..theme import ACTION_BADGE_MAP


def rec_cards(items: List[Dict[str, Any]], *, on_apply, on_dismiss, on_modify) -> None:
    if not items:
        st.info("No recommendations.")
        return
    for i, r in enumerate(items):
        t = r.get("type")
        badge = ACTION_BADGE_MAP.get(str(t).upper(), str(t))
        title = f"{badge} • Train {r.get('train_id')} at {r.get('station_id') or r.get('at_station')}"
        with st.container():
            st.markdown(f"**{title}**")
            col = st.columns([3, 1, 1, 1])
            with col[0]:
                st.caption(r.get("why", ""))
            with col[1]:
                if st.button("Apply", key=f"apply_{i}"):
                    on_apply(r)
            with col[2]:
                if st.button("Dismiss", key=f"dismiss_{i}"):
                    on_dismiss(r)
            with col[3]:
                mins = st.number_input("Min", min_value=0.0, max_value=10.0, value=float(r.get("minutes", 0.0)), key=f"mins_{i}")
                if st.button("Modify", key=f"modify_{i}"):
                    r2 = dict(r)
                    r2["minutes"] = float(mins)
                    on_modify(r, r2)
            st.divider()

