from __future__ import annotations

from typing import Dict, Any
import streamlit as st
from ..utils import kv_table


def explain_drawer(item: Dict[str, Any]) -> None:
    with st.sidebar:
        st.subheader("Explain")
        st.caption(item.get("why", ""))
        impact = item.get("impact", {})
        safety = item.get("safety_checks", item.get("safety", {}))
        if impact:
            st.caption("Impact")
            st.dataframe(kv_table(impact), hide_index=True, use_container_width=True)
        if safety:
            st.caption("Safety checks")
            if isinstance(safety, dict):
                st.dataframe(kv_table(safety), hide_index=True, use_container_width=True)
            elif isinstance(safety, list):
                for x in safety:
                    st.write(f"• {x}")

