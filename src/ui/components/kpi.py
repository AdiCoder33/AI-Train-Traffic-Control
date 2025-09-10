from __future__ import annotations

from typing import Dict
import streamlit as st


def kpi_cards(sim_kpis: Dict[str, float], plan_kpis: Dict[str, float] | None = None) -> None:
    otp = float(sim_kpis.get("otp_exit_pct", sim_kpis.get("otp", 0.0)))
    avg = float(sim_kpis.get("avg_exit_delay_min", sim_kpis.get("avg_delay_min", 0.0)))
    c1, c2, c3 = st.columns(3)
    c1.metric("OTP (%)", f"{otp:.1f}")
    c2.metric("Avg Delay (min)", f"{avg:.1f}")
    c3.metric("Conflicts", f"{int(sim_kpis.get('total_conflicts', 0))}")

