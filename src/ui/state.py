from __future__ import annotations

"""Session state helpers for role, scope and filters."""

from dataclasses import dataclass
from typing import Optional
import streamlit as st


@dataclass
class UiState:
    role: str = "SC"  # SC | CREW | ADM | OM | DH | AN
    scope: str = "all_india"
    date: str = "2024-01-01"
    station_id: Optional[str] = None
    train_id: Optional[str] = None
    auto_refresh_sec: int = 0
    latency_ms_p95: float = 0.0


def get_state() -> UiState:
    if "ui_state" not in st.session_state:
        st.session_state["ui_state"] = UiState()
    return st.session_state["ui_state"]


def set_role(role: str) -> None:
    get_state().role = role


def set_scope(scope: str) -> None:
    get_state().scope = scope


def set_station(station_id: str | None) -> None:
    get_state().station_id = station_id


def set_train(train_id: str | None) -> None:
    get_state().train_id = train_id


def set_date(date: str) -> None:
    get_state().date = date


def set_latency_p95(ms: float) -> None:
    get_state().latency_ms_p95 = ms

