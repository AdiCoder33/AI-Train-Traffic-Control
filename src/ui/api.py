from __future__ import annotations

"""Thin API client for FastAPI backend with caching and graceful fallbacks.

All methods return Python dicts/lists suitable for UI consumption. When API
is unreachable, functions return empty payloads or minimal mocks so that the
UI remains demoable.
"""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
import time

import streamlit as st
import requests


@dataclass
class ApiConfig:
    host: str = "127.0.0.1"
    port: int = 8000
    token: Optional[str] = None

    @property
    def base(self) -> str:
        return f"http://{self.host}:{self.port}"

    @property
    def headers(self) -> Dict[str, str]:
        h: Dict[str, str] = {"Content-Type": "application/json"}
        if self.token:
            h["Authorization"] = f"Bearer {self.token}"
        return h


def _latency(fn):  # decorator to time API calls
    def wrap(*args, **kwargs):
        t0 = time.perf_counter()
        try:
            return fn(*args, **kwargs), (time.perf_counter() - t0)
        except Exception:
            return None, (time.perf_counter() - t0)
    return wrap


@st.cache_data(ttl=5.0, show_spinner=False)
def get_state(cfg: ApiConfig, scope: str, date: str, *, station_id: Optional[str] = None, train_id: Optional[str] = None) -> Dict[str, Any]:
    try:
        params = {"scope": scope, "date": date}
        if station_id:
            params["station_id"] = station_id
        if train_id:
            params["train_id"] = train_id
        r = requests.get(f"{cfg.base}/state", params=params, headers=cfg.headers, timeout=5)
        if r.status_code == 200:
            return r.json()
    except Exception:
        pass
    return {"platform_occupancy": [], "waiting_ledger": [], "sim_kpis": {}}


@st.cache_data(ttl=5.0, show_spinner=False)
def get_radar(cfg: ApiConfig, scope: str, date: str, *, station_id: Optional[str] = None, train_id: Optional[str] = None) -> Dict[str, Any]:
    try:
        params = {"scope": scope, "date": date}
        if station_id:
            params["station_id"] = station_id
        if train_id:
            params["train_id"] = train_id
        r = requests.get(f"{cfg.base}/radar", params=params, headers=cfg.headers, timeout=5)
        if r.status_code == 200:
            return r.json()
    except Exception:
        pass
    return {"radar": [], "risk_kpis": {}}


@st.cache_data(ttl=5.0, show_spinner=False)
def get_recommendations(cfg: ApiConfig, scope: str, date: str, *, station_id: Optional[str] = None, train_id: Optional[str] = None) -> Dict[str, Any]:
    try:
        params = {"scope": scope, "date": date}
        if station_id:
            params["station_id"] = station_id
        if train_id:
            params["train_id"] = train_id
        r = requests.get(f"{cfg.base}/recommendations", params=params, headers=cfg.headers, timeout=5)
        if r.status_code == 200:
            return r.json()
    except Exception:
        pass
    return {"rec_plan": [], "plan_metrics": {}, "alt_options": [], "audit_log": {}, "plan_version": ""}


def post_feedback(cfg: ApiConfig, scope: str, date: str, action: Dict[str, Any], decision: str, *, reason: Optional[str] = None, modified: Optional[Dict[str, Any]] = None) -> Tuple[bool, str]:
    try:
        payload = {"scope": scope, "date": date, "action": action, "decision": decision, "reason": reason, "modified": modified}
        r = requests.post(f"{cfg.base}/feedback", json=payload, headers=cfg.headers, timeout=5)
        return (r.status_code == 200, (r.text if r.status_code != 200 else "ok"))
    except Exception as e:
        return False, str(e)


def ai_ask(cfg: ApiConfig, scope: str, date: str, query: str, *, station_id: Optional[str] = None, train_id: Optional[str] = None) -> Dict[str, Any]:
    try:
        body = {"scope": scope, "date": date, "query": query}
        if station_id:
            body["station_id"] = station_id
        if train_id:
            body["train_id"] = train_id
        r = requests.post(f"{cfg.base}/ai/ask", json=body, headers=cfg.headers, timeout=8)
        if r.status_code == 200:
            return r.json()
    except Exception:
        pass
    return {"result": {"answer": "No response", "details": {}}, "whoami": {}}


def ai_suggest(cfg: ApiConfig, scope: str, date: str, *, station_id: Optional[str] = None, train_id: Optional[str] = None, max_hold_min: int = 3) -> Dict[str, Any]:
    try:
        body = {"scope": scope, "date": date, "max_hold_min": int(max_hold_min)}
        if station_id:
            body["station_id"] = station_id
        if train_id:
            body["train_id"] = train_id
        r = requests.post(f"{cfg.base}/ai/suggest", json=body, headers=cfg.headers, timeout=12)
        if r.status_code == 200:
            return r.json()
    except Exception:
        pass
    return {"result": {"source": "", "suggestions": []}, "whoami": {}}


def admin_call(cfg: ApiConfig, path: str, *, method: str = "POST", params: Optional[Dict[str, Any]] = None, body: Optional[Dict[str, Any]] = None, timeout: int = 30) -> Tuple[bool, Dict[str, Any]]:
    url = f"{cfg.base}{path}"
    try:
        if method == "POST":
            r = requests.post(url, params=params or {}, json=body or {}, headers=cfg.headers, timeout=timeout)
        else:
            r = requests.get(url, params=params or {}, headers=cfg.headers, timeout=timeout)
        if r.status_code == 200:
            return True, r.json()
        return False, {"status_code": r.status_code, "text": r.text}
    except Exception as e:
        return False, {"error": str(e)}

