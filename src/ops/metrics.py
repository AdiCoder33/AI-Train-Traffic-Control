from __future__ import annotations

try:
    from prometheus_client import Counter, Gauge, Histogram, CollectorRegistry, generate_latest, CONTENT_TYPE_LATEST
except Exception:  # pragma: no cover - optional
    Counter = Gauge = Histogram = object  # type: ignore
    CollectorRegistry = object  # type: ignore
    def generate_latest(*args, **kwargs):  # type: ignore
        return b""
    CONTENT_TYPE_LATEST = "text/plain"

registry = None

def setup_registry():
    global registry
    if registry is None and hasattr(CollectorRegistry, "__call__"):
        registry = CollectorRegistry()
    return registry

def text_metrics() -> tuple[bytes, str]:
    if registry is None:
        setup_registry()
    try:
        return generate_latest(registry), CONTENT_TYPE_LATEST  # type: ignore
    except Exception:
        return b"", "text/plain"

