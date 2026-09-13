import time
import uuid
from typing import Callable, Optional
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST
from starlette.requests import Request
from starlette.responses import Response

# Prometheus Metrics Definitions
REQUESTS_TOTAL = Counter(
    "edgeflow_requests_total",
    "Total HTTP requests handled by EdgeFlow",
    ["method", "endpoint", "status_code"],
)

REQUEST_DURATION_SECONDS = Histogram(
    "edgeflow_request_duration_seconds",
    "Request latency in seconds",
    ["method", "endpoint"],
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0],
)

RATE_LIMIT_EXCEEDED = Counter(
    "edgeflow_rate_limit_exceeded_total",
    "Count of rate limit rejections (HTTP 429)",
    ["tier", "client_id"],
)

CACHE_HITS = Counter(
    "edgeflow_cache_hits_total",
    "Count of cache hits",
    ["endpoint"],
)

CACHE_MISSES = Counter(
    "edgeflow_cache_misses_total",
    "Count of cache misses",
    ["endpoint"],
)

UPSTREAM_FAILURES = Counter(
    "edgeflow_upstream_failures_total",
    "Count of upstream service failures",
    ["upstream_id", "status_code"],
)

FAILOVER_EVENTS = Counter(
    "edgeflow_failovers_total",
    "Count of automatic failover occurrences",
    ["from_target", "to_target"],
)

CIRCUIT_STATE = Gauge(
    "edgeflow_circuit_breaker_state",
    "Circuit breaker state: 0=CLOSED, 1=HALF_OPEN, 2=OPEN",
    ["circuit_id"],
)


def get_prometheus_metrics() -> Response:
    """Return latest Prometheus metrics as HTTP Response."""
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


def generate_request_id() -> str:
    """Generate unique tracing request ID."""
    return f"ef-{uuid.uuid4().hex[:12]}"


class RequestTimer:
    """Context manager / helper for measuring request latency."""

    def __init__(self):
        self.start_time: float = 0.0
        self.end_time: float = 0.0

    def start(self) -> None:
        self.start_time = time.perf_counter()

    def stop(self) -> float:
        self.end_time = time.perf_counter()
        return (self.end_time - self.start_time) * 1000.0  # ms
