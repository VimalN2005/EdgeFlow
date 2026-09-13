import pytest
from edgeflow.core.load_balancer import (
    LoadBalancer,
    LoadBalancingStrategy,
    UpstreamTarget,
)


def test_round_robin_selection():
    lb = LoadBalancer()
    t1 = UpstreamTarget(id="t1", url="http://srv1:8000")
    t2 = UpstreamTarget(id="t2", url="http://srv2:8000")
    t3 = UpstreamTarget(id="t3", url="http://srv3:8000")
    targets = [t1, t2, t3]

    chosen1 = lb.select_target(targets, LoadBalancingStrategy.ROUND_ROBIN, pool_id="p1")
    chosen2 = lb.select_target(targets, LoadBalancingStrategy.ROUND_ROBIN, pool_id="p1")
    chosen3 = lb.select_target(targets, LoadBalancingStrategy.ROUND_ROBIN, pool_id="p1")
    chosen4 = lb.select_target(targets, LoadBalancingStrategy.ROUND_ROBIN, pool_id="p1")

    assert chosen1.id == "t1"
    assert chosen2.id == "t2"
    assert chosen3.id == "t3"
    assert chosen4.id == "t1"


def test_round_robin_skips_unhealthy():
    lb = LoadBalancer()
    t1 = UpstreamTarget(id="t1", url="http://srv1:8000", is_healthy=False)
    t2 = UpstreamTarget(id="t2", url="http://srv2:8000", is_healthy=True)
    targets = [t1, t2]

    for _ in range(3):
        chosen = lb.select_target(targets, LoadBalancingStrategy.ROUND_ROBIN, pool_id="p2")
        assert chosen.id == "t2"


def test_least_connections():
    lb = LoadBalancer()
    t1 = UpstreamTarget(id="t1", url="http://srv1:8000", active_connections=5)
    t2 = UpstreamTarget(id="t2", url="http://srv2:8000", active_connections=1)
    t3 = UpstreamTarget(id="t3", url="http://srv3:8000", active_connections=3)
    targets = [t1, t2, t3]

    chosen = lb.select_target(targets, LoadBalancingStrategy.LEAST_CONNECTIONS)
    assert chosen.id == "t2"


def test_latency_aware():
    lb = LoadBalancer()
    t1 = UpstreamTarget(id="t1", url="http://srv1:8000", avg_latency_ms=120.0)
    t2 = UpstreamTarget(id="t2", url="http://srv2:8000", avg_latency_ms=15.0)
    t3 = UpstreamTarget(id="t3", url="http://srv3:8000", avg_latency_ms=85.0)
    targets = [t1, t2, t3]

    chosen = lb.select_target(targets, LoadBalancingStrategy.LATENCY_AWARE)
    assert chosen.id == "t2"
