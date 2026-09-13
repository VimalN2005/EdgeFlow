import pytest
import httpx
from unittest.mock import AsyncMock, patch
from edgeflow.core.load_balancer import LoadBalancingStrategy, UpstreamTarget
from edgeflow.core.router import RouteRule, RouterEngine


@pytest.mark.asyncio
async def test_router_route_matching():
    router = RouterEngine()
    rule1 = RouteRule(
        id="r1",
        path_prefix="/v1/chat",
        model_alias="gpt-4o",
        primary_targets=[UpstreamTarget(id="t1", url="http://test1")],
    )
    rule2 = RouteRule(
        id="r2",
        path_prefix="/api/orders",
        primary_targets=[UpstreamTarget(id="t2", url="http://test2")],
    )
    router.register_route(rule1)
    router.register_route(rule2)

    # Match by path
    m1 = router.match_route("/v1/chat/completions")
    assert m1 is not None and m1.id == "r1"

    # Match by model alias
    m_model = router.match_route("/unknown/path", model_name="gpt-4o")
    assert m_model is not None and m_model.id == "r1"

    # Match second path
    m2 = router.match_route("/api/orders/status")
    assert m2 is not None and m2.id == "r2"


@pytest.mark.asyncio
async def test_router_failover_mechanism():
    router = RouterEngine()
    primary = UpstreamTarget(id="primary-failing", url="http://primary:8000")
    fallback = UpstreamTarget(id="fallback-healthy", url="http://fallback:8000")

    rule = RouteRule(
        id="failover-rule",
        path_prefix="/v1/test",
        strategy=LoadBalancingStrategy.ROUND_ROBIN,
        primary_targets=[primary],
        fallback_targets=[fallback],
    )
    router.register_route(rule)

    # Mock _forward_request to fail on primary and succeed on fallback
    async def mock_forward(target, method, path, headers, content, params, timeout):
        if target.id == "primary-failing":
            return httpx.Response(503, request=httpx.Request("POST", "http://primary/test"), content=b"Unavailable")
        else:
            return httpx.Response(200, request=httpx.Request("POST", "http://fallback/test"), content=b'{"status":"ok_fallback"}')

    with patch.object(router, "_forward_request", side_effect=mock_forward):
        result = await router.dispatch(
            route=rule,
            method="POST",
            subpath="/v1/test",
            headers={},
            content=b"",
            params={},
        )
        assert result.is_fallback is True
        assert result.target_id == "fallback-healthy"
        assert result.response.status_code == 200
        assert result.response.content == b'{"status":"ok_fallback"}'
