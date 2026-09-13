import pytest
import httpx
from unittest.mock import patch
from edgeflow.core.router import DispatchResult, router_engine


@pytest.mark.asyncio
async def test_health_endpoints(client: httpx.AsyncClient):
    resp = await client.get("/healthz")
    assert resp.status_code == 200
    assert resp.json()["status"] == "healthy"

    resp_ready = await client.get("/readyz")
    assert resp_ready.status_code == 200
    assert resp_ready.json()["status"] == "ready"


@pytest.mark.asyncio
async def test_gateway_chat_completions_with_caching(client: httpx.AsyncClient):
    mock_dispatch = DispatchResult(
        response=httpx.Response(
            200,
            headers={"content-type": "application/json"},
            content=b'{"id":"chatcmpl-test","choices":[{"message":{"content":"Hello from EdgeFlow Mock"}}]}',
        ),
        target_id="openai-primary",
        target_url="http://127.0.0.1:9001",
        latency_ms=12.4,
        is_fallback=False,
    )

    with patch.object(router_engine, "dispatch", return_value=mock_dispatch):
        payload = {
            "model": "gpt-4o",
            "messages": [{"role": "user", "content": "Hello EdgeFlow"}],
        }

        # First call: Cache MISS
        r1 = await client.post(
            "/v1/chat/completions",
            json=payload,
            headers={"X-API-Key": "ef-live-pro-key"},
        )
        assert r1.status_code == 200
        assert r1.headers["X-Cache"] == "MISS"
        assert r1.headers["X-EdgeFlow-Target"] == "openai-primary"
        assert "choices" in r1.json()

        # Second identical call: Cache HIT
        r2 = await client.post(
            "/v1/chat/completions",
            json=payload,
            headers={"X-API-Key": "ef-live-pro-key"},
        )
        assert r2.status_code == 200
        assert r2.headers["X-Cache"] == "HIT"
        assert "choices" in r2.json()


@pytest.mark.asyncio
async def test_gateway_rate_limit_rejection(client: httpx.AsyncClient):
    # ef-live-free-key has rate_limit_free (30 requests). Send 35 requests.
    for i in range(30):
        resp = await client.get("/healthz")
        assert resp.status_code == 200

    # Test rejection directly on gateway endpoint with low quota mock
    mock_dispatch = DispatchResult(
        response=httpx.Response(200, content=b'{"ok":true}'),
        target_id="mock",
        target_url="http://127.0.0.1:9001",
        latency_ms=1.0,
    )

    with patch.object(router_engine, "dispatch", return_value=mock_dispatch):
        # Consume allowed quota
        for _ in range(30):
            res = await client.post(
                "/v1/chat/completions",
                json={"prompt": "test"},
                headers={"X-API-Key": "ef-live-free-key"},
            )
            if res.status_code == 429:
                break
        
        # Subsequent requests must be 429
        res_limit = await client.post(
            "/v1/chat/completions",
            json={"prompt": "test"},
            headers={"X-API-Key": "ef-live-free-key"},
        )
        assert res_limit.status_code == 429
        assert "Rate limit exceeded" in res_limit.json()["error"]
        assert "Retry-After" in res_limit.headers


@pytest.mark.asyncio
async def test_admin_routes_and_circuits(client: httpx.AsyncClient):
    # Admin endpoint requires admin authorization
    resp = await client.get(
        "/api/v1/admin/routes",
        headers={"X-API-Key": "ef-live-admin-key"},
    )
    assert resp.status_code == 200
    assert "routes" in resp.json()

    resp_circuits = await client.get(
        "/api/v1/admin/circuits",
        headers={"X-API-Key": "ef-live-admin-key"},
    )
    assert resp_circuits.status_code == 200
    assert "circuits" in resp_circuits.json()


@pytest.mark.asyncio
async def test_prometheus_metrics_endpoint(client: httpx.AsyncClient):
    resp = await client.get("/metrics")
    assert resp.status_code == 200
    assert "edgeflow_requests_total" in resp.text


@pytest.mark.asyncio
async def test_gateway_chat_completions_streaming(client: httpx.AsyncClient):
    async def fake_stream_chunks():
        yield b'data: {"choices":[{"delta":{"content":"Streaming token"}}}\n\n'
        yield b'data: [DONE]\n\n'

    mock_resp = httpx.Response(200, headers={"content-type": "text/event-stream"})
    mock_resp.aiter_bytes = fake_stream_chunks

    with patch.object(router_engine, "dispatch_stream", return_value=(mock_resp, "openai-primary", False)):
        payload = {
            "model": "gpt-4o",
            "messages": [{"role": "user", "content": "Stream test"}],
            "stream": True,
        }
        res = await client.post(
            "/v1/chat/completions",
            json=payload,
            headers={"X-API-Key": "ef-live-pro-key"},
        )
        assert res.status_code == 200
        assert res.headers["X-EdgeFlow-Stream"] == "true"
        assert res.headers["X-EdgeFlow-Target"] == "openai-primary"
        assert "text/event-stream" in res.headers["content-type"]
        assert "Streaming token" in res.text
        assert "[DONE]" in res.text

