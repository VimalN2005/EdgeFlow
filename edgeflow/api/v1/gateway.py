import json
import logging
import time
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from fastapi.responses import JSONResponse, Response as StarletteResponse, StreamingResponse
from edgeflow.config import settings
from edgeflow.core.auth import ClientIdentity, authenticate_request
from edgeflow.core.cache import response_cache
from edgeflow.core.circuit_breaker import CircuitBreakerOpenException
from edgeflow.core.rate_limiter import rate_limiter
from edgeflow.core.router import router_engine
from edgeflow.core.telemetry import REQUEST_DURATION_SECONDS, REQUESTS_TOTAL, generate_request_id

logger = logging.getLogger("edgeflow.gateway")

router = APIRouter(tags=["Gateway & Proxy"])


async def handle_gateway_request(
    request: Request,
    path_override: Optional[str] = None,
    identity: ClientIdentity = Depends(authenticate_request),
) -> StarletteResponse:
    """Core Gateway Pipeline executing:

    Auth -> Rate Limit -> Cache Lookup -> Router -> Upstream -> Cache Store -> Egress
    """
    t_start = time.perf_counter()
    req_id = request.headers.get("X-Request-ID") or generate_request_id()
    method = request.method
    path = path_override or request.url.path
    query_str = str(request.url.query)

    # Read incoming body
    body_bytes = await request.body()

    # 1. Rate Limiting Check
    rate_res = await rate_limiter.check_rate_limit(identity)
    if not rate_res.allowed:
        REQUESTS_TOTAL.labels(method=method, endpoint=path, status_code="429").inc()
        return JSONResponse(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            content={
                "error": "Rate limit exceeded.",
                "tier": identity.tier,
                "limit": rate_res.limit,
                "retry_after_seconds": rate_res.retry_after,
            },
            headers={
                "X-Request-ID": req_id,
                "Retry-After": str(rate_res.retry_after or 60),
                "X-RateLimit-Limit": str(rate_res.limit),
                "X-RateLimit-Remaining": "0",
                "X-RateLimit-Reset": str(int(rate_res.reset_after_seconds)),
            },
        )

    # 2. Response Cache Lookup
    cache_key = response_cache.generate_cache_key(method, path, query_str, body_bytes)
    if response_cache.is_cacheable(method, dict(request.headers), body_bytes):
        cached = await response_cache.get(cache_key, endpoint=path)
        if cached:
            status_code, content, headers = cached
            latency_ms = (time.perf_counter() - t_start) * 1000.0
            REQUESTS_TOTAL.labels(method=method, endpoint=path, status_code=str(status_code)).inc()
            REQUEST_DURATION_SECONDS.labels(method=method, endpoint=path).observe(latency_ms / 1000.0)

            out_headers = dict(headers)
            out_headers.update({
                "X-Request-ID": req_id,
                "X-Cache": "HIT",
                "X-Response-Time": f"{latency_ms:.2f}ms",
                "X-RateLimit-Limit": str(rate_res.limit),
                "X-RateLimit-Remaining": str(rate_res.remaining),
            })
            return StarletteResponse(content=content, status_code=status_code, headers=out_headers)

    # 3. Model & Stream Extraction (for intelligent LLM routing)
    model_name: Optional[str] = None
    is_stream: bool = False
    if body_bytes and "application/json" in request.headers.get("content-type", ""):
        try:
            body_json = json.loads(body_bytes)
            model_name = body_json.get("model")
            is_stream = bool(body_json.get("stream", False))
        except Exception:
            pass

    # 4. Route Matching
    route = router_engine.match_route(path, model_name=model_name)
    if not route:
        REQUESTS_TOTAL.labels(method=method, endpoint=path, status_code="404").inc()
        return JSONResponse(
            status_code=status.HTTP_404_NOT_FOUND,
            content={
                "error": "No matching upstream route found for request.",
                "path": path,
                "model": model_name,
            },
            headers={"X-Request-ID": req_id},
        )

    # 5. Upstream Dispatch (Load Balancing, Circuit Breakers, Retries, Failover)
    subpath = path[len(route.path_prefix):] if path.startswith(route.path_prefix) else path

    # Handle Streaming SSE Requests
    if is_stream:
        try:
            stream_resp, target_id, is_fallback = await router_engine.dispatch_stream(
                route=route,
                method=method,
                subpath=subpath,
                headers=dict(request.headers),
                content=body_bytes,
                params=dict(request.query_params),
            )
        except CircuitBreakerOpenException as cbe:
            REQUESTS_TOTAL.labels(method=method, endpoint=path, status_code="503").inc()
            return JSONResponse(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                content={"error": str(cbe)},
                headers={"X-Request-ID": req_id, "Retry-After": str(int(cbe.remaining_seconds))},
            )
        except Exception as exc:
            REQUESTS_TOTAL.labels(method=method, endpoint=path, status_code="502").inc()
            logger.error("All upstream backends failed for streaming route '%s': %s", route.id, exc)
            return JSONResponse(
                status_code=status.HTTP_502_BAD_GATEWAY,
                content={"error": f"Upstream service unavailable for streaming: {str(exc)}"},
                headers={"X-Request-ID": req_id},
            )

        async def stream_generator():
            try:
                async for chunk in stream_resp.aiter_bytes():
                    yield chunk
            finally:
                await stream_resp.aclose()

        s_headers = {
            "X-Request-ID": req_id,
            "X-EdgeFlow-Target": target_id,
            "X-EdgeFlow-Fallback": "true" if is_fallback else "false",
            "X-EdgeFlow-Stream": "true",
            "X-RateLimit-Limit": str(rate_res.limit),
            "X-RateLimit-Remaining": str(rate_res.remaining),
            "X-RateLimit-Reset": str(int(rate_res.reset_after_seconds)),
        }
        return StreamingResponse(
            stream_generator(),
            status_code=stream_resp.status_code,
            headers=s_headers,
            media_type=stream_resp.headers.get("content-type", "text/event-stream"),
        )

    try:
        dispatch_result = await router_engine.dispatch(
            route=route,
            method=method,
            subpath=subpath,
            headers=dict(request.headers),
            content=body_bytes,
            params=dict(request.query_params),
        )
    except CircuitBreakerOpenException as cbe:
        REQUESTS_TOTAL.labels(method=method, endpoint=path, status_code="503").inc()
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={"error": str(cbe)},
            headers={"X-Request-ID": req_id, "Retry-After": str(int(cbe.remaining_seconds))},
        )
    except Exception as exc:
        REQUESTS_TOTAL.labels(method=method, endpoint=path, status_code="502").inc()
        logger.error("All upstream backends failed for route '%s': %s", route.id, exc)
        return JSONResponse(
            status_code=status.HTTP_502_BAD_GATEWAY,
            content={"error": f"Upstream service unavailable: {str(exc)}"},
            headers={"X-Request-ID": req_id},
        )

    total_latency_ms = (time.perf_counter() - t_start) * 1000.0
    upstream_resp = dispatch_result.response

    REQUESTS_TOTAL.labels(method=method, endpoint=path, status_code=str(upstream_resp.status_code)).inc()
    REQUEST_DURATION_SECONDS.labels(method=method, endpoint=path).observe(total_latency_ms / 1000.0)

    # 6. Store in Cache if eligible
    if response_cache.is_cacheable(method, dict(request.headers), body_bytes) and upstream_resp.status_code == 200:
        await response_cache.set(
            cache_key=cache_key,
            status_code=upstream_resp.status_code,
            content=upstream_resp.content,
            headers=dict(upstream_resp.headers),
        )

    # 7. Assemble Response Headers
    response_headers = {
        k: v
        for k, v in upstream_resp.headers.items()
        if k.lower() not in ("content-length", "content-encoding", "transfer-encoding")
    }
    response_headers.update({
        "X-Request-ID": req_id,
        "X-Cache": "MISS",
        "X-EdgeFlow-Target": dispatch_result.target_id,
        "X-EdgeFlow-Fallback": "true" if dispatch_result.is_fallback else "false",
        "X-Upstream-Latency": f"{dispatch_result.latency_ms:.2f}ms",
        "X-Response-Time": f"{total_latency_ms:.2f}ms",
        "X-RateLimit-Limit": str(rate_res.limit),
        "X-RateLimit-Remaining": str(rate_res.remaining),
        "X-RateLimit-Reset": str(int(rate_res.reset_after_seconds)),
    })

    return StarletteResponse(
        content=upstream_resp.content,
        status_code=upstream_resp.status_code,
        headers=response_headers,
    )


# --- Universal Endpoints ---

@router.post("/v1/chat/completions", summary="OpenAI-compatible LLM Proxy with Auto-Failover")
async def chat_completions(request: Request, identity: ClientIdentity = Depends(authenticate_request)):
    """Routes chat completion requests intelligently across configured model providers."""
    return await handle_gateway_request(request, path_override="/v1/chat/completions", identity=identity)


@router.get("/v1/models", summary="List Available Models")
async def list_models(request: Request, identity: ClientIdentity = Depends(authenticate_request)):
    """List available LLM models registered in EdgeFlow."""
    models = []
    for r in router_engine.routes.values():
        if r.model_alias:
            models.append({
                "id": r.model_alias,
                "object": "model",
                "owned_by": "edgeflow",
                "strategy": r.strategy.value,
                "primary_count": len(r.primary_targets),
                "fallback_count": len(r.fallback_targets),
            })
    return {"object": "list", "data": models}


@router.api_route("/proxy/{route_id}/{subpath:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH"], summary="Universal Upstream Proxy")
async def proxy_endpoint(route_id: str, subpath: str, request: Request, identity: ClientIdentity = Depends(authenticate_request)):
    """Generic proxy forwarding request directly to a named route."""
    target_path = f"/{subpath}"
    route = router_engine.routes.get(route_id)
    if not route:
        raise HTTPException(status_code=404, detail=f"Route '{route_id}' not found.")
    return await handle_gateway_request(request, path_override=target_path, identity=identity)


@router.api_route("/gateway/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH"], summary="Direct Path Match Proxy")
async def gateway_catchall(path: str, request: Request, identity: ClientIdentity = Depends(authenticate_request)):
    """Catch-all proxy mapping raw request paths to matching upstream routes."""
    return await handle_gateway_request(request, path_override=f"/{path}", identity=identity)
