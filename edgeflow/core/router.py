import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple
import httpx
from edgeflow.config import settings
from edgeflow.core.circuit_breaker import CircuitBreaker, CircuitBreakerOpenException, CircuitState
from edgeflow.core.load_balancer import LoadBalancer, LoadBalancingStrategy, UpstreamTarget, load_balancer
from edgeflow.core.retry import execute_with_retry
from edgeflow.core.telemetry import FAILOVER_EVENTS, UPSTREAM_FAILURES

logger = logging.getLogger("edgeflow.router")


@dataclass
class RouteRule:
    """Routing rule mapping path prefix or model name to primary and fallback upstream pools."""
    id: str
    path_prefix: str
    strategy: LoadBalancingStrategy = LoadBalancingStrategy.ROUND_ROBIN
    primary_targets: List[UpstreamTarget] = field(default_factory=list)
    fallback_targets: List[UpstreamTarget] = field(default_factory=list)
    model_alias: Optional[str] = None
    timeout: float = 10.0


@dataclass
class DispatchResult:
    """Result of an upstream dispatch execution."""
    response: httpx.Response
    target_id: str
    target_url: str
    latency_ms: float
    is_fallback: bool = False
    retries_used: int = 0


class RouterEngine:
    """Intelligent dynamic router with circuit breaking, load balancing,

    and multi-stage model/service failover.
    """

    def __init__(self):
        self.routes: Dict[str, RouteRule] = {}
        self.circuit_breakers: Dict[str, CircuitBreaker] = {}
        self._http_client: Optional[httpx.AsyncClient] = None
        self._lock = asyncio.Lock()

    def get_circuit_breaker(self, target_id: str) -> CircuitBreaker:
        """Retrieve or create CircuitBreaker for a target."""
        if target_id not in self.circuit_breakers:
            self.circuit_breakers[target_id] = CircuitBreaker(circuit_id=target_id)
        return self.circuit_breakers[target_id]

    async def get_http_client(self) -> httpx.AsyncClient:
        """Get or initialize pooled async HTTP client."""
        if self._http_client is None or self._http_client.is_closed:
            limits = httpx.Limits(max_keepalive_connections=100, max_connections=200)
            self._http_client = httpx.AsyncClient(
                limits=limits,
                timeout=httpx.Timeout(
                    connect=settings.default_connect_timeout,
                    read=settings.default_read_timeout,
                    write=10.0,
                    pool=10.0,
                ),
            )
        return self._http_client

    def register_route(self, route: RouteRule) -> None:
        """Register a new routing rule."""
        self.routes[route.id] = route
        logger.info(
            "Registered route '%s' (prefix='%s', strategy='%s', primaries=%d, fallbacks=%d)",
            route.id,
            route.path_prefix,
            route.strategy,
            len(route.primary_targets),
            len(route.fallback_targets),
        )

    def match_route(self, path: str, model_name: Optional[str] = None) -> Optional[RouteRule]:
        """Match incoming request against routes by model alias or path prefix."""
        # 1. First priority: match by model alias (for LLM routing)
        if model_name:
            for route in self.routes.values():
                if route.model_alias and route.model_alias.lower() == model_name.lower():
                    return route

        # 2. Match by path prefix (longest prefix first)
        sorted_routes = sorted(
            self.routes.values(),
            key=lambda r: len(r.path_prefix),
            reverse=True,
        )
        for route in sorted_routes:
            if path.startswith(route.path_prefix):
                return route

        # 3. Default route if available
        return self.routes.get("default")

    async def _forward_request(
        self,
        target: UpstreamTarget,
        method: str,
        path: str,
        headers: Dict[str, str],
        content: bytes,
        params: Dict[str, str],
        timeout: float,
    ) -> httpx.Response:
        """Forward raw request to the designated upstream target."""
        client = await self.get_http_client()
        url = target.url.rstrip("/") + ("/" + path.lstrip("/") if path else "")

        # Prepare headers: forward original headers while replacing Host
        fwd_headers = {k: v for k, v in headers.items() if k.lower() not in ("host", "content-length")}
        if target.headers:
            fwd_headers.update(target.headers)

        async def send():
            return await client.request(
                method=method,
                url=url,
                headers=fwd_headers,
                content=content,
                params=params,
                timeout=timeout,
            )

        return await execute_with_retry(send)

    async def dispatch(
        self,
        route: RouteRule,
        method: str,
        subpath: str,
        headers: Dict[str, str],
        content: bytes,
        params: Dict[str, str],
    ) -> DispatchResult:
        """Route request through load balancer with circuit breaker and failover."""
        # 1. Attempt Primary Pool
        primary_target = load_balancer.select_target(
            targets=route.primary_targets,
            strategy=route.strategy,
            pool_id=f"{route.id}:primary",
        )

        if primary_target is not None:
            cb = self.get_circuit_breaker(primary_target.id)
            if await cb.can_execute():
                t0 = time.perf_counter()
                try:
                    resp = await self._forward_request(
                        target=primary_target,
                        method=method,
                        path=subpath,
                        headers=headers,
                        content=content,
                        params=params,
                        timeout=route.timeout,
                    )
                    latency = (time.perf_counter() - t0) * 1000.0

                    if resp.status_code < 500:
                        # Success or expected client error (e.g. 400, 404)
                        await cb.record_success()
                        primary_target.record_completion(latency, success=True)
                        return DispatchResult(
                            response=resp,
                            target_id=primary_target.id,
                            target_url=primary_target.url,
                            latency_ms=latency,
                            is_fallback=False,
                        )
                    else:
                        # 5xx error from upstream
                        UPSTREAM_FAILURES.labels(upstream_id=primary_target.id, status_code=str(resp.status_code)).inc()
                        await cb.record_failure()
                        primary_target.record_completion(latency, success=False)
                        logger.warning(
                            "Primary target '%s' returned HTTP %d. Triggering failover...",
                            primary_target.id,
                            resp.status_code,
                        )
                except Exception as exc:
                    latency = (time.perf_counter() - t0) * 1000.0
                    UPSTREAM_FAILURES.labels(upstream_id=primary_target.id, status_code="504").inc()
                    await cb.record_failure()
                    primary_target.record_completion(latency, success=False)
                    logger.warning(
                        "Primary target '%s' encountered error (%s). Triggering failover...",
                        primary_target.id,
                        exc,
                    )
            else:
                logger.warning("Primary target '%s' circuit is OPEN! Immediate failover.", primary_target.id)
        else:
            logger.warning("No healthy primary target available in route '%s'. Triggering failover.", route.id)

        # 2. Trigger Fallback Pool
        if not route.fallback_targets:
            raise RuntimeError(f"No available upstream targets for route '{route.id}' and no fallback configured.")

        fallback_target = load_balancer.select_target(
            targets=route.fallback_targets,
            strategy=route.strategy,
            pool_id=f"{route.id}:fallback",
        )

        if fallback_target is None:
            raise RuntimeError(f"All fallback targets exhausted or unhealthy for route '{route.id}'.")

        from_id = primary_target.id if primary_target else "none"
        FAILOVER_EVENTS.labels(from_target=from_id, to_target=fallback_target.id).inc()
        logger.info("Engaging fallback target '%s' for route '%s'", fallback_target.id, route.id)

        cb_fallback = self.get_circuit_breaker(fallback_target.id)
        if not await cb_fallback.can_execute():
            raise CircuitBreakerOpenException(fallback_target.id, remaining_seconds=cb_fallback.recovery_time_seconds)

        t0 = time.perf_counter()
        try:
            resp = await self._forward_request(
                target=fallback_target,
                method=method,
                path=subpath,
                headers=headers,
                content=content,
                params=params,
                timeout=route.timeout,
            )
            latency = (time.perf_counter() - t0) * 1000.0

            if resp.status_code < 500:
                await cb_fallback.record_success()
                fallback_target.record_completion(latency, success=True)
            else:
                await cb_fallback.record_failure()
                fallback_target.record_completion(latency, success=False)

            return DispatchResult(
                response=resp,
                target_id=fallback_target.id,
                target_url=fallback_target.url,
                latency_ms=latency,
                is_fallback=True,
            )
        except Exception as exc:
            latency = (time.perf_counter() - t0) * 1000.0
            await cb_fallback.record_failure()
            fallback_target.record_completion(latency, success=False)
            raise RuntimeError(f"Fallback target '{fallback_target.id}' failed: {exc}") from exc

    async def close(self) -> None:
        if self._http_client is not None:
            await self._http_client.aclose()


router_engine = RouterEngine()
