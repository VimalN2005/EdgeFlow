from typing import Dict, List, Optional
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field
from edgeflow.core.auth import ClientIdentity, authenticate_request
from edgeflow.core.cache import response_cache
from edgeflow.core.circuit_breaker import CircuitState
from edgeflow.core.load_balancer import LoadBalancingStrategy, UpstreamTarget
from edgeflow.core.router import RouteRule, router_engine

router = APIRouter(prefix="/api/v1/admin", tags=["Admin & Control Plane"])


def require_admin(identity: ClientIdentity = Depends(authenticate_request)) -> ClientIdentity:
    """Dependency ensuring request is made by an authorized admin."""
    if not identity.is_admin and identity.tier != "enterprise":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Administrative privileges required.",
        )
    return identity


class UpstreamTargetCreate(BaseModel):
    id: str
    url: str
    weight: int = 1
    model_name: Optional[str] = None
    headers: Dict[str, str] = Field(default_factory=dict)


class RouteCreateRequest(BaseModel):
    id: str
    path_prefix: str
    strategy: LoadBalancingStrategy = LoadBalancingStrategy.ROUND_ROBIN
    primary_targets: List[UpstreamTargetCreate]
    fallback_targets: List[UpstreamTargetCreate] = Field(default_factory=list)
    model_alias: Optional[str] = None
    timeout: float = 10.0


@router.get("/routes", summary="List All Active Routes")
async def list_routes(_: ClientIdentity = Depends(require_admin)):
    """Retrieve full routing table with health status and metrics."""
    result = []
    for r in router_engine.routes.values():
        result.append({
            "id": r.id,
            "path_prefix": r.path_prefix,
            "strategy": r.strategy.value,
            "model_alias": r.model_alias,
            "timeout": r.timeout,
            "primary_targets": [
                {
                    "id": t.id,
                    "url": t.url,
                    "weight": t.weight,
                    "is_healthy": t.is_healthy,
                    "active_connections": t.active_connections,
                    "avg_latency_ms": round(t.avg_latency_ms, 2),
                    "total_requests": t.total_requests,
                    "total_failures": t.total_failures,
                }
                for t in r.primary_targets
            ],
            "fallback_targets": [
                {
                    "id": t.id,
                    "url": t.url,
                    "weight": t.weight,
                    "is_healthy": t.is_healthy,
                    "active_connections": t.active_connections,
                    "avg_latency_ms": round(t.avg_latency_ms, 2),
                    "total_requests": t.total_requests,
                    "total_failures": t.total_failures,
                }
                for t in r.fallback_targets
            ],
        })
    return {"routes": result, "count": len(result)}


@router.post("/routes", summary="Register or Update a Route", status_code=status.HTTP_201_CREATED)
async def create_route(data: RouteCreateRequest, _: ClientIdentity = Depends(require_admin)):
    """Dynamically register or update a route in EdgeFlow router."""
    primaries = [
        UpstreamTarget(
            id=t.id,
            url=t.url,
            weight=t.weight,
            model_name=t.model_name,
            headers=t.headers,
        )
        for t in data.primary_targets
    ]
    fallbacks = [
        UpstreamTarget(
            id=t.id,
            url=t.url,
            weight=t.weight,
            model_name=t.model_name,
            headers=t.headers,
        )
        for t in data.fallback_targets
    ]

    rule = RouteRule(
        id=data.id,
        path_prefix=data.path_prefix,
        strategy=data.strategy,
        primary_targets=primaries,
        fallback_targets=fallbacks,
        model_alias=data.model_alias,
        timeout=data.timeout,
    )
    router_engine.register_route(rule)
    return {"message": f"Route '{data.id}' successfully registered.", "route_id": data.id}


@router.get("/circuits", summary="Inspect Circuit Breakers")
async def list_circuits(_: ClientIdentity = Depends(require_admin)):
    """Inspect state of all target circuit breakers."""
    circuits = {}
    for target_id, cb in router_engine.circuit_breakers.items():
        circuits[target_id] = {
            "circuit_id": cb.circuit_id,
            "state": cb.state.value,
            "failure_count": cb._failure_count,
            "failure_threshold": cb.failure_threshold,
            "recovery_time_seconds": cb.recovery_time_seconds,
        }
    return {"circuits": circuits}


@router.post("/circuits/{circuit_id}/reset", summary="Reset Circuit Breaker")
async def reset_circuit(circuit_id: str, _: ClientIdentity = Depends(require_admin)):
    """Manually reset a circuit breaker to CLOSED."""
    cb = router_engine.circuit_breakers.get(circuit_id)
    if not cb:
        raise HTTPException(status_code=404, detail=f"Circuit '{circuit_id}' not found.")
    await cb.reset()
    return {"message": f"Circuit '{circuit_id}' reset to CLOSED."}


@router.post("/cache/flush", summary="Flush Response Cache")
async def flush_cache(_: ClientIdentity = Depends(require_admin)):
    """Purge all cached responses across EdgeFlow."""
    purged_count = await response_cache.flush_all()
    return {"message": "Cache successfully cleared.", "purged_keys": purged_count}
