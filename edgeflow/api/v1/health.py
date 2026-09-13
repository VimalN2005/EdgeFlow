from fastapi import APIRouter, Response, status
from edgeflow.config import settings
from edgeflow.core.redis_client import redis_manager

router = APIRouter(tags=["Health & Probes"])


@router.get("/healthz", summary="Liveness Probe")
async def healthz():
    """Kubernetes liveness probe - returns 200 OK if service is alive."""
    return {
        "status": "healthy",
        "app": settings.app_name,
        "version": settings.version,
    }


@router.get("/readyz", summary="Readiness Probe")
async def readyz(response: Response):
    """Kubernetes readiness probe - checks Redis and core engine connectivity."""
    redis_healthy = await redis_manager.is_healthy()
    
    if not redis_healthy:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        return {
            "status": "unready",
            "redis_connected": False,
            "engine": "degraded",
        }

    return {
        "status": "ready",
        "redis_connected": True,
        "redis_engine": "fake_redis" if redis_manager.is_fake else "real_redis",
        "version": settings.version,
    }
