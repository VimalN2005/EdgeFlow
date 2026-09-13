import logging
import time
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from edgeflow.api.v1.admin import router as admin_router
from edgeflow.api.v1.gateway import router as gateway_router
from edgeflow.api.v1.health import router as health_router
from edgeflow.config import settings
from edgeflow.core.load_balancer import LoadBalancingStrategy, UpstreamTarget
from edgeflow.core.redis_client import redis_manager
from edgeflow.core.router import RouteRule, router_engine
from edgeflow.core.telemetry import generate_request_id, get_prometheus_metrics

# Configure Logging
logging.basicConfig(
    level=logging.DEBUG if settings.debug else logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("edgeflow")


def seed_default_routes():
    """Register initial routing configurations for out-of-the-box readiness."""
    # 1. LLM Chat Completions Route with Primary (e.g. OpenAI) and Fallback (e.g. Anthropic)
    llm_route = RouteRule(
        id="llm-chat",
        path_prefix="/v1/chat/completions",
        strategy=LoadBalancingStrategy.ROUND_ROBIN,
        primary_targets=[
            UpstreamTarget(
                id="openai-primary",
                url="http://127.0.0.1:9001",
                weight=3,
                model_name="gpt-4o",
            ),
            UpstreamTarget(
                id="openai-secondary",
                url="http://127.0.0.1:9002",
                weight=2,
                model_name="gpt-4o",
            ),
        ],
        fallback_targets=[
            UpstreamTarget(
                id="claude-fallback",
                url="http://127.0.0.1:9003",
                weight=1,
                model_name="claude-3-5-sonnet",
            ),
        ],
        model_alias="gpt-4o",
        timeout=15.0,
    )
    router_engine.register_route(llm_route)

    # 2. General API / Microservices Route
    api_route = RouteRule(
        id="api-services",
        path_prefix="/api",
        strategy=LoadBalancingStrategy.LEAST_CONNECTIONS,
        primary_targets=[
            UpstreamTarget(id="api-srv-1", url="http://127.0.0.1:9001", weight=1),
            UpstreamTarget(id="api-srv-2", url="http://127.0.0.1:9002", weight=1),
        ],
        fallback_targets=[
            UpstreamTarget(id="api-srv-fallback", url="http://127.0.0.1:9003", weight=1),
        ],
        timeout=10.0,
    )
    router_engine.register_route(api_route)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifecycle manager for startup and shutdown routines."""
    logger.info("Initializing EdgeFlow Gateway v%s...", settings.version)
    await redis_manager.initialize()
    seed_default_routes()
    logger.info("EdgeFlow successfully started on http://%s:%d", settings.host, settings.port)
    yield
    logger.info("Shutting down EdgeFlow...")
    await router_engine.close()
    await redis_manager.close()
    logger.info("EdgeFlow stopped.")


app = FastAPI(
    title=settings.app_name,
    version=settings.version,
    description="High-performance API gateway for intelligent request routing, rate limiting, caching, load balancing, retries, and service/model failover.",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)

# CORS Middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def correlation_and_timing_middleware(request: Request, call_next):
    """Inject correlation ID and track request execution time."""
    req_id = request.headers.get("X-Request-ID") or generate_request_id()
    t0 = time.perf_counter()

    response = await call_next(request)

    latency_ms = (time.perf_counter() - t0) * 1000.0
    response.headers["X-Request-ID"] = req_id
    if "X-Response-Time" not in response.headers:
        response.headers["X-Response-Time"] = f"{latency_ms:.2f}ms"

    return response


# Include API Subrouters
app.include_router(health_router)
app.include_router(admin_router)
app.include_router(gateway_router)


# Metrics endpoint
@app.get("/metrics", tags=["Telemetry"], summary="Prometheus Metrics")
async def metrics():
    """Exposes gateway metrics in standard Prometheus exposition format."""
    return get_prometheus_metrics()


# Root endpoint
@app.get("/", tags=["Info"], summary="Gateway Status")
async def root():
    return {
        "app": settings.app_name,
        "version": settings.version,
        "status": "online",
        "docs": "/docs",
        "metrics": "/metrics",
        "flow": "Client -> EdgeFlow -> Auth -> Rate Limit -> Router -> Service/LLM -> Response",
        "redis_engine": "fake_redis" if redis_manager.is_fake else "real_redis",
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=settings.host, port=settings.port)
