import asyncio
import pytest
import pytest_asyncio
import fakeredis.aioredis as fake_aioredis
import httpx
from edgeflow.config import settings
from edgeflow.core.redis_client import redis_manager
from edgeflow.core.router import router_engine
from edgeflow.main import app, seed_default_routes


@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for each test case."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest_asyncio.fixture(autouse=True)
async def setup_test_redis():
    """Ensure every test uses a clean isolated FakeRedis instance."""
    fake_client = fake_aioredis.FakeRedis(decode_responses=True)
    redis_manager._client = fake_client
    redis_manager._is_fake = True
    # Seed routes
    seed_default_routes()
    yield
    await fake_client.flushall()
    await fake_client.aclose()


@pytest_asyncio.fixture
async def client():
    """Async HTTP test client for EdgeFlow app."""
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as ac:
        yield ac
