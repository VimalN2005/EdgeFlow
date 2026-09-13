import logging
from typing import Optional
import redis.asyncio as aioredis
from edgeflow.config import settings

logger = logging.getLogger("edgeflow.redis")


class RedisManager:
    """Manages asynchronous Redis connections with automatic fallback to FakeRedis

    when external Redis is unavailable.
    """

    def __init__(self, url: Optional[str] = None):
        self.url = url or settings.redis_url
        self._client: Optional[aioredis.Redis] = None
        self._is_fake: bool = False

    async def initialize(self) -> aioredis.Redis:
        """Initialize connection to Redis or fallback to fakeredis."""
        try:
            client = aioredis.from_url(
                self.url,
                socket_connect_timeout=settings.redis_timeout_seconds,
                socket_timeout=settings.redis_timeout_seconds,
                max_connections=settings.redis_max_connections,
                decode_responses=True,
            )
            # Test connection
            await client.ping()
            self._client = client
            self._is_fake = False
            logger.info("Connected successfully to external Redis at %s", self.url)
            return self._client
        except Exception as exc:
            logger.warning(
                "External Redis unavailable (%s). Falling back to in-memory FakeRedis engine.",
                exc,
            )
            import fakeredis.aioredis as fake_aioredis
            self._client = fake_aioredis.FakeRedis(decode_responses=True)
            self._is_fake = True
            logger.info("EdgeFlow is operating with high-performance In-Memory Redis.")
            return self._client

    @property
    def client(self) -> aioredis.Redis:
        if self._client is None:
            raise RuntimeError("Redis client is not initialized. Call initialize() first.")
        return self._client

    @property
    def is_fake(self) -> bool:
        return self._is_fake

    async def is_healthy(self) -> bool:
        """Check if Redis connection is active and responsive."""
        if self._client is None:
            return False
        try:
            return await self._client.ping()
        except Exception:
            return False

    async def close(self) -> None:
        """Close connection pool."""
        if self._client is not None:
            await self._client.aclose()
            logger.info("Redis connection closed.")


redis_manager = RedisManager()
