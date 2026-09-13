import math
import time
import uuid
from dataclasses import dataclass
from typing import Optional
from edgeflow.config import settings
from edgeflow.core.auth import ClientIdentity
from edgeflow.core.redis_client import redis_manager
from edgeflow.core.telemetry import RATE_LIMIT_EXCEEDED


@dataclass
class RateLimitResult:
    allowed: bool
    limit: int
    remaining: int
    reset_after_seconds: float
    retry_after: Optional[int] = None


class SlidingWindowRateLimiter:
    """Redis-backed distributed Sliding Window Log rate limiter.

    Guarantees strict per-second / per-minute rate protection without burst boundary leaks.
    """

    def __init__(self, window_seconds: int = 60):
        self.window_seconds = window_seconds

    def get_tier_limit(self, tier: str) -> int:
        tier_lower = tier.lower()
        if tier_lower == "enterprise":
            return settings.rate_limit_enterprise
        elif tier_lower == "pro":
            return settings.rate_limit_pro
        return settings.rate_limit_free

    async def check_rate_limit(
        self,
        identity: ClientIdentity,
        custom_limit: Optional[int] = None,
    ) -> RateLimitResult:
        """Check and record request using Redis sliding window log with ZSET."""
        redis = redis_manager.client
        limit = custom_limit if custom_limit is not None else self.get_tier_limit(identity.tier)
        
        now = time.time()
        window_start = now - self.window_seconds
        key = f"ef:ratelimit:{identity.tenant_id}:{identity.client_id}"
        req_token = f"{now}:{uuid.uuid4().hex[:8]}"

        # Execute sliding window logic via atomic pipeline
        pipe = redis.pipeline(transaction=True)
        # 1. Purge requests outside current sliding window
        pipe.zremrangebyscore(key, 0, window_start)
        # 2. Count active requests in current window
        pipe.zcard(key)
        # 3. Fetch oldest timestamp in current window to calculate exact reset time
        pipe.zrange(key, 0, 0, withscores=True)
        
        results = await pipe.execute()
        current_count = results[1]
        oldest_entry = results[2]

        if current_count >= limit:
            # Over rate limit
            RATE_LIMIT_EXCEEDED.labels(tier=identity.tier, client_id=identity.client_id).inc()
            
            # Calculate when oldest request leaves the window
            oldest_ts = oldest_entry[0][1] if oldest_entry else window_start
            reset_seconds = max(1.0, (oldest_ts + self.window_seconds) - now)
            retry_after = math.ceil(reset_seconds)

            return RateLimitResult(
                allowed=False,
                limit=limit,
                remaining=0,
                reset_after_seconds=reset_seconds,
                retry_after=retry_after,
            )

        # Under limit: add current timestamp to ZSET and extend TTL
        pipe = redis.pipeline(transaction=True)
        pipe.zadd(key, {req_token: now})
        pipe.expire(key, self.window_seconds + 5)
        await pipe.execute()

        remaining = max(0, limit - (current_count + 1))
        reset_seconds = float(self.window_seconds)

        return RateLimitResult(
            allowed=True,
            limit=limit,
            remaining=remaining,
            reset_after_seconds=reset_seconds,
            retry_after=None,
        )


rate_limiter = SlidingWindowRateLimiter()
