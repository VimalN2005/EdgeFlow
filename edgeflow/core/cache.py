import hashlib
import json
from typing import Any, Dict, Optional, Tuple
from edgeflow.config import settings
from edgeflow.core.redis_client import redis_manager
from edgeflow.core.telemetry import CACHE_HITS, CACHE_MISSES


class ResponseCache:
    """Redis-backed response cache for deterministic API and model inference outputs."""

    def __init__(self, default_ttl: Optional[int] = None):
        self.default_ttl = default_ttl or settings.cache_default_ttl_seconds

    @staticmethod
    def generate_cache_key(method: str, path: str, query: str, body_bytes: bytes) -> str:
        """Generate SHA-256 fingerprint for the request."""
        hasher = hashlib.sha256()
        hasher.update(method.upper().encode("utf-8"))
        hasher.update(b":")
        hasher.update(path.encode("utf-8"))
        hasher.update(b":")
        hasher.update(query.encode("utf-8"))
        hasher.update(b":")
        hasher.update(body_bytes)
        return f"ef:cache:{hasher.hexdigest()}"

    def is_cacheable(self, method: str, headers: Dict[str, str], body_bytes: bytes) -> bool:
        """Determine if request qualifies for caching."""
        if not settings.cache_enabled:
            return False
        # Cache GET and POST (for deterministic LLM prompt requests)
        if method.upper() not in ("GET", "POST"):
            return False
        # Honor no-cache directive
        cache_control = headers.get("cache-control", "").lower()
        if "no-cache" in cache_control or "no-store" in cache_control:
            return False
        # Avoid caching oversized payloads
        if len(body_bytes) > settings.cache_max_body_bytes:
            return False
        return True

    async def get(self, cache_key: str, endpoint: str = "unknown") -> Optional[Tuple[int, bytes, Dict[str, str]]]:
        """Fetch cached response. Returns (status_code, content, headers) or None."""
        if not settings.cache_enabled:
            return None
        try:
            redis = redis_manager.client
            raw = await redis.get(cache_key)
            if not raw:
                CACHE_MISSES.labels(endpoint=endpoint).inc()
                return None

            data = json.loads(raw)
            status_code = data["status"]
            content = data["content"].encode("utf-8")
            headers = data.get("headers", {})
            CACHE_HITS.labels(endpoint=endpoint).inc()
            return status_code, content, headers
        except Exception:
            return None

    async def set(
        self,
        cache_key: str,
        status_code: int,
        content: bytes,
        headers: Dict[str, str],
        ttl: Optional[int] = None,
    ) -> None:
        """Store response in cache if status is 200 OK."""
        if not settings.cache_enabled or status_code != 200:
            return
        try:
            redis = redis_manager.client
            effective_ttl = ttl or self.default_ttl

            # Filter headers to keep only essential content headers
            allowed_headers = {
                k.lower(): v
                for k, v in headers.items()
                if k.lower() in ("content-type", "x-model-backend", "x-provider")
            }

            payload = json.dumps({
                "status": status_code,
                "content": content.decode("utf-8", errors="replace"),
                "headers": allowed_headers,
            })
            await redis.set(cache_key, payload, ex=effective_ttl)
        except Exception:
            pass

    async def flush_all(self) -> int:
        """Purge all cached responses."""
        try:
            redis = redis_manager.client
            keys = await redis.keys("ef:cache:*")
            if keys:
                return await redis.delete(*keys)
            return 0
        except Exception:
            return 0


response_cache = ResponseCache()
