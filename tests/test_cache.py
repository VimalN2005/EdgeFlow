import pytest
from edgeflow.core.cache import ResponseCache


@pytest.mark.asyncio
async def test_cache_key_determinism():
    cache = ResponseCache()
    key1 = cache.generate_cache_key("POST", "/v1/chat/completions", "", b'{"model":"gpt-4o"}')
    key2 = cache.generate_cache_key("POST", "/v1/chat/completions", "", b'{"model":"gpt-4o"}')
    key3 = cache.generate_cache_key("POST", "/v1/chat/completions", "", b'{"model":"claude"}')
    
    assert key1 == key2
    assert key1 != key3


@pytest.mark.asyncio
async def test_cache_set_and_get():
    cache = ResponseCache()
    key = "ef:cache:test-key"
    content = b'{"response": "cached-content"}'
    headers = {"content-type": "application/json"}

    # Initial get should miss
    miss = await cache.get(key)
    assert miss is None

    # Store
    await cache.set(key, 200, content, headers, ttl=30)

    # Retrieval should hit
    hit = await cache.get(key)
    assert hit is not None
    status_code, retrieved_content, retrieved_headers = hit
    assert status_code == 200
    assert retrieved_content == content
    assert retrieved_headers.get("content-type") == "application/json"


@pytest.mark.asyncio
async def test_cacheable_rules():
    cache = ResponseCache()
    assert cache.is_cacheable("GET", {}, b"") is True
    assert cache.is_cacheable("POST", {}, b"sample") is True
    # no-cache header bypass
    assert cache.is_cacheable("GET", {"cache-control": "no-cache"}, b"") is False
    # DELETE not cacheable
    assert cache.is_cacheable("DELETE", {}, b"") is False
