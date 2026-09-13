import pytest
from edgeflow.core.auth import ClientIdentity
from edgeflow.core.rate_limiter import SlidingWindowRateLimiter


@pytest.mark.asyncio
async def test_rate_limiter_allowed_within_quota():
    limiter = SlidingWindowRateLimiter(window_seconds=60)
    identity = ClientIdentity(client_id="user-1", tenant_id="t1", tier="free", role="user")

    # Tier free limit is 30, test custom limit of 5
    for i in range(5):
        res = await limiter.check_rate_limit(identity, custom_limit=5)
        assert res.allowed is True
        assert res.remaining == 5 - (i + 1)


@pytest.mark.asyncio
async def test_rate_limiter_blocks_on_excess():
    limiter = SlidingWindowRateLimiter(window_seconds=60)
    identity = ClientIdentity(client_id="user-block", tenant_id="t2", tier="free", role="user")

    # Consume all 3 quota tokens
    for _ in range(3):
        await limiter.check_rate_limit(identity, custom_limit=3)

    # 4th request must be rejected
    rejected = await limiter.check_rate_limit(identity, custom_limit=3)
    assert rejected.allowed is False
    assert rejected.remaining == 0
    assert rejected.retry_after is not None
    assert rejected.retry_after > 0


@pytest.mark.asyncio
async def test_rate_limiter_tier_differences():
    limiter = SlidingWindowRateLimiter(window_seconds=60)
    free_id = ClientIdentity(client_id="f1", tenant_id="t_free", tier="free", role="user")
    pro_id = ClientIdentity(client_id="p1", tenant_id="t_pro", tier="pro", role="user")
    ent_id = ClientIdentity(client_id="e1", tenant_id="t_ent", tier="enterprise", role="user")

    res_free = await limiter.check_rate_limit(free_id)
    res_pro = await limiter.check_rate_limit(pro_id)
    res_ent = await limiter.check_rate_limit(ent_id)

    assert res_free.limit < res_pro.limit < res_ent.limit
