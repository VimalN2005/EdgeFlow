import jwt
import pytest
from fastapi import HTTPException
from starlette.requests import Request
from edgeflow.config import settings
from edgeflow.core.auth import authenticate_request, decode_jwt_token


@pytest.mark.asyncio
async def test_anonymous_auth():
    scope = {
        "type": "http",
        "method": "GET",
        "path": "/healthz",
        "headers": [],
        "client": ("127.0.0.1", 12345),
    }
    req = Request(scope)
    identity = await authenticate_request(req)
    assert identity.tier == "free"
    assert identity.role == "guest"
    assert "anon-" in identity.client_id


@pytest.mark.asyncio
async def test_valid_api_key_auth():
    scope = {
        "type": "http",
        "method": "GET",
        "path": "/api/data",
        "headers": [(b"x-api-key", b"ef-live-pro-key")],
    }
    req = Request(scope)
    identity = await authenticate_request(req)
    assert identity.tier == "pro"
    assert identity.tenant_id == "client-pro"
    assert identity.role == "user"


@pytest.mark.asyncio
async def test_invalid_api_key_auth():
    scope = {
        "type": "http",
        "method": "GET",
        "path": "/api/data",
        "headers": [(b"x-api-key", b"invalid-random-key")],
    }
    req = Request(scope)
    with pytest.raises(HTTPException) as exc_info:
        await authenticate_request(req)
    assert exc_info.value.status_code == 401


@pytest.mark.asyncio
async def test_jwt_bearer_token_auth():
    payload = {
        "sub": "user_42",
        "tenant_id": "tenant_enterprise",
        "tier": "enterprise",
        "role": "admin",
    }
    token = jwt.encode(payload, settings.gateway_secret_key, algorithm=settings.jwt_algorithm)
    identity = decode_jwt_token(token)
    assert identity is not None
    assert identity.client_id == "user_42"
    assert identity.tier == "enterprise"
    assert identity.is_admin is True
