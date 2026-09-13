from typing import Dict, List, Optional
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class UpstreamTargetConfig(BaseSettings):
    """Configuration for an individual upstream target backend."""
    id: str
    url: str
    weight: int = 1
    timeout: float = 10.0
    model_name: Optional[str] = None
    headers: Dict[str, str] = Field(default_factory=dict)


class EdgeFlowSettings(BaseSettings):
    """Global configuration settings for EdgeFlow Gateway."""
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )

    # Core App
    app_name: str = "EdgeFlow"
    version: str = "0.1.0"
    environment: str = "development"
    debug: bool = False
    host: str = "0.0.0.0"
    port: int = 8000

    # Redis Settings
    redis_url: str = "redis://localhost:6379/0"
    redis_timeout_seconds: float = 2.0
    redis_max_connections: int = 50

    # Security & Auth
    api_key_header: str = "X-API-Key"
    gateway_secret_key: str = "edgeflow-super-secret-key-change-in-production"
    jwt_algorithm: str = "HS256"

    # Pre-configured API keys for quick setup / dev
    # Format: {"key_string": {"tenant_id": "...", "tier": "free|pro|enterprise"}}
    api_keys: Dict[str, Dict[str, str]] = Field(
        default_factory=lambda: {
            "ef-live-admin-key": {"tenant_id": "admin-tenant", "tier": "enterprise", "role": "admin"},
            "ef-live-pro-key": {"tenant_id": "client-pro", "tier": "pro", "role": "user"},
            "ef-live-free-key": {"tenant_id": "client-free", "tier": "free", "role": "user"},
        }
    )

    # Rate Limiter Settings (Requests per minute per tier)
    rate_limit_free: int = 30
    rate_limit_pro: int = 120
    rate_limit_enterprise: int = 600

    # Cache Settings
    cache_enabled: bool = True
    cache_default_ttl_seconds: int = 60
    cache_max_body_bytes: int = 1_048_576  # 1MB limit for cached bodies

    # Resilience & Circuit Breaker Defaults
    default_connect_timeout: float = 3.0
    default_read_timeout: float = 15.0
    default_max_retries: int = 2
    retry_backoff_base: float = 0.2  # Base delay in seconds
    circuit_failure_threshold: int = 5
    circuit_recovery_time_seconds: float = 15.0


settings = EdgeFlowSettings()
