import asyncio
import logging
import random
from typing import Callable, Coroutine, List, Optional, Set, TypeVar
import httpx
from edgeflow.config import settings

logger = logging.getLogger("edgeflow.retry")

T = TypeVar("T")

RETRYABLE_STATUS_CODES: Set[int] = {429, 500, 502, 503, 504}


def calculate_backoff_delay(attempt: int, base: float = 0.1, max_delay: float = 2.0) -> float:
    """Calculate exponential backoff with full jitter."""
    calculated = min(max_delay, base * (2 ** attempt))
    # Full jitter: random uniform between 0 and calculated
    return random.uniform(0.0, calculated)


async def execute_with_retry(
    action: Callable[[], Coroutine[None, None, httpx.Response]],
    max_retries: Optional[int] = None,
    retryable_statuses: Optional[Set[int]] = None,
    on_retry_callback: Optional[Callable[[int, Exception, float], None]] = None,
) -> httpx.Response:
    """Execute async HTTP action with exponential backoff retries."""
    retries = max_retries if max_retries is not None else settings.default_max_retries
    statuses = retryable_statuses or RETRYABLE_STATUS_CODES
    last_response: Optional[httpx.Response] = None
    last_error: Optional[Exception] = None

    for attempt in range(retries + 1):
        try:
            response = await action()
            if response.status_code not in statuses or attempt == retries:
                return response
            
            # Status code indicates transient failure, calculate backoff
            last_response = response
            delay = calculate_backoff_delay(attempt, base=settings.retry_backoff_base)
            logger.warning(
                "Upstream returned HTTP %d on attempt %d/%d. Retrying in %.3fs...",
                response.status_code,
                attempt + 1,
                retries + 1,
                delay,
            )
            if on_retry_callback:
                on_retry_callback(attempt, Exception(f"HTTP {response.status_code}"), delay)
            await asyncio.sleep(delay)

        except (httpx.ConnectError, httpx.ConnectTimeout, httpx.ReadTimeout, httpx.RemoteProtocolError) as exc:
            last_error = exc
            if attempt == retries:
                raise exc
            delay = calculate_backoff_delay(attempt, base=settings.retry_backoff_base)
            logger.warning(
                "Upstream network error (%s) on attempt %d/%d. Retrying in %.3fs...",
                str(exc),
                attempt + 1,
                retries + 1,
                delay,
            )
            if on_retry_callback:
                on_retry_callback(attempt, exc, delay)
            await asyncio.sleep(delay)

    if last_response is not None:
        return last_response
    if last_error is not None:
        raise last_error
    raise RuntimeError("Retry loop exited unexpectedly without result.")
