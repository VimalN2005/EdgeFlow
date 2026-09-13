import asyncio
import enum
import logging
import time
from typing import Optional
from edgeflow.config import settings
from edgeflow.core.telemetry import CIRCUIT_STATE

logger = logging.getLogger("edgeflow.circuit_breaker")


class CircuitState(str, enum.Enum):
    CLOSED = "CLOSED"        # Normal operation: traffic flows through
    OPEN = "OPEN"            # Tripped: fast fail all incoming requests
    HALF_OPEN = "HALF_OPEN"  # Recovery probe: limited requests allowed


class CircuitBreakerOpenException(Exception):
    """Raised when request is attempted while circuit is in OPEN state."""
    def __init__(self, circuit_id: str, remaining_seconds: float):
        self.circuit_id = circuit_id
        self.remaining_seconds = remaining_seconds
        super().__init__(f"Circuit '{circuit_id}' is OPEN. Retry in {remaining_seconds:.1f}s")


class CircuitBreaker:
    """Three-state Circuit Breaker pattern implementation with sliding failure window

    and half-open recovery probing.
    """

    def __init__(
        self,
        circuit_id: str,
        failure_threshold: Optional[int] = None,
        recovery_time_seconds: Optional[float] = None,
        half_open_max_probes: int = 2,
    ):
        self.circuit_id = circuit_id
        self.failure_threshold = failure_threshold or settings.circuit_failure_threshold
        self.recovery_time_seconds = recovery_time_seconds or settings.circuit_recovery_time_seconds
        self.half_open_max_probes = half_open_max_probes

        self._state: CircuitState = CircuitState.CLOSED
        self._failure_count: int = 0
        self._success_count: int = 0
        self._last_failure_time: float = 0.0
        self._lock = asyncio.Lock()

        # Update gauge
        self._update_metric()

    @property
    def state(self) -> CircuitState:
        return self._state

    def _update_metric(self) -> None:
        state_val = 0 if self._state == CircuitState.CLOSED else (1 if self._state == CircuitState.HALF_OPEN else 2)
        CIRCUIT_STATE.labels(circuit_id=self.circuit_id).set(state_val)

    async def can_execute(self) -> bool:
        """Check if request can pass through circuit breaker.

        Transitions from OPEN to HALF_OPEN if recovery timeout has elapsed.
        """
        async with self._lock:
            now = time.time()
            if self._state == CircuitState.OPEN:
                elapsed = now - self._last_failure_time
                if elapsed >= self.recovery_time_seconds:
                    logger.info("Circuit '%s' entered HALF_OPEN state (probing upstream)", self.circuit_id)
                    self._state = CircuitState.HALF_OPEN
                    self._success_count = 0
                    self._update_metric()
                    return True
                else:
                    return False
            return True

    async def record_success(self) -> None:
        """Record a successful response."""
        async with self._lock:
            if self._state == CircuitState.HALF_OPEN:
                self._success_count += 1
                if self._success_count >= self.half_open_max_probes:
                    logger.info("Circuit '%s' recovered! Resetting to CLOSED state.", self.circuit_id)
                    self._state = CircuitState.CLOSED
                    self._failure_count = 0
                    self._success_count = 0
                    self._update_metric()
            elif self._state == CircuitState.CLOSED:
                self._failure_count = 0

    async def record_failure(self) -> None:
        """Record a failure (5xx or connection error)."""
        async with self._lock:
            self._last_failure_time = time.time()
            if self._state == CircuitState.HALF_OPEN:
                logger.warning("Circuit '%s' probe failed! Re-opening circuit for %ss", self.circuit_id, self.recovery_time_seconds)
                self._state = CircuitState.OPEN
                self._update_metric()
            elif self._state == CircuitState.CLOSED:
                self._failure_count += 1
                if self._failure_count >= self.failure_threshold:
                    logger.warning(
                        "Circuit '%s' tripped OPEN! (%d consecutive failures). Will probe in %ss",
                        self.circuit_id,
                        self._failure_count,
                        self.recovery_time_seconds,
                    )
                    self._state = CircuitState.OPEN
                    self._update_metric()

    async def reset(self) -> None:
        """Manually reset circuit breaker to CLOSED."""
        async with self._lock:
            self._state = CircuitState.CLOSED
            self._failure_count = 0
            self._success_count = 0
            self._update_metric()
