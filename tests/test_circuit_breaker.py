import asyncio
import pytest
from edgeflow.core.circuit_breaker import CircuitBreaker, CircuitState


@pytest.mark.asyncio
async def test_circuit_breaker_transitions():
    # Low threshold for testing
    cb = CircuitBreaker(
        circuit_id="test-cb",
        failure_threshold=3,
        recovery_time_seconds=0.1,  # Fast 100ms recovery for tests
        half_open_max_probes=2,
    )

    # Initial state: CLOSED
    assert cb.state == CircuitState.CLOSED
    assert await cb.can_execute() is True

    # 1st and 2nd failure: remains CLOSED
    await cb.record_failure()
    assert cb.state == CircuitState.CLOSED
    await cb.record_failure()
    assert cb.state == CircuitState.CLOSED

    # 3rd failure: trips to OPEN!
    await cb.record_failure()
    assert cb.state == CircuitState.OPEN
    assert await cb.can_execute() is False

    # Wait for recovery timeout (150ms > 100ms)
    await asyncio.sleep(0.15)

    # Calling can_execute() should transition to HALF_OPEN probe
    can_probe = await cb.can_execute()
    assert can_probe is True
    assert cb.state == CircuitState.HALF_OPEN

    # 1st probe success: remains HALF_OPEN
    await cb.record_success()
    assert cb.state == CircuitState.HALF_OPEN

    # 2nd probe success: recovers to CLOSED!
    await cb.record_success()
    assert cb.state == CircuitState.CLOSED
    assert await cb.can_execute() is True


@pytest.mark.asyncio
async def test_circuit_breaker_half_open_failure():
    cb = CircuitBreaker(
        circuit_id="test-cb-probe-fail",
        failure_threshold=2,
        recovery_time_seconds=0.05,
    )

    await cb.record_failure()
    await cb.record_failure()
    assert cb.state == CircuitState.OPEN

    await asyncio.sleep(0.06)
    assert await cb.can_execute() is True
    assert cb.state == CircuitState.HALF_OPEN

    # Probe failure should immediately re-open circuit
    await cb.record_failure()
    assert cb.state == CircuitState.OPEN
