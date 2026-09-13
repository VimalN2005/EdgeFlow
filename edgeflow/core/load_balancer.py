import enum
import itertools
import math
import random
from dataclasses import dataclass, field
from typing import Dict, List, Optional


class LoadBalancingStrategy(str, enum.Enum):
    ROUND_ROBIN = "round_robin"
    WEIGHTED_ROUND_ROBIN = "weighted_round_robin"
    LEAST_CONNECTIONS = "least_connections"
    LATENCY_AWARE = "latency_aware"
    RANDOM = "random"


@dataclass
class UpstreamTarget:
    """Represents a specific backend server or model instance."""
    id: str
    url: str
    weight: int = 1
    model_name: Optional[str] = None
    is_healthy: bool = True
    active_connections: int = 0
    avg_latency_ms: float = 0.0
    total_requests: int = 0
    total_failures: int = 0
    headers: Dict[str, str] = field(default_factory=dict)

    def record_completion(self, latency_ms: float, success: bool = True) -> None:
        """Update moving average latency and connection count."""
        self.active_connections = max(0, self.active_connections - 1)
        self.total_requests += 1
        if success:
            # Exponential moving average (alpha=0.2)
            if self.avg_latency_ms == 0.0:
                self.avg_latency_ms = latency_ms
            else:
                self.avg_latency_ms = (0.8 * self.avg_latency_ms) + (0.2 * latency_ms)
        else:
            self.total_failures += 1


class LoadBalancer:
    """Multi-algorithm load balancer with health check awareness."""

    def __init__(self):
        self._round_robin_indices: Dict[str, int] = {}
        self._weighted_rr_counters: Dict[str, int] = {}

    def select_target(
        self,
        targets: List[UpstreamTarget],
        strategy: LoadBalancingStrategy = LoadBalancingStrategy.ROUND_ROBIN,
        pool_id: str = "default",
    ) -> Optional[UpstreamTarget]:
        """Select healthy target using configured balancing strategy."""
        healthy_targets = [t for t in targets if t.is_healthy]
        if not healthy_targets:
            return None

        if len(healthy_targets) == 1:
            target = healthy_targets[0]
            target.active_connections += 1
            return target

        chosen: UpstreamTarget

        if strategy == LoadBalancingStrategy.ROUND_ROBIN:
            idx = self._round_robin_indices.get(pool_id, 0)
            chosen = healthy_targets[idx % len(healthy_targets)]
            self._round_robin_indices[pool_id] = (idx + 1) % len(healthy_targets)

        elif strategy == LoadBalancingStrategy.WEIGHTED_ROUND_ROBIN:
            # Interleaved weighted selection
            total_weight = sum(max(1, t.weight) for t in healthy_targets)
            counter = self._weighted_rr_counters.get(pool_id, 0) % total_weight
            self._weighted_rr_counters[pool_id] = counter + 1
            
            cumulative = 0
            chosen = healthy_targets[0]
            for t in healthy_targets:
                cumulative += max(1, t.weight)
                if counter < cumulative:
                    chosen = t
                    break

        elif strategy == LoadBalancingStrategy.LEAST_CONNECTIONS:
            # Choose target with lowest active in-flight connections
            chosen = min(healthy_targets, key=lambda t: t.active_connections)

        elif strategy == LoadBalancingStrategy.LATENCY_AWARE:
            # Choose target with lowest moving average latency
            # Targets with 0 measured latency are prioritized for exploration
            untested = [t for t in healthy_targets if t.avg_latency_ms == 0.0]
            if untested:
                chosen = untested[0]
            else:
                chosen = min(healthy_targets, key=lambda t: t.avg_latency_ms)

        elif strategy == LoadBalancingStrategy.RANDOM:
            chosen = random.choice(healthy_targets)

        else:
            chosen = healthy_targets[0]

        chosen.active_connections += 1
        return chosen


load_balancer = LoadBalancer()
