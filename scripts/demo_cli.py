import asyncio
import os
import sys
import time

# Ensure UTF-8 output on Windows
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

# Ensure edgeflow package is on path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import fakeredis.aioredis as fake_aioredis
import httpx
from rich.console import Console
from rich.panel import Panel
from edgeflow.config import settings
from edgeflow.core.load_balancer import LoadBalancingStrategy, UpstreamTarget
from edgeflow.core.redis_client import redis_manager
from edgeflow.core.router import RouteRule, router_engine
from edgeflow.main import app

console = Console(legacy_windows=False)


async def run_live_demo():
    console.print(
        Panel.fit(
            "[bold cyan]EDGEFLOW: INTELLIGENT GATEWAY & TRAFFIC ROUTER DEMO[/bold cyan]\n"
            "[white]FastAPI + Redis + Sliding-Window Rate Limiting + Response Caching + Failover[/white]",
            border_style="cyan",
        )
    )

    # Setup isolated in-memory redis
    fake_client = fake_aioredis.FakeRedis(decode_responses=True)
    redis_manager._client = fake_client
    redis_manager._is_fake = True

    # 1. Health Probe
    console.print("\n[bold yellow]--- Step 1: Health & Kubernetes Probes ---[/bold yellow]")
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        r_ready = await client.get("/readyz")
        console.print(f"[green][OK] /readyz Probe:[/green] {r_ready.json()}")

        # 2. Seed Demo Route with Primary & Fallback
        console.print("\n[bold yellow]--- Step 2: Registering Intelligent Route with Fallback ---[/bold yellow]")
        demo_route = RouteRule(
            id="demo-llm",
            path_prefix="/v1/chat/completions",
            strategy=LoadBalancingStrategy.ROUND_ROBIN,
            primary_targets=[
                UpstreamTarget(id="openai-us-east", url="http://upstream-primary:9001", weight=1),
                UpstreamTarget(id="openai-us-west", url="http://upstream-primary:9002", weight=1),
            ],
            fallback_targets=[
                UpstreamTarget(id="claude-backup", url="http://upstream-fallback:9003", weight=1),
            ],
            model_alias="gpt-4o",
        )
        router_engine.register_route(demo_route)
        console.print("[green][OK] Route 'demo-llm' registered with 2 Primaries and 1 Fallback target.[/green]")

        # 3. Response Caching Test
        console.print("\n[bold yellow]--- Step 3: Response Caching (SHA-256 Request Fingerprinting) ---[/bold yellow]")
        from edgeflow.core.router import DispatchResult
        mock_dispatch = DispatchResult(
            response=httpx.Response(
                200,
                headers={"content-type": "application/json"},
                content=b'{"id":"chatcmpl-demo","choices":[{"message":{"content":"Cached response example."}}]}',
            ),
            target_id="openai-us-east",
            target_url="http://upstream-primary:9001",
            latency_ms=15.2,
        )

        from unittest.mock import patch
        with patch.object(router_engine, "dispatch", return_value=mock_dispatch):
            payload = {"model": "gpt-4o", "messages": [{"role": "user", "content": "What is EdgeFlow?"}]}
            headers = {"X-API-Key": "ef-live-pro-key"}

            # Request 1 (Cache MISS)
            t0 = time.perf_counter()
            r1 = await client.post("/v1/chat/completions", json=payload, headers=headers)
            ms1 = (time.perf_counter() - t0) * 1000.0
            console.print(f"Request 1: Status={r1.status_code} | [bold red]X-Cache={r1.headers.get('X-Cache')}[/bold red] | Target={r1.headers.get('X-EdgeFlow-Target')} | Latency={ms1:.2f}ms")

            # Request 2 (Cache HIT)
            t0 = time.perf_counter()
            r2 = await client.post("/v1/chat/completions", json=payload, headers=headers)
            ms2 = (time.perf_counter() - t0) * 1000.0
            console.print(f"Request 2: Status={r2.status_code} | [bold green]X-Cache={r2.headers.get('X-Cache')}[/bold green] | Target={r2.headers.get('X-EdgeFlow-Target')} | Latency={ms2:.2f}ms (Sub-millisecond)")

        # 4. Multi-tier Sliding Window Rate Limiter
        console.print("\n[bold yellow]--- Step 4: Multi-tier Sliding Window Rate Limiting ---[/bold yellow]")
        settings.rate_limit_free = 3
        console.print("Testing 'free' tier quota (Limit: 3 req/min):")

        with patch.object(router_engine, "dispatch", return_value=mock_dispatch):
            for i in range(1, 5):
                r = await client.post(
                    "/v1/chat/completions",
                    json={"model": "gpt-4o", "messages": [{"role": "user", "content": f"msg {i}"}]},
                    headers={"X-API-Key": "ef-live-free-key", "Cache-Control": "no-cache"},
                )
                if r.status_code == 200:
                    console.print(f"  Request {i}: [green]HTTP 200 OK[/green] (Remaining: {r.headers.get('X-RateLimit-Remaining')})")
                else:
                    console.print(f"  Request {i}: [bold red]HTTP 429 Too Many Requests[/bold red] (Retry-After: {r.headers.get('Retry-After')}s)")

        # 5. Automatic Primary Failover Demonstration
        console.print("\n[bold yellow]--- Step 5: Automatic Multi-Provider Failover Simulation ---[/bold yellow]")
        console.print("Simulating primary OpenAI outage (HTTP 503 Service Unavailable)...")

        # Mock primary failing and fallback succeeding
        async def simulated_failover(route, method, subpath, headers, content, params):
            console.print("  [red][FAIL] Primary Target 'openai-us-east' failed with HTTP 503.[/red]")
            console.print("  [cyan]--> EdgeFlow engaging fallback pool 'claude-backup'...[/cyan]")
            return DispatchResult(
                response=httpx.Response(200, content=b'{"provider":"claude-backup","text":"Hello from standby!"}'),
                target_id="claude-backup",
                target_url="http://upstream-fallback:9003",
                latency_ms=22.0,
                is_fallback=True,
            )

        with patch.object(router_engine, "dispatch", side_effect=simulated_failover):
            r_failover = await client.post(
                "/v1/chat/completions",
                json={"model": "gpt-4o", "messages": [{"role": "user", "content": "Failover prompt"}]},
                headers={"X-API-Key": "ef-live-pro-key", "Cache-Control": "no-cache"},
            )
            console.print(f"  [bold green][SUCCESS] Caller received response seamlessly:[/bold green] Status={r_failover.status_code}")
            console.print(f"  Target: [bold cyan]{r_failover.headers.get('X-EdgeFlow-Target')}[/bold cyan] | Fallback Engaged: [bold green]{r_failover.headers.get('X-EdgeFlow-Fallback')}[/bold green]")

    console.print("\n[bold green][DONE] Live Demonstration Completed Successfully![/bold green]\n")


if __name__ == "__main__":
    asyncio.run(run_live_demo())
