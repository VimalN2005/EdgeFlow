import argparse
import asyncio
import json
import logging
import os
import statistics
import sys
import time
from typing import Dict, List

# Ensure edgeflow package is resolvable
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Suppress verbose HTTP client logging during benchmarks
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("edgeflow").setLevel(logging.WARNING)

import fakeredis.aioredis as fake_aioredis
import httpx
from edgeflow.config import settings
from edgeflow.core.redis_client import redis_manager
from edgeflow.core.router import DispatchResult, router_engine
from edgeflow.main import app, seed_default_routes


async def run_scenario(
    name: str,
    client: httpx.AsyncClient,
    method: str,
    url: str,
    headers: Dict[str, str],
    json_data: dict,
    total_requests: int = 1000,
    concurrency: int = 50,
) -> Dict[str, float]:
    """Execute concurrent requests and gather exact latency and throughput metrics."""
    semaphore = asyncio.Semaphore(concurrency)
    latencies_ms: List[float] = []
    success_count = 0
    rejected_count = 0

    async def worker():
        nonlocal success_count, rejected_count
        async with semaphore:
            t0 = time.perf_counter()
            try:
                resp = await client.request(
                    method=method,
                    url=url,
                    headers=headers,
                    json=json_data if json_data else None,
                )
                t1 = time.perf_counter()
                latency = (t1 - t0) * 1000.0
                latencies_ms.append(latency)
                if resp.status_code in (200, 201):
                    success_count += 1
                elif resp.status_code == 429:
                    rejected_count += 1
            except Exception:
                pass

    # Warm-up request
    await client.request(method=method, url=url, headers=headers, json=json_data if json_data else None)

    # Main measurement run
    start_time = time.perf_counter()
    tasks = [asyncio.create_task(worker()) for _ in range(total_requests)]
    await asyncio.gather(*tasks)
    total_duration = time.perf_counter() - start_time

    rps = total_requests / total_duration if total_duration > 0 else 0
    latencies_ms.sort()

    def get_percentile(p: float) -> float:
        idx = int(len(latencies_ms) * p / 100.0)
        idx = min(idx, len(latencies_ms) - 1)
        return latencies_ms[idx]

    stats = {
        "scenario": name,
        "requests": total_requests,
        "concurrency": concurrency,
        "duration_seconds": round(total_duration, 3),
        "rps": round(rps, 1),
        "success_count": success_count,
        "rate_limited_count": rejected_count,
        "min_ms": round(latencies_ms[0], 2) if latencies_ms else 0.0,
        "mean_ms": round(statistics.mean(latencies_ms), 2) if latencies_ms else 0.0,
        "p50_ms": round(get_percentile(50), 2) if latencies_ms else 0.0,
        "p90_ms": round(get_percentile(90), 2) if latencies_ms else 0.0,
        "p95_ms": round(get_percentile(95), 2) if latencies_ms else 0.0,
        "p99_ms": round(get_percentile(99), 2) if latencies_ms else 0.0,
        "max_ms": round(latencies_ms[-1], 2) if latencies_ms else 0.0,
    }
    return stats


async def main():
    parser = argparse.ArgumentParser(description="EdgeFlow Performance Benchmarks")
    parser.add_argument("--requests", type=int, default=1000, help="Total requests per scenario")
    parser.add_argument("--concurrency", type=int, default=50, help="Concurrency level")
    parser.add_argument("--output", type=str, default="benchmark_results.json", help="Output file")
    args = parser.parse_args()

    # Ensure clean redis & routes
    fake_client = fake_aioredis.FakeRedis(decode_responses=True)
    redis_manager._client = fake_client
    redis_manager._is_fake = True
    seed_default_routes()

    # Set high enterprise rate limit for pure throughput testing
    settings.rate_limit_enterprise = 100000

    # Mock an ultra-fast upstream response to measure Gateway proxy overhead cleanly
    mock_resp = httpx.Response(
        200,
        headers={"content-type": "application/json"},
        content=b'{"id":"chatcmpl-bench","choices":[{"message":{"content":"Hello Bench"}}]}',
    )
    mock_dispatch = DispatchResult(
        response=mock_resp,
        target_id="mock-openai",
        target_url="http://127.0.0.1:9001",
        latency_ms=0.5,
    )

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        print(f"\n=========================================================================")
        print(f"            EDGEFLOW HIGH-PERFORMANCE BENCHMARK SUITE")
        print(f"            Requests: {args.requests} | Concurrency: {args.concurrency}")
        print(f"=========================================================================\n")

        results = []

        # 1. Baseline Ingress Health Probe
        print("[1/4] Benchmarking Baseline Ingress (/healthz)...")
        r1 = await run_scenario(
            name="Baseline Ingress (/healthz)",
            client=client,
            method="GET",
            url="/healthz",
            headers={},
            json_data={},
            total_requests=args.requests,
            concurrency=args.concurrency,
        )
        results.append(r1)

        # Flush redis keys between tests
        await fake_client.flushall()

        # 2. Response Cache HIT (Auth + Rate Limiter + Cache Hit + Response)
        print("[2/4] Benchmarking Response Cache HIT (/v1/chat/completions)...")
        payload = {"model": "gpt-4o", "messages": [{"role": "user", "content": "Benchmark test"}]}
        auth_headers = {"X-API-Key": "ef-live-admin-key"}

        from unittest.mock import patch
        with patch.object(router_engine, "dispatch", return_value=mock_dispatch):
            # Prime cache
            await client.post("/v1/chat/completions", headers=auth_headers, json=payload)

            r2 = await run_scenario(
                name="Response Cache HIT (Auth + Redis + Cache)",
                client=client,
                method="POST",
                url="/v1/chat/completions",
                headers=auth_headers,
                json_data=payload,
                total_requests=args.requests,
                concurrency=args.concurrency,
            )
            results.append(r2)

        # Flush redis cache keys but keep ratelimit
        await fake_client.flushall()

        # 3. Dynamic Routed Upstream Proxy (Auth + Rate Limit + Cache Miss + LB + Upstream Dispatch + Cache Write)
        print("[3/4] Benchmarking Dynamic Routed Upstream Proxy (Full Lifecycle)...")
        with patch.object(router_engine, "dispatch", return_value=mock_dispatch):
            r3 = await run_scenario(
                name="Dynamic Routed Proxy (Full Lifecycle)",
                client=client,
                method="POST",
                url="/v1/chat/completions",
                headers={"X-API-Key": "ef-live-admin-key", "Cache-Control": "no-cache"},
                json_data={"model": "gpt-4o", "messages": [{"role": "user", "content": "uncached test"}]},
                total_requests=args.requests,
                concurrency=args.concurrency,
            )
            results.append(r3)

        # 4. Fast-Fail Rate Limiter (DDoS / Quota exhaustion protection)
        print("[4/4] Benchmarking Rate Limiter Fast-Fail Rejection (HTTP 429)...")
        # Set limit low for free tier
        settings.rate_limit_free = 1
        r4 = await run_scenario(
            name="Rate Limiter Fast-Fail (HTTP 429 Rejection)",
            client=client,
            method="POST",
            url="/v1/chat/completions",
            headers={"X-API-Key": "ef-live-free-key"},
            json_data={"model": "gpt-4o", "messages": [{"role": "user", "content": "rate limit test"}]},
            total_requests=args.requests,
            concurrency=args.concurrency,
        )
        results.append(r4)

    # Save to JSON
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)

    # Print Table
    print("\n" + "=" * 96)
    print(f"{'Scenario':<44} | {'RPS':<8} | {'P50 (ms)':<9} | {'P95 (ms)':<9} | {'P99 (ms)':<9}")
    print("-" * 96)
    for res in results:
        print(
            f"{res['scenario']:<44} | {res['rps']:<8} | {res['p50_ms']:<9} | {res['p95_ms']:<9} | {res['p99_ms']:<9}"
        )
    print("=" * 96)
    print(f"\nExact empirical benchmarks saved to {args.output}\n")


if __name__ == "__main__":
    asyncio.run(main())
