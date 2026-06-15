"""Demo: cache stampede, with and without cachefence.

Simulates a hot key expiring while N requests arrive at once. Counts how many
times the "database" gets hit to rebuild the value.

Run::

    python examples/stampede_demo.py

Requires a Redis server on localhost:6379.
"""

import asyncio
import time

from redis.asyncio import BlockingConnectionPool, Redis

from cachefence import CacheFence

CONCURRENCY = 500
DB_LATENCY = 0.05  # seconds per "query"


async def naive_get_or_set(redis, key, ttl, recompute):
    """The cache-aside pattern almost everyone writes by hand."""
    cached = await redis.get(key)
    if cached is not None:
        return cached
    value = await recompute()
    await redis.set(key, value, ex=ttl)
    return value


async def run(label, getter):
    pool = BlockingConnectionPool(max_connections=30, timeout=15)
    redis = Redis(connection_pool=pool, decode_responses=True)
    await redis.flushall()

    db_hits = {"n": 0}

    async def recompute():
        db_hits["n"] += 1
        await asyncio.sleep(DB_LATENCY)
        return "rebuilt-value"

    start = time.monotonic()
    await asyncio.gather(*(getter(redis, recompute) for _ in range(CONCURRENCY)))
    elapsed = time.monotonic() - start

    await redis.aclose()
    print(f"{label:<22} DB hits: {db_hits['n']:>4}   wall time: {elapsed:.2f}s")
    return db_hits["n"]


async def main():
    print(f"\n{CONCURRENCY} concurrent requests hit a cold key "
          f"(each DB query takes {DB_LATENCY*1000:.0f}ms)\n")

    naive = await run(
        "naive cache-aside",
        lambda r, rc: naive_get_or_set(r, "hot", 60, rc),
    )

    cache = None

    async def with_fence(redis, recompute):
        nonlocal cache
        if cache is None:
            cache = CacheFence(redis)
        return await cache.get_or_set("hot", 60, recompute)

    fenced = await run("with cachefence", with_fence)

    print(f"\ncachefence cut DB load by {(1 - fenced / naive) * 100:.0f}% "
          f"({naive} -> {fenced} queries)\n")


if __name__ == "__main__":
    asyncio.run(main())
