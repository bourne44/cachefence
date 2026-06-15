"""Tests for cachefence.

The headline test is ``test_no_stampede_on_concurrent_miss``: it fires many
concurrent requests at a cold key and asserts the expensive recompute runs only
once. That's the entire reason this library exists.
"""

import asyncio

import fakeredis.aioredis
import pytest

from cachefence import CacheFence, RecomputeError

pytestmark = pytest.mark.asyncio


@pytest.fixture
async def redis():
    client = fakeredis.aioredis.FakeRedis()
    yield client
    await client.flushall()
    await client.aclose()


async def test_basic_get_or_set_caches(redis):
    cache = CacheFence(redis)
    calls = 0

    async def recompute():
        nonlocal calls
        calls += 1
        return {"hello": "world"}

    first = await cache.get_or_set("k", ttl=60, recompute=recompute)
    second = await cache.get_or_set("k", ttl=60, recompute=recompute)

    assert first == {"hello": "world"}
    assert second == {"hello": "world"}
    assert calls == 1  # second read served from cache


async def test_sync_recompute_supported(redis):
    cache = CacheFence(redis)
    value = await cache.get_or_set("k", ttl=60, recompute=lambda: 42)
    assert value == 42


async def test_no_stampede_on_concurrent_miss(redis):
    """100 concurrent requests for a cold key -> recompute runs exactly once."""
    cache = CacheFence(redis)
    calls = 0

    async def slow_recompute():
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.1)  # simulate a slow DB query
        return "expensive-value"

    results = await asyncio.gather(
        *(cache.get_or_set("hot", ttl=60, recompute=slow_recompute) for _ in range(100))
    )

    assert all(r == "expensive-value" for r in results)
    assert calls == 1  # the whole point: no stampede


async def test_invalidate_forces_recompute(redis):
    cache = CacheFence(redis)
    calls = 0

    async def recompute():
        nonlocal calls
        calls += 1
        return calls

    assert await cache.get_or_set("k", ttl=60, recompute=recompute) == 1
    await cache.invalidate("k")
    assert await cache.get_or_set("k", ttl=60, recompute=recompute) == 2


async def test_recompute_error_wrapped(redis):
    cache = CacheFence(redis)

    async def boom():
        raise ValueError("db down")

    with pytest.raises(RecomputeError):
        await cache.get_or_set("k", ttl=60, recompute=boom)


async def test_namespace_applied(redis):
    cache = CacheFence(redis, namespace="app:")
    await cache.get_or_set("k", ttl=60, recompute=lambda: 1)
    assert await redis.exists("app:k")
    assert not await redis.exists("k")


async def test_expiry_then_refresh(redis):
    cache = CacheFence(redis)
    calls = 0

    async def recompute():
        nonlocal calls
        calls += 1
        return calls

    await cache.get_or_set("k", ttl=0.1, recompute=recompute)
    await asyncio.sleep(0.2)  # let it expire
    result = await cache.get_or_set("k", ttl=0.1, recompute=recompute)
    assert result == 2
    assert calls == 2
