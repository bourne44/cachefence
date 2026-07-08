"""Tests for cachefence.

The headline test is ``test_no_stampede_on_concurrent_miss``: it fires many
concurrent requests at a cold key and asserts the expensive recompute runs only
once. That's the entire reason this library exists. The rest of the suite proves
the two mechanisms individually — XFetch early refresh and the distributed
compare-and-delete lock (both the Lua path and its WATCH/MULTI fallback) — plus
the observability counters and the serve-stale shield.
"""

import asyncio
import pickle

import fakeredis.aioredis
import pytest

import cachefence.cache
from cachefence import CacheFence, RecomputeError

pytestmark = pytest.mark.asyncio

# XFetch fires when ``delta * beta * -ln(random())`` reaches the time left before
# expiry. A huge beta makes it fire on the very next read, which turns the
# probabilistic mechanism into something a test can assert deterministically.
_ALWAYS = 1e9


@pytest.fixture
async def redis():
    client = fakeredis.aioredis.FakeRedis()
    yield client
    await client.flushall()
    await client.aclose()


@pytest.fixture
def steady_dice(monkeypatch):
    """Pin XFetch's random draw so early-refresh tests are deterministic."""
    monkeypatch.setattr(cachefence.cache.random, "random", lambda: 0.5)


# --- basics ---------------------------------------------------------------


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


async def test_custom_serializer_roundtrips_non_json(redis):
    """A pickle serializer stores values plain JSON can't — proving it's used."""
    cache = CacheFence(redis, serializer=pickle.dumps, deserializer=pickle.loads)
    payload = {"a", "b", "c"}  # a set is not JSON-serializable

    stored = await cache.get_or_set("k", 60, lambda: payload)
    fetched = await cache.get_or_set("k", 60, lambda: payload)

    assert stored == payload
    assert fetched == payload
    assert isinstance(fetched, set)  # came back through pickle, still a set


# --- the core guarantee: no stampede on a hard miss -----------------------


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
    # 100 workers missed; one rebuilt; the other 99 rode its rebuild.
    assert cache.stats.misses == 100
    assert cache.stats.recomputes == 1
    assert cache.stats.stampedes_prevented == 99


async def test_waiter_rebuilds_after_lock_timeout(redis):
    """A crashed holder (lock held, value never written) must not hang waiters:
    after wait_for_lock they rebuild the value themselves."""
    cache = CacheFence(redis, wait_for_lock=0.2)
    rkey = cache._key("k")
    # Simulate the crash: the rebuild lock is held, but no value ever appears.
    await redis.set(cache._lock_key(rkey), b"ghost-holder", px=5000)

    calls = 0

    async def recompute():
        nonlocal calls
        calls += 1
        return "rebuilt"

    result = await cache.get_or_set("k", 60, recompute)
    assert result == "rebuilt"
    assert calls == 1


# --- mechanism #1: XFetch probabilistic early refresh ---------------------


async def test_early_refresh_is_single_flight(redis, steady_dice):
    """Near expiry, exactly one worker refreshes ahead of time while everyone
    else keeps serving the still-valid value."""
    cache = CacheFence(redis)
    calls = 0

    async def recompute():
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.05)  # hold the refresh open so others pile in
        return calls

    await cache.get_or_set("k", ttl=60, recompute=recompute)  # prime -> calls == 1
    assert calls == 1

    results = await asyncio.gather(
        *(cache.get_or_set("k", ttl=60, recompute=recompute, beta=_ALWAYS)
          for _ in range(50))
    )

    assert calls == 2  # one — and only one — early refresh
    assert all(r in (1, 2) for r in results)  # old or freshly refreshed value
    assert cache.stats.early_refreshes == 1
    assert cache.stats.stampedes_prevented == 49


async def test_beta_zero_disables_early_refresh(redis, steady_dice):
    """beta=0 turns XFetch off: the value is served until it truly expires."""
    cache = CacheFence(redis)
    calls = 0

    async def recompute():
        nonlocal calls
        calls += 1
        return calls

    await cache.get_or_set("k", 60, recompute)  # calls == 1
    for _ in range(10):
        assert await cache.get_or_set("k", 60, recompute, beta=0) == 1

    assert calls == 1  # never refreshed early
    assert cache.stats.hits == 10


# --- mechanism #2: the distributed compare-and-delete lock ----------------


async def test_release_lock_only_deletes_own_lock(redis):
    """Compare-and-delete: a worker whose lock expired must not delete the lock
    another worker now holds. (Runs via the WATCH/MULTI fallback here, since
    fakeredis has no scripting.)"""
    cache = CacheFence(redis)
    rkey = "k"
    lock_key = cache._lock_key(rkey)

    token = await cache._acquire(rkey)
    assert token is not None
    # Our lock "expired" and someone else grabbed it:
    await redis.set(lock_key, b"another-worker")

    await cache._release_lock(rkey, token)  # must be a no-op
    assert await redis.get(lock_key) == b"another-worker"


async def test_release_lock_frees_when_owned(redis):
    cache = CacheFence(redis)
    rkey = "k"
    token = await cache._acquire(rkey)
    assert token is not None

    await cache._release_lock(rkey, token)
    assert await redis.get(cache._lock_key(rkey)) is None


async def test_scripting_absence_is_detected_and_falls_back(redis, monkeypatch):
    """On a server that rejects scripting, the first release detects it and
    switches to the WATCH/MULTI fallback for the rest of the instance's life."""
    cache = CacheFence(redis)
    assert cache._lua_ok is True

    async def no_scripting(keys, args):
        raise RuntimeError("unknown command 'evalsha'")

    monkeypatch.setattr(cache, "_release", no_scripting)

    rkey = "k"
    token = await cache._acquire(rkey)
    await cache._release_lock(rkey, token)

    assert cache._lua_ok is False  # detected and flipped
    assert await redis.get(cache._lock_key(rkey)) is None  # fallback freed it


async def test_lua_path_compare_and_delete(redis, monkeypatch):
    """Exercise the Lua branch by standing in for a scripting-capable server."""
    cache = CacheFence(redis)
    rkey = "k"
    lock_key = cache._lock_key(rkey)

    async def fake_release(keys, args):
        # Server-side atomic compare-and-delete, in Python.
        if await redis.get(keys[0]) == args[0].encode():
            await redis.delete(keys[0])
            return 1
        return 0

    monkeypatch.setattr(cache, "_release", fake_release)

    # Not owned -> preserved.
    token = await cache._acquire(rkey)
    await redis.set(lock_key, b"other")
    await cache._release_lock(rkey, token)
    assert cache._lua_ok is True  # never fell back
    assert await redis.get(lock_key) == b"other"

    # Owned -> deleted.
    await redis.delete(lock_key)
    token2 = await cache._acquire(rkey)
    await cache._release_lock(rkey, token2)
    assert cache._lua_ok is True
    assert await redis.get(lock_key) is None


# --- observability --------------------------------------------------------


async def test_stats_track_hits_and_misses(redis):
    cache = CacheFence(redis)

    await cache.get_or_set("k", 60, lambda: "v")  # miss + recompute
    await cache.get_or_set("k", 60, lambda: "v")  # hit
    await cache.get_or_set("k", 60, lambda: "v")  # hit

    s = cache.stats
    assert (s.misses, s.recomputes, s.hits) == (1, 1, 2)
    assert s.hit_rate == pytest.approx(2 / 3)

    s.reset()
    assert (s.hits, s.misses, s.recomputes) == (0, 0, 0)


# --- resilience: serve stale on error -------------------------------------


async def test_serve_stale_on_error_shields_during_refresh(redis, steady_dice):
    """With serve_stale_on_error, a failed early refresh returns the still-valid
    cached value instead of raising — the cache as an outage shield."""
    cache = CacheFence(redis, serve_stale_on_error=True)
    fail = False

    async def recompute():
        if fail:
            raise ValueError("db down")
        await asyncio.sleep(0.01)  # ensure delta > 0 so XFetch can trigger
        return "good"

    await cache.get_or_set("k", 60, recompute)  # cache "good"
    fail = True
    result = await cache.get_or_set("k", 60, recompute, beta=_ALWAYS)

    assert result == "good"  # shielded, not raised
    assert cache.stats.recompute_errors == 1


async def test_error_propagates_without_serve_stale(redis, steady_dice):
    """Default behavior: a failed refresh raises even though a value is cached."""
    cache = CacheFence(redis)  # serve_stale_on_error defaults to False
    fail = False

    async def recompute():
        if fail:
            raise ValueError("db down")
        await asyncio.sleep(0.01)
        return "good"

    await cache.get_or_set("k", 60, recompute)
    fail = True
    with pytest.raises(RecomputeError):
        await cache.get_or_set("k", 60, recompute, beta=_ALWAYS)


async def test_serve_stale_does_not_apply_on_hard_miss(redis):
    """There is nothing valid to serve on a hard miss, so the error still
    propagates even with serve_stale_on_error."""
    cache = CacheFence(redis, serve_stale_on_error=True)

    async def boom():
        raise ValueError("nothing to serve")

    with pytest.raises(RecomputeError):
        await cache.get_or_set("missing", 60, boom)
