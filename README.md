# cachefence

[![CI](https://github.com/bourne44/cachefence/actions/workflows/tests.yml/badge.svg)](https://github.com/bourne44/cachefence/actions/workflows/tests.yml)
[![PyPI](https://img.shields.io/pypi/v/cachefence)](https://pypi.org/project/cachefence/)
[![Python](https://img.shields.io/pypi/pyversions/cachefence)](https://pypi.org/project/cachefence/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow)](LICENSE)

**Cache-aside for Redis without the stampede.**

When a hot cache key expires, naive cache-aside lets *every* concurrent request
miss at the same instant and pile onto your database to rebuild the same value.
That's a cache stampede (a.k.a. thundering herd), and it's one of the most common
ways a cache makes things *worse* under load.

cachefence stops it:

```
500 concurrent requests hit a cold key (each DB query takes 50ms)

naive cache-aside      DB hits:  500
with cachefence        DB hits:    1
```

Same workload, one extra import: **500 database queries become 1.**

## When you need this

If only a handful of requests ever share a key, plain cache-aside is fine — you
don't need this. cachefence earns its keep when *many* requests hit the *same*
key at the *same* moment and rebuilding it is expensive (a slow query, an
upstream call). That's exactly when the naive pattern turns one expired key into
a pile of identical database queries.

## Install

```bash
pip install cachefence
```

Requires Python 3.11+ and a Redis server (4.2+).

## Usage

```python
from redis.asyncio import Redis
from cachefence import CacheFence

redis = Redis()
cache = CacheFence(redis)

async def get_user(user_id: int) -> dict:
    return await cache.get_or_set(
        key=f"user:{user_id}",
        ttl=60,                                  # fresh for 60 seconds
        recompute=lambda: load_user_from_db(user_id),
    )
```

`recompute` can be sync or async. It runs at most once per refresh, no matter how
many requests arrive together. Invalidate manually when the underlying data
changes:

```python
await cache.invalidate(f"user:{user_id}")
```

cachefence is fully typed: the value type flows from `recompute`, so
`get_or_set(key, ttl, load_user)` is inferred as whatever `load_user` returns —
no casts, no `Any`.

## How it works

cachefence layers two mechanisms so a key almost never goes cold *and* a cold key
is never rebuilt more than once:

1. **Probabilistic early refresh (XFetch).** Each read rolls a weighted dice; as
   the key nears expiry, one lucky request is nudged to refresh it *ahead of
   time* while everyone else keeps serving the still-valid cached value. The
   weighting uses how long the last recompute took, so expensive keys refresh
   earlier. Based on Vattani, Chierichetti & Lowenstein, *"Optimal Probabilistic
   Cache Stampede Prevention"* (VLDB 2015).

2. **Distributed rebuild lock.** On a true miss, workers race for a short-lived
   Redis lock. The winner rebuilds; the rest wait briefly and pick up the fresh
   value the moment it lands, with a bounded fallback so a crashed rebuilder
   never hangs requests forever.

The lock is released with a compare-and-delete (Lua when the server supports it,
an optimistic `WATCH`/`MULTI` transaction otherwise) so a worker can never delete
a lock it no longer owns.

## Configuration

```python
cache = CacheFence(
    redis,
    beta=1.0,                    # XFetch aggressiveness; higher = refresh earlier
    lock_timeout=10.0,           # seconds before a rebuild lock auto-expires
    wait_for_lock=5.0,           # max seconds a waiter blocks before rebuilding itself
    namespace="app:",            # optional key prefix
    serve_stale_on_error=False,  # serve the valid cached value if a refresh fails
)
```

Custom serialization (default is JSON):

```python
import pickle
cache = CacheFence(redis, serializer=pickle.dumps, deserializer=pickle.loads)
# serializer returns bytes, deserializer takes bytes
```

## Observability

Every cache tracks what it's doing. Read `cache.stats` to watch the mechanisms
work — especially `stampedes_prevented`, the count of database queries you
*didn't* make:

```python
await asyncio.gather(*(get_user(1) for _ in range(500)))

print(cache.stats)
# CacheStats(hits=0, misses=500, early_refreshes=0, recomputes=1,
#            recompute_errors=0, stampedes_prevented=499)
print(cache.stats.hit_rate)  # 0.0 cold; climbs as the key stays warm
```

## Resilience

By default a failed `recompute` raises `RecomputeError`. Pass
`serve_stale_on_error=True` and, when a refresh fails while a still-valid value
is cached, cachefence returns that value instead — so a blip in your backing
store doesn't become an error for every request. On a hard miss there is nothing
valid to serve, so the error still propagates.

## A note on connection pools

Under a genuine burst (hundreds of simultaneous coroutines), the default
`redis-py` pool can raise `MaxConnectionsError` because waiters don't block for a
free connection. Use a blocking pool sized for your concurrency:

```python
from redis.asyncio import BlockingConnectionPool, Redis

pool = BlockingConnectionPool(max_connections=30, timeout=15)
redis = Redis(connection_pool=pool)
```

## Run the demo

```bash
git clone https://github.com/bourne44/cachefence
cd cachefence
pip install -e ".[test]"
python examples/stampede_demo.py
```

## Development

```bash
pip install -e ".[dev]"
ruff check .
mypy --strict src/cachefence/
pytest
```

The suite proves each guarantee in isolation: a 100-way concurrent-miss test
(recompute runs exactly once), single-flight XFetch early refresh, the
compare-and-delete lock and its `WATCH`/`MULTI` fallback, the crashed-holder
timeout path, custom serialization, and the observability counters.

See [CHANGELOG.md](CHANGELOG.md) for release history.

## License

MIT
