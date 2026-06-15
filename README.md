# cachefence

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
    beta=1.0,          # XFetch aggressiveness; higher = refresh earlier
    lock_timeout=10.0, # seconds before a rebuild lock auto-expires
    wait_for_lock=5.0, # max seconds a waiter blocks before rebuilding itself
    namespace="app:",  # optional key prefix
)
```

Custom serialization (default is JSON):

```python
import pickle
cache = CacheFence(redis, serializer=pickle.dumps, deserializer=pickle.loads)
# serializer returns bytes, deserializer takes bytes
```

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
pip install -e ".[test]"
pytest
```

The test suite includes a 100-way concurrent-miss test asserting the recompute
runs exactly once — the core guarantee of the library.

## License

MIT
