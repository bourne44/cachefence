"""cachefence — cache-aside for Redis without the stampede.

When a hot key expires, naive cache-aside lets every concurrent request miss at
once and hammer your database to rebuild the same value. cachefence prevents that
with two cooperating mechanisms:

1. Probabilistic early recomputation (XFetch): a single worker is nudged to
   refresh the value *before* it actually expires, so the key rarely goes cold.
2. A distributed lock: if the value is gone, exactly one worker rebuilds it while
   everyone else briefly waits or serves the stale value.

Basic usage::

    from redis.asyncio import Redis
    from cachefence import CacheFence

    redis = Redis()
    cache = CacheFence(redis)

    async def get_user(user_id: int) -> dict:
        return await cache.get_or_set(
            key=f"user:{user_id}",
            ttl=60,
            recompute=lambda: load_user_from_db(user_id),
        )
"""

from .cache import CacheFence
from .errors import CacheFenceError, RecomputeError
from .stats import CacheStats

__all__ = ["CacheFence", "CacheFenceError", "CacheStats", "RecomputeError"]
__version__ = "0.1.0"
