"""Core CacheFence implementation."""

from __future__ import annotations

import asyncio
import inspect
import json
import math
import random
import time
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TypeVar, cast, overload

from redis.asyncio import Redis

from .errors import RecomputeError
from .stats import CacheStats

T = TypeVar("T")
Recompute = Callable[[], "Awaitable[T] | T"]
Serializer = Callable[[object], bytes]
Deserializer = Callable[[bytes], object]

# Compare-and-delete: release the lock only if we still own it. Prevents a worker
# whose lock already expired from deleting a lock another worker now holds. Runs as
# a Lua script by default; if the server rejects scripting at runtime we fall back
# to a WATCH/MULTI transaction, which gives the same atomic guarantee.
_RELEASE_LOCK = """
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
else
    return 0
end
"""

# Field names inside the cache hash. The value field is the one read on every
# hit, so it gets a one-byte name; the metadata fields stay spelled out.
_F_VALUE = b"v"
_F_DELTA = b"delta"
_F_EXPIRY = b"expiry"


def _default_serializer(value: object) -> bytes:
    return json.dumps(value).encode()


def _default_deserializer(raw: bytes) -> object:
    return json.loads(raw)


@dataclass(frozen=True, slots=True)
class _Entry:
    """A value read back from the cache, with the metadata XFetch needs."""

    value: object
    delta: float   # seconds the last recompute took
    expiry: float  # absolute unix time at which the value goes stale


class CacheFence:
    """Cache-aside helper for Redis with built-in stampede protection.

    Parameters
    ----------
    redis:
        A ``redis.asyncio.Redis`` client. It is used in raw-bytes mode
        internally, so ``decode_responses`` on the client is irrelevant.
    beta:
        XFetch aggressiveness. Higher refreshes earlier. ``1.0`` is the value
        from the original paper and a sensible default.
    lock_timeout:
        Seconds a rebuild lock is held before it auto-expires, so a crashed
        worker cannot block rebuilds forever.
    wait_for_lock:
        Maximum seconds a worker waits for another worker's rebuild before
        rebuilding the value itself.
    serializer / deserializer:
        Convert values to/from the ``bytes`` stored in Redis. Defaults to JSON.
    namespace:
        Optional prefix applied to every key.
    serve_stale_on_error:
        If ``True``, when a ``recompute`` fails during an early refresh while a
        still-valid value is cached, return that value instead of raising. The
        cache then acts as a shield during a backing-store outage. On a hard
        miss there is no value to serve, so the error still propagates. Defaults
        to ``False`` (fail closed).

    The recompute return type flows through :meth:`get_or_set`, so a single
    ``CacheFence`` can safely serve keys of different types::

        user: dict = await cache.get_or_set("u:1", 60, load_user)   # -> dict
        count: int = await cache.get_or_set("n", 60, lambda: 7)     # -> int
    """

    __slots__ = (
        "_redis", "_beta", "_lock_timeout", "_wait_for_lock",
        "_dumps", "_loads", "_ns", "_release", "_lua_ok",
        "_serve_stale", "_stats",
    )

    def __init__(
        self,
        redis: Redis[bytes],
        *,
        beta: float = 1.0,
        lock_timeout: float = 10.0,
        wait_for_lock: float = 5.0,
        serializer: Serializer = _default_serializer,
        deserializer: Deserializer = _default_deserializer,
        namespace: str = "",
        serve_stale_on_error: bool = False,
    ) -> None:
        self._redis = redis
        self._beta = beta
        self._lock_timeout = lock_timeout
        self._wait_for_lock = wait_for_lock
        self._dumps = serializer
        self._loads = deserializer
        self._ns = namespace
        self._serve_stale = serve_stale_on_error
        self._release = redis.register_script(_RELEASE_LOCK)
        self._lua_ok = True  # flips to False if the server rejects scripting
        self._stats = CacheStats()

    @property
    def stats(self) -> CacheStats:
        """Live counters (hits, misses, stampedes prevented, ...)."""
        return self._stats

    def _key(self, key: str) -> str:
        return f"{self._ns}{key}" if self._ns else key

    @staticmethod
    def _lock_key(rkey: str) -> str:
        return f"{rkey}:lock"

    # Two overloads so the return type flows for both async and sync recompute
    # callables. The async form is listed first: a coroutine function matches
    # ``Callable[[], Awaitable[T]]`` and binds T to the awaited type, whereas a
    # plain ``Callable[[], T]`` would otherwise bind T to the coroutine itself.
    @overload
    async def get_or_set(
        self,
        key: str,
        ttl: float,
        recompute: Callable[[], Awaitable[T]],
        *,
        beta: float | None = None,
    ) -> T: ...
    @overload
    async def get_or_set(
        self,
        key: str,
        ttl: float,
        recompute: Callable[[], T],
        *,
        beta: float | None = None,
    ) -> T: ...

    async def get_or_set(
        self,
        key: str,
        ttl: float,
        recompute: Recompute[T],
        *,
        beta: float | None = None,
    ) -> T:
        """Return the cached value for ``key``, recomputing it if needed.

        ``recompute`` may be sync or async, and its return type is the return
        type of this call. ``ttl`` is the fresh lifetime in seconds. At most one
        worker recomputes at a time; the rest serve the still-valid cached value
        or wait briefly, never stampeding the backing store.
        """
        rkey = self._key(key)
        beta = self._beta if beta is None else beta

        entry = await self._read(rkey)
        if entry is not None:
            if not self._should_refresh_early(entry, beta):
                self._stats.hits += 1
                return cast(T, entry.value)
            # Near expiry: one worker wins the lock and refreshes ahead of time
            # while everyone else keeps serving the value that is still valid.
            token = await self._acquire(rkey)
            if token is None:
                self._stats.hits += 1
                self._stats.stampedes_prevented += 1
                return cast(T, entry.value)
            try:
                value = await self._recompute_and_store(rkey, ttl, recompute)
            except RecomputeError:
                if self._serve_stale:
                    # The backing store failed, but the cached value is still
                    # valid. Shield the request instead of failing it.
                    return cast(T, entry.value)
                raise
            else:
                self._stats.early_refreshes += 1
                return value
            finally:
                await self._release_lock(rkey, token)

        # Hard miss: the value is gone. Exactly one worker rebuilds it.
        return await self._rebuild_on_miss(rkey, ttl, recompute)

    async def invalidate(self, key: str) -> None:
        """Delete a cached key so the next read recomputes it."""
        await self._redis.delete(self._key(key))

    # --- internals ---------------------------------------------------------

    async def _read(self, rkey: str) -> _Entry | None:
        data: dict[bytes, bytes] = await self._redis.hgetall(rkey)
        raw = data.get(_F_VALUE)
        if raw is None:
            return None
        return _Entry(
            value=self._loads(raw),
            delta=float(data[_F_DELTA]),
            expiry=float(data[_F_EXPIRY]),
        )

    def _should_refresh_early(self, entry: _Entry, beta: float) -> bool:
        # XFetch (Vattani et al., VLDB 2015): -ln(uniform(0,1]) is exponentially
        # distributed; scaling it by delta*beta makes expensive-to-rebuild keys
        # refresh earlier, spreading recomputes out instead of bunching them at
        # expiry. The gap widens as we approach expiry, so the trigger probability
        # rises smoothly toward 1. beta=0 disables early refresh entirely.
        gap = entry.delta * beta * -math.log(random.random() or 1e-12)
        return time.time() + gap >= entry.expiry

    async def _acquire(self, rkey: str) -> str | None:
        """Try to take the rebuild lock. Return the ownership token, or None."""
        token = uuid.uuid4().hex
        acquired = await self._redis.set(
            self._lock_key(rkey),
            token,
            nx=True,
            px=int(self._lock_timeout * 1000),
        )
        return token if acquired else None

    async def _release_lock(self, rkey: str, token: str) -> None:
        lock_key = self._lock_key(rkey)
        if self._lua_ok:
            try:
                await self._release(keys=[lock_key], args=[token])
                return
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                message = str(exc).lower()
                if "evalsha" in message or "unknown command" in message:
                    self._lua_ok = False  # server lacks scripting; use fallback
                else:
                    return  # never fail a request because unlock hiccuped
        await self._release_lock_fallback(lock_key, token)

    async def _release_lock_fallback(self, lock_key: str, token: str) -> None:
        """Compare-and-delete via an optimistic WATCH/MULTI transaction."""
        wanted = token.encode()
        try:
            async with self._redis.pipeline(transaction=True) as pipe:
                await pipe.watch(lock_key)
                if await pipe.get(lock_key) == wanted:
                    pipe.multi()
                    pipe.delete(lock_key)
                    await pipe.execute()
                else:
                    await pipe.reset()
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001
            pass  # the lock's own TTL will clean it up

    async def _recompute_and_store(
        self, rkey: str, ttl: float, recompute: Recompute[T]
    ) -> T:
        start = time.monotonic()
        self._stats.recomputes += 1
        try:
            result = recompute()
            if inspect.isawaitable(result):
                result = await result
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._stats.recompute_errors += 1
            raise RecomputeError(str(exc)) from exc

        value = cast(T, result)
        delta = time.monotonic() - start
        async with self._redis.pipeline(transaction=True) as pipe:
            pipe.hset(rkey, mapping={
                _F_VALUE: self._dumps(value),
                _F_DELTA: delta,
                _F_EXPIRY: time.time() + ttl,
            })
            pipe.pexpire(rkey, int(ttl * 1000))
            await pipe.execute()
        return value

    async def _rebuild_on_miss(
        self, rkey: str, ttl: float, recompute: Recompute[T]
    ) -> T:
        self._stats.misses += 1
        token = await self._acquire(rkey)
        if token is not None:
            try:
                return await self._recompute_and_store(rkey, ttl, recompute)
            finally:
                await self._release_lock(rkey, token)

        # Another worker holds the lock. Wait for the value to appear, backing
        # off so we don't busy-poll Redis.
        deadline = time.monotonic() + self._wait_for_lock
        delay = 0.02
        while time.monotonic() < deadline:
            await asyncio.sleep(delay)
            entry = await self._read(rkey)
            if entry is not None:
                self._stats.stampedes_prevented += 1
                return cast(T, entry.value)
            delay = min(delay * 1.5, 0.2)

        # The holder crashed or is pathologically slow. Rebuild ourselves rather
        # than hang the request indefinitely.
        return await self._recompute_and_store(rkey, ttl, recompute)
