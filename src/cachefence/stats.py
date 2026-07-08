"""Runtime counters exposed by :class:`~cachefence.cache.CacheFence`.

Every :class:`CacheFence` owns one :class:`CacheStats`, reachable as
``cache.stats``. The counters are plain integers bumped inline on the event
loop, so reads are cheap and always consistent within a single loop. They let
you see the library actually doing its job::

    100 requests hit a cold key -> misses: 100, recomputes: 1, stampedes_prevented: 99
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class CacheStats:
    """Cumulative counters for one :class:`CacheFence` instance."""

    #: Reads served from a cached value without recomputing. Includes reads
    #: during another worker's early refresh, where the still-valid value is
    #: returned immediately.
    hits: int = 0

    #: Reads that found no cached value (a hard miss). One of these turns into a
    #: recompute; the rest ride it (see :attr:`stampedes_prevented`).
    misses: int = 0

    #: Times this client won the XFetch race and recomputed a still-valid key
    #: *ahead of* its expiry, so the key never actually went cold.
    early_refreshes: int = 0

    #: Times the ``recompute`` callable was actually invoked (early refresh,
    #: hard-miss rebuild, or self-rebuild after a lock wait timed out).
    recomputes: int = 0

    #: Times ``recompute`` raised. Wrapped and re-raised as ``RecomputeError``
    #: unless ``serve_stale_on_error`` shielded the request.
    recompute_errors: int = 0

    #: The headline metric: times this client avoided a recompute by riding
    #: another worker's in-flight rebuild — either waiting out a miss and
    #: reading the fresh value, or serving the valid value during someone
    #: else's early refresh. Every one of these is a database query you didn't
    #: make.
    stampedes_prevented: int = 0

    @property
    def hit_rate(self) -> float:
        """Fraction of reads served from cache, in ``[0.0, 1.0]``."""
        total = self.hits + self.misses
        return self.hits / total if total else 0.0

    def reset(self) -> None:
        """Zero every counter."""
        self.hits = 0
        self.misses = 0
        self.early_refreshes = 0
        self.recomputes = 0
        self.recompute_errors = 0
        self.stampedes_prevented = 0
