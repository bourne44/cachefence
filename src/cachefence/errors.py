"""Exception types raised by cachefence."""


class CacheFenceError(Exception):
    """Base class for all cachefence errors."""


class RecomputeError(CacheFenceError):
    """Raised when the user-supplied recompute callable fails.

    The error always propagates: cachefence does not fall back to a stale
    value, even during an early refresh where one is still available. The
    original exception is available via ``__cause__``.
    """
