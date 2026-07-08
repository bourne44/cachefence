"""Exception types raised by cachefence."""


class CacheFenceError(Exception):
    """Base class for all cachefence errors."""


class RecomputeError(CacheFenceError):
    """Raised when the user-supplied recompute callable fails.

    By default the error propagates: cachefence does not fall back to a stale
    value, even during an early refresh where one is still available. Construct
    the cache with ``serve_stale_on_error=True`` to instead serve the
    still-valid cached value when an early refresh fails. The original exception
    is available via ``__cause__``.
    """
