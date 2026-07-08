# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project follows
[Semantic Versioning](https://semver.org/spec/v2.0.0/).

## [0.1.0] — 2026-07-08

First public release.

### Added
- `CacheFence.get_or_set()` — cache-aside for Redis with stampede protection:
  probabilistic early refresh (XFetch; Vattani, Chierichetti & Lowenstein,
  VLDB 2015) plus a distributed rebuild lock, so a hot key expiring rebuilds at
  most once no matter how many requests arrive together.
- Compare-and-delete lock release — Lua where the server supports scripting, an
  optimistic `WATCH`/`MULTI` transaction otherwise — so a worker can never
  delete a lock it no longer owns.
- Sync **or** async `recompute` callables; the return type flows through
  `get_or_set`, so the library is genuinely typed.
- `CacheStats` observability via `cache.stats`: hits, misses, early refreshes,
  recomputes, recompute errors, and — the headline metric — stampedes prevented.
- `serve_stale_on_error`: serve the still-valid cached value when an early
  refresh fails, letting the cache shield the app during a backing-store outage.
- Custom serialization, key namespacing, and a per-call `beta` override.
- `py.typed`; tested on Python 3.11–3.13, including a 100-way concurrent-miss
  test asserting the recompute runs exactly once.

[0.1.0]: https://github.com/bourne44/cachefence/releases/tag/v0.1.0
