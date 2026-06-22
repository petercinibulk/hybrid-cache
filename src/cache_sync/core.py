from __future__ import annotations

import asyncio
import random
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, ParamSpec, TypeVar, cast

from cache_sync.distributed_cache import DistributedCache
from cache_sync.invalidation import InvalidationBus

T = TypeVar("T")
P = ParamSpec("P")
_CACHE_OPTION_DEFAULTS = {
    "ttl_seconds": 60.0,
    "fail_safe_seconds": 300.0,
    "hard_timeout_seconds": 5.0,
    "jitter_seconds": 0.0,
}


class _Unset:
    __slots__ = ()


_UNSET = _Unset()

if TYPE_CHECKING:
    from cache_sync.decorators import CachedFunction


@dataclass(frozen=True, slots=True, init=False)
class CacheOptions:
    """Runtime policy for cache freshness, factory timeouts, and TTL jitter."""

    ttl_seconds: float = 60
    fail_safe_seconds: float = 300
    hard_timeout_seconds: float = 5
    jitter_seconds: float = 0
    _supplied: frozenset[str] = field(
        default_factory=frozenset,
        repr=False,
        compare=False,
    )

    def __init__(
        self,
        ttl_seconds: float | _Unset = _UNSET,
        fail_safe_seconds: float | _Unset = _UNSET,
        hard_timeout_seconds: float | _Unset = _UNSET,
        jitter_seconds: float | _Unset = _UNSET,
    ) -> None:
        values = {
            "ttl_seconds": ttl_seconds,
            "fail_safe_seconds": fail_safe_seconds,
            "hard_timeout_seconds": hard_timeout_seconds,
            "jitter_seconds": jitter_seconds,
        }
        supplied = frozenset(name for name, value in values.items() if value is not _UNSET)

        for name, default in _CACHE_OPTION_DEFAULTS.items():
            value = values[name]
            object.__setattr__(self, name, default if value is _UNSET else value)

        object.__setattr__(self, "_supplied", supplied)

    def merge_over(self, defaults: CacheOptions) -> CacheOptions:
        """Return this option object's supplied fields over cache defaults."""

        values = {
            name: getattr(self if name in self._supplied else defaults, name)
            for name in _CACHE_OPTION_DEFAULTS
        }
        return CacheOptions(**values)


@dataclass(slots=True)
class CacheEntry:
    """In-memory cache entry with freshness and fail-safe deadlines."""

    value: object
    expires_at: float
    fail_safe_until: float

    @property
    def is_fresh(self) -> bool:
        return time.monotonic() < self.expires_at

    @property
    def is_fail_safe_available(self) -> bool:
        return time.monotonic() < self.fail_safe_until


class CacheSync:
    """Async two-level cache with optional distributed storage and invalidation."""

    def __init__(
        self,
        *,
        distributed_cache: DistributedCache | None = None,
        invalidation_bus: InvalidationBus | None = None,
        options: CacheOptions | None = None,
    ) -> None:
        """Create a cache using optional L2 storage and invalidation providers."""

        self._memory: dict[str, CacheEntry] = {}
        self._locks: dict[str, asyncio.Lock] = {}
        self._distributed_cache = distributed_cache
        self._invalidation_bus = invalidation_bus
        self._options = options or CacheOptions()

    async def start(self) -> None:
        """Start the configured invalidation bus, if any."""

        if self._invalidation_bus is None:
            return

        await self._invalidation_bus.start(
            remove_local=self.remove_local,
            clear_local=self.clear_memory,
        )

    async def stop(self) -> None:
        """Stop the configured invalidation bus, if any."""

        if self._invalidation_bus is not None:
            await self._invalidation_bus.stop()

    def cached(
        self,
        key: str | Callable[..., str] | None = None,
        *,
        options: CacheOptions | None = None,
    ) -> Callable[[Callable[P, Awaitable[T]]], CachedFunction[P, T]]:
        """Decorate an async function using this cache instance."""

        from cache_sync.decorators import CachedFunction

        def decorator(func: Callable[P, Awaitable[T]]) -> CachedFunction[P, T]:
            return CachedFunction(self, func, key, options)

        return decorator

    async def get_or_set(
        self,
        key: str,
        factory: Callable[[], Awaitable[T]],
        *,
        options: CacheOptions | None = None,
    ) -> T:
        """Return a cached value or compute, store, and return a new value."""

        opts = self._effective_options(options)
        entry = self._memory.get(key)

        if entry and entry.is_fresh:
            return cast(T, entry.value)

        lock = self._locks.setdefault(key, asyncio.Lock())

        async with lock:
            entry = self._memory.get(key)
            if entry and entry.is_fresh:
                return cast(T, entry.value)

            if self._distributed_cache is not None:
                cached_value = await self._distributed_cache.get(key)
                if cached_value is not None:
                    self._set_memory(key, cached_value, opts)
                    return cast(T, cached_value)

            stale = entry if entry and entry.is_fail_safe_available else None

            try:
                value = await asyncio.wait_for(
                    factory(),
                    timeout=opts.hard_timeout_seconds,
                )
                await self.set(key, value, options=opts, publish_invalidation=False)
                return value
            except Exception:
                if stale is not None:
                    return cast(T, stale.value)
                raise

    async def set(
        self,
        key: str,
        value: object,
        *,
        options: CacheOptions | None = None,
        publish_invalidation: bool = True,
    ) -> None:
        """Store a value in local memory and optional distributed storage."""

        opts = self._effective_options(options)
        self._set_memory(key, value, opts)

        if self._distributed_cache is not None:
            await self._distributed_cache.set(
                key,
                value,
                ttl_seconds=self._ttl_with_jitter(opts),
            )

        if publish_invalidation and self._invalidation_bus is not None:
            await self._invalidation_bus.invalidate(key)

    async def remove(self, key: str) -> None:
        """Remove a key locally, from distributed storage, and from peer nodes."""

        self.remove_local(key)

        if self._distributed_cache is not None:
            await self._distributed_cache.delete(key)

        if self._invalidation_bus is not None:
            await self._invalidation_bus.invalidate(key)

    async def clear(self) -> None:
        """Clear all local entries and publish a clear message to peer nodes."""

        self.clear_memory()

        if self._invalidation_bus is not None:
            await self._invalidation_bus.clear()

    def remove_local(self, key: str) -> None:
        """Remove a key from only this process's in-memory cache."""

        self._memory.pop(key, None)

    def clear_memory(self) -> None:
        """Clear only this process's in-memory cache."""

        self._memory.clear()

    def _effective_options(self, options: CacheOptions | None) -> CacheOptions:
        if options is None:
            return self._options
        return options.merge_over(self._options)

    def _set_memory(self, key: str, value: object, opts: CacheOptions) -> None:
        ttl = self._ttl_with_jitter(opts)
        now = time.monotonic()
        self._memory[key] = CacheEntry(
            value=value,
            expires_at=now + ttl,
            fail_safe_until=now + ttl + opts.fail_safe_seconds,
        )

    def _ttl_with_jitter(self, opts: CacheOptions) -> float:
        if opts.jitter_seconds <= 0:
            return opts.ttl_seconds
        return opts.ttl_seconds + random.uniform(0, opts.jitter_seconds)
