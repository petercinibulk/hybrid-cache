from __future__ import annotations

import asyncio
import random
import time
from collections import OrderedDict
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, ParamSpec, TypeVar, cast

from async_hybrid_cache.distributed_cache import DistributedCache
from async_hybrid_cache.invalidation import InvalidationBus

T = TypeVar("T")
P = ParamSpec("P")
DEFAULT_SCOPE = "__default__"
_CACHE_OPTION_DEFAULTS = {
    "ttl_seconds": 60.0,
    "fail_safe_seconds": 300.0,
    "hard_timeout_seconds": 5.0,
    "jitter_seconds": 0.0,
    "lru_max_keys": None,
}


class _Unset:
    __slots__ = ()


_UNSET = _Unset()

if TYPE_CHECKING:
    from async_hybrid_cache.decorators import CachedFunction


@dataclass(frozen=True, slots=True, init=False)
class CacheOptions:
    """Runtime policy for freshness, factory timeouts, TTL jitter, and key count."""

    ttl_seconds: float = 60
    fail_safe_seconds: float = 300
    hard_timeout_seconds: float = 5
    jitter_seconds: float = 0
    lru_max_keys: int | None = None
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
        lru_max_keys: int | None | _Unset = _UNSET,
    ) -> None:
        values = {
            "ttl_seconds": ttl_seconds,
            "fail_safe_seconds": fail_safe_seconds,
            "hard_timeout_seconds": hard_timeout_seconds,
            "jitter_seconds": jitter_seconds,
            "lru_max_keys": lru_max_keys,
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


@dataclass(frozen=True, slots=True)
class ScopedCache:
    """Named view over an `AsyncHybridCache` instance with scope-specific defaults."""

    _cache: AsyncHybridCache
    name: str
    options: CacheOptions | None = None

    async def get(
        self,
        key: str,
        *,
        options: CacheOptions | None = None,
    ) -> object | None:
        """Return a fresh scoped cached value, if one exists."""

        return await self._cache.get(
            key,
            scope=self.name,
            options=options,
            _scope_options=self.options,
        )

    async def get_or_set(
        self,
        key: str,
        factory: Callable[[], Awaitable[T]],
        *,
        options: CacheOptions | None = None,
    ) -> T:
        """Return a scoped cached value or compute, store, and return a new value."""

        return await self._cache.get_or_set(
            key,
            factory,
            scope=self.name,
            options=options,
            _scope_options=self.options,
        )

    async def set(
        self,
        key: str,
        value: object,
        *,
        options: CacheOptions | None = None,
        publish_invalidation: bool = True,
    ) -> None:
        """Store a value in this scope."""

        await self._cache.set(
            key,
            value,
            scope=self.name,
            options=options,
            _scope_options=self.options,
            publish_invalidation=publish_invalidation,
        )

    async def remove(self, key: str) -> None:
        """Remove a key from this scope locally, from L2, and from peers."""

        await self._cache.remove(key, scope=self.name)

    async def clear(self) -> None:
        """Clear this scope locally and from peer nodes."""

        await self._cache.clear(scope=self.name)

    def remove_local(self, key: str) -> None:
        """Remove a key from only this process's in-memory scoped cache."""

        self._cache.remove_local(key, scope=self.name)

    def clear_memory(self) -> None:
        """Clear only this process's in-memory scoped cache."""

        self._cache.clear_memory(scope=self.name)


class AsyncHybridCache:
    """Async two-level cache with optional distributed storage and invalidation."""

    def __init__(
        self,
        *,
        distributed_cache: DistributedCache | None = None,
        invalidation_bus: InvalidationBus | None = None,
        options: CacheOptions | None = None,
    ) -> None:
        """Create a cache using optional L2 storage and invalidation providers."""

        self._memory: dict[str, OrderedDict[str, CacheEntry]] = {}
        self._locks: dict[tuple[str, str], asyncio.Lock] = {}
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
        scope: str | None = None,
    ) -> Callable[[Callable[P, Awaitable[T]]], CachedFunction[P, T]]:
        """Decorate an async function using this cache instance."""

        from async_hybrid_cache.decorators import CachedFunction

        def decorator(func: Callable[P, Awaitable[T]]) -> CachedFunction[P, T]:
            return CachedFunction(self, func, key, options, scope)

        return decorator

    def scope(self, name: str, *, options: CacheOptions | None = None) -> ScopedCache:
        """Return a scoped cache view with optional scope-level default options."""

        return ScopedCache(self, name, options)

    async def get(
        self,
        key: str,
        *,
        scope: str = DEFAULT_SCOPE,
        options: CacheOptions | None = None,
        _scope_options: CacheOptions | None = None,
    ) -> object | None:
        """Return a fresh cached value, if one exists."""

        opts = self._effective_options(options, scope_options=_scope_options)
        memory = self._scope_memory(scope)
        entry = memory.get(key)

        if entry and entry.is_fresh:
            memory.move_to_end(key)
            return entry.value

        if self._distributed_cache is None:
            return None

        cached_value = await self._distributed_cache.get(self._distributed_key(scope, key))
        if cached_value is None:
            return None

        self._set_memory(key, cached_value, opts, scope=scope)
        return cached_value

    async def get_or_set(
        self,
        key: str,
        factory: Callable[[], Awaitable[T]],
        *,
        scope: str = DEFAULT_SCOPE,
        options: CacheOptions | None = None,
        _scope_options: CacheOptions | None = None,
    ) -> T:
        """Return a cached value or compute, store, and return a new value."""

        opts = self._effective_options(options, scope_options=_scope_options)
        memory = self._scope_memory(scope)
        entry = memory.get(key)

        if entry and entry.is_fresh:
            memory.move_to_end(key)
            return cast(T, entry.value)

        lock = self._locks.setdefault((scope, key), asyncio.Lock())

        async with lock:
            entry = memory.get(key)
            if entry and entry.is_fresh:
                memory.move_to_end(key)
                return cast(T, entry.value)

            if self._distributed_cache is not None:
                cached_value = await self._distributed_cache.get(self._distributed_key(scope, key))
                if cached_value is not None:
                    self._set_memory(key, cached_value, opts, scope=scope)
                    return cast(T, cached_value)

            stale = entry if entry and entry.is_fail_safe_available else None

            try:
                value = await asyncio.wait_for(
                    factory(),
                    timeout=opts.hard_timeout_seconds,
                )
                await self.set(
                    key,
                    value,
                    scope=scope,
                    options=opts,
                    publish_invalidation=False,
                )
                return value
            except Exception:
                if stale is not None:
                    memory.move_to_end(key)
                    return cast(T, stale.value)
                raise

    async def set(
        self,
        key: str,
        value: object,
        *,
        scope: str = DEFAULT_SCOPE,
        options: CacheOptions | None = None,
        _scope_options: CacheOptions | None = None,
        publish_invalidation: bool = True,
    ) -> None:
        """Store a value in local memory and optional distributed storage."""

        opts = self._effective_options(options, scope_options=_scope_options)
        self._set_memory(key, value, opts, scope=scope)

        if self._distributed_cache is not None:
            await self._distributed_cache.set(
                self._distributed_key(scope, key),
                value,
                ttl_seconds=self._ttl_with_jitter(opts),
            )

        if publish_invalidation and self._invalidation_bus is not None:
            await self._invalidation_bus.invalidate(key, scope=scope)

    async def remove(self, key: str, *, scope: str = DEFAULT_SCOPE) -> None:
        """Remove a key locally, from distributed storage, and from peer nodes."""

        self.remove_local(key, scope=scope)

        if self._distributed_cache is not None:
            await self._distributed_cache.delete(self._distributed_key(scope, key))

        if self._invalidation_bus is not None:
            await self._invalidation_bus.invalidate(key, scope=scope)

    async def clear(self, *, scope: str | None = None) -> None:
        """Clear all local entries and publish a clear message to peer nodes."""

        self.clear_memory(scope=scope)

        if self._invalidation_bus is not None:
            await self._invalidation_bus.clear(scope=scope)

    def remove_local(self, key: str, scope: str = DEFAULT_SCOPE) -> None:
        """Remove a key from only this process's in-memory cache."""

        memory = self._memory.get(scope)
        if memory is not None:
            memory.pop(key, None)

    def clear_memory(self, scope: str | None = None) -> None:
        """Clear only this process's in-memory cache."""

        if scope is None:
            self._memory.clear()
            return

        self._memory.pop(scope, None)

    def _effective_options(
        self,
        options: CacheOptions | None,
        *,
        scope_options: CacheOptions | None = None,
    ) -> CacheOptions:
        defaults = self._options

        if scope_options is not None:
            defaults = scope_options.merge_over(defaults)

        if options is None:
            return defaults

        return options.merge_over(defaults)

    def _scope_memory(self, scope: str) -> OrderedDict[str, CacheEntry]:
        return self._memory.setdefault(scope, OrderedDict())

    def _set_memory(self, key: str, value: object, opts: CacheOptions, *, scope: str) -> None:
        memory = self._scope_memory(scope)
        ttl = self._ttl_with_jitter(opts)
        now = time.monotonic()
        memory[key] = CacheEntry(
            value=value,
            expires_at=now + ttl,
            fail_safe_until=now + ttl + opts.fail_safe_seconds,
        )
        memory.move_to_end(key)
        self._enforce_lru_max_keys(memory, opts)

    def _enforce_lru_max_keys(
        self,
        memory: OrderedDict[str, CacheEntry],
        opts: CacheOptions,
    ) -> None:
        if opts.lru_max_keys is None:
            return

        if opts.lru_max_keys <= 0:
            memory.clear()
            return

        while len(memory) > opts.lru_max_keys:
            memory.popitem(last=False)

    def _ttl_with_jitter(self, opts: CacheOptions) -> float:
        if opts.jitter_seconds <= 0:
            return opts.ttl_seconds
        return opts.ttl_seconds + random.uniform(0, opts.jitter_seconds)

    def _distributed_key(self, scope: str, key: str) -> str:
        return f"{len(scope)}:{scope}{key}"
