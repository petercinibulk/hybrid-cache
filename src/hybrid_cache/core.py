from __future__ import annotations

import asyncio
import random
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TypeVar, cast

from hybrid_cache.distributed_cache import DistributedCache
from hybrid_cache.invalidator import Invalidator

T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class CacheOptions:
    ttl_seconds: float = 60
    fail_safe_seconds: float = 300
    hard_timeout_seconds: float = 5
    jitter_seconds: float = 0


@dataclass(slots=True)
class CacheEntry:
    value: object
    expires_at: float
    fail_safe_until: float

    @property
    def is_fresh(self) -> bool:
        return time.monotonic() < self.expires_at

    @property
    def is_fail_safe_available(self) -> bool:
        return time.monotonic() < self.fail_safe_until


class HybridCache:
    def __init__(
        self,
        *,
        distributed_cache: DistributedCache | None = None,
        invalidator: Invalidator | None = None,
        options: CacheOptions | None = None,
    ) -> None:
        self._memory: dict[str, CacheEntry] = {}
        self._locks: dict[str, asyncio.Lock] = {}
        self._distributed_cache = distributed_cache
        self._invalidator = invalidator
        self._options = options or CacheOptions()

    async def start(self) -> None:
        if self._invalidator is None:
            return

        await self._invalidator.start(
            remove_local=self.remove_local,
            clear_local=self.clear_memory,
        )

    async def stop(self) -> None:
        if self._invalidator is not None:
            await self._invalidator.stop()

    async def get_or_set(
        self,
        key: str,
        factory: Callable[[], Awaitable[T]],
        *,
        options: CacheOptions | None = None,
    ) -> T:
        opts = options or self._options
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
        opts = options or self._options
        self._set_memory(key, value, opts)

        if self._distributed_cache is not None:
            await self._distributed_cache.set(
                key,
                value,
                ttl_seconds=self._ttl_with_jitter(opts),
            )

        if publish_invalidation and self._invalidator is not None:
            await self._invalidator.invalidate(key)

    async def remove(self, key: str) -> None:
        self.remove_local(key)

        if self._distributed_cache is not None:
            await self._distributed_cache.delete(key)

        if self._invalidator is not None:
            await self._invalidator.invalidate(key)

    async def clear(self) -> None:
        self.clear_memory()

        if self._invalidator is not None:
            await self._invalidator.clear()

    def remove_local(self, key: str) -> None:
        self._memory.pop(key, None)

    def clear_memory(self) -> None:
        self._memory.clear()

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
