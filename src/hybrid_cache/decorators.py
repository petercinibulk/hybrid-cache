from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Generic, ParamSpec, TypeVar

from hybrid_cache.core import CacheOptions, HybridCache

P = ParamSpec("P")
T = TypeVar("T")


class CachedFunction(Generic[P, T]):
    def __init__(
        self,
        cache: HybridCache,
        func: Callable[P, Awaitable[T]],
        key: str | Callable[..., str],
        options: CacheOptions | None,
    ) -> None:
        self._cache = cache
        self._func = func
        self._key = key
        self._options = options

    async def __call__(self, *args: P.args, **kwargs: P.kwargs) -> T:
        return await self._cache.get_or_set(
            self.cache_key(*args, **kwargs),
            lambda: self._func(*args, **kwargs),
            options=self._options,
        )

    async def remove_cached(self, *args: P.args, **kwargs: P.kwargs) -> None:
        await self._cache.remove(self.cache_key(*args, **kwargs))

    def cache_key(self, *args: P.args, **kwargs: P.kwargs) -> str:
        if isinstance(self._key, str):
            return self._key
        return self._key(*args, **kwargs)


def cached(
    cache: HybridCache,
    key: str | Callable[..., str],
    *,
    options: CacheOptions | None = None,
) -> Callable[[Callable[P, Awaitable[T]]], CachedFunction[P, T]]:
    def decorator(func: Callable[P, Awaitable[T]]) -> CachedFunction[P, T]:
        return CachedFunction(cache, func, key, options)

    return decorator
