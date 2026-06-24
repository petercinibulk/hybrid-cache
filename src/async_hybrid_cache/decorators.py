from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any, Generic, ParamSpec, TypeVar

from async_hybrid_cache.core import CacheOptions

if TYPE_CHECKING:
    from async_hybrid_cache.core import AsyncHybridCache

P = ParamSpec("P")
T = TypeVar("T")


class CachedFunction(Generic[P, T]):
    """Callable wrapper returned by `AsyncHybridCache.cached` with cache helpers."""

    def __init__(
        self,
        cache: AsyncHybridCache,
        func: Callable[P, Awaitable[T]],
        key: str | Callable[..., str] | None,
        options: CacheOptions | None,
        scope: str | None,
    ) -> None:
        self._cache = cache
        self._func = func
        self._key = key
        self._options = options
        self._scope = scope or default_cache_scope(func)

    async def __call__(self, *args: P.args, **kwargs: P.kwargs) -> T:
        """Return the cached result for this call or invoke the wrapped function."""

        return await self._cache.get_or_set(
            self.cache_key(*args, **kwargs),
            lambda: self._func(*args, **kwargs),
            scope=self._scope,
            options=self._options,
        )

    async def remove_cached(self, *args: P.args, **kwargs: P.kwargs) -> None:
        """Remove the cached value associated with this call's cache key."""

        await self._cache.remove(self.cache_key(*args, **kwargs), scope=self._scope)

    def cache_key(self, *args: P.args, **kwargs: P.kwargs) -> str:
        """Build the cache key for the supplied function arguments."""

        if isinstance(self._key, str):
            return self._key
        if self._key is None:
            return default_cache_key(self._func, *args, **kwargs)
        return self._key(*args, **kwargs)


def default_cache_key(func: Callable[..., Awaitable[Any]], *args: Any, **kwargs: Any) -> str:
    """Build a deterministic key from a function and call arguments."""

    signature = inspect.signature(func)
    bound = signature.bind(*args, **kwargs)
    bound.apply_defaults()
    arguments = ",".join(
        f"{name}={_stable_key_part(value)}" for name, value in bound.arguments.items()
    )
    module = getattr(func, "__module__", type(func).__module__)
    qualname = getattr(func, "__qualname__", type(func).__qualname__)

    return f"{module}.{qualname}({arguments})"


def default_cache_scope(func: Callable[..., Awaitable[Any]]) -> str:
    """Build the default scope for a decorated cached function."""

    module = getattr(func, "__module__", type(func).__module__)
    qualname = getattr(func, "__qualname__", type(func).__qualname__)

    return f"{module}.{qualname}"


def _stable_key_part(value: Any) -> str:
    if isinstance(value, dict):
        items = sorted(
            (_stable_key_part(key), _stable_key_part(item_value))
            for key, item_value in value.items()
        )
        return "{" + ",".join(f"{key}:{item_value}" for key, item_value in items) + "}"

    if isinstance(value, (list, tuple)):
        opener, closer = ("[", "]") if isinstance(value, list) else ("(", ")")
        return opener + ",".join(_stable_key_part(item) for item in value) + closer

    if isinstance(value, (set, frozenset)):
        items = sorted(_stable_key_part(item) for item in value)
        return "{" + ",".join(items) + "}"

    return repr(value)
