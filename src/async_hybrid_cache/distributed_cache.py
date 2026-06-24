from __future__ import annotations

from typing import Protocol

from async_hybrid_cache.serializers import PickleSerializer as PickleSerializer
from async_hybrid_cache.serializers import Serializer as Serializer


class DistributedCache(Protocol):
    """Protocol for optional shared L2 cache storage."""

    async def get(self, key: str) -> object | None: ...

    async def set(self, key: str, value: object, ttl_seconds: float) -> None: ...

    async def delete(self, key: str) -> None: ...
