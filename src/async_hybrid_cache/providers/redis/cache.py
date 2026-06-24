from __future__ import annotations

from redis.asyncio import Redis

from async_hybrid_cache.serializers import PickleSerializer, Serializer


class RedisDistributedCache:
    """Distributed cache implementation backed by Redis string keys."""

    def __init__(
        self,
        redis: Redis,
        *,
        prefix: str = "async-hybrid-cache:",
        serializer: Serializer | None = None,
    ) -> None:
        """Create a Redis distributed cache with an optional key prefix."""

        self._redis = redis
        self._prefix = prefix
        self._serializer = serializer or PickleSerializer()

    async def get(self, key: str) -> object | None:
        """Return a deserialized value or `None` when the key is missing."""

        value = await self._redis.get(self._key(key))

        if value is None:
            return None

        if isinstance(value, str):
            value = value.encode("utf-8")

        return self._serializer.loads(value)

    async def set(self, key: str, value: object, ttl_seconds: float) -> None:
        """Serialize and store a value with a Redis expiration."""

        await self._redis.set(
            self._key(key),
            self._serializer.dumps(value),
            ex=max(1, int(ttl_seconds)),
        )

    async def delete(self, key: str) -> None:
        """Delete a key from Redis."""

        await self._redis.delete(self._key(key))

    def _key(self, key: str) -> str:
        return f"{self._prefix}{key}"
