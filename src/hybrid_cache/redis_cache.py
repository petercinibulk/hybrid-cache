from __future__ import annotations

from redis.asyncio import Redis

from hybrid_cache.distributed_cache import PickleSerializer, Serializer


class RedisDistributedCache:
    def __init__(
        self,
        redis: Redis,
        *,
        prefix: str = "hybrid-cache:",
        serializer: Serializer | None = None,
    ) -> None:
        self._redis = redis
        self._prefix = prefix
        self._serializer = serializer or PickleSerializer()

    async def get(self, key: str) -> object | None:
        value = await self._redis.get(self._key(key))

        if value is None:
            return None

        if isinstance(value, str):
            value = value.encode("utf-8")

        return self._serializer.loads(value)

    async def set(self, key: str, value: object, ttl_seconds: float) -> None:
        await self._redis.set(
            self._key(key),
            self._serializer.dumps(value),
            ex=max(1, int(ttl_seconds)),
        )

    async def delete(self, key: str) -> None:
        await self._redis.delete(self._key(key))

    def _key(self, key: str) -> str:
        return f"{self._prefix}{key}"
