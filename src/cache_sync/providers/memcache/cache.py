from __future__ import annotations

import hashlib
import re

from aiomcache import Client

from cache_sync.serializers import PickleSerializer, Serializer

_INVALID_MEMCACHE_KEY = re.compile(rb"[\x00-\x20\x7f]")
_MAX_MEMCACHE_KEY_BYTES = 250
_HASHED_KEY_PREFIX = b"cache-sync:sha256:"


class MemcachedDistributedCache:
    """Distributed cache implementation backed by Memcached."""

    def __init__(
        self,
        client: Client,
        *,
        prefix: str = "cache-sync:",
        serializer: Serializer | None = None,
    ) -> None:
        """Create a Memcached distributed cache with an optional key prefix."""

        self._client = client
        self._prefix = prefix
        self._serializer = serializer or PickleSerializer()

    async def get(self, key: str) -> object | None:
        """Return a deserialized value or `None` when the key is missing."""

        value = await self._client.get(self._key(key))

        if value is None:
            return None

        return self._serializer.loads(value)

    async def set(self, key: str, value: object, ttl_seconds: float) -> None:
        """Serialize and store a value with a Memcached expiration."""

        await self._client.set(
            self._key(key),
            self._serializer.dumps(value),
            exptime=max(1, int(ttl_seconds)),
        )

    async def delete(self, key: str) -> None:
        """Delete a key from Memcached."""

        await self._client.delete(self._key(key))

    def _key(self, key: str) -> bytes:
        prefix = self._prefix.encode()
        raw_key = f"{self._prefix}{key}".encode()

        if (
            len(raw_key) <= _MAX_MEMCACHE_KEY_BYTES
            and _INVALID_MEMCACHE_KEY.search(raw_key) is None
        ):
            return raw_key

        digest = hashlib.sha256(raw_key).hexdigest().encode()
        hashed_key = prefix + b"sha256:" + digest
        if (
            len(hashed_key) <= _MAX_MEMCACHE_KEY_BYTES
            and _INVALID_MEMCACHE_KEY.search(prefix) is None
        ):
            return hashed_key

        return _HASHED_KEY_PREFIX + digest
