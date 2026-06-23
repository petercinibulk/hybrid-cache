from __future__ import annotations

from typing import cast

from aiomcache import Client

from cache_sync import JsonSerializer, MemcachedDistributedCache


class FakeMemcacheClient:
    def __init__(self) -> None:
        self.values: dict[bytes, bytes] = {}
        self.set_calls: list[tuple[bytes, bytes, int]] = []
        self.deleted: list[bytes] = []

    async def get(self, key: bytes) -> bytes | None:
        return self.values.get(key)

    async def set(self, key: bytes, value: bytes, *, exptime: int) -> None:
        self.set_calls.append((key, value, exptime))
        self.values[key] = value

    async def delete(self, key: bytes) -> None:
        self.deleted.append(key)
        self.values.pop(key, None)


async def test_memcached_distributed_cache_round_trips_values() -> None:
    client = FakeMemcacheClient()
    cache = MemcachedDistributedCache(
        cast(Client, client),
        prefix="test:",
        serializer=JsonSerializer(),
    )

    await cache.set("user:1", {"id": 1}, ttl_seconds=1.2)

    assert client.set_calls == [(b"test:user:1", b'{"id": 1}', 1)]
    assert await cache.get("user:1") == {"id": 1}


async def test_memcached_distributed_cache_deletes_values() -> None:
    client = FakeMemcacheClient()
    cache = MemcachedDistributedCache(cast(Client, client))

    await cache.set("user:1", "value", ttl_seconds=60)
    await cache.delete("user:1")

    assert client.deleted == [b"cache-sync:user:1"]
    assert await cache.get("user:1") is None


async def test_memcached_distributed_cache_hashes_invalid_keys() -> None:
    client = FakeMemcacheClient()
    cache = MemcachedDistributedCache(cast(Client, client))

    key = "user id with spaces"

    await cache.set(key, "value", ttl_seconds=60)

    stored_key = next(iter(client.values))
    assert stored_key.startswith(b"cache-sync:sha256:")
    assert b" " not in stored_key
    assert await cache.get(key) == "value"


async def test_memcached_distributed_cache_hashes_invalid_prefixes() -> None:
    client = FakeMemcacheClient()
    cache = MemcachedDistributedCache(cast(Client, client), prefix="invalid prefix:")

    await cache.set("user:1", "value", ttl_seconds=60)

    stored_key = next(iter(client.values))
    assert stored_key.startswith(b"cache-sync:sha256:")
    assert b" " not in stored_key
    assert await cache.get("user:1") == "value"
