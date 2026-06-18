# hybrid-cache

Async hybrid Python cache with in-memory L1 caching, optional Redis L2 caching, pluggable invalidation, Redis Streams support, stampede protection, fail-safe stale values, and typed decorators.

## Development

```bash
uv sync
uv run ruff check .
uv run ruff format --check .
uv run ty check
uv run pytest
uv build --no-sources
```

## Usage

```python
from redis.asyncio import Redis

from hybrid_cache import (
    CacheOptions,
    HybridCache,
    RedisDistributedCache,
    RedisInvalidator,
    cached,
)

redis = Redis.from_url("redis://localhost:6379/0", decode_responses=False)
cache = HybridCache(
    distributed_cache=RedisDistributedCache(redis),
    invalidator=RedisInvalidator(redis),
    options=CacheOptions(
        ttl_seconds=60,
        fail_safe_seconds=300,
        hard_timeout_seconds=5,
        jitter_seconds=5,
    ),
)

await cache.start()


@cached(cache, lambda user_id: f"user:{user_id}")
async def get_user(user_id: str) -> dict[str, str]:
    return {"id": user_id, "name": "Peter"}


user = await get_user("123")
await get_user.remove_cached("123")
await cache.stop()
```

## Architecture

```text
HybridCache -> Invalidator -> Backplane
```

- `Backplane` transports `BackplaneMessage` instances between nodes.
- `Invalidator` publishes cache invalidations and applies received messages to the local L1 cache.
- `BackplaneInvalidator` works with any `Backplane` implementation.
- `RedisStreamsBackplane` implements the transport with Redis Streams.
- `RedisInvalidator` combines `BackplaneInvalidator` with `RedisStreamsBackplane`.

Reads follow this order:

```text
memory L1 -> distributed L2 -> factory
```

The default Redis serializer uses `pickle`. Only use it when Redis data is trusted. Supply a custom `Serializer` for JSON, msgpack, or another format.
