# Get Started

In this tutorial you will cache one async function, read the value from the cache, and remove it when the source data changes.

## Install

```bash
uv add async-hybrid-cache
```

## Create an in-memory cache

```python
from async_hybrid_cache import CacheOptions, AsyncHybridCache

cache = AsyncHybridCache(
    options=CacheOptions(
        ttl_seconds=60,
        fail_safe_seconds=300,
        hard_timeout_seconds=5,
        jitter_seconds=5,
    ),
)

await cache.start()
```

## Cache an async function

```python
@cache.cached(lambda user_id: f"user:{user_id}")
async def get_user(user_id: str) -> dict[str, str]:
    return {"id": user_id, "name": "Peter"}


user = await get_user("123")
```

The first call runs `get_user`. Later calls with the same key return the cached value until the entry expires or is removed.

## Remove one cached value

```python
await get_user.remove_cached("123")
```

Use this after your application changes the underlying user record.

## Add Redis when you need shared values

Install the Redis extra:

```bash
uv add "async-hybrid-cache[redis]"
```

Create a Redis-backed cache:

```python
from redis.asyncio import Redis

from async_hybrid_cache import (
    CacheOptions,
    AsyncHybridCache,
    RedisDistributedCache,
    RedisStreamsInvalidationBus,
)

redis = Redis.from_url("redis://localhost:6379/0", decode_responses=False)

cache = AsyncHybridCache(
    distributed_cache=RedisDistributedCache(redis),
    invalidation_bus=RedisStreamsInvalidationBus(redis),
    options=CacheOptions(ttl_seconds=60, fail_safe_seconds=300),
)

await cache.start()
```

With this setup, each application instance keeps a fast local memory cache, Redis stores shared values, and Redis Streams carries invalidation messages between instances.

## Stop the cache during shutdown

```python
await cache.stop()
await redis.aclose()
```
