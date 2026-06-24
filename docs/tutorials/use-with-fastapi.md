# Use With FastAPI

In this tutorial you will add `Async Hybrid Cache` to a FastAPI application, cache a
service function, and invalidate the cached value after a write.

## Install dependencies

Install FastAPI, an ASGI server, and `async-hybrid-cache`:

```bash
uv add async-hybrid-cache fastapi uvicorn
```

## Create the cache

Create one `AsyncHybridCache` instance for the application:

```python
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from async_hybrid_cache import CacheOptions, AsyncHybridCache


cache = AsyncHybridCache(
    options=CacheOptions(
        ttl_seconds=60,
        fail_safe_seconds=300,
        lru_max_keys=1_000,
    ),
)
```

The cache defaults apply to every cached function unless that function supplies
its own `CacheOptions`.

## Add a small data source

This tutorial uses an in-memory dictionary so you can focus on the cache flow:

```python
USERS = {
    "123": {"id": "123", "name": "Peter"},
}


class UpdateUser(BaseModel):
    name: str


async def fetch_user_from_database(user_id: str) -> dict[str, str] | None:
    return USERS.get(user_id)


async def save_user_to_database(user_id: str, payload: UpdateUser) -> dict[str, str]:
    user = {"id": user_id, "name": payload.name}
    USERS[user_id] = user
    return user
```

In a real application, these functions would call your database or service
client.

## Cache a service function

Cache the service function rather than the FastAPI route handler:

```python
@cache.cached(
    lambda user_id: f"user:{user_id}",
    options=CacheOptions(ttl_seconds=30, lru_max_keys=500),
)
async def load_user(user_id: str) -> dict[str, str]:
    user = await fetch_user_from_database(user_id)
    if user is None:
        raise HTTPException(status_code=404, detail="User not found")
    return user
```

!!! note "Use explicit keys for endpoints"

    Prefer caching a service function with an explicit key instead of decorating
    the FastAPI endpoint directly with the default generated key. Endpoint
    functions often receive request objects, dependency instances,
    authentication context, or other per-request values. If those values become
    part of the default key, the cache may miss more often than expected or
    cache data without the right tenant, user, locale, or permission boundary.

## Start and stop the cache

Start the cache in the FastAPI lifespan and stop it during shutdown:

```python
@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    await cache.start()
    try:
        yield
    finally:
        await cache.stop()


app = FastAPI(lifespan=lifespan)
```

## Add read and write endpoints

Read through the cached service function:

```python
@app.get("/users/{user_id}")
async def get_user(user_id: str) -> dict[str, str]:
    return await load_user(user_id)
```

After a write, remove the cached value and read it again:

```python
@app.put("/users/{user_id}")
async def update_user(user_id: str, payload: UpdateUser) -> dict[str, str]:
    await save_user_to_database(user_id, payload)
    await load_user.remove_cached(user_id)
    return await load_user(user_id)
```

`remove_cached()` builds the same key as the decorated function call, removes the
local value, removes the distributed value when configured, and publishes an
invalidation message when an invalidation bus is configured.

## Run the application

Save the example as `main.py`, then run:

```bash
uv run uvicorn main:app --reload
```

Open `http://127.0.0.1:8000/users/123` to read the cached user.

## Add Redis for multiple workers

When you run multiple FastAPI workers or multiple application instances, each
process has its own local L1 cache. Add Redis for shared values and Redis Streams
for cross-instance invalidation:

```bash
uv add "async-hybrid-cache[redis]"
```

```python
from redis.asyncio import Redis

from async_hybrid_cache import RedisDistributedCache, RedisStreamsInvalidationBus


redis = Redis.from_url("redis://localhost:6379/0", decode_responses=False)

cache = AsyncHybridCache(
    distributed_cache=RedisDistributedCache(redis),
    invalidation_bus=RedisStreamsInvalidationBus(redis),
    options=CacheOptions(ttl_seconds=60, fail_safe_seconds=300, lru_max_keys=1_000),
)
```

Then close Redis in the lifespan shutdown:

```python
@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    await cache.start()
    try:
        yield
    finally:
        await cache.stop()
        await redis.aclose()
```

