# Use Cache Scopes

In this tutorial you will create separate cache scopes for different kinds of
data. Each scope has its own local LRU limit and can be read, written, removed,
or cleared without affecting other scopes.

## Create scoped caches

Start with cache-wide defaults, then create manual scopes with their own policy:

```python
from async_hybrid_cache import CacheOptions, AsyncHybridCache

cache = AsyncHybridCache(
    options=CacheOptions(
        ttl_seconds=60,
        fail_safe_seconds=300,
        lru_max_keys=1_000,
    ),
)

users = cache.scope(
    "users",
    options=CacheOptions(ttl_seconds=30, lru_max_keys=500),
)
products = cache.scope(
    "products",
    options=CacheOptions(ttl_seconds=120, lru_max_keys=2_000),
)
```

The cache defaults are still used for any option omitted by a scope. In this
example, both scopes inherit `fail_safe_seconds=300`.

## Set and get values manually

Use a scoped cache when your application already has the value and wants to put
it in the cache explicitly:

```python
await users.set("user:123", {"id": "123", "name": "Peter"})

user = await users.get("user:123")
```

The same key can exist in another scope without colliding:

```python
await products.set("user:123", {"id": "user:123", "name": "Notebook"})

product = await products.get("user:123")
```

## Compute missing values

Use `get_or_set` when the value should be loaded only on a cache miss:

```python
async def load_user_from_database(user_id: str) -> dict[str, str]:
    return {"id": user_id, "name": "Peter"}


user = await users.get_or_set(
    "user:123",
    lambda: load_user_from_database("123"),
)
```

The first call runs the factory and stores the result in the `users` scope. Later
calls return the cached value until the entry expires, is evicted by the scope's
LRU limit, or is removed.

## Remove or clear scoped data

Remove one scoped key after the source record changes:

```python
await users.remove("user:123")
```

Clear one scope without clearing the whole cache:

```python
await users.clear()
```

`remove` and `clear` also publish scoped invalidation messages when the cache has
an invalidation bus configured.

## Use function scopes

Decorated functions get their own scope by default:

```python
@cache.cached(
    lambda user_id: f"user:{user_id}",
    options=CacheOptions(ttl_seconds=30, lru_max_keys=500),
)
async def get_user(user_id: str) -> dict[str, str]:
    return await load_user_from_database(user_id)
```

The function's `lru_max_keys` limit applies only to that function's local scope.
Other decorated functions keep their own LRU buckets.

Use an explicit scope when multiple functions should share the same local LRU
bucket:

```python
@cache.cached(lambda user_id: f"user:{user_id}", scope="users")
async def get_user(user_id: str) -> dict[str, str]:
    return await load_user_from_database(user_id)


@cache.cached(lambda user_id: f"user-settings:{user_id}", scope="users")
async def get_user_settings(user_id: str) -> dict[str, str]:
    return {"theme": "dark"}
```

Both functions now share the `users` scope.

