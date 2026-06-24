# Configure Cache Policy

Cache policy controls freshness, stale fallback, timeout behavior, TTL spread, and
local cache size.

## Set defaults for the whole cache

```python
from async_hybrid_cache import CacheOptions, AsyncHybridCache

cache = AsyncHybridCache(
    options=CacheOptions(
        ttl_seconds=120,
        fail_safe_seconds=600,
        hard_timeout_seconds=3,
        jitter_seconds=10,
        lru_max_keys=1_000,
    ),
)
```

## Override policy for one cached function scope

```python
from async_hybrid_cache import CacheOptions


@cache.cached(
    lambda product_id: f"product:{product_id}",
    options=CacheOptions(ttl_seconds=30, fail_safe_seconds=300, lru_max_keys=500),
)
async def get_product(product_id: str) -> dict[str, str]:
    ...
```

Overrides apply to that cached function's scope. Supplied values replace the defaults
configured on `AsyncHybridCache`, and omitted values inherit those defaults.

Each decorated function gets its own local scope by default. `lru_max_keys` limits keys
within that function's scope, not the whole `AsyncHybridCache` instance.

## Configure a manual scope

Use a manual scope when you call the cache directly instead of decorating a function:

```python
users = cache.scope("users", options=CacheOptions(ttl_seconds=30, lru_max_keys=500))

await users.set("user:123", user)
user = await users.get("user:123")
await users.remove("user:123")
await users.clear()
```

Manual scopes inherit cache defaults the same way decorated function scopes do.

## Choose practical values

| Option | Use it for | Common starting point |
| --- | --- | --- |
| `ttl_seconds` | How long a value is fresh | 30 to 300 seconds |
| `fail_safe_seconds` | How long stale data can be used after a refresh error | 5 to 10 times the TTL |
| `hard_timeout_seconds` | Maximum time to wait for the value factory | Smaller than your request timeout |
| `jitter_seconds` | Spreading expirations across time | 5 to 10 percent of the TTL |
| `lru_max_keys` | Limiting per-scope memory growth by evicting the least-recently-used key when a new key is added | Based on your process memory budget |

Use lower TTLs for frequently changing data. Use higher TTLs for expensive values that can be slightly out of date.
