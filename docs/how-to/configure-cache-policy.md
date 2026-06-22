# Configure Cache Policy

Cache policy controls freshness, stale fallback, timeout behavior, TTL spread, and
local cache size.

## Set defaults for the whole cache

```python
from cache_sync import CacheOptions, CacheSync

cache = CacheSync(
    options=CacheOptions(
        ttl_seconds=120,
        fail_safe_seconds=600,
        hard_timeout_seconds=3,
        jitter_seconds=10,
        max_keys=1_000,
    ),
)
```

## Override policy for one cached function

```python
from cache_sync import CacheOptions


@cache.cached(
    lambda product_id: f"product:{product_id}",
    options=CacheOptions(ttl_seconds=30, fail_safe_seconds=300),
)
async def get_product(product_id: str) -> dict[str, str]:
    ...
```

Overrides apply only to the affected cached key. Supplied values replace the defaults
configured on `CacheSync`, and omitted values inherit those defaults.

## Choose practical values

| Option | Use it for | Common starting point |
| --- | --- | --- |
| `ttl_seconds` | How long a value is fresh | 30 to 300 seconds |
| `fail_safe_seconds` | How long stale data can be used after a refresh error | 5 to 10 times the TTL |
| `hard_timeout_seconds` | Maximum time to wait for the value factory | Smaller than your request timeout |
| `jitter_seconds` | Spreading expirations across time | 5 to 10 percent of the TTL |
| `max_keys` | Limiting local memory growth by evicting the oldest key when a new key is added | Based on your process memory budget |

Use lower TTLs for frequently changing data. Use higher TTLs for expensive values that can be slightly out of date.
