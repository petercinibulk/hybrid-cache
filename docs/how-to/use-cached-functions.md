# Use Cached Functions

Use `@cache.cached()` on async functions whose return value can be reused for the same inputs.

## Use the default key

```python
from async_hybrid_cache import AsyncHybridCache

cache = AsyncHybridCache()


@cache.cached()
async def get_settings(tenant_id: str) -> dict[str, str]:
    ...
```

When no key is supplied, `Async Hybrid Cache` builds one from the function module, qualified name, and bound arguments.

## Use a custom key

```python
@cache.cached(lambda tenant_id: f"settings:{tenant_id}")
async def get_settings(tenant_id: str) -> dict[str, str]:
    ...
```

A custom key is easier to share with explicit invalidation calls and easier to inspect in Redis.

## Share one key across calls

```python
@cache.cached("global-settings")
async def get_global_settings() -> dict[str, str]:
    ...
```

Use a fixed key for a value that does not vary by input.

## Remove the cached value for a call

```python
await get_settings.remove_cached("tenant-123")
```

`remove_cached` uses the same key rules as the cached function call.
