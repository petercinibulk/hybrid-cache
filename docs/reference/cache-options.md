# Cache Options

`CacheOptions` controls freshness, stale fallback, factory timeout, TTL jitter, and
the maximum number of local keys to retain per scope.

| Option | Type | Default | Meaning |
| --- | --- | --- | --- |
| `ttl_seconds` | `float` | `60` | Fresh lifetime for values in local memory and distributed storage |
| `fail_safe_seconds` | `float` | `300` | Extra time a stale local value can be returned after a refresh error |
| `hard_timeout_seconds` | `float` | `5` | Maximum time to wait for the value factory |
| `jitter_seconds` | `float` | `0` | Random extra seconds added to the TTL |
| `lru_max_keys` | `int \| None` | `None` | Maximum number of least-recently-used keys to keep in a local scope; `None` keeps all keys |

Pass options to the cache for global defaults:

```python
cache = AsyncHybridCache(options=CacheOptions(ttl_seconds=120, lru_max_keys=1_000))
```

Pass options to `@cache.cached` for that function's scope:

```python
@cache.cached(options=CacheOptions(ttl_seconds=30, lru_max_keys=500))
async def load_value() -> str:
    ...
```

Create a manual scope when you are not using `@cache.cached`:

```python
users = cache.scope("users", options=CacheOptions(ttl_seconds=30, lru_max_keys=500))

await users.set("user:123", user)
user = await users.get("user:123")
await users.remove("user:123")
await users.clear()
```

Only options supplied to the cache constructor establish cache-wide defaults. Decorated
functions and manual scopes inherit those defaults, and supplied scope options override
only the fields they set. Per-call options passed to `get_or_set` or `set` override the
scope defaults for that operation.
