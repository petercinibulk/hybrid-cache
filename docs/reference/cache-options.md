# Cache Options

`CacheOptions` controls freshness, stale fallback, factory timeout, TTL jitter, and
the maximum number of local keys to retain.

| Option | Type | Default | Meaning |
| --- | --- | --- | --- |
| `ttl_seconds` | `float` | `60` | Fresh lifetime for values in local memory and distributed storage |
| `fail_safe_seconds` | `float` | `300` | Extra time a stale local value can be returned after a refresh error |
| `hard_timeout_seconds` | `float` | `5` | Maximum time to wait for the value factory |
| `jitter_seconds` | `float` | `0` | Random extra seconds added to the TTL |
| `max_keys` | `int \| None` | `None` | Maximum number of keys to keep in local memory; `None` keeps all keys |

Pass options to the cache for global defaults:

```python
cache = CacheSync(options=CacheOptions(ttl_seconds=120, max_keys=1_000))
```

Pass options to `@cache.cached`, `get_or_set`, or `set` for a specific cached key:

```python
@cache.cached(options=CacheOptions(ttl_seconds=30))
async def load_value() -> str:
    ...
```

Only options supplied to the cache constructor establish cache-wide defaults. Options
supplied anywhere else override those defaults only for the affected cached key; omitted
fields inherit from the cache defaults.
