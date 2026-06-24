# async-hybrid-cache

Async hybrid Python cache with in-memory L1 caching, optional Redis or Memcached L2 caching, pluggable invalidation, stampede protection, fail-safe stale values, and typed decorators.

## Features

- Async-first API for Python 3.12 and newer.
- Fast in-process L1 cache with optional Redis-backed or Memcached-backed L2 storage.
- Pluggable invalidation buses for Redis Streams, RabbitMQ, Kafka, and PostgreSQL.
- Request stampede protection with per-key refresh coordination.
- Fail-safe stale reads for short backend outages.
- Typed decorators that preserve the wrapped function signature.
- Serializer choices for JSON, pickle, and Pydantic models.

## Documentation

The end-user documentation is published at <https://petercinibulk.github.io/async-hybrid-cache/> and is built from [`docs/`](docs/index.md) with Zensical.

## Install

```bash
uv add async-hybrid-cache
```

Install optional providers only when your application uses them:

```bash
uv add "async-hybrid-cache[redis]"
uv add "async-hybrid-cache[memcache]"
uv add "async-hybrid-cache[rabbitmq]"
uv add "async-hybrid-cache[kafka]"
uv add "async-hybrid-cache[postgres]"
uv add "async-hybrid-cache[all]"
```

| Extra | Installs | Use when |
| --- | --- | --- |
| `redis` | `redis` | You need Redis L2 storage or Redis Streams invalidation. |
| `memcache` | `aiomcache` | You need Memcached L2 storage. |
| `rabbitmq` | `aio-pika` | You use RabbitMQ as the invalidation bus. |
| `kafka` | `aiokafka` | You use Kafka as the invalidation bus. |
| `postgres` | `asyncpg` | You use PostgreSQL `LISTEN`/`NOTIFY` for invalidation. |
| `pydantic` | `pydantic` | You want Pydantic model serialization helpers. |
| `all` | all provider dependencies | You want every optional provider available. |

## Quick Start

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


@cache.cached(lambda user_id: f"user:{user_id}")
async def get_user(user_id: str) -> dict[str, str]:
    return {"id": user_id, "name": "Peter"}


user = await get_user("123")
await get_user.remove_cached("123")
await cache.stop()
```

## Project

- License: MIT
- Source: <https://github.com/petercinibulk/async-hybrid-cache>
- Issues: <https://github.com/petercinibulk/async-hybrid-cache/issues>
