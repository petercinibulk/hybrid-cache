# Serializers

Serializers convert values to bytes for distributed cache storage and back again when values are read.

| Serializer | Use it for | Notes |
| --- | --- | --- |
| `PickleSerializer` | Trusted Python objects | Default for Redis distributed cache |
| `JsonSerializer` | JSON-compatible values | Good for simple dictionaries, lists, strings, numbers, and booleans |
| `PydanticSerializer` | Pydantic model instances | Requires the `pydantic` extra |

## Use JSON with Redis

```python
from async_hybrid_cache import JsonSerializer, RedisDistributedCache

distributed_cache = RedisDistributedCache(
    redis,
    serializer=JsonSerializer(),
)
```

## Use Pydantic models

```python
from async_hybrid_cache import PydanticSerializer, RedisDistributedCache

distributed_cache = RedisDistributedCache(
    redis,
    serializer=PydanticSerializer(User),
)
```

Only use pickle when Redis data is trusted by your application.
