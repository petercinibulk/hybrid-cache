"""Public API for async-hybrid-cache."""

from typing import TYPE_CHECKING, Any

from async_hybrid_cache.core import AsyncHybridCache, CacheOptions, ScopedCache
from async_hybrid_cache.decorators import CachedFunction
from async_hybrid_cache.distributed_cache import DistributedCache
from async_hybrid_cache.invalidation import (
    InvalidationBus,
    InvalidationHandler,
    InvalidationMessage,
    InvalidationTransport,
    TransportInvalidationBus,
)
from async_hybrid_cache.serializers import (
    JsonSerializer,
    PickleSerializer,
    PydanticSerializer,
    Serializer,
)

if TYPE_CHECKING:
    from async_hybrid_cache.providers.kafka import KafkaInvalidationBus
    from async_hybrid_cache.providers.memcache import MemcachedDistributedCache
    from async_hybrid_cache.providers.postgres import PostgresNotifyInvalidationBus
    from async_hybrid_cache.providers.rabbitmq import RabbitMQInvalidationBus
    from async_hybrid_cache.providers.redis import (
        RedisDistributedCache,
        RedisStreamsInvalidationBus,
    )

__all__ = [
    "AsyncHybridCache",
    "CacheOptions",
    "CachedFunction",
    "DistributedCache",
    "InvalidationBus",
    "InvalidationHandler",
    "InvalidationMessage",
    "InvalidationTransport",
    "JsonSerializer",
    "KafkaInvalidationBus",
    "MemcachedDistributedCache",
    "PickleSerializer",
    "PostgresNotifyInvalidationBus",
    "PydanticSerializer",
    "RabbitMQInvalidationBus",
    "RedisDistributedCache",
    "RedisStreamsInvalidationBus",
    "ScopedCache",
    "Serializer",
    "TransportInvalidationBus",
]


def __getattr__(name: str) -> Any:
    if name == "RedisDistributedCache":
        from async_hybrid_cache.providers.redis import RedisDistributedCache

        return RedisDistributedCache

    if name == "RedisStreamsInvalidationBus":
        from async_hybrid_cache.providers.redis import RedisStreamsInvalidationBus

        return RedisStreamsInvalidationBus

    if name == "MemcachedDistributedCache":
        from async_hybrid_cache.providers.memcache import MemcachedDistributedCache

        return MemcachedDistributedCache

    if name == "RabbitMQInvalidationBus":
        from async_hybrid_cache.providers.rabbitmq import RabbitMQInvalidationBus

        return RabbitMQInvalidationBus

    if name == "KafkaInvalidationBus":
        from async_hybrid_cache.providers.kafka import KafkaInvalidationBus

        return KafkaInvalidationBus

    if name == "PostgresNotifyInvalidationBus":
        from async_hybrid_cache.providers.postgres import PostgresNotifyInvalidationBus

        return PostgresNotifyInvalidationBus

    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)
