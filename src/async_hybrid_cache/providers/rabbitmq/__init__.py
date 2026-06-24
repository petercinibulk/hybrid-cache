"""RabbitMQ provider exports."""

from async_hybrid_cache.providers.rabbitmq.invalidation_bus import RabbitMQInvalidationBus

__all__ = [
    "RabbitMQInvalidationBus",
]
