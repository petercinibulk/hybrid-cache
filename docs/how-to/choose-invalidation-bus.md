# Choose an Invalidation Bus

An invalidation bus tells other application instances to remove local L1 entries after a value changes.

## Use Redis Streams when Redis is already your cache backend

```python
from async_hybrid_cache import RedisDistributedCache, RedisStreamsInvalidationBus

cache = AsyncHybridCache(
    distributed_cache=RedisDistributedCache(redis),
    invalidation_bus=RedisStreamsInvalidationBus(redis),
)
```

This is the simplest distributed setup when Redis is already part of your app.

## Use RabbitMQ for broker-based fanout

```python
from aio_pika import connect_robust
from async_hybrid_cache import RabbitMQInvalidationBus

connection = await connect_robust("amqp://guest:guest@localhost/")
cache = AsyncHybridCache(
    invalidation_bus=RabbitMQInvalidationBus(connection),
)
```

RabbitMQ uses a fanout exchange so each running application instance receives each invalidation.

## Use Kafka when it is already your platform bus

```python
from async_hybrid_cache import KafkaInvalidationBus

cache = AsyncHybridCache(
    invalidation_bus=KafkaInvalidationBus(
        bootstrap_servers="localhost:9092",
    ),
)
```

By default, every node gets a unique consumer group. Do not share one `group_id` across application instances unless you intentionally want Kafka to load-balance messages, which is usually wrong for cache invalidation.

## Use PostgreSQL notifications for a lightweight option

```python
import asyncpg
from async_hybrid_cache import PostgresNotifyInvalidationBus

connection = await asyncpg.connect("postgresql://localhost/app")
cache = AsyncHybridCache(
    invalidation_bus=PostgresNotifyInvalidationBus(connection),
)
```

Use PostgreSQL `LISTEN`/`NOTIFY` when you already depend on PostgreSQL and want invalidation without an extra broker.

## Provider selection guide

| Provider | Best fit |
| --- | --- |
| Redis Streams | You already use Redis for distributed cache values |
| RabbitMQ | You already use RabbitMQ for fanout messages |
| Kafka | Kafka is standard in your platform and topics are easy to provision |
| PostgreSQL | Smaller deployments that already have PostgreSQL |
