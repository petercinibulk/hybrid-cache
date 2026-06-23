# Provider Capabilities

| Provider | Distributed cache | Invalidation bus | Notes |
| --- | --- | --- | --- |
| Redis | `RedisDistributedCache` | `RedisStreamsInvalidationBus` | Can provide both shared values and invalidation |
| Memcached | `MemcachedDistributedCache` | No | Stores shared values only |
| RabbitMQ | No | `RabbitMQInvalidationBus` | Uses a fanout exchange |
| Kafka | No | `KafkaInvalidationBus` | Uses a topic and a unique consumer group per node by default |
| PostgreSQL | No | `PostgresNotifyInvalidationBus` | Uses `LISTEN`/`NOTIFY` |

Distributed cache and invalidation are independent. You can use Redis or Memcached for shared cached values without an invalidation bus, an invalidation bus without shared L2 storage, or both together.

## Default names

| Provider | Default name |
| --- | --- |
<<<<<<< HEAD
| Redis distributed key prefix | `async-hybrid-cache:` |
| Redis invalidation stream | `async-hybrid-cache:invalidations` |
| RabbitMQ exchange | `async-hybrid-cache-invalidations` |
| Kafka topic | `async-hybrid-cache-invalidations` |
| PostgreSQL channel | `async_hybrid_cache_invalidations` |
=======
| Redis distributed key prefix | `cache-sync:` |
| Redis invalidation stream | `cache-sync:invalidations` |
| Memcached distributed key prefix | `cache-sync:` |
| RabbitMQ exchange | `cache-sync-invalidations` |
| Kafka topic | `cache-sync-invalidations` |
| PostgreSQL channel | `cache_sync_invalidations` |
>>>>>>> e298429 (feat: add memcache distributed cache support (#7))
