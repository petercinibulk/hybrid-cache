# Installation Extras

| Extra | Installs | Enables |
| --- | --- | --- |
| `redis` | `redis>=5.0.0` | Redis distributed cache and Redis Streams invalidation |
| `memcache` | `aiomcache>=0.8.0` | Memcached distributed cache |
| `rabbitmq` | `aio-pika>=9.0.0` | RabbitMQ invalidation |
| `kafka` | `aiokafka>=0.10.0` | Kafka invalidation |
| `postgres` | `asyncpg>=0.29.0` | PostgreSQL `LISTEN`/`NOTIFY` invalidation |
| `pydantic` | `pydantic>=1.10.0` | Pydantic model serialization |
| `all` | All optional provider dependencies | Full provider set |

Install extras with your package manager:

```bash
uv add "async-hybrid-cache[redis]"
```
