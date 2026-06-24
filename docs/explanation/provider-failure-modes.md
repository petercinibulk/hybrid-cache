# Provider Failure Modes

`AsyncHybridCache` separates value storage from invalidation. That makes provider behavior easier to reason about, but each provider still has operational tradeoffs.

## Distributed cache failures

When a distributed cache is configured, local memory remains the first lookup layer. If the distributed cache is unavailable, cache reads and writes that need that provider can fail unless a local value is still valid or eligible for fail-safe use.

Use `fail_safe_seconds` when stale data is preferable to an application error during a short outage. Fail-safe values are local to the process that already has the value.

## Invalidation bus failures

Invalidation messages keep separate application instances from serving stale local values after a remove or clear operation. If an invalidation bus is down, the local instance still removes its own value, but peer instances might keep their L1 values until TTL expiry.

For data that must be invalidated immediately across every process, keep TTLs short enough for your risk tolerance and monitor the invalidation bus as part of application health.

## Provider notes

| Provider | Failure behavior to plan for |
| --- | --- |
| Redis distributed cache | Redis outages can prevent shared L2 reads and writes. Existing local L1 values can still be served until they expire or become fail-safe values. |
| Redis Streams invalidation | Consumers that are down can miss timely invalidation until they resume processing. Size stream retention for your recovery window. |
| RabbitMQ invalidation | Fanout queues are per running consumer. Instances that are offline when a message is published should be treated as potentially stale until TTL expiry after they return. |
| Kafka invalidation | Topic retention can allow consumers to catch up, but only if retention and consumer group behavior match your deployment model. |
| PostgreSQL notifications | `LISTEN`/`NOTIFY` is lightweight, but notifications are not a durable queue for offline consumers. Reconnected instances should rely on TTL expiry for missed messages. |

## Recovery strategy

Choose a TTL that bounds the maximum age of stale local values, then use invalidation to make most changes visible sooner. Use fail-safe stale reads only for values where serving an older result is acceptable.
