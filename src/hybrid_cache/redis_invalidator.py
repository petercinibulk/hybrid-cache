from __future__ import annotations

from redis.asyncio import Redis

from hybrid_cache.invalidator import BackplaneInvalidator
from hybrid_cache.redis_streams_backplane import RedisStreamsBackplane


class RedisInvalidator(BackplaneInvalidator):
    def __init__(
        self,
        redis: Redis,
        *,
        stream_name: str = "hybrid-cache:invalidations",
        node_name: str | None = None,
        max_length: int = 10_000,
    ) -> None:
        super().__init__(
            RedisStreamsBackplane(
                redis,
                stream_name=stream_name,
                node_name=node_name,
                max_length=max_length,
            )
        )
