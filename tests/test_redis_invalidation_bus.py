from __future__ import annotations

from typing import Any, cast

from async_hybrid_cache import RedisStreamsInvalidationBus


class FakeRedis:
    def __init__(self) -> None:
        self.added: list[tuple[str, dict[Any, Any], int | None, bool]] = []
        self.acked: list[tuple[str, str, bytes | str]] = []

    async def xadd(
        self,
        stream_name: str,
        fields: dict[Any, Any],
        *,
        maxlen: int | None = None,
        approximate: bool = False,
    ) -> None:
        self.added.append((stream_name, fields, maxlen, approximate))

    async def xack(
        self,
        stream_name: str,
        group_name: str,
        message_id: bytes | str,
    ) -> None:
        self.acked.append((stream_name, group_name, message_id))


async def test_redis_streams_invalidation_bus_publishes_invalidations() -> None:
    redis = FakeRedis()
    bus = RedisStreamsInvalidationBus(
        cast(Any, redis),
        stream_name="invalidations",
        max_length=50,
    )

    await bus.invalidate("user:1", scope="users")
    await bus.clear(scope="users")

    assert redis.added[0][0] == "invalidations"
    assert redis.added[0][1]["action"] == "remove"
    assert redis.added[0][1]["key"] == "user:1"
    assert redis.added[0][1]["scope"] == "users"
    assert redis.added[0][2:] == (50, True)
    assert redis.added[1][1]["action"] == "clear"
    assert redis.added[1][1]["scope"] == "users"


async def test_redis_streams_invalidation_bus_applies_remote_messages() -> None:
    redis = FakeRedis()
    bus = RedisStreamsInvalidationBus(
        cast(Any, redis),
        stream_name="invalidations",
        node_name="node",
    )
    removed: list[tuple[str, str]] = []
    cleared: list[str | None] = []

    bus._remove_local = lambda key, scope: removed.append((key, scope))
    bus._clear_local = cleared.append
    await bus._process_message(
        "1-0",
        {
            "action": "remove",
            "source_id": "another-node",
            "key": "user:1",
            "scope": "users",
        },
    )
    await bus._process_message(
        "2-0",
        {
            "action": "clear",
            "source_id": "another-node",
            "scope": "users",
        },
    )

    assert removed == [("user:1", "users")]
    assert cleared == ["users"]
    assert redis.acked == [
        ("invalidations", "async-hybrid-cache-node:node", "1-0"),
        ("invalidations", "async-hybrid-cache-node:node", "2-0"),
    ]
