from __future__ import annotations

import json

from async_hybrid_cache import PostgresNotifyInvalidationBus


class FakeConnection:
    def __init__(self) -> None:
        self.listeners: list[tuple[str, object]] = []
        self.removed_listeners: list[tuple[str, object]] = []
        self.executed: list[tuple[str, str, str]] = []

    async def add_listener(self, channel: str, callback: object) -> None:
        self.listeners.append((channel, callback))

    async def remove_listener(self, channel: str, callback: object) -> None:
        self.removed_listeners.append((channel, callback))

    async def execute(self, query: str, channel: str, payload: str) -> None:
        self.executed.append((query, channel, payload))


async def test_postgres_notify_invalidation_bus_publishes_and_manages_listener() -> None:
    connection = FakeConnection()
    bus = PostgresNotifyInvalidationBus(connection, channel="invalidations", node_name="node")

    await bus.start(remove_local=lambda key, scope: None, clear_local=lambda scope: None)
    await bus.invalidate("user:1", scope="users")
    await bus.clear(scope="users")
    await bus.stop()

    assert connection.listeners == [("invalidations", bus._handle_notification)]
    assert connection.removed_listeners == [("invalidations", bus._handle_notification)]
    assert connection.executed[0][0] == "select pg_notify($1, $2)"
    assert connection.executed[0][1] == "invalidations"
    assert json.loads(connection.executed[0][2]) == {
        "action": "remove",
        "source_id": bus._source_id,
        "key": "user:1",
        "scope": "users",
    }
    assert json.loads(connection.executed[1][2]) == {
        "action": "clear",
        "source_id": bus._source_id,
        "scope": "users",
    }


async def test_postgres_notify_invalidation_bus_applies_remote_messages_and_ignores_self() -> None:
    connection = FakeConnection()
    bus = PostgresNotifyInvalidationBus(connection, channel="invalidations", node_name="node")
    removed: list[tuple[str, str]] = []
    cleared: list[str | None] = []

    await bus.start(
        remove_local=lambda key, scope: removed.append((key, scope)),
        clear_local=cleared.append,
    )
    bus._handle_notification(
        None,
        1,
        "invalidations",
        '{"action":"remove","source_id":"remote","key":"user:1","scope":"users"}',
    )
    bus._handle_notification(
        None,
        1,
        "invalidations",
        '{"action":"clear","source_id":"remote","scope":"users"}',
    )
    bus._handle_notification(
        None,
        1,
        "invalidations",
        json.dumps(
            {
                "action": "remove",
                "source_id": bus._source_id,
                "key": "self",
                "scope": "users",
            }
        ),
    )
    bus._handle_notification(None, 1, "invalidations", "not-json")

    assert removed == [("user:1", "users")]
    assert cleared == ["users"]
