from __future__ import annotations

import json
import sys
import types
from typing import Any

import pytest

from async_hybrid_cache import RabbitMQInvalidationBus


class FakeMessage:
    def __init__(self, *, body: bytes, content_type: str | None = None) -> None:
        self.body = body
        self.content_type = content_type


class FakeIncomingMessage:
    def __init__(self, body: bytes) -> None:
        self.body = body
        self.processed = False

    def process(self) -> FakeIncomingMessage:
        return self

    async def __aenter__(self) -> None:
        self.processed = True

    async def __aexit__(self, *args: object) -> None:
        return None


class FakeExchange:
    def __init__(self) -> None:
        self.published: list[tuple[FakeMessage, str]] = []

    async def publish(self, message: FakeMessage, *, routing_key: str) -> None:
        self.published.append((message, routing_key))


class FakeQueue:
    def __init__(self) -> None:
        self.bound_exchange: FakeExchange | None = None
        self.consumer: Any | None = None
        self.cancelled: list[str] = []

    async def bind(self, exchange: FakeExchange) -> None:
        self.bound_exchange = exchange

    async def consume(self, callback: Any) -> str:
        self.consumer = callback
        return "consumer-tag"

    async def cancel(self, consumer_tag: str) -> None:
        self.cancelled.append(consumer_tag)


class FakeChannel:
    def __init__(self) -> None:
        self.exchange = FakeExchange()
        self.queue = FakeQueue()
        self.declared_exchange: tuple[str, str] | None = None
        self.declared_queue: dict[str, bool] | None = None
        self.closed = False

    async def declare_exchange(self, name: str, exchange_type: str) -> FakeExchange:
        self.declared_exchange = (name, exchange_type)
        return self.exchange

    async def declare_queue(self, *, exclusive: bool, auto_delete: bool) -> FakeQueue:
        self.declared_queue = {"exclusive": exclusive, "auto_delete": auto_delete}
        return self.queue

    async def close(self) -> None:
        self.closed = True


class FakeConnection:
    def __init__(self) -> None:
        self.channel_instance = FakeChannel()

    async def channel(self) -> FakeChannel:
        return self.channel_instance


@pytest.fixture
def fake_aio_pika(monkeypatch: pytest.MonkeyPatch) -> None:
    module = types.SimpleNamespace(
        ExchangeType=types.SimpleNamespace(FANOUT="fanout"),
        Message=FakeMessage,
    )
    monkeypatch.setitem(sys.modules, "aio_pika", module)


async def test_rabbitmq_invalidation_bus_declares_fanout_queue_and_publishes(
    fake_aio_pika: None,
) -> None:
    connection = FakeConnection()
    bus = RabbitMQInvalidationBus(connection, exchange_name="invalidations", node_name="node")

    await bus.start(remove_local=lambda key, scope: None, clear_local=lambda scope: None)
    await bus.invalidate("user:1", scope="users")
    await bus.clear(scope="users")

    channel = connection.channel_instance
    assert channel.declared_exchange == ("invalidations", "fanout")
    assert channel.declared_queue == {"exclusive": True, "auto_delete": True}
    assert channel.queue.bound_exchange is channel.exchange
    assert channel.queue.consumer is not None

    remove_message, remove_routing_key = channel.exchange.published[0]
    clear_message, clear_routing_key = channel.exchange.published[1]
    assert remove_routing_key == ""
    assert clear_routing_key == ""
    assert remove_message.content_type == "application/json"
    assert json.loads(remove_message.body) == {
        "action": "remove",
        "source_id": bus._source_id,
        "key": "user:1",
        "scope": "users",
    }
    assert json.loads(clear_message.body) == {
        "action": "clear",
        "source_id": bus._source_id,
        "scope": "users",
    }


async def test_rabbitmq_invalidation_bus_applies_remote_messages_and_ignores_self(
    fake_aio_pika: None,
) -> None:
    connection = FakeConnection()
    bus = RabbitMQInvalidationBus(connection, node_name="node")
    removed: list[tuple[str, str]] = []
    cleared: list[str | None] = []

    await bus.start(
        remove_local=lambda key, scope: removed.append((key, scope)),
        clear_local=cleared.append,
    )
    consumer = connection.channel_instance.queue.consumer
    assert consumer is not None

    await consumer(
        FakeIncomingMessage(
            b'{"action":"remove","source_id":"remote","key":"user:1","scope":"users"}'
        )
    )
    await consumer(FakeIncomingMessage(b'{"action":"clear","source_id":"remote","scope":"users"}'))
    await consumer(
        FakeIncomingMessage(
            json.dumps(
                {
                    "action": "remove",
                    "source_id": bus._source_id,
                    "key": "self",
                    "scope": "users",
                }
            ).encode()
        )
    )
    await consumer(FakeIncomingMessage(b"not-json"))

    assert removed == [("user:1", "users")]
    assert cleared == ["users"]

    await bus.stop()

    assert connection.channel_instance.queue.cancelled == ["consumer-tag"]
    assert connection.channel_instance.closed
