from __future__ import annotations

import asyncio
import json
import sys
import types
from typing import ClassVar

import pytest

from async_hybrid_cache import KafkaInvalidationBus


class FakeRecord:
    def __init__(self, value: bytes) -> None:
        self.value = value


class FakeProducer:
    instances: ClassVar[list[FakeProducer]] = []

    def __init__(self, **kwargs: object) -> None:
        self.kwargs = kwargs
        self.started = False
        self.stopped = False
        self.sent: list[tuple[str, bytes]] = []
        FakeProducer.instances.append(self)

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True

    async def send_and_wait(self, topic: str, value: bytes) -> None:
        self.sent.append((topic, value))


class FakeConsumer:
    instances: ClassVar[list[FakeConsumer]] = []

    def __init__(self, *topics: str, **kwargs: object) -> None:
        self.topics = topics
        self.kwargs = kwargs
        self.started = False
        self.stopped = False
        self.records: asyncio.Queue[FakeRecord] = asyncio.Queue()
        FakeConsumer.instances.append(self)

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True

    def __aiter__(self) -> FakeConsumer:
        return self

    async def __anext__(self) -> FakeRecord:
        return await self.records.get()


@pytest.fixture
def fake_aiokafka(monkeypatch: pytest.MonkeyPatch) -> None:
    FakeProducer.instances.clear()
    FakeConsumer.instances.clear()
    module = types.SimpleNamespace(
        AIOKafkaProducer=FakeProducer,
        AIOKafkaConsumer=FakeConsumer,
    )
    monkeypatch.setitem(sys.modules, "aiokafka", module)


async def test_kafka_invalidation_bus_starts_clients_and_publishes(
    fake_aiokafka: None,
) -> None:
    bus = KafkaInvalidationBus(
        bootstrap_servers="localhost:9092",
        topic="invalidations",
        node_name="node",
    )

    await bus.start(remove_local=lambda key, scope: None, clear_local=lambda scope: None)
    await bus.invalidate("user:1", scope="users")
    await bus.clear(scope="users")
    await bus.stop()

    producer = FakeProducer.instances[0]
    consumer = FakeConsumer.instances[0]
    assert producer.kwargs == {"bootstrap_servers": "localhost:9092"}
    assert consumer.topics == ("invalidations",)
    assert consumer.kwargs == {
        "bootstrap_servers": "localhost:9092",
        "group_id": "async-hybrid-cache-node:node",
        "auto_offset_reset": "latest",
    }
    assert producer.started
    assert producer.stopped
    assert consumer.started
    assert consumer.stopped
    assert json.loads(producer.sent[0][1]) == {
        "action": "remove",
        "source_id": bus._source_id,
        "key": "user:1",
        "scope": "users",
    }
    assert json.loads(producer.sent[1][1]) == {
        "action": "clear",
        "source_id": bus._source_id,
        "scope": "users",
    }


async def test_kafka_invalidation_bus_uses_explicit_group_id(fake_aiokafka: None) -> None:
    bus = KafkaInvalidationBus(
        bootstrap_servers=["localhost:9092"],
        group_id="shared-group",
    )

    await bus.start(remove_local=lambda key, scope: None, clear_local=lambda scope: None)
    await bus.stop()

    assert FakeConsumer.instances[0].kwargs["group_id"] == "shared-group"


async def test_kafka_invalidation_bus_applies_remote_messages_and_ignores_malformed(
    fake_aiokafka: None,
) -> None:
    bus = KafkaInvalidationBus(bootstrap_servers="localhost:9092", topic="invalidations")
    removed: list[tuple[str, str]] = []
    cleared: list[str | None] = []

    await bus.start(
        remove_local=lambda key, scope: removed.append((key, scope)),
        clear_local=cleared.append,
    )
    consumer = FakeConsumer.instances[0]
    await consumer.records.put(
        FakeRecord(b'{"action":"remove","source_id":"remote","key":"user:1","scope":"users"}')
    )
    await consumer.records.put(
        FakeRecord(b'{"action":"clear","source_id":"remote","scope":"users"}')
    )
    await consumer.records.put(
        FakeRecord(
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
    await consumer.records.put(FakeRecord(b"not-json"))
    await asyncio.sleep(0)

    assert removed == [("user:1", "users")]
    assert cleared == ["users"]

    await bus.stop()
