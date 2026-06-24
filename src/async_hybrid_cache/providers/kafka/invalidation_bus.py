from __future__ import annotations

import asyncio
import json
import socket
import uuid
from collections.abc import Sequence
from contextlib import suppress
from typing import Any

from async_hybrid_cache.invalidation import (
    ClearLocal,
    InvalidationMessage,
    RemoveLocal,
)


class KafkaInvalidationBus:
    """Invalidation bus backed by a Kafka topic."""

    def __init__(
        self,
        *,
        bootstrap_servers: str | Sequence[str],
        topic: str = "async-hybrid-cache-invalidations",
        node_name: str | None = None,
        group_id: str | None = None,
    ) -> None:
        """Create a Kafka invalidation bus.

        By default, each node gets a unique consumer group so every node receives
        every invalidation. Supplying the same `group_id` for multiple nodes will
        load-balance messages and is usually wrong for cache invalidation.
        """

        self._bootstrap_servers = bootstrap_servers
        self._topic = topic
        self._source_id = str(uuid.uuid4())
        self._node_name = node_name or f"{socket.gethostname()}-{self._source_id}"
        self._group_id = group_id or f"async-hybrid-cache-node:{self._node_name}"
        self._remove_local: RemoveLocal | None = None
        self._clear_local: ClearLocal | None = None
        self._producer: Any | None = None
        self._consumer: Any | None = None
        self._listener_task: asyncio.Task[None] | None = None

    async def start(
        self,
        *,
        remove_local: RemoveLocal,
        clear_local: ClearLocal,
    ) -> None:
        """Start the Kafka producer, consumer, and listener task."""

        if self._listener_task is not None:
            return

        try:
            from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
        except ImportError as ex:  # pragma: no cover - exercised only without optional deps
            msg = "Install async-hybrid-cache with the kafka dependency group to use Kafka."
            raise RuntimeError(msg) from ex

        self._remove_local = remove_local
        self._clear_local = clear_local
        self._producer = AIOKafkaProducer(bootstrap_servers=self._bootstrap_servers)
        self._consumer = AIOKafkaConsumer(
            self._topic,
            bootstrap_servers=self._bootstrap_servers,
            group_id=self._group_id,
            auto_offset_reset="latest",
        )
        await self._producer.start()
        await self._consumer.start()
        self._listener_task = asyncio.create_task(self._listen())

    async def stop(self) -> None:
        """Stop the listener task and close Kafka clients."""

        if self._listener_task is not None:
            self._listener_task.cancel()

            with suppress(asyncio.CancelledError):
                await self._listener_task

        if self._consumer is not None:
            await self._consumer.stop()

        if self._producer is not None:
            await self._producer.stop()

        self._listener_task = None
        self._consumer = None
        self._producer = None
        self._remove_local = None
        self._clear_local = None

    async def invalidate(self, key: str, *, scope: str) -> None:
        """Publish a scoped key-removal message to the Kafka topic."""

        await self._publish(InvalidationMessage.remove(key, scope=scope))

    async def clear(self, *, scope: str | None = None) -> None:
        """Publish a clear message to the Kafka topic."""

        await self._publish(InvalidationMessage.clear(scope=scope))

    async def _publish(self, message: InvalidationMessage) -> None:
        if self._producer is None:
            msg = "KafkaInvalidationBus must be started before publishing."
            raise RuntimeError(msg)

        await self._producer.send_and_wait(
            self._topic,
            self._encode_message(message),
        )

    async def _listen(self) -> None:
        consumer = self._consumer
        if consumer is None:
            return

        async for record in consumer:
            self._apply_payload(record.value)

    def _apply_payload(self, payload: bytes | str) -> None:
        message = self._decode_message(payload)

        if message is not None:
            self._apply_message(message)

    def _apply_message(self, message: InvalidationMessage) -> None:
        if message.action == "remove" and message.key is not None and message.scope is not None:
            remove_local = self._remove_local
            if remove_local is not None:
                remove_local(message.key, message.scope)
            return

        if message.action == "clear":
            clear_local = self._clear_local
            if clear_local is not None:
                clear_local(message.scope)

    def _encode_message(self, message: InvalidationMessage) -> bytes:
        payload: dict[str, str] = {
            "action": message.action,
            "source_id": self._source_id,
        }

        if message.key is not None:
            payload["key"] = message.key

        if message.scope is not None:
            payload["scope"] = message.scope

        return json.dumps(payload, separators=(",", ":")).encode("utf-8")

    def _decode_message(self, payload: bytes | str) -> InvalidationMessage | None:
        if isinstance(payload, bytes):
            payload = payload.decode("utf-8")

        try:
            data = json.loads(payload)
        except (TypeError, ValueError, UnicodeDecodeError):
            return None

        if not isinstance(data, dict) or data.get("source_id") == self._source_id:
            return None

        scope = data.get("scope")

        if data.get("action") == "remove" and isinstance(data.get("key"), str):
            if isinstance(scope, str):
                return InvalidationMessage.remove(data["key"], scope=scope)
            return None

        if data.get("action") == "clear":
            return InvalidationMessage.clear(scope=scope if isinstance(scope, str) else None)

        return None
