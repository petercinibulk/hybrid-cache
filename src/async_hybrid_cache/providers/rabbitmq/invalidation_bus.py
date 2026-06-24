from __future__ import annotations

import json
import socket
import uuid
from contextlib import suppress
from typing import Any

from async_hybrid_cache.invalidation import (
    ClearLocal,
    InvalidationMessage,
    RemoveLocal,
)


class RabbitMQInvalidationBus:
    """Invalidation bus backed by a RabbitMQ fanout exchange."""

    def __init__(
        self,
        connection: Any,
        *,
        exchange_name: str = "async-hybrid-cache-invalidations",
        node_name: str | None = None,
    ) -> None:
        """Create a RabbitMQ invalidation bus using an existing connection."""

        self._connection = connection
        self._exchange_name = exchange_name
        self._source_id = str(uuid.uuid4())
        self._node_name = node_name or f"{socket.gethostname()}-{self._source_id}"
        self._remove_local: RemoveLocal | None = None
        self._clear_local: ClearLocal | None = None
        self._channel: Any | None = None
        self._exchange: Any | None = None
        self._queue: Any | None = None
        self._consumer_tag: str | None = None

    async def start(
        self,
        *,
        remove_local: RemoveLocal,
        clear_local: ClearLocal,
    ) -> None:
        """Declare the fanout exchange, bind an exclusive queue, and consume."""

        if self._channel is not None:
            return

        try:
            from aio_pika import ExchangeType
        except ImportError as ex:  # pragma: no cover - exercised only without optional deps
            msg = "Install async-hybrid-cache with the rabbitmq dependency group to use RabbitMQ."
            raise RuntimeError(msg) from ex

        self._remove_local = remove_local
        self._clear_local = clear_local
        self._channel = await self._connection.channel()
        self._exchange = await self._channel.declare_exchange(
            self._exchange_name,
            ExchangeType.FANOUT,
        )
        self._queue = await self._channel.declare_queue(
            exclusive=True,
            auto_delete=True,
        )
        await self._queue.bind(self._exchange)
        self._consumer_tag = await self._queue.consume(self._handle_incoming_message)

    async def stop(self) -> None:
        """Cancel consumption and close the created channel."""

        if self._queue is not None and self._consumer_tag is not None:
            with suppress(Exception):
                await self._queue.cancel(self._consumer_tag)

        if self._channel is not None:
            with suppress(Exception):
                await self._channel.close()

        self._channel = None
        self._exchange = None
        self._queue = None
        self._consumer_tag = None
        self._remove_local = None
        self._clear_local = None

    async def invalidate(self, key: str, *, scope: str) -> None:
        """Publish a scoped key-removal message to the fanout exchange."""

        await self._publish(InvalidationMessage.remove(key, scope=scope))

    async def clear(self, *, scope: str | None = None) -> None:
        """Publish a clear message to the fanout exchange."""

        await self._publish(InvalidationMessage.clear(scope=scope))

    async def _publish(self, message: InvalidationMessage) -> None:
        if self._exchange is None:
            msg = "RabbitMQInvalidationBus must be started before publishing."
            raise RuntimeError(msg)

        try:
            from aio_pika import Message
        except ImportError as ex:  # pragma: no cover - exercised only without optional deps
            msg = "Install async-hybrid-cache with the rabbitmq dependency group to use RabbitMQ."
            raise RuntimeError(msg) from ex

        await self._exchange.publish(
            Message(
                body=self._encode_message(message),
                content_type="application/json",
            ),
            routing_key="",
        )

    async def _handle_incoming_message(self, incoming_message: Any) -> None:
        async with incoming_message.process():
            self._apply_payload(incoming_message.body)

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
