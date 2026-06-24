from __future__ import annotations

import json
import socket
import uuid
from typing import Any

from async_hybrid_cache.invalidation import (
    ClearLocal,
    InvalidationMessage,
    RemoveLocal,
)


class PostgresNotifyInvalidationBus:
    """Invalidation bus backed by PostgreSQL LISTEN/NOTIFY."""

    def __init__(
        self,
        connection: Any,
        *,
        channel: str = "async_hybrid_cache_invalidations",
        node_name: str | None = None,
    ) -> None:
        """Create a Postgres notification invalidation bus."""

        self._connection = connection
        self._channel = channel
        self._source_id = str(uuid.uuid4())
        self._node_name = node_name or f"{socket.gethostname()}-{self._source_id}"
        self._remove_local: RemoveLocal | None = None
        self._clear_local: ClearLocal | None = None
        self._started = False

    async def start(
        self,
        *,
        remove_local: RemoveLocal,
        clear_local: ClearLocal,
    ) -> None:
        """Register a notification listener on the configured channel."""

        if self._started:
            return

        self._remove_local = remove_local
        self._clear_local = clear_local
        await self._connection.add_listener(self._channel, self._handle_notification)
        self._started = True

    async def stop(self) -> None:
        """Remove the notification listener and clear local callbacks."""

        if self._started:
            await self._connection.remove_listener(self._channel, self._handle_notification)

        self._started = False
        self._remove_local = None
        self._clear_local = None

    async def invalidate(self, key: str, *, scope: str) -> None:
        """Publish a scoped key-removal notification."""

        await self._publish(InvalidationMessage.remove(key, scope=scope))

    async def clear(self, *, scope: str | None = None) -> None:
        """Publish a clear notification."""

        await self._publish(InvalidationMessage.clear(scope=scope))

    async def _publish(self, message: InvalidationMessage) -> None:
        await self._connection.execute(
            "select pg_notify($1, $2)",
            self._channel,
            self._encode_message(message),
        )

    def _handle_notification(
        self,
        connection: Any,
        pid: int,
        channel: str,
        payload: str,
    ) -> None:
        del connection, pid, channel
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

    def _encode_message(self, message: InvalidationMessage) -> str:
        payload: dict[str, str] = {
            "action": message.action,
            "source_id": self._source_id,
        }

        if message.key is not None:
            payload["key"] = message.key

        if message.scope is not None:
            payload["scope"] = message.scope

        return json.dumps(payload, separators=(",", ":"))

    def _decode_message(self, payload: str) -> InvalidationMessage | None:
        try:
            data = json.loads(payload)
        except (TypeError, ValueError):
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
