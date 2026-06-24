from __future__ import annotations

import asyncio
import socket
import uuid
from collections.abc import Mapping
from contextlib import suppress
from typing import cast

from redis.asyncio import Redis
from redis.exceptions import ResponseError
from redis.typing import EncodableT, FieldT

from async_hybrid_cache.invalidation import (
    ClearLocal,
    InvalidationAction,
    InvalidationMessage,
    RemoveLocal,
)

type RedisFields = Mapping[bytes | str, bytes | str]
type RedisMessage = tuple[bytes | str, RedisFields]
type RedisStreamResponse = list[tuple[bytes | str, list[RedisMessage]]]


class RedisStreamsInvalidationBus:
    """Invalidation bus backed by Redis Streams consumer groups."""

    def __init__(
        self,
        redis: Redis,
        *,
        stream_name: str = "async-hybrid-cache:invalidations",
        node_name: str | None = None,
        max_length: int = 10_000,
    ) -> None:
        """Create a Redis Streams invalidation bus."""

        self._redis = redis
        self._stream_name = stream_name
        self._source_id = str(uuid.uuid4())
        self._node_name = node_name or f"{socket.gethostname()}-{self._source_id}"
        self._group_name = f"async-hybrid-cache-node:{self._node_name}"
        self._consumer_name = self._node_name
        self._max_length = max_length
        self._remove_local: RemoveLocal | None = None
        self._clear_local: ClearLocal | None = None
        self._listener_task: asyncio.Task[None] | None = None
        self._stopped = asyncio.Event()

    async def start(
        self,
        *,
        remove_local: RemoveLocal,
        clear_local: ClearLocal,
    ) -> None:
        """Create the consumer group if needed and start the listener task."""

        if self._listener_task is not None:
            return

        self._remove_local = remove_local
        self._clear_local = clear_local
        self._stopped.clear()
        await self._ensure_group()
        self._listener_task = asyncio.create_task(self._listen())

    async def stop(self) -> None:
        """Cancel the listener task and clear local callbacks."""

        self._stopped.set()

        if self._listener_task is None:
            return

        self._listener_task.cancel()

        with suppress(asyncio.CancelledError):
            await self._listener_task

        self._listener_task = None
        self._remove_local = None
        self._clear_local = None

    async def invalidate(self, key: str, *, scope: str) -> None:
        """Publish a scoped key-removal message to the stream."""

        await self._publish(InvalidationMessage.remove(key, scope=scope))

    async def clear(self, *, scope: str | None = None) -> None:
        """Publish a clear message to the stream."""

        await self._publish(InvalidationMessage.clear(scope=scope))

    async def _publish(self, message: InvalidationMessage) -> None:
        fields: dict[FieldT, EncodableT] = {
            "action": message.action,
            "source_id": self._source_id,
        }

        if message.key is not None:
            fields["key"] = message.key

        if message.scope is not None:
            fields["scope"] = message.scope

        await self._redis.xadd(
            self._stream_name,
            fields,
            maxlen=self._max_length,
            approximate=True,
        )

    async def _ensure_group(self) -> None:
        try:
            await self._redis.xgroup_create(
                self._stream_name,
                self._group_name,
                id="$",
                mkstream=True,
            )
        except ResponseError as ex:
            if "BUSYGROUP" not in str(ex):
                raise

    async def _listen(self) -> None:
        while not self._stopped.is_set():
            response = cast(
                RedisStreamResponse,
                await self._redis.xreadgroup(
                    groupname=self._group_name,
                    consumername=self._consumer_name,
                    streams={self._stream_name: ">"},
                    count=25,
                    block=5_000,
                ),
            )

            for _, messages in response:
                for message_id, fields in messages:
                    await self._process_message(message_id, fields)

    async def _process_message(
        self,
        message_id: bytes | str,
        fields: RedisFields,
    ) -> None:
        source_id = self._get_field(fields, "source_id")

        if source_id != self._source_id:
            self._apply_message(self._to_message(fields))

        await self._redis.xack(
            self._stream_name,
            self._group_name,
            message_id,
        )

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

    def _to_message(self, fields: RedisFields) -> InvalidationMessage:
        action = cast(InvalidationAction, self._get_field(fields, "action"))
        scope = self._get_optional_field(fields, "scope")

        if action == "remove" and scope is not None:
            return InvalidationMessage.remove(self._get_field(fields, "key"), scope=scope)

        return InvalidationMessage.clear(scope=scope)

    def _get_field(self, fields: RedisFields, key: str) -> str:
        value = fields.get(key)
        if value is None:
            value = fields[key.encode("utf-8")]

        return value.decode("utf-8") if isinstance(value, bytes) else value

    def _get_optional_field(self, fields: RedisFields, key: str) -> str | None:
        value = fields.get(key)
        if value is None:
            value = fields.get(key.encode("utf-8"))
        if value is None:
            return None

        return value.decode("utf-8") if isinstance(value, bytes) else value
