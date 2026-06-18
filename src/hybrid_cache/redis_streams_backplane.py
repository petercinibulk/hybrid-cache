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

from hybrid_cache.backplane import BackplaneAction, BackplaneHandler, BackplaneMessage

type RedisFields = Mapping[bytes | str, bytes | str]
type RedisMessage = tuple[bytes | str, RedisFields]
type RedisStreamResponse = list[tuple[bytes | str, list[RedisMessage]]]


class RedisStreamsBackplane:
    def __init__(
        self,
        redis: Redis,
        *,
        stream_name: str = "hybrid-cache:invalidations",
        node_name: str | None = None,
        max_length: int = 10_000,
    ) -> None:
        self._redis = redis
        self._stream_name = stream_name
        self._source_id = str(uuid.uuid4())
        self._node_name = node_name or f"{socket.gethostname()}-{self._source_id}"
        self._group_name = f"hybrid-cache-node:{self._node_name}"
        self._consumer_name = self._node_name
        self._max_length = max_length
        self._handler: BackplaneHandler | None = None
        self._listener_task: asyncio.Task[None] | None = None
        self._stopped = asyncio.Event()

    async def start(self, handler: BackplaneHandler) -> None:
        if self._listener_task is not None:
            return

        self._handler = handler
        self._stopped.clear()
        await self._ensure_group()
        self._listener_task = asyncio.create_task(self._listen())

    async def stop(self) -> None:
        self._stopped.set()

        if self._listener_task is None:
            return

        self._listener_task.cancel()

        with suppress(asyncio.CancelledError):
            await self._listener_task

        self._listener_task = None
        self._handler = None

    async def publish(self, message: BackplaneMessage) -> None:
        fields: dict[FieldT, EncodableT] = {
            "action": message.action,
            "source_id": self._source_id,
        }

        if message.key is not None:
            fields["key"] = message.key

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
            handler = self._handler
            if handler is not None:
                await handler(self._to_message(fields))

        await self._redis.xack(
            self._stream_name,
            self._group_name,
            message_id,
        )

    def _to_message(self, fields: RedisFields) -> BackplaneMessage:
        action = cast(BackplaneAction, self._get_field(fields, "action"))

        if action == "remove":
            return BackplaneMessage.remove(self._get_field(fields, "key"))

        return BackplaneMessage.clear()

    def _get_field(self, fields: RedisFields, key: str) -> str:
        value = fields.get(key)
        if value is None:
            value = fields[key.encode("utf-8")]

        return value.decode("utf-8") if isinstance(value, bytes) else value
