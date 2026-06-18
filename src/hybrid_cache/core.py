from __future__ import annotations

import asyncio
import pickle
import random
import socket
import time
import uuid
from collections.abc import Awaitable, Callable, Mapping
from contextlib import suppress
from dataclasses import dataclass
from functools import update_wrapper
from typing import Generic, ParamSpec, Protocol, TypeVar, cast, runtime_checkable

from redis.asyncio import Redis

P = ParamSpec("P")
T = TypeVar("T")

RedisFields = Mapping[bytes | str, bytes | str]
RedisMessage = tuple[bytes | str, RedisFields]
RedisStreamResponse = list[tuple[bytes | str, list[RedisMessage]]]


@runtime_checkable
class Serializer(Protocol):
    def dumps(self, value: object) -> bytes: ...
    def loads(self, value: bytes) -> object: ...


class PickleSerializer:
    def dumps(self, value: object) -> bytes:
        return pickle.dumps(value)

    def loads(self, value: bytes) -> object:
        return pickle.loads(value)


@runtime_checkable
class DistributedCache(Protocol):
    async def get(self, key: str) -> object | None: ...
    async def set(self, key: str, value: object, ttl_seconds: float) -> None: ...
    async def delete(self, key: str) -> None: ...


@dataclass(frozen=True, slots=True)
class CacheOptions:
    ttl_seconds: float = 60
    fail_safe_seconds: float = 300
    hard_timeout_seconds: float = 5
    jitter_seconds: float = 0


@dataclass(slots=True)
class CacheEntry:
    value: object
    expires_at: float
    fail_safe_until: float

    @property
    def is_fresh(self) -> bool:
        return time.monotonic() < self.expires_at

    @property
    def is_fail_safe_available(self) -> bool:
        return time.monotonic() < self.fail_safe_until


class RedisDistributedCache:
    def __init__(
        self,
        redis: Redis,
        *,
        prefix: str = "hybrid-cache:",
        serializer: Serializer | None = None,
    ) -> None:
        self._redis = redis
        self._prefix = prefix
        self._serializer = serializer or PickleSerializer()

    async def get(self, key: str) -> object | None:
        value = await self._redis.get(self._key(key))

        if value is None:
            return None

        if isinstance(value, str):
            value = value.encode("utf-8")

        return self._serializer.loads(value)

    async def set(self, key: str, value: object, ttl_seconds: float) -> None:
        await self._redis.set(
            self._key(key),
            self._serializer.dumps(value),
            ex=max(1, int(ttl_seconds)),
        )

    async def delete(self, key: str) -> None:
        await self._redis.delete(self._key(key))

    def _key(self, key: str) -> str:
        return f"{self._prefix}{key}"


class HybridCache:
    def __init__(
        self,
        *,
        distributed_cache: DistributedCache | None = None,
        redis: Redis | None = None,
        options: CacheOptions | None = None,
        key_prefix: str = "hybrid-cache:",
        serializer: Serializer | None = None,
        invalidation_stream: str = "hybrid-cache:invalidations",
        node_name: str | None = None,
    ) -> None:
        self._memory: dict[str, CacheEntry] = {}
        self._locks: dict[str, asyncio.Lock] = {}
        self._options = options or CacheOptions()

        self._redis = redis
        self._distributed_cache = distributed_cache
        if self._distributed_cache is None and redis is not None:
            self._distributed_cache = RedisDistributedCache(
                redis,
                prefix=key_prefix,
                serializer=serializer,
            )

        self._invalidation_stream = invalidation_stream
        self._node_id = str(uuid.uuid4())
        self._node_name = node_name or f"{socket.gethostname()}-{self._node_id}"
        self._group_name = f"hybrid-cache-node:{self._node_name}"
        self._consumer_name = self._node_name
        self._listener_task: asyncio.Task[None] | None = None
        self._stopped = asyncio.Event()

    async def start(self) -> None:
        if self._redis is None or self._listener_task is not None:
            return

        self._stopped.clear()
        await self._ensure_invalidation_group()
        self._listener_task = asyncio.create_task(self._listen_for_invalidations())

    async def stop(self) -> None:
        self._stopped.set()

        if self._listener_task is None:
            return

        self._listener_task.cancel()

        with suppress(asyncio.CancelledError):
            await self._listener_task

        self._listener_task = None

    async def get_or_set(
        self,
        key: str,
        factory: Callable[[], Awaitable[T]],
        *,
        options: CacheOptions | None = None,
    ) -> T:
        opts = options or self._options

        entry = self._memory.get(key)
        if entry and entry.is_fresh:
            return cast(T, entry.value)

        lock = self._locks.setdefault(key, asyncio.Lock())

        async with lock:
            entry = self._memory.get(key)
            if entry and entry.is_fresh:
                return cast(T, entry.value)

            if self._distributed_cache is not None:
                cached_value = await self._distributed_cache.get(key)
                if cached_value is not None:
                    self._set_memory(key, cached_value, opts)
                    return cast(T, cached_value)

            stale = entry if entry and entry.is_fail_safe_available else None

            try:
                value = await asyncio.wait_for(
                    factory(),
                    timeout=opts.hard_timeout_seconds,
                )

                await self.set(key, value, options=opts, publish_invalidation=False)
                return value

            except Exception:
                if stale is not None:
                    return cast(T, stale.value)

                raise

    async def set(
        self,
        key: str,
        value: object,
        *,
        options: CacheOptions | None = None,
        publish_invalidation: bool = True,
    ) -> None:
        opts = options or self._options
        self._set_memory(key, value, opts)

        if self._distributed_cache is not None:
            await self._distributed_cache.set(
                key,
                value,
                ttl_seconds=self._ttl_with_jitter(opts),
            )

        if publish_invalidation:
            await self._publish_invalidation("remove", key=key)

    async def remove(self, key: str) -> None:
        self.remove_local(key)

        if self._distributed_cache is not None:
            await self._distributed_cache.delete(key)

        await self._publish_invalidation("remove", key=key)

    async def clear(self) -> None:
        self.clear_memory()
        await self._publish_invalidation("clear")

    def remove_local(self, key: str) -> None:
        self._memory.pop(key, None)

    def clear_memory(self) -> None:
        self._memory.clear()

    def _set_memory(self, key: str, value: object, opts: CacheOptions) -> None:
        ttl = self._ttl_with_jitter(opts)
        now = time.monotonic()

        self._memory[key] = CacheEntry(
            value=value,
            expires_at=now + ttl,
            fail_safe_until=now + ttl + opts.fail_safe_seconds,
        )

    def _ttl_with_jitter(self, opts: CacheOptions) -> float:
        if opts.jitter_seconds <= 0:
            return opts.ttl_seconds

        return opts.ttl_seconds + random.uniform(0, opts.jitter_seconds)

    async def _publish_invalidation(
        self,
        event_type: str,
        *,
        key: str | None = None,
    ) -> None:
        if self._redis is None:
            return

        fields = {
            "type": event_type,
            "node_id": self._node_id,
        }

        if key is not None:
            fields["key"] = key

        await self._redis.xadd(
            self._invalidation_stream,
            fields,
            maxlen=10_000,
            approximate=True,
        )

    async def _ensure_invalidation_group(self) -> None:
        if self._redis is None:
            return

        try:
            await self._redis.xgroup_create(
                self._invalidation_stream,
                self._group_name,
                id="$",
                mkstream=True,
            )
        except Exception as ex:
            if "BUSYGROUP" not in str(ex):
                raise

    async def _listen_for_invalidations(self) -> None:
        if self._redis is None:
            return

        while not self._stopped.is_set():
            response = cast(
                RedisStreamResponse,
                await self._redis.xreadgroup(
                    groupname=self._group_name,
                    consumername=self._consumer_name,
                    streams={self._invalidation_stream: ">"},
                    count=25,
                    block=5_000,
                ),
            )

            for _, messages in response:
                for message_id, fields in messages:
                    await self._process_invalidation(message_id, fields)

    async def _process_invalidation(
        self,
        message_id: bytes | str,
        fields: RedisFields,
    ) -> None:
        if self._redis is None:
            return

        try:
            self._handle_invalidation(fields)
            await self._redis.xack(
                self._invalidation_stream,
                self._group_name,
                message_id,
            )
        except Exception:
            return

    def _handle_invalidation(self, fields: RedisFields) -> None:
        event_type = self._get_field(fields, "type")
        node_id = self._get_field(fields, "node_id")

        if node_id == self._node_id:
            return

        if event_type == "remove":
            self.remove_local(self._get_field(fields, "key"))
        elif event_type == "clear":
            self.clear_memory()

    def _get_field(self, fields: RedisFields, key: str) -> str:
        value = fields.get(key)
        if value is None:
            value = fields[key.encode("utf-8")]

        return value.decode("utf-8") if isinstance(value, bytes) else value


class CachedFunction(Generic[P, T]):
    def __init__(
        self,
        *,
        cache: HybridCache,
        func: Callable[P, Awaitable[T]],
        key: str | Callable[P, str],
        options: CacheOptions | None,
    ) -> None:
        self._cache = cache
        self._func = func
        self._key = key
        self._options = options

        update_wrapper(self, func)

    async def __call__(self, *args: P.args, **kwargs: P.kwargs) -> T:
        cache_key = self.cache_key(*args, **kwargs)

        return await self._cache.get_or_set(
            cache_key,
            lambda: self._func(*args, **kwargs),
            options=self._options,
        )

    async def remove_cached(self, *args: P.args, **kwargs: P.kwargs) -> None:
        await self._cache.remove(self.cache_key(*args, **kwargs))

    def cache_key(self, *args: P.args, **kwargs: P.kwargs) -> str:
        if isinstance(self._key, str):
            return self._key

        return self._key(*args, **kwargs)


def cached(
    cache: HybridCache,
    key: str | Callable[P, str],
    *,
    options: CacheOptions | None = None,
) -> Callable[[Callable[P, Awaitable[T]]], CachedFunction[P, T]]:
    def decorator(func: Callable[P, Awaitable[T]]) -> CachedFunction[P, T]:
        return CachedFunction(
            cache=cache,
            func=func,
            key=key,
            options=options,
        )

    return decorator
