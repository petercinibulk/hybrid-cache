from __future__ import annotations

import pickle
from typing import Protocol


class Serializer(Protocol):
    def dumps(self, value: object) -> bytes: ...

    def loads(self, value: bytes) -> object: ...


class PickleSerializer:
    def dumps(self, value: object) -> bytes:
        return pickle.dumps(value)

    def loads(self, value: bytes) -> object:
        return pickle.loads(value)


class DistributedCache(Protocol):
    async def get(self, key: str) -> object | None: ...

    async def set(self, key: str, value: object, ttl_seconds: float) -> None: ...

    async def delete(self, key: str) -> None: ...
