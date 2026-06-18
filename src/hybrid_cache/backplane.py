from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Literal, Protocol

type BackplaneAction = Literal["remove", "clear"]


@dataclass(frozen=True, slots=True)
class BackplaneMessage:
    action: BackplaneAction
    key: str | None = None

    @classmethod
    def remove(cls, key: str) -> BackplaneMessage:
        return cls(action="remove", key=key)

    @classmethod
    def clear(cls) -> BackplaneMessage:
        return cls(action="clear")


type BackplaneHandler = Callable[[BackplaneMessage], Awaitable[None]]


class Backplane(Protocol):
    async def start(self, handler: BackplaneHandler) -> None: ...

    async def stop(self) -> None: ...

    async def publish(self, message: BackplaneMessage) -> None: ...
