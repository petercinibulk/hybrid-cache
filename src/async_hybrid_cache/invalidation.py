from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Literal, Protocol

type InvalidationAction = Literal["remove", "clear"]
type RemoveLocal = Callable[[str, str], None]
type ClearLocal = Callable[[str | None], None]


@dataclass(frozen=True, slots=True)
class InvalidationMessage:
    """Message sent between cache nodes to remove keys or clear local memory."""

    action: InvalidationAction
    key: str | None = None
    scope: str | None = None

    @classmethod
    def remove(cls, key: str, *, scope: str) -> InvalidationMessage:
        """Create a message that removes one scoped key from peer local caches."""

        return cls(action="remove", key=key, scope=scope)

    @classmethod
    def clear(cls, *, scope: str | None = None) -> InvalidationMessage:
        """Create a message that clears one scope or all peer local caches."""

        return cls(action="clear", scope=scope)


type InvalidationHandler = Callable[[InvalidationMessage], Awaitable[None]]


class InvalidationTransport(Protocol):
    """Low-level transport used by `TransportInvalidationBus`."""

    async def start(self, handler: InvalidationHandler) -> None: ...

    async def stop(self) -> None: ...

    async def publish(self, message: InvalidationMessage) -> None: ...


class InvalidationBus(Protocol):
    """Protocol for publishing and receiving cache invalidation events."""

    async def start(
        self,
        *,
        remove_local: RemoveLocal,
        clear_local: ClearLocal,
    ) -> None: ...

    async def stop(self) -> None: ...

    async def invalidate(self, key: str, *, scope: str) -> None: ...

    async def clear(self, *, scope: str | None = None) -> None: ...


class TransportInvalidationBus:
    """Adapt an `InvalidationTransport` into the `InvalidationBus` protocol."""

    def __init__(self, transport: InvalidationTransport) -> None:
        """Create an invalidation bus backed by a generic transport."""

        self._transport = transport
        self._remove_local: RemoveLocal | None = None
        self._clear_local: ClearLocal | None = None

    async def start(
        self,
        *,
        remove_local: RemoveLocal,
        clear_local: ClearLocal,
    ) -> None:
        """Start listening for remote invalidation messages."""

        self._remove_local = remove_local
        self._clear_local = clear_local
        await self._transport.start(self._handle_message)

    async def stop(self) -> None:
        """Stop listening and release local callbacks."""

        await self._transport.stop()
        self._remove_local = None
        self._clear_local = None

    async def invalidate(self, key: str, *, scope: str) -> None:
        """Publish a message instructing peers to remove one scoped key."""

        await self._transport.publish(InvalidationMessage.remove(key, scope=scope))

    async def clear(self, *, scope: str | None = None) -> None:
        """Publish a message instructing peers to clear one scope or all local memory."""

        await self._transport.publish(InvalidationMessage.clear(scope=scope))

    async def _handle_message(self, message: InvalidationMessage) -> None:
        remove_local = self._remove_local
        clear_local = self._clear_local

        if (
            message.action == "remove"
            and message.key is not None
            and message.scope is not None
        ):
            if remove_local is not None:
                remove_local(message.key, message.scope)
            return

        if message.action == "clear" and clear_local is not None:
            clear_local(message.scope)
