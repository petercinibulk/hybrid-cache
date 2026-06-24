from __future__ import annotations

from async_hybrid_cache.invalidation import (
    InvalidationHandler,
    InvalidationMessage,
    TransportInvalidationBus,
)


class FakeTransport:
    def __init__(self) -> None:
        self.handler: InvalidationHandler | None = None
        self.published: list[InvalidationMessage] = []

    async def start(self, handler: InvalidationHandler) -> None:
        self.handler = handler

    async def stop(self) -> None:
        self.handler = None

    async def publish(self, message: InvalidationMessage) -> None:
        self.published.append(message)

    async def emit(self, message: InvalidationMessage) -> None:
        if self.handler is not None:
            await self.handler(message)


async def test_transport_invalidation_bus_publishes_and_applies_messages() -> None:
    transport = FakeTransport()
    bus = TransportInvalidationBus(transport)
    removed: list[tuple[str, str]] = []
    cleared: list[str | None] = []

    await bus.start(
        remove_local=lambda key, scope: removed.append((key, scope)),
        clear_local=cleared.append,
    )
    await bus.invalidate("user:1", scope="users")
    await bus.clear(scope="users")

    assert transport.published == [
        InvalidationMessage.remove("user:1", scope="users"),
        InvalidationMessage.clear(scope="users"),
    ]

    await transport.emit(InvalidationMessage.remove("user:2", scope="users"))
    await transport.emit(InvalidationMessage.clear())

    assert removed == [("user:2", "users")]
    assert cleared == [None]


async def test_transport_invalidation_bus_uses_transport_without_cache_storage() -> None:
    transport = FakeTransport()
    bus = TransportInvalidationBus(transport)
    removed: list[tuple[str, str]] = []

    await bus.start(
        remove_local=lambda key, scope: removed.append((key, scope)),
        clear_local=lambda scope: None,
    )
    await bus.invalidate("user:1", scope="users")
    await transport.emit(InvalidationMessage.remove("user:2", scope="users"))

    assert transport.published == [InvalidationMessage.remove("user:1", scope="users")]
    assert removed == [("user:2", "users")]
