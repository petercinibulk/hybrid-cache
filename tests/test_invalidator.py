from __future__ import annotations

from hybrid_cache import BackplaneInvalidator, BackplaneMessage
from hybrid_cache.backplane import BackplaneHandler


class FakeBackplane:
    def __init__(self) -> None:
        self.handler: BackplaneHandler | None = None
        self.published: list[BackplaneMessage] = []

    async def start(self, handler: BackplaneHandler) -> None:
        self.handler = handler

    async def stop(self) -> None:
        self.handler = None

    async def publish(self, message: BackplaneMessage) -> None:
        self.published.append(message)

    async def emit(self, message: BackplaneMessage) -> None:
        if self.handler is not None:
            await self.handler(message)


async def test_backplane_invalidator_publishes_and_applies_messages() -> None:
    backplane = FakeBackplane()
    invalidator = BackplaneInvalidator(backplane)
    removed: list[str] = []
    clear_count = 0

    def clear_local() -> None:
        nonlocal clear_count
        clear_count += 1

    await invalidator.start(remove_local=removed.append, clear_local=clear_local)
    await invalidator.invalidate("user:1")
    await invalidator.clear()

    assert backplane.published == [
        BackplaneMessage.remove("user:1"),
        BackplaneMessage.clear(),
    ]

    await backplane.emit(BackplaneMessage.remove("user:2"))
    await backplane.emit(BackplaneMessage.clear())

    assert removed == ["user:2"]
    assert clear_count == 1
