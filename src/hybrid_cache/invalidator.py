from __future__ import annotations

from collections.abc import Callable
from typing import Protocol

from hybrid_cache.backplane import Backplane, BackplaneMessage

type RemoveLocal = Callable[[str], None]
type ClearLocal = Callable[[], None]


class Invalidator(Protocol):
    async def start(
        self,
        *,
        remove_local: RemoveLocal,
        clear_local: ClearLocal,
    ) -> None: ...

    async def stop(self) -> None: ...

    async def invalidate(self, key: str) -> None: ...

    async def clear(self) -> None: ...


class BackplaneInvalidator:
    def __init__(self, backplane: Backplane) -> None:
        self._backplane = backplane
        self._remove_local: RemoveLocal | None = None
        self._clear_local: ClearLocal | None = None

    async def start(
        self,
        *,
        remove_local: RemoveLocal,
        clear_local: ClearLocal,
    ) -> None:
        self._remove_local = remove_local
        self._clear_local = clear_local
        await self._backplane.start(self._handle_message)

    async def stop(self) -> None:
        await self._backplane.stop()
        self._remove_local = None
        self._clear_local = None

    async def invalidate(self, key: str) -> None:
        await self._backplane.publish(BackplaneMessage.remove(key))

    async def clear(self) -> None:
        await self._backplane.publish(BackplaneMessage.clear())

    async def _handle_message(self, message: BackplaneMessage) -> None:
        remove_local = self._remove_local
        clear_local = self._clear_local

        if message.action == "remove" and message.key is not None:
            if remove_local is not None:
                remove_local(message.key)
            return

        if message.action == "clear" and clear_local is not None:
            clear_local()
