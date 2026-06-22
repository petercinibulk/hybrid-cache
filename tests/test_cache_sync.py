from __future__ import annotations

import asyncio

import pytest

from cache_sync import CacheOptions, CacheSync
from cache_sync.invalidation import ClearLocal, RemoveLocal


class RecordingInvalidationBus:
    def __init__(self) -> None:
        self.invalidated: list[str] = []
        self.clear_count = 0
        self.remove_local: RemoveLocal | None = None
        self.clear_local: ClearLocal | None = None

    async def start(
        self,
        *,
        remove_local: RemoveLocal,
        clear_local: ClearLocal,
    ) -> None:
        self.remove_local = remove_local
        self.clear_local = clear_local

    async def stop(self) -> None:
        self.remove_local = None
        self.clear_local = None

    async def invalidate(self, key: str) -> None:
        self.invalidated.append(key)

    async def clear(self) -> None:
        self.clear_count += 1

    def emit_remove(self, key: str) -> None:
        if self.remove_local is not None:
            self.remove_local(key)


class RecordingDistributedCache:
    def __init__(self) -> None:
        self.values: dict[str, object] = {}
        self.deleted: list[str] = []

    async def get(self, key: str) -> object | None:
        return self.values.get(key)

    async def set(self, key: str, value: object, ttl_seconds: float) -> None:
        self.values[key] = value

    async def delete(self, key: str) -> None:
        self.deleted.append(key)
        self.values.pop(key, None)


@pytest.mark.asyncio
async def test_get_or_set_returns_cached_value() -> None:
    cache = CacheSync(options=CacheOptions(ttl_seconds=60))
    calls = 0

    async def factory() -> str:
        nonlocal calls
        calls += 1
        return "value"

    first = await cache.get_or_set("key", factory)
    second = await cache.get_or_set("key", factory)

    assert first == "value"
    assert second == "value"
    assert calls == 1


@pytest.mark.asyncio
async def test_get_or_set_prevents_stampede() -> None:
    cache = CacheSync(options=CacheOptions(ttl_seconds=60))
    calls = 0

    async def factory() -> str:
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.01)
        return "value"

    values = await asyncio.gather(*(cache.get_or_set("key", factory) for _ in range(10)))

    assert values == ["value"] * 10
    assert calls == 1


@pytest.mark.asyncio
async def test_fail_safe_returns_stale_value() -> None:
    cache = CacheSync(options=CacheOptions(ttl_seconds=0.01, fail_safe_seconds=60))

    async def working_factory() -> str:
        return "stale"

    await cache.get_or_set("key", working_factory)
    await asyncio.sleep(0.02)

    async def failing_factory() -> str:
        raise RuntimeError("factory failed")

    value = await cache.get_or_set("key", failing_factory)

    assert value == "stale"


@pytest.mark.asyncio
async def test_remove_deletes_cached_value() -> None:
    cache = CacheSync(options=CacheOptions(ttl_seconds=60))
    calls = 0

    async def factory() -> str:
        nonlocal calls
        calls += 1
        return f"value-{calls}"

    assert await cache.get_or_set("key", factory) == "value-1"
    await cache.remove("key")
    assert await cache.get_or_set("key", factory) == "value-2"


@pytest.mark.asyncio
async def test_invalidation_bus_works_without_distributed_cache() -> None:
    bus = RecordingInvalidationBus()
    cache = CacheSync(
        invalidation_bus=bus,
        options=CacheOptions(ttl_seconds=60),
    )
    calls = 0

    async def factory() -> str:
        nonlocal calls
        calls += 1
        return f"value-{calls}"

    await cache.start()

    assert await cache.get_or_set("key", factory) == "value-1"
    bus.emit_remove("key")
    assert await cache.get_or_set("key", factory) == "value-2"

    await cache.stop()


@pytest.mark.asyncio
async def test_invalidation_bus_and_distributed_cache_are_independent() -> None:
    distributed_cache = RecordingDistributedCache()
    bus = RecordingInvalidationBus()
    cache = CacheSync(
        distributed_cache=distributed_cache,
        invalidation_bus=bus,
        options=CacheOptions(ttl_seconds=60),
    )

    await cache.set("key", "value")

    assert distributed_cache.values == {"key": "value"}
    assert bus.invalidated == ["key"]

    await cache.remove("key")

    assert distributed_cache.values == {}
    assert distributed_cache.deleted == ["key"]
    assert bus.invalidated == ["key", "key"]


@pytest.mark.asyncio
async def test_decorator_preserves_return_type_and_remove_cached() -> None:
    cache = CacheSync(options=CacheOptions(ttl_seconds=60))
    calls = 0

    @cache.cached(lambda user_id: f"user:{user_id}")
    async def get_user(user_id: str) -> dict[str, str]:
        nonlocal calls
        calls += 1
        return {"id": user_id}

    first = await get_user("123")
    second = await get_user("123")
    await get_user.remove_cached("123")
    third = await get_user("123")

    assert first == {"id": "123"}
    assert second == {"id": "123"}
    assert third == {"id": "123"}
    assert calls == 2
    assert get_user.cache_key("123") == "user:123"


@pytest.mark.asyncio
async def test_decorator_defaults_to_function_arguments_cache_key() -> None:
    cache = CacheSync(options=CacheOptions(ttl_seconds=60))
    calls = 0

    @cache.cached()
    async def get_user(user_id: str, *, include_disabled: bool = False) -> dict[str, object]:
        nonlocal calls
        calls += 1
        return {
            "call": calls,
            "include_disabled": include_disabled,
            "user_id": user_id,
        }

    first = await get_user("123")
    second = await get_user("123")
    explicit_default = await get_user("123", include_disabled=False)
    different_kwargs = await get_user(user_id="123", include_disabled=True)
    same_kwargs = await get_user(include_disabled=True, user_id="123")
    await get_user.remove_cached(user_id="123", include_disabled=True)
    refreshed = await get_user("123", include_disabled=True)

    assert first == second == explicit_default
    assert different_kwargs == same_kwargs
    assert refreshed["call"] == 3
    assert calls == 3
    assert get_user.cache_key("123").endswith("get_user(user_id='123',include_disabled=False)")


@pytest.mark.asyncio
async def test_decorator_accepts_cache_policy_overrides() -> None:
    cache = CacheSync(options=CacheOptions(ttl_seconds=60, fail_safe_seconds=60))
    calls = 0

    @cache.cached(options=CacheOptions(ttl_seconds=0.01))
    async def get_value() -> str:
        nonlocal calls
        calls += 1
        return f"value-{calls}"

    assert await get_value() == "value-1"
    await asyncio.sleep(0.02)
    assert await get_value() == "value-2"
    assert calls == 2


@pytest.mark.asyncio
async def test_cache_policy_overrides_apply_only_to_supplied_key() -> None:
    cache = CacheSync(options=CacheOptions(ttl_seconds=60))

    fast_calls = 0
    slow_calls = 0

    async def fast_factory() -> str:
        nonlocal fast_calls
        fast_calls += 1
        return f"fast-{fast_calls}"

    async def slow_factory() -> str:
        nonlocal slow_calls
        slow_calls += 1
        return f"slow-{slow_calls}"

    assert (
        await cache.get_or_set(
            "fast",
            fast_factory,
            options=CacheOptions(ttl_seconds=0.01),
        )
        == "fast-1"
    )
    assert await cache.get_or_set("slow", slow_factory) == "slow-1"
    await asyncio.sleep(0.02)

    assert await cache.get_or_set("fast", fast_factory) == "fast-2"
    assert await cache.get_or_set("slow", slow_factory) == "slow-1"


@pytest.mark.asyncio
async def test_cache_policy_overrides_inherit_unsupplied_constructor_defaults() -> None:
    cache = CacheSync(options=CacheOptions(ttl_seconds=60, hard_timeout_seconds=0.01))

    async def slow_factory() -> str:
        await asyncio.sleep(0.02)
        return "value"

    with pytest.raises(TimeoutError):
        await cache.get_or_set(
            "key",
            slow_factory,
            options=CacheOptions(ttl_seconds=60),
        )


@pytest.mark.asyncio
async def test_max_keys_removes_oldest_cached_key() -> None:
    cache = CacheSync(options=CacheOptions(ttl_seconds=60, max_keys=2))

    await cache.set("first", "one")
    await cache.set("second", "two")
    await cache.set("third", "three")

    assert list(cache._memory) == ["second", "third"]


@pytest.mark.asyncio
async def test_max_keys_does_not_remove_key_when_value_is_refreshed() -> None:
    cache = CacheSync(options=CacheOptions(ttl_seconds=60, max_keys=2))

    await cache.set("first", "one")
    await cache.set("second", "two")
    await cache.set("first", "updated")

    assert list(cache._memory) == ["first", "second"]
    assert cache._memory["first"].value == "updated"


@pytest.mark.asyncio
async def test_max_keys_composes_with_decorator_and_ttl() -> None:
    cache = CacheSync(options=CacheOptions(ttl_seconds=60, max_keys=2))
    calls = 0

    @cache.cached(lambda key: key, options=CacheOptions(ttl_seconds=0.01))
    async def load(key: str) -> str:
        nonlocal calls
        calls += 1
        return f"{key}-{calls}"

    assert await load("first") == "first-1"
    assert await load("second") == "second-2"
    assert await load("third") == "third-3"
    assert list(cache._memory) == ["second", "third"]

    await asyncio.sleep(0.02)

    assert await load("second") == "second-4"
    assert list(cache._memory) == ["second", "third"]


@pytest.mark.asyncio
async def test_max_keys_override_can_raise_or_disable_constructor_limit() -> None:
    cache = CacheSync(options=CacheOptions(ttl_seconds=60, max_keys=1))

    await cache.set("first", "one")
    await cache.set("second", "two", options=CacheOptions(max_keys=2))

    assert list(cache._memory) == ["first", "second"]

    await cache.set("third", "three", options=CacheOptions(max_keys=None))

    assert list(cache._memory) == ["first", "second", "third"]
