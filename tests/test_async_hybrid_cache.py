from __future__ import annotations

import asyncio

import pytest

from async_hybrid_cache import AsyncHybridCache, CacheOptions
from async_hybrid_cache.invalidation import ClearLocal, RemoveLocal


class RecordingInvalidationBus:
    def __init__(self) -> None:
        self.invalidated: list[tuple[str, str]] = []
        self.cleared: list[str | None] = []
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

    async def invalidate(self, key: str, *, scope: str) -> None:
        self.invalidated.append((key, scope))

    async def clear(self, *, scope: str | None = None) -> None:
        self.cleared.append(scope)

    def emit_remove(self, key: str, *, scope: str = "__default__") -> None:
        if self.remove_local is not None:
            self.remove_local(key, scope)


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
    cache = AsyncHybridCache(options=CacheOptions(ttl_seconds=60))
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
    cache = AsyncHybridCache(options=CacheOptions(ttl_seconds=60))
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
    cache = AsyncHybridCache(options=CacheOptions(ttl_seconds=0.01, fail_safe_seconds=60))

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
    cache = AsyncHybridCache(options=CacheOptions(ttl_seconds=60))
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
    cache = AsyncHybridCache(
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
    cache = AsyncHybridCache(
        distributed_cache=distributed_cache,
        invalidation_bus=bus,
        options=CacheOptions(ttl_seconds=60),
    )

    await cache.set("key", "value")

    assert distributed_cache.values == {"11:__default__key": "value"}
    assert bus.invalidated == [("key", "__default__")]

    await cache.remove("key")

    assert distributed_cache.values == {}
    assert distributed_cache.deleted == ["11:__default__key"]
    assert bus.invalidated == [("key", "__default__"), ("key", "__default__")]


@pytest.mark.asyncio
async def test_decorator_preserves_return_type_and_remove_cached() -> None:
    cache = AsyncHybridCache(options=CacheOptions(ttl_seconds=60))
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
    cache = AsyncHybridCache(options=CacheOptions(ttl_seconds=60))
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
    cache = AsyncHybridCache(options=CacheOptions(ttl_seconds=60, fail_safe_seconds=60))
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
    cache = AsyncHybridCache(options=CacheOptions(ttl_seconds=60))

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
    cache = AsyncHybridCache(options=CacheOptions(ttl_seconds=60, hard_timeout_seconds=0.01))

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
async def test_lru_max_keys_removes_least_recently_used_key() -> None:
    cache = AsyncHybridCache(options=CacheOptions(ttl_seconds=60, lru_max_keys=2))

    await cache.set("first", "one")
    await cache.set("second", "two")
    await cache.set("third", "three")

    assert list(cache._memory["__default__"]) == ["second", "third"]


@pytest.mark.asyncio
async def test_lru_max_keys_treats_fresh_read_as_recent_use() -> None:
    cache = AsyncHybridCache(options=CacheOptions(ttl_seconds=60, lru_max_keys=2))
    calls = 0

    async def factory() -> str:
        nonlocal calls
        calls += 1
        return f"value-{calls}"

    await cache.set("first", "one")
    await cache.set("second", "two")

    assert await cache.get_or_set("first", factory) == "one"

    await cache.set("third", "three")

    assert list(cache._memory["__default__"]) == ["first", "third"]
    assert calls == 0


@pytest.mark.asyncio
async def test_lru_max_keys_treats_refreshed_key_as_recent_use() -> None:
    cache = AsyncHybridCache(options=CacheOptions(ttl_seconds=60, lru_max_keys=2))

    await cache.set("first", "one")
    await cache.set("second", "two")
    await cache.set("first", "updated")

    assert list(cache._memory["__default__"]) == ["second", "first"]
    assert cache._memory["__default__"]["first"].value == "updated"


@pytest.mark.asyncio
async def test_lru_max_keys_composes_with_decorator_and_ttl() -> None:
    cache = AsyncHybridCache(options=CacheOptions(ttl_seconds=60, lru_max_keys=2))
    calls = 0

    @cache.cached(lambda key: key, options=CacheOptions(ttl_seconds=0.01))
    async def load(key: str) -> str:
        nonlocal calls
        calls += 1
        return f"{key}-{calls}"

    assert await load("first") == "first-1"
    assert await load("second") == "second-2"
    assert await load("third") == "third-3"
    memory = next(iter(cache._memory.values()))
    assert list(memory) == ["second", "third"]

    await asyncio.sleep(0.02)

    assert await load("second") == "second-4"
    assert list(memory) == ["third", "second"]


@pytest.mark.asyncio
async def test_lru_max_keys_zero_clears_local_memory() -> None:
    cache = AsyncHybridCache(options=CacheOptions(ttl_seconds=60, lru_max_keys=2))

    await cache.set("first", "one")
    await cache.set("second", "two", options=CacheOptions(lru_max_keys=0))

    assert cache._memory["__default__"] == {}


@pytest.mark.asyncio
async def test_lru_max_keys_override_can_raise_or_disable_constructor_limit() -> None:
    cache = AsyncHybridCache(options=CacheOptions(ttl_seconds=60, lru_max_keys=1))

    await cache.set("first", "one")
    await cache.set("second", "two", options=CacheOptions(lru_max_keys=2))

    assert list(cache._memory["__default__"]) == ["first", "second"]

    await cache.set("third", "three", options=CacheOptions(lru_max_keys=None))

    assert list(cache._memory["__default__"]) == ["first", "second", "third"]


@pytest.mark.asyncio
async def test_manual_scope_has_independent_lru_limit_and_operations() -> None:
    bus = RecordingInvalidationBus()
    cache = AsyncHybridCache(invalidation_bus=bus, options=CacheOptions(ttl_seconds=60))
    users = cache.scope("users", options=CacheOptions(lru_max_keys=2))
    products = cache.scope("products", options=CacheOptions(lru_max_keys=1))

    await users.set("1", "ada")
    await users.set("2", "grace")
    await users.set("3", "katherine")
    await products.set("1", "keyboard")
    await products.set("2", "monitor")

    assert list(cache._memory["users"]) == ["2", "3"]
    assert list(cache._memory["products"]) == ["2"]
    assert await users.get("2") == "grace"

    await users.remove("2")

    assert await users.get("2") is None
    assert list(cache._memory["users"]) == ["3"]
    assert list(cache._memory["products"]) == ["2"]
    assert bus.invalidated[-1] == ("2", "users")

    await products.clear()

    assert "products" not in cache._memory
    assert cache._memory["users"]
    assert bus.cleared == ["products"]


@pytest.mark.asyncio
async def test_manual_scope_options_inherit_cache_defaults() -> None:
    cache = AsyncHybridCache(options=CacheOptions(ttl_seconds=0.01, fail_safe_seconds=60))
    users = cache.scope("users", options=CacheOptions(lru_max_keys=2))

    await users.set("1", "ada")
    await asyncio.sleep(0.02)

    assert await users.get("1") is None


@pytest.mark.asyncio
async def test_cached_functions_get_independent_default_lru_scopes() -> None:
    cache = AsyncHybridCache(options=CacheOptions(ttl_seconds=60, lru_max_keys=1))
    user_calls = 0
    product_calls = 0

    @cache.cached(lambda key: key)
    async def load_user(key: str) -> str:
        nonlocal user_calls
        user_calls += 1
        return f"user-{key}-{user_calls}"

    @cache.cached(lambda key: key)
    async def load_product(key: str) -> str:
        nonlocal product_calls
        product_calls += 1
        return f"product-{key}-{product_calls}"

    assert await load_user("1") == "user-1-1"
    assert await load_product("1") == "product-1-1"
    assert await load_user("2") == "user-2-2"
    assert await load_product("1") == "product-1-1"

    assert user_calls == 2
    assert product_calls == 1
    assert len(cache._memory) == 2


@pytest.mark.asyncio
async def test_cached_functions_can_share_an_explicit_scope() -> None:
    cache = AsyncHybridCache(options=CacheOptions(ttl_seconds=60, lru_max_keys=1))
    user_calls = 0
    product_calls = 0

    @cache.cached(lambda key: f"user:{key}", scope="shared")
    async def load_user(key: str) -> str:
        nonlocal user_calls
        user_calls += 1
        return f"user-{key}-{user_calls}"

    @cache.cached(lambda key: f"product:{key}", scope="shared")
    async def load_product(key: str) -> str:
        nonlocal product_calls
        product_calls += 1
        return f"product-{key}-{product_calls}"

    assert await load_user("1") == "user-1-1"
    assert await load_product("1") == "product-1-1"
    assert await load_user("1") == "user-1-2"

    assert list(cache._memory["shared"]) == ["user:1"]
