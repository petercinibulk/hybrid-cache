from __future__ import annotations

import pytest

from async_hybrid_cache import JsonSerializer, PickleSerializer, PydanticSerializer


def test_pickle_serializer_round_trips_python_objects() -> None:
    serializer = PickleSerializer()
    value = {"items": [1, 2, 3], "enabled": True}

    assert serializer.loads(serializer.dumps(value)) == value


def test_json_serializer_round_trips_json_values() -> None:
    serializer = JsonSerializer()
    value = {"items": [1, 2, 3], "enabled": True}

    assert serializer.loads(serializer.dumps(value)) == value


def test_pydantic_serializer_round_trips_models() -> None:
    pydantic = pytest.importorskip("pydantic")

    class User(pydantic.BaseModel):
        id: str
        name: str

    serializer = PydanticSerializer(User)
    value = User(id="123", name="Peter")

    assert serializer.loads(serializer.dumps(value)) == value
