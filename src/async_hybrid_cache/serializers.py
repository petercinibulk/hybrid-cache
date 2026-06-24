from __future__ import annotations

import json
import pickle
from collections.abc import Callable
from typing import Generic, Protocol, TypeVar, cast

T = TypeVar("T")


class Serializer(Protocol):
    """Protocol for converting distributed-cache values to and from bytes."""

    def dumps(self, value: object) -> bytes: ...

    def loads(self, value: bytes) -> object: ...


class PickleSerializer:
    """Serialize arbitrary trusted Python objects with pickle."""

    def dumps(self, value: object) -> bytes:
        """Serialize a Python object to bytes."""

        return pickle.dumps(value)

    def loads(self, value: bytes) -> object:
        """Deserialize bytes into a Python object."""

        return pickle.loads(value)


class JsonSerializer:
    """Serialize JSON-compatible values as UTF-8 JSON bytes."""

    def dumps(self, value: object) -> bytes:
        """Serialize a JSON-compatible value to bytes."""

        return json.dumps(value).encode("utf-8")

    def loads(self, value: bytes) -> object:
        """Deserialize UTF-8 JSON bytes."""

        return json.loads(value.decode("utf-8"))


class PydanticSerializer(Generic[T]):
    """Serialize and deserialize Pydantic model instances."""

    def __init__(self, model_type: type[T]) -> None:
        """Create a serializer for the supplied Pydantic model type."""

        self._model_type = model_type

    def dumps(self, value: object) -> bytes:
        """Serialize a Pydantic model instance to JSON bytes."""

        model_dump_json = getattr(value, "model_dump_json", None)
        if callable(model_dump_json):
            return cast(Callable[[], str], model_dump_json)().encode("utf-8")

        json_method = getattr(value, "json", None)
        if callable(json_method):
            return cast(Callable[[], str], json_method)().encode("utf-8")

        msg = "PydanticSerializer can only dump Pydantic model instances"
        raise TypeError(msg)

    def loads(self, value: bytes) -> T:
        """Deserialize JSON bytes into the configured Pydantic model type."""

        raw = value.decode("utf-8")

        model_validate_json = getattr(self._model_type, "model_validate_json", None)
        if callable(model_validate_json):
            return cast(Callable[[str], T], model_validate_json)(raw)

        parse_raw = getattr(self._model_type, "parse_raw", None)
        if callable(parse_raw):
            return cast(Callable[[str], T], parse_raw)(raw)

        msg = "PydanticSerializer requires a Pydantic model type"
        raise TypeError(msg)
