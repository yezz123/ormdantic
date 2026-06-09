"""Migration document serialization helpers."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from typing import Any


def toml_loads(payload: str | bytes | bytearray) -> dict[str, Any]:
    text = payload.decode() if isinstance(payload, (bytes, bytearray)) else payload
    try:
        import tomllib
    except ModuleNotFoundError:  # pragma: no cover - Python < 3.11
        try:
            import tomli as tomllib  # type: ignore[no-redef]
        except ModuleNotFoundError as exc:  # pragma: no cover
            raise RuntimeError(
                "Reading TOML migration files on Python < 3.11 requires tomli"
            ) from exc
    return tomllib.loads(text)


def toml_dumps(payload: Mapping[str, Any]) -> str:
    lines = [
        f"{toml_key(key)} = {toml_value(value)}"
        for key, value in payload.items()
        if value is not None
    ]
    return "\n".join(lines) + "\n"


def toml_key(key: str) -> str:
    if key.replace("_", "").replace("-", "").isalnum():
        return key
    return json.dumps(key)


def toml_value(value: Any) -> str:
    if value is None:
        raise ValueError("TOML does not support null values")
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int | float):
        return str(value)
    if isinstance(value, str):
        return json.dumps(value)
    if isinstance(value, Mapping):
        parts = [
            f"{toml_key(str(key))} = {toml_value(item)}"
            for key, item in value.items()
            if item is not None
        ]
        return "{ " + ", ".join(parts) + " }"
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return "[" + ", ".join(toml_value(item) for item in value) + "]"
    raise TypeError(f"unsupported TOML value {value!r}")
