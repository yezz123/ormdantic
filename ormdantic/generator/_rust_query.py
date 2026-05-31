from __future__ import annotations

import importlib
from typing import Any, TypedDict, cast

try:
    _ormdantic: Any | None = importlib.import_module("ormdantic._ormdantic")
except ImportError:  # pragma: no cover - exercised when extension is not built
    _ormdantic = None


class CompiledQuery(TypedDict):
    sql: str
    params: list[str]
    operation: str


def rust_available() -> bool:
    return _ormdantic is not None and hasattr(_ormdantic, "compile_select_pk")


def compile_select_pk(
    *,
    dialect: str,
    table: str,
    primary_key: str,
    columns: list[str],
) -> CompiledQuery | None:
    if _ormdantic is None or not hasattr(_ormdantic, "compile_select_pk"):
        return None
    return cast(
        CompiledQuery,
        _ormdantic.compile_select_pk(dialect, table, primary_key, columns),
    )


def compile_insert(
    *,
    dialect: str,
    table: str,
    columns: list[str],
) -> CompiledQuery | None:
    if _ormdantic is None or not hasattr(_ormdantic, "compile_insert"):
        return None
    return cast(CompiledQuery, _ormdantic.compile_insert(dialect, table, columns))


def compile_delete_pk(
    *,
    dialect: str,
    table: str,
    primary_key: str,
) -> CompiledQuery | None:
    if _ormdantic is None or not hasattr(_ormdantic, "compile_delete_pk"):
        return None
    return cast(
        CompiledQuery,
        _ormdantic.compile_delete_pk(dialect, table, primary_key),
    )
