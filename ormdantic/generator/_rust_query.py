from __future__ import annotations

import importlib
from dataclasses import dataclass
from typing import Any, Mapping, TypedDict, cast

try:
    _ormdantic: Any | None = importlib.import_module("ormdantic._ormdantic")
except ImportError:  # pragma: no cover - exercised when extension is not built
    _ormdantic = None


class CompiledQuery(TypedDict):
    sql: str
    params: list[str]
    operation: str


@dataclass(frozen=True)
class RustQuery:
    sql: str
    values: tuple[Any, ...]
    operation: str


def rust_available() -> bool:
    return _ormdantic is not None and hasattr(_ormdantic, "compile_select_pk")


def compile_select_pk(
    *,
    dialect: str,
    table: str,
    primary_key: str,
    columns: list[str],
    aliases: list[str] | None = None,
) -> CompiledQuery | None:
    if _ormdantic is None or not hasattr(_ormdantic, "compile_select_pk"):
        return None
    return cast(
        CompiledQuery,
        _ormdantic.compile_select_pk(dialect, table, primary_key, columns, aliases),
    )


def compile_find_many(
    *,
    dialect: str,
    table: str,
    columns: list[str],
    filter_columns: list[str],
    order_columns: list[str],
    order_direction: str,
    limit: int | None = None,
    offset: int | None = None,
    aliases: list[str] | None = None,
) -> CompiledQuery | None:
    if _ormdantic is None or not hasattr(_ormdantic, "compile_find_many"):
        return None
    return cast(
        CompiledQuery,
        _ormdantic.compile_find_many(
            dialect,
            table,
            columns,
            filter_columns,
            order_columns,
            order_direction,
            limit,
            offset,
            aliases,
        ),
    )


def compile_joined_find_many(
    *,
    dialect: str,
    table: str,
    columns: list[tuple[str, str, str]],
    joins: list[tuple[str, str, str, str, str, str]],
    filter_columns: list[str],
    order_columns: list[str],
    order_direction: str,
    limit: int | None = None,
    offset: int | None = None,
) -> CompiledQuery | None:
    if _ormdantic is None or not hasattr(_ormdantic, "compile_joined_find_many"):
        return None
    return cast(
        CompiledQuery,
        _ormdantic.compile_joined_find_many(
            dialect,
            table,
            columns,
            joins,
            filter_columns,
            order_columns,
            order_direction,
            limit,
            offset,
        ),
    )


def compile_count(
    *,
    dialect: str,
    table: str,
    filter_columns: list[str],
) -> CompiledQuery | None:
    if _ormdantic is None or not hasattr(_ormdantic, "compile_count"):
        return None
    return cast(
        CompiledQuery,
        _ormdantic.compile_count(dialect, table, filter_columns),
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


def compile_update(
    *,
    dialect: str,
    table: str,
    primary_key: str,
    columns: list[str],
) -> CompiledQuery | None:
    if _ormdantic is None or not hasattr(_ormdantic, "compile_update"):
        return None
    return cast(
        CompiledQuery,
        _ormdantic.compile_update(dialect, table, primary_key, columns),
    )


def compile_upsert(
    *,
    dialect: str,
    table: str,
    primary_key: str,
    columns: list[str],
) -> CompiledQuery | None:
    if _ormdantic is None or not hasattr(_ormdantic, "compile_upsert"):
        return None
    return cast(
        CompiledQuery,
        _ormdantic.compile_upsert(dialect, table, primary_key, columns),
    )


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


def bind_compiled_query(
    compiled: CompiledQuery | None,
    values_by_param: Mapping[str, Any],
) -> RustQuery | None:
    if compiled is None:
        return None
    return RustQuery(
        sql=compiled["sql"],
        values=tuple(values_by_param[param] for param in compiled["params"]),
        operation=compiled["operation"],
    )
