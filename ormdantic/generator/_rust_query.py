"""Private bridge from Python query builders to Rust SQL compilation."""

from __future__ import annotations

import importlib
from dataclasses import dataclass
from typing import Any, Mapping, TypedDict, cast

try:
    _ormdantic: Any | None = importlib.import_module("ormdantic._ormdantic")
except ImportError:  # pragma: no cover - exercised when extension is not built
    _ormdantic = None


class CompiledQuery(TypedDict):
    """Dictionary returned by Rust query compiler functions."""

    sql: str
    params: list[str]
    operation: str


FilterSpec = tuple[str, str, list[str]]


@dataclass(frozen=True)
class RustQuery:
    """SQL plus ordered bind values ready for native execution."""

    sql: str
    values: tuple[Any, ...]
    operation: str


def rust_available() -> bool:
    """Return whether the Rust extension exposes query compilation."""
    return _ormdantic is not None and hasattr(_ormdantic, "compile_select_pk")


def _require_extension(symbol: str) -> Any:
    if _ormdantic is None or not hasattr(_ormdantic, symbol):
        raise RuntimeError(
            "Ormdantic vNext requires the Rust extension for SQL compilation. "
            "Install the package with maturin or reinstall the wheel."
        )
    return _ormdantic


def compile_select_pk(
    *,
    dialect: str,
    table: str,
    primary_key: str,
    columns: list[str],
    aliases: list[str] | None = None,
) -> CompiledQuery:
    """Compile a primary-key select query through Rust."""
    rust = _require_extension("compile_select_pk")
    return cast(
        CompiledQuery,
        rust.compile_select_pk(dialect, table, primary_key, columns, aliases),
    )


def compile_find_many(
    *,
    dialect: str,
    table: str,
    columns: list[str],
    filter_columns: list[FilterSpec],
    order_columns: list[str],
    order_direction: str,
    limit: int | None = None,
    offset: int | None = None,
    aliases: list[str] | None = None,
) -> CompiledQuery:
    """Compile a filtered select query through Rust."""
    rust = _require_extension("compile_find_many")
    return cast(
        CompiledQuery,
        rust.compile_find_many(
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
    filter_columns: list[FilterSpec],
    order_columns: list[str],
    order_direction: str,
    limit: int | None = None,
    offset: int | None = None,
) -> CompiledQuery:
    """Compile a joined relationship select query through Rust."""
    rust = _require_extension("compile_joined_find_many")
    return cast(
        CompiledQuery,
        rust.compile_joined_find_many(
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
    filter_columns: list[FilterSpec],
) -> CompiledQuery:
    """Compile a count query through Rust."""
    rust = _require_extension("compile_count")
    return cast(
        CompiledQuery,
        rust.compile_count(dialect, table, filter_columns),
    )


def compile_insert(
    *,
    dialect: str,
    table: str,
    columns: list[str],
) -> CompiledQuery:
    """Compile an insert query through Rust."""
    rust = _require_extension("compile_insert")
    return cast(CompiledQuery, rust.compile_insert(dialect, table, columns))


def compile_update(
    *,
    dialect: str,
    table: str,
    primary_key: str,
    columns: list[str],
) -> CompiledQuery:
    """Compile an update query through Rust."""
    rust = _require_extension("compile_update")
    return cast(
        CompiledQuery,
        rust.compile_update(dialect, table, primary_key, columns),
    )


def compile_upsert(
    *,
    dialect: str,
    table: str,
    primary_key: str,
    columns: list[str],
) -> CompiledQuery:
    """Compile an upsert query through Rust."""
    rust = _require_extension("compile_upsert")
    return cast(
        CompiledQuery,
        rust.compile_upsert(dialect, table, primary_key, columns),
    )


def compile_delete_pk(
    *,
    dialect: str,
    table: str,
    primary_key: str,
) -> CompiledQuery:
    """Compile a primary-key delete query through Rust."""
    rust = _require_extension("compile_delete_pk")
    return cast(
        CompiledQuery,
        rust.compile_delete_pk(dialect, table, primary_key),
    )


def bind_compiled_query(
    compiled: CompiledQuery,
    values_by_param: Mapping[str, Any],
) -> RustQuery:
    """Bind a compiled query using the parameter order returned by Rust."""
    return RustQuery(
        sql=compiled["sql"],
        values=tuple(values_by_param[param] for param in compiled["params"]),
        operation=compiled["operation"],
    )
