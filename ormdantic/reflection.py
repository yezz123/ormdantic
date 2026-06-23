"""Database reflection helpers."""

from __future__ import annotations

import keyword
import re
from collections.abc import Sequence
from dataclasses import replace
from time import perf_counter
from typing import Any

from ormdantic._migrations.models import (
    ColumnSnapshot,
    SchemaDiff,
    SchemaSnapshot,
    TableSnapshot,
)
from ormdantic.errors import ReflectionError, classify_native_error

_CacheKey = tuple[str | None, tuple[str, ...] | None, tuple[str, ...] | None]


class Inspector:
    """Reflect live database metadata."""

    def __init__(self, database: Any) -> None:
        self._database = database
        self._schema_cache: dict[_CacheKey, SchemaSnapshot] = {}

    def invalidate_cache(
        self,
        *,
        schema: str | None = None,
        include_tables: Sequence[str] | None = None,
        exclude_tables: Sequence[str] | None = None,
        name_patterns: Sequence[str] | None = None,
    ) -> None:
        """Invalidate cached reflection results."""
        if (
            schema is None
            and include_tables is None
            and exclude_tables is None
            and name_patterns is None
        ):
            self._schema_cache.clear()
            return
        self._schema_cache.pop(
            self._cache_key(schema, include_tables, exclude_tables, name_patterns),
            None,
        )

    clear_cache = invalidate_cache

    async def schema(
        self,
        *,
        schema: str | None = None,
        include_tables: Sequence[str] | None = None,
        exclude_tables: Sequence[str] | None = None,
        name_patterns: Sequence[str] | None = None,
        refresh: bool = False,
    ) -> SchemaSnapshot:
        """Return a reflected schema snapshot."""
        return await self._reflect(
            "schema",
            None,
            lambda: self._schema(
                schema=schema,
                include_tables=include_tables,
                exclude_tables=exclude_tables,
                name_patterns=name_patterns,
                refresh=refresh,
            ),
        )

    async def schema_dict(
        self,
        *,
        schema: str | None = None,
        include_tables: Sequence[str] | None = None,
        exclude_tables: Sequence[str] | None = None,
        name_patterns: Sequence[str] | None = None,
        refresh: bool = False,
    ) -> dict[str, Any]:
        """Return a reflected schema snapshot as a dictionary."""
        return await self._reflect(
            "schema_dict",
            None,
            lambda: self._schema(
                schema=schema,
                include_tables=include_tables,
                exclude_tables=exclude_tables,
                name_patterns=name_patterns,
                refresh=refresh,
            ).to_dict(),
        )

    async def table_names(
        self,
        *,
        schema: str | None = None,
        include_tables: Sequence[str] | None = None,
        exclude_tables: Sequence[str] | None = None,
        name_patterns: Sequence[str] | None = None,
        refresh: bool = False,
    ) -> list[str]:
        """Return table names for supported dialects."""
        return await self._reflect(
            "table_names",
            None,
            lambda: [
                table.name
                for table in self._schema(
                    schema=schema,
                    include_tables=include_tables,
                    exclude_tables=exclude_tables,
                    name_patterns=name_patterns,
                    refresh=refresh,
                ).tables
            ],
        )

    async def tables(
        self,
        *,
        schema: str | None = None,
        include_tables: Sequence[str] | None = None,
        exclude_tables: Sequence[str] | None = None,
        name_patterns: Sequence[str] | None = None,
        refresh: bool = False,
    ) -> list[dict[str, Any]]:
        """Return reflected table metadata."""
        return await self._reflect(
            "tables",
            None,
            lambda: [
                table.to_dict()
                for table in self._schema(
                    schema=schema,
                    include_tables=include_tables,
                    exclude_tables=exclude_tables,
                    name_patterns=name_patterns,
                    refresh=refresh,
                ).tables
            ],
        )

    async def namespaces(
        self,
        *,
        schema: str | None = None,
        include_tables: Sequence[str] | None = None,
        exclude_tables: Sequence[str] | None = None,
        name_patterns: Sequence[str] | None = None,
        refresh: bool = False,
    ) -> list[dict[str, Any]]:
        """Return reflected namespace/schema metadata."""
        return await self._reflect(
            "namespaces",
            None,
            lambda: [
                namespace.to_dict()
                for namespace in self._schema(
                    schema=schema,
                    include_tables=include_tables,
                    exclude_tables=exclude_tables,
                    name_patterns=name_patterns,
                    refresh=refresh,
                ).namespaces
            ],
        )

    async def schema_names(
        self,
        *,
        schema: str | None = None,
        include_tables: Sequence[str] | None = None,
        exclude_tables: Sequence[str] | None = None,
        name_patterns: Sequence[str] | None = None,
        refresh: bool = False,
    ) -> list[str]:
        """Return reflected namespace/schema names."""
        return await self._reflect(
            "schema_names",
            None,
            lambda: [
                namespace.name
                for namespace in self._schema(
                    schema=schema,
                    include_tables=include_tables,
                    exclude_tables=exclude_tables,
                    name_patterns=name_patterns,
                    refresh=refresh,
                ).namespaces
            ],
        )

    async def columns(
        self,
        table: str,
        *,
        schema: str | None = None,
        refresh: bool = False,
    ) -> list[dict[str, Any]]:
        """Return column metadata for a table."""
        return await self._reflect(
            "columns",
            table,
            lambda: [
                _column_metadata(column)
                for column in self._table(table, schema=schema, refresh=refresh).columns
            ],
        )

    async def indexes(
        self,
        table: str,
        *,
        schema: str | None = None,
        refresh: bool = False,
    ) -> list[dict[str, Any]]:
        """Return index metadata for a table."""
        return await self._reflect(
            "indexes",
            table,
            lambda: [
                index.to_dict()
                for index in self._table(table, schema=schema, refresh=refresh).indexes
            ],
        )

    async def foreign_keys(
        self,
        table: str,
        *,
        schema: str | None = None,
        refresh: bool = False,
    ) -> list[dict[str, Any]]:
        """Return foreign key metadata for a table."""
        return await self._reflect(
            "foreign_keys",
            table,
            lambda: _foreign_key_metadata(
                self._table(table, schema=schema, refresh=refresh)
            ),
        )

    async def unique_constraints(
        self,
        table: str,
        *,
        schema: str | None = None,
        refresh: bool = False,
    ) -> list[dict[str, Any]]:
        """Return UNIQUE constraint metadata for a table."""
        return await self._reflect(
            "unique_constraints",
            table,
            lambda: [
                constraint
                for constraint in _constraint_metadata(
                    self._table(table, schema=schema, refresh=refresh)
                )
                if constraint["type"] == "unique"
            ],
        )

    async def check_constraints(
        self,
        table: str,
        *,
        schema: str | None = None,
        refresh: bool = False,
    ) -> list[dict[str, Any]]:
        """Return CHECK constraint metadata for a table."""
        return await self._reflect(
            "check_constraints",
            table,
            lambda: [
                constraint
                for constraint in _constraint_metadata(
                    self._table(table, schema=schema, refresh=refresh)
                )
                if constraint["type"] == "check"
            ],
        )

    async def constraints(
        self,
        table: str,
        *,
        schema: str | None = None,
        refresh: bool = False,
    ) -> list[dict[str, Any]]:
        """Return primary key, unique, check, foreign key, and exclusion constraints."""
        return await self._reflect(
            "constraints",
            table,
            lambda: _constraint_metadata(
                self._table(table, schema=schema, refresh=refresh)
            ),
        )

    async def compare_to_models(
        self,
        *,
        schema: str | None = None,
        include_tables: Sequence[str] | None = None,
        exclude_tables: Sequence[str] | None = None,
        name_patterns: Sequence[str] | None = None,
        refresh: bool = False,
    ) -> SchemaDiff:
        """Compare the reflected schema with registered Ormdantic models."""
        return await self._reflect(
            "compare_to_models",
            None,
            lambda: self._compare_to_models(
                schema=schema,
                include_tables=include_tables,
                exclude_tables=exclude_tables,
                name_patterns=name_patterns,
                refresh=refresh,
            ),
        )

    async def scaffold_models(
        self,
        *,
        schema: str | None = None,
        include_tables: Sequence[str] | None = None,
        exclude_tables: Sequence[str] | None = None,
        name_patterns: Sequence[str] | None = None,
        database_variable: str = "db",
        refresh: bool = False,
    ) -> str:
        """Generate Pydantic model scaffolding from reflected tables."""
        return await self._reflect(
            "scaffold_models",
            None,
            lambda: _scaffold_models(
                self._schema(
                    schema=schema,
                    include_tables=include_tables,
                    exclude_tables=exclude_tables,
                    name_patterns=name_patterns,
                    refresh=refresh,
                ),
                database_variable=database_variable,
            ),
        )

    async def _reflect(self, operation: str, table: str | None, call: Any) -> Any:
        context = self._database._context(
            "reflection", reflection=operation, table=table
        )
        payload = {
            "database": self._database,
            "operation": "reflection",
            "reflection": operation,
            "table_name": table,
            "backend": context["backend"],
        }
        await self._database._events.dispatch("before_reflection", **payload)
        started = perf_counter()
        try:
            result = call()
        except Exception as exc:
            duration_ms = (perf_counter() - started) * 1000
            error = classify_native_error(
                exc,
                default=ReflectionError,
                message=f"reflection failed for {operation}",
                context=context,
            )
            await self._database._events.dispatch(
                "after_reflection",
                **payload,
                duration_ms=duration_ms,
                error=error,
            )
            raise error from exc
        await self._database._events.dispatch(
            "after_reflection",
            **payload,
            duration_ms=(perf_counter() - started) * 1000,
            row_count=_row_count(result),
            error=None,
        )
        return result

    def _schema(
        self,
        *,
        schema: str | None,
        include_tables: Sequence[str] | None,
        exclude_tables: Sequence[str] | None,
        name_patterns: Sequence[str] | None,
        refresh: bool,
    ) -> SchemaSnapshot:
        key = self._cache_key(schema, include_tables, exclude_tables, name_patterns)
        if not refresh and key in self._schema_cache:
            return self._schema_cache[key]
        include = _include_patterns(include_tables, name_patterns)
        snapshot = self._database.migrations.live_snapshot(
            include_tables=include,
            exclude_tables=exclude_tables,
            schema=schema,
        )
        self._schema_cache[key] = snapshot
        return snapshot

    def _table(
        self,
        table: str,
        *,
        schema: str | None,
        refresh: bool,
    ) -> TableSnapshot:
        snapshot = self._schema(
            schema=schema,
            include_tables=(table,),
            exclude_tables=None,
            name_patterns=None,
            refresh=refresh,
        )
        for item in snapshot.tables:
            if item.name == table and (schema is None or item.schema == schema):
                return item
        qualifier = f"{schema}.{table}" if schema is not None else table
        raise KeyError(f"reflected table '{qualifier}' was not found")

    def _compare_to_models(
        self,
        *,
        schema: str | None,
        include_tables: Sequence[str] | None,
        exclude_tables: Sequence[str] | None,
        name_patterns: Sequence[str] | None,
        refresh: bool,
    ) -> SchemaDiff:
        include = _include_patterns(include_tables, name_patterns)
        before = self._schema(
            schema=schema,
            include_tables=include,
            exclude_tables=exclude_tables,
            name_patterns=None,
            refresh=refresh,
        )
        after = self._database.migrations.snapshot()
        after = _filter_snapshot(after, include, exclude_tables)
        if schema is not None:
            after = _apply_default_schema(after, schema)
        return self._database.migrations.diff(before, after)

    def _cache_key(
        self,
        schema: str | None,
        include_tables: Sequence[str] | None,
        exclude_tables: Sequence[str] | None,
        name_patterns: Sequence[str] | None,
    ) -> _CacheKey:
        return (
            schema,
            _include_patterns(include_tables, name_patterns),
            _tuple_or_none(exclude_tables),
        )


def _include_patterns(
    include_tables: Sequence[str] | None,
    name_patterns: Sequence[str] | None,
) -> tuple[str, ...] | None:
    patterns = [str(pattern) for pattern in include_tables or ()]
    patterns.extend(str(pattern) for pattern in name_patterns or ())
    return tuple(patterns) if patterns else None


def _tuple_or_none(values: Sequence[str] | None) -> tuple[str, ...] | None:
    return tuple(str(value) for value in values) if values else None


def _row_count(result: Any) -> int | None:
    if isinstance(result, SchemaSnapshot):
        return len(result.tables)
    if isinstance(result, SchemaDiff):
        return len(result.changes)
    if isinstance(result, str):
        return None
    if isinstance(result, dict):
        tables = result.get("tables")
        return len(tables) if isinstance(tables, list) else None
    if hasattr(result, "__len__"):
        return len(result)
    return None


def _column_metadata(column: ColumnSnapshot) -> dict[str, Any]:
    payload = column.to_dict()
    payload["type"] = column.kind
    payload["default"] = column.server_default
    return payload


def _foreign_key_metadata(table: TableSnapshot) -> list[dict[str, Any]]:
    foreign_keys: list[dict[str, Any]] = []
    for column in table.columns:
        if column.foreign_table is None or column.foreign_column is None:
            continue
        foreign_keys.append(
            {
                "name": column.foreign_key_name,
                "from": column.name,
                "to": column.foreign_column,
                "table": column.foreign_table,
                "columns": [column.name],
                "foreign_table": column.foreign_table,
                "foreign_columns": [column.foreign_column],
                "on_delete": column.on_delete,
                "on_update": column.on_update,
                "deferrable": column.deferrable,
                "initially_deferred": column.initially_deferred,
                "validated": True,
            }
        )
    for constraint in table.foreign_key_constraints:
        payload = constraint.to_dict()
        payload["from"] = constraint.columns[0] if constraint.columns else None
        payload["to"] = (
            constraint.foreign_columns[0] if constraint.foreign_columns else None
        )
        payload["table"] = constraint.foreign_table
        foreign_keys.append(payload)
    return foreign_keys


def _constraint_metadata(table: TableSnapshot) -> list[dict[str, Any]]:
    constraints: list[dict[str, Any]] = []
    primary_key = [column.name for column in table.columns if column.primary_key]
    if not primary_key and table.primary_key:
        primary_key = [table.primary_key]
    if primary_key:
        constraints.append(
            {
                "type": "primary_key",
                "name": None,
                "columns": primary_key,
            }
        )
    for column in table.columns:
        if column.unique:
            constraints.append(
                {
                    "type": "unique",
                    "name": None,
                    "columns": [column.name],
                }
            )
        for check in column.checks:
            constraints.append(
                {
                    "type": "check",
                    "name": None,
                    "column": column.name,
                    "check": list(check),
                }
            )
    for columns in table.unique_constraints:
        constraints.append(
            {
                "type": "unique",
                "name": None,
                "columns": list(columns),
            }
        )
    for constraint in table.named_unique_constraints:
        payload = constraint.to_dict()
        payload["type"] = "unique"
        constraints.append(payload)
    for check in table.check_constraints:
        payload = check.to_dict()
        payload["type"] = "check"
        constraints.append(payload)
    for foreign_key in _foreign_key_metadata(table):
        payload = dict(foreign_key)
        payload["type"] = "foreign_key"
        constraints.append(payload)
    for exclusion in table.exclusion_constraints:
        payload = exclusion.to_dict()
        payload["type"] = "exclusion"
        constraints.append(payload)
    return constraints


def _filter_snapshot(
    snapshot: SchemaSnapshot,
    include_tables: Sequence[str] | None,
    exclude_tables: Sequence[str] | None,
) -> SchemaSnapshot:
    from ormdantic.migrations import _filter_snapshot_for_autogenerate_scope

    return _filter_snapshot_for_autogenerate_scope(
        snapshot,
        include_tables=include_tables,
        exclude_tables=exclude_tables,
        schema=None,
    )


def _apply_default_schema(snapshot: SchemaSnapshot, schema: str) -> SchemaSnapshot:
    return replace(
        snapshot,
        tables=[
            replace(table, schema=schema) if table.schema is None else table
            for table in snapshot.tables
        ],
        enum_types=[
            replace(enum_type, schema=schema) if enum_type.schema is None else enum_type
            for enum_type in snapshot.enum_types
        ],
        sequences=[
            replace(sequence, schema=schema) if sequence.schema is None else sequence
            for sequence in snapshot.sequences
        ],
        views=[
            replace(view, schema=schema) if view.schema is None else view
            for view in snapshot.views
        ],
    )


def _scaffold_models(snapshot: SchemaSnapshot, *, database_variable: str) -> str:
    database_variable = _python_identifier(database_variable)
    lines = [
        "from __future__ import annotations",
        "",
        "from datetime import date, datetime",
        "from decimal import Decimal",
        "from typing import Any",
        "from uuid import UUID",
        "",
        "from pydantic import BaseModel, Field",
        "",
        "from ormdantic import Ormdantic",
        "",
        f'{database_variable} = Ormdantic("DATABASE_URL")',
        "",
    ]
    used_class_names: set[str] = set()
    for table in snapshot.tables:
        class_name = _unique_name(_class_name(table.name), used_class_names)
        decorator_args = [repr(table.name), f"pk={table.primary_key!r}"]
        if table.schema is not None:
            decorator_args.append(f"schema={table.schema!r}")
        lines.append(f"@{database_variable}.table({', '.join(decorator_args)})")
        lines.append(f"class {class_name}(BaseModel):")
        if not table.columns:
            lines.append("    pass")
        else:
            used_fields: set[str] = set()
            for column in table.columns:
                lines.append(
                    "    "
                    + _field_line(
                        column, _unique_name(_field_name(column.name), used_fields)
                    )
                )
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def _field_line(column: ColumnSnapshot, field_name: str) -> str:
    annotation = _python_type(column.kind)
    optional = (
        column.nullable or column.server_default is not None or column.autoincrement
    )
    if optional:
        annotation = f"{annotation} | None"
    field = f"{field_name}: {annotation}"
    if field_name == column.name:
        return f"{field} = None" if optional else field
    if optional:
        return f"{field} = Field(default=None, alias={column.name!r})"
    return f"{field} = Field(alias={column.name!r})"


def _python_type(kind: str) -> str:
    normalized = kind.split(":", 1)[0]
    return {
        "str": "str",
        "int": "int",
        "float": "float",
        "bool": "bool",
        "uuid": "UUID",
        "date": "date",
        "datetime": "datetime",
        "decimal": "Decimal",
        "bytes": "bytes",
        "dict": "dict[str, Any]",
        "list": "list[Any]",
        "json": "dict[str, Any]",
        "model_json": "dict[str, Any]",
        "enum": "str",
    }.get(normalized, "Any")


def _class_name(table_name: str) -> str:
    parts = [part for part in re.split(r"[^0-9A-Za-z]+", table_name) if part]
    name = "".join(part[:1].upper() + part[1:] for part in parts)
    if not name or name[0].isdigit():
        name = f"Reflected{name or 'Table'}"
    return name


def _field_name(column_name: str) -> str:
    return _python_identifier(column_name)


def _python_identifier(value: str) -> str:
    name = re.sub(r"\W", "_", value)
    if not name:
        name = "value"
    if name[0].isdigit():
        name = f"_{name}"
    if keyword.iskeyword(name):
        name = f"{name}_"
    return name


def _unique_name(name: str, used: set[str]) -> str:
    candidate = name
    index = 2
    while candidate in used:
        candidate = f"{name}_{index}"
        index += 1
    used.add(candidate)
    return candidate
