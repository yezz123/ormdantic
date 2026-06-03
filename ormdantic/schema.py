"""Schema descriptor helpers for the Rust runtime."""

from __future__ import annotations

import importlib
from collections.abc import Callable
from decimal import Decimal
from enum import Enum
from typing import Any, get_origin

from pydantic import BaseModel

from ormdantic._introspect import (
    FieldMetadata,
    is_dict_annotation,
    is_list_annotation,
    model_fields,
)
from ormdantic.errors import TypeConversionError
from ormdantic.models import Map

try:
    _ormdantic: Any | None = importlib.import_module("ormdantic._ormdantic")
except ImportError:  # pragma: no cover - exercised when extension is not built
    _ormdantic = None


def validate_table_map(table_map: Map) -> int | None:
    """Validate current Python table metadata through Rust when available."""
    if _ormdantic is None or not hasattr(_ormdantic, "validate_schema_tables"):
        return None
    tables = []
    for table in table_map.name_to_data.values():
        columns = [
            column_descriptor(table_map, table, field_name, field)
            for field_name, field in model_fields(table.model).items()
            if field_name not in table.back_references
        ]
        relationships = []
        for field_name, relationship in table.relationships.items():
            related = table_map.name_to_data[relationship.foreign_table]
            relationships.append(
                (
                    field_name,
                    relationship.foreign_table,
                    related.pk,
                    relationship.back_references,
                )
            )
        tables.append(
            (
                table.model.__name__,
                table.tablename,
                table.pk,
                columns,
                index_descriptors(table),
                table.unique_constraints,
                relationships,
            )
        )
    return int(_ormdantic.validate_schema_tables(tables))


def compile_create_table_sql(table_map: Map, tablename: str, dialect: str) -> list[str]:
    """Compile create-table DDL statements for a registered table."""
    rust = _require_schema_symbol("compile_create_table_sql")
    table = table_map.name_to_data[tablename]
    columns = [
        column_descriptor(table_map, table, field_name, field)
        for field_name, field in model_fields(table.model).items()
        if field_name not in table.back_references
    ]
    return list(
        rust.compile_create_table_sql(
            dialect,
            table.tablename,
            columns,
            index_descriptors(table),
            table.unique_constraints,
        )
    )


def compile_drop_table_sql(tablename: str, dialect: str) -> str:
    """Compile a drop-table DDL statement for a table name."""
    rust = _require_schema_symbol("compile_drop_table_sql")
    return str(rust.compile_drop_table_sql(dialect, tablename))


def column_descriptor(
    table_map: Map, table: Any, field_name: str, field: FieldMetadata
) -> tuple[
    str,
    str,
    bool,
    bool,
    str | None,
    str | None,
    int | None,
    bool,
    list[tuple[str, str, str]],
]:
    """Return a compact Rust schema descriptor for one model field."""
    relationship = table.relationships.get(field_name)
    foreign_table = relationship.foreign_table if relationship else None
    foreign_column = table_map.name_to_data[foreign_table].pk if foreign_table else None
    return (
        field_name,
        field_kind(field),
        not field.required,
        field_name == table.pk,
        foreign_table,
        foreign_column,
        field.max_length,
        field_name in table.unique,
        check_constraints(field_name, field),
    )


def index_descriptors(table: Any) -> list[tuple[str, list[str], bool]]:
    """Return compact Rust index descriptors for a table."""
    indexes = [
        (f"{table.tablename}_{column}_idx", [column], False) for column in table.indexed
    ]
    indexes.extend(
        (f"{table.tablename}_{column}_unique_idx", [column], True)
        for column in table.unique
    )
    return indexes


def field_kind(field: FieldMetadata) -> str:
    """Map a Pydantic field to a Rust schema field kind."""
    annotation = field.annotation
    if get_origin(annotation) is Callable or annotation is Callable:
        raise TypeConversionError(annotation)
    if is_dict_annotation(annotation):
        return "dict"
    if is_list_annotation(annotation):
        return "list"
    if annotation.__class__.__name__ == "UnionType" or getattr(
        annotation, "__origin__", None
    ):
        for arg in field.args:
            if arg is type(None):
                continue
            if is_dict_annotation(arg):
                return "dict"
            if is_list_annotation(arg):
                return "list"
            if isinstance(arg, type) and issubclass(arg, BaseModel):
                return "uuid"
            if isinstance(arg, type):
                annotation = arg
                break
    if getattr(annotation, "__name__", "") == "UUID":
        return "uuid"
    if annotation is str:
        return "str"
    if annotation is int:
        return "int"
    if annotation is float:
        return "float"
    if annotation is bool:
        return "bool"
    if annotation is Decimal:
        return "decimal"
    if annotation is bytes:
        return "bytes"
    if getattr(annotation, "__name__", "") == "date":
        return "date"
    if getattr(annotation, "__name__", "") == "datetime":
        return "datetime"
    if isinstance(annotation, type) and issubclass(annotation, Enum):
        return "enum"
    if isinstance(annotation, type) and issubclass(annotation, BaseModel):
        return "model_json"
    return "json"


def check_constraints(
    field_name: str, field: FieldMetadata
) -> list[tuple[str, str, str]]:
    """Return structured DDL check constraints for a Pydantic field."""
    checks = []
    if field.ge is not None:
        checks.append(("comparison", ">=", str(field.ge)))
    if field.gt is not None:
        checks.append(("comparison", ">", str(field.gt)))
    if field.le is not None:
        checks.append(("comparison", "<=", str(field.le)))
    if field.lt is not None:
        checks.append(("comparison", "<", str(field.lt)))
    if field.min_length is not None:
        checks.append(("length", ">=", str(field.min_length)))
    if field.max_length is not None:
        checks.append(("length", "<=", str(field.max_length)))
    return checks


def _require_schema_symbol(symbol: str) -> Any:
    if _ormdantic is None or not hasattr(_ormdantic, symbol):
        raise RuntimeError(
            "Ormdantic requires the Rust extension for schema compilation. "
            "Install the package with maturin or reinstall the wheel."
        )
    return _ormdantic
