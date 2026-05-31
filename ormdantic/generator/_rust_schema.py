from __future__ import annotations

import importlib
from collections.abc import Callable
from typing import Any, get_origin

from pydantic import BaseModel

from ormdantic._introspect import FieldMetadata, is_dict_annotation, is_list_annotation, model_fields
from ormdantic.handler import TypeConversionError
from ormdantic.models import Map

try:
    _ormdantic: Any | None = importlib.import_module("ormdantic._ormdantic")
except ImportError:  # pragma: no cover - exercised when extension is not built
    _ormdantic = None


def validate_table_map(table_map: Map) -> int | None:
    """Validate current Python table metadata through Rust when available."""
    if _ormdantic is None or not hasattr(_ormdantic, "validate_schema_tables"):
        return None

    tables = [
        (table.tablename, table.pk, list(table.columns))
        for table in table_map.name_to_data.values()
    ]
    return int(_ormdantic.validate_schema_tables(tables))


def compile_create_table_sql(table_map: Map, tablename: str, dialect: str) -> list[str]:
    rust = _require_schema_symbol("compile_create_table_sql")
    table = table_map.name_to_data[tablename]
    columns = [
        _column_descriptor(table_map, table, field_name, field)
        for field_name, field in model_fields(table.model).items()
        if field_name not in table.back_references
    ]
    return list(
        rust.compile_create_table_sql(
            dialect,
            table.tablename,
            columns,
            table.unique_constraints,
        )
    )


def compile_drop_table_sql(tablename: str, dialect: str) -> str:
    rust = _require_schema_symbol("compile_drop_table_sql")
    return str(rust.compile_drop_table_sql(dialect, tablename))


def _require_schema_symbol(symbol: str) -> Any:
    if _ormdantic is None or not hasattr(_ormdantic, symbol):
        raise RuntimeError(
            "Ormdantic vNext requires the Rust extension for schema compilation. "
            "Install the package with maturin or reinstall the wheel."
        )
    return _ormdantic


def _column_descriptor(
    table_map: Map, table: Any, field_name: str, field: FieldMetadata
) -> tuple[str, str, bool, bool, str | None, str | None, int | None]:
    relationship = table.relationships.get(field_name)
    foreign_table = relationship.foreign_table if relationship else None
    foreign_column = table_map.name_to_data[foreign_table].pk if foreign_table else None
    return (
        field_name,
        _field_kind(field),
        not field.required,
        field_name == table.pk,
        foreign_table,
        foreign_column,
        field.max_length,
    )


def _field_kind(field: FieldMetadata) -> str:
    annotation = field.annotation
    if get_origin(annotation) is Callable or annotation is Callable:
        raise TypeConversionError(annotation)
    if is_dict_annotation(annotation):
        return "dict"
    if is_list_annotation(annotation):
        return "list"
    if annotation.__class__.__name__ == "UnionType" or getattr(annotation, "__origin__", None):
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
    if getattr(annotation, "__name__", "") == "date":
        return "date"
    if getattr(annotation, "__name__", "") == "datetime":
        return "datetime"
    if isinstance(annotation, type) and issubclass(annotation, BaseModel):
        return "model_json"
    return "json"
