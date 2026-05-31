"""Module providing OrmTableGenerator."""

import uuid
from datetime import date, datetime
from types import NoneType, UnionType
from typing import Any, Union, get_args, get_origin

from pydantic import BaseModel
from sqlalchemy import (
    JSON,
    Boolean,
    Column,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    MetaData,
    String,
    Table,
    UniqueConstraint,
)
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.asyncio import AsyncEngine

from ormdantic._introspect import FieldMetadata, is_dict_annotation, is_list_annotation, model_fields
from ormdantic.handler import TableName_From_Model, TypeConversionError
from ormdantic.models import Map, OrmTable


class OrmTableGenerator:
    def __init__(
        self,
        engine: AsyncEngine,
        metadata: MetaData,
        table_map: Map,
    ) -> None:
        """Initialize OrmTableGenerator."""
        self._engine = engine
        self._metadata = metadata
        self._table_map = table_map
        self._tables: list[str] = []

    async def init(self) -> None:
        """Generate SQL Alchemy tables."""
        for tablename, table_data in self._table_map.name_to_data.items():
            unique_constraints = (
                UniqueConstraint(*cols, name=f"{'_'.join(cols)}_constraint")
                for cols in table_data.unique_constraints
            )
            self._tables.append(tablename)
            Table(
                tablename,
                self._metadata,
                *self._get_columns(table_data),
                *unique_constraints,
            )
        async with self._engine.begin() as conn:
            await conn.run_sync(self._metadata.create_all)

    def _get_columns(
        self,
        table_data: OrmTable,  # type: ignore
    ) -> tuple[Column[Any] | Column[Any], ...]:
        columns = []
        for field_name, field in model_fields(table_data.model).items():
            kwargs = {
                "primary_key": field_name == table_data.pk,
                "index": field_name in table_data.indexed,
                "unique": field_name in table_data.unique,
                "nullable": not field.required,
            }
            if field_name in table_data.back_references:
                continue
            column = self._get_column(field_name, field, **kwargs)
            if column is not None:
                columns.append(column)
        return tuple(columns)

    def _get_column(
        self, field_name: str, field: FieldMetadata, **kwargs: Any
    ) -> Column[Any] | None:
        annotation = field.annotation
        origin = get_origin(annotation)
        if is_dict_annotation(annotation):
            return Column(field_name, JSON, **kwargs)
        if is_list_annotation(annotation):
            return self._get_column_from_type_args(
                field_name, field, **kwargs
            ) or Column(field_name, JSON, **kwargs)
        if origin:
            if origin in {UnionType, Union}:
                column = self._get_column_from_type_args(
                    field_name, field, **kwargs
                )
                if column is not None:
                    return column
                args = [
                    arg for arg in get_args(annotation) if arg is not NoneType
                ]
                if len(args) == 1:
                    return self._get_column_for_annotation(
                        field_name, args[0], field, **kwargs
                    )
                return Column(field_name, JSON, **kwargs)
            else:
                raise TypeConversionError(annotation)  # pragma: no cover
        return self._get_column_for_annotation(field_name, annotation, field, **kwargs)

    def _get_column_for_annotation(
        self,
        field_name: str,
        annotation: Any,
        field: FieldMetadata,
        **kwargs: Any,
    ) -> Column[Any] | None:
        if is_dict_annotation(annotation):
            return Column(field_name, JSON, **kwargs)
        if is_list_annotation(annotation):
            return Column(field_name, JSON, **kwargs)
        if annotation is uuid.UUID:
            col_type = (
                postgresql.UUID if self._engine.name == "postgres" else String(36)
            )
            return Column(field_name, col_type, **kwargs)  # type: ignore[arg-type]
        try:
            if issubclass(annotation, BaseModel):
                return Column(field_name, JSON, **kwargs)
            if issubclass(annotation, str):
                return Column(field_name, String(field.max_length), **kwargs)
            if issubclass(annotation, float):
                return Column(field_name, Float, **kwargs)
            if issubclass(annotation, int):
                # bool is a subclass of int -> nested check
                if issubclass(annotation, bool):
                    return Column(field_name, Boolean, **kwargs)
                return Column(field_name, Integer, **kwargs)
            if issubclass(annotation, date):
                # datetime is a subclass of date -> nested check
                if issubclass(annotation, datetime):
                    return Column(field_name, DateTime, **kwargs)
                return Column(field_name, Date, **kwargs)
        except TypeError as exc:
            raise TypeConversionError(annotation) from exc

        # Catchall for dict/list or any other.
        return Column(field_name, JSON, **kwargs)

    def _get_column_from_type_args(
        self, field_name: str, field: FieldMetadata, **kwargs: Any
    ) -> Column[Any] | None:
        for arg in get_args(field.annotation):
            if arg in [it.model for it in self._table_map.name_to_data.values()]:
                foreign_table = TableName_From_Model(arg, self._table_map)
                foreign_data = self._table_map.name_to_data[foreign_table]
                return Column(
                    field_name,
                    ForeignKey(f"{foreign_table}.{foreign_data.pk}"),
                    **kwargs,
                )
        return None  # pragma: no cover
