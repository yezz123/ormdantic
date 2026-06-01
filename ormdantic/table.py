"""Thin Python table facade over Rust-owned table handles."""

from __future__ import annotations

from enum import Enum
from typing import Any, Generic

from ormdantic.engine import NativeResult
from ormdantic.events import EventRegistry
from ormdantic.expressions import QueryExpression
from ormdantic.loaders import LoaderOption, loader_depth
from ormdantic.models import Map, OrmTable, Result
from ormdantic.serializer import OrmSerializer
from ormdantic.types import ModelType
from ormdantic.values import py_type_to_sql


class Order(Enum):
    """Sort direction for table queries."""

    asc = "asc"
    desc = "desc"


FILTER_OPERATORS = {
    "eq",
    "ne",
    "lt",
    "le",
    "gt",
    "ge",
    "like",
    "ilike",
    "in",
    "not_in",
    "is_null",
    "is_not_null",
}


class Table(Generic[ModelType]):
    """User-facing table handle backed by a Rust `PyTableHandle`."""

    def __init__(
        self,
        *,
        table_data: OrmTable[ModelType],
        table_map: Map,
        rust_handle: Any,
        events: EventRegistry,
    ) -> None:
        self._table_data = table_data
        self._table_map = table_map
        self._rust_handle = rust_handle
        self._events = events
        self.tablename = table_data.tablename
        self.columns = table_data.columns

    async def find_one(
        self,
        pk: Any,
        depth: int = 0,
        load: list[LoaderOption] | None = None,
    ) -> ModelType | None:
        """Find a model by primary key."""
        depth = max(depth, loader_depth(load))
        result = self._rust_handle.find_one(py_type_to_sql(self._table_map, pk), depth)
        return self._deserialize(result, is_array=False, depth=depth)

    async def find_many(
        self,
        where: dict[str, Any] | QueryExpression | None = None,
        order_by: list[str] | None = None,
        order: Order = Order.asc,
        limit: int = 0,
        offset: int = 0,
        depth: int = 0,
        load: list[LoaderOption] | None = None,
    ) -> Result[ModelType]:
        """Find many model instances."""
        depth = max(depth, loader_depth(load))
        filters, values = self._compile_where(where)
        result = self._rust_handle.find_many(
            filters,
            values,
            order_by or [],
            order.value,
            limit or None,
            offset or None,
            depth,
        )
        data = self._deserialize(result, is_array=True, depth=depth) or []
        return Result(offset=offset, limit=limit, data=data)

    async def insert(self, model_instance: ModelType) -> ModelType:
        """Insert a model instance."""
        await self._events.dispatch(
            "before_insert", model=model_instance, table=self._table_data
        )
        self._rust_handle.insert(self._payload(model_instance))
        await self._events.dispatch(
            "after_insert", model=model_instance, table=self._table_data
        )
        return model_instance

    async def update(self, model_instance: ModelType) -> ModelType:
        """Update a model instance."""
        await self._events.dispatch(
            "before_update", model=model_instance, table=self._table_data
        )
        self._rust_handle.update(self._payload(model_instance))
        await self._events.dispatch(
            "after_update", model=model_instance, table=self._table_data
        )
        return model_instance

    async def upsert(self, model_instance: ModelType) -> ModelType:
        """Insert or update a model instance."""
        await self._events.dispatch(
            "before_upsert", model=model_instance, table=self._table_data
        )
        self._rust_handle.upsert(self._payload(model_instance))
        await self._events.dispatch(
            "after_upsert", model=model_instance, table=self._table_data
        )
        return model_instance

    async def delete(self, pk: Any) -> bool:
        """Delete a model by primary key."""
        await self._events.dispatch("before_delete", pk=pk, table=self._table_data)
        self._rust_handle.delete(py_type_to_sql(self._table_map, pk))
        await self._events.dispatch("after_delete", pk=pk, table=self._table_data)
        return True

    async def count(
        self, where: dict[str, Any] | QueryExpression | None = None, depth: int = 0
    ) -> int:
        """Count records matching an optional filter."""
        filters, values = self._compile_where(where)
        result = self._rust_handle.count(filters, values)
        return NativeResult(
            columns=list(result["columns"]),
            rows=[tuple(row) for row in result["rows"]],
        ).scalar()

    def _deserialize(
        self, result: dict[str, Any], *, is_array: bool, depth: int
    ) -> Any:
        native_result = NativeResult(
            columns=list(result["columns"]),
            rows=[tuple(row) for row in result["rows"]],
        )
        return OrmSerializer[ModelType | None](
            table_data=self._table_data,
            table_map=self._table_map,
            result_set=native_result,
            is_array=is_array,
            depth=depth,
        ).deserialize()

    def _payload(self, model_instance: ModelType) -> dict[str, Any]:
        return {
            column: py_type_to_sql(self._table_map, getattr(model_instance, column))
            for column in self._table_data.columns
        }

    def _parse_where(
        self, where: dict[str, Any]
    ) -> tuple[list[tuple[str, str, list[str]]], dict[str, Any]]:
        filters: list[tuple[str, str, list[str]]] = []
        values: dict[str, Any] = {}
        for key, value in where.items():
            column, operator = self._split_filter_key(key)
            if operator in {"is_null", "is_not_null"}:
                filters.append((column, operator, []))
                continue
            if operator in {"in", "not_in"}:
                param_names = []
                for idx, item in enumerate(value):
                    param_name = f"{column}__{operator}_{idx}"
                    param_names.append(param_name)
                    values[param_name] = py_type_to_sql(self._table_map, item)
                filters.append((column, operator, param_names))
                continue
            filters.append((column, operator, [key]))
            values[key] = py_type_to_sql(self._table_map, value)
        return filters, values

    @staticmethod
    def _normalize_where(where: dict[str, Any] | None) -> dict[str, Any]:
        if where is None:
            return {}
        return where

    def _compile_where(
        self, where: dict[str, Any] | QueryExpression | None
    ) -> tuple[Any, dict[str, Any]]:
        if isinstance(where, QueryExpression):
            return self._parse_expression(where, "expr")
        return self._parse_where(self._normalize_where(where))

    def _parse_expression(
        self, expression: QueryExpression, prefix: str
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        if expression.connector == "leaf":
            filters, values = self._parse_where(expression.filters or {})
            filters, values = self._rename_filter_params(filters, values, prefix)
            return {"connector": "leaf", "filters": filters}, values
        children = []
        expression_values: dict[str, Any] = {}
        for idx, child in enumerate(expression.children):
            child_tree, child_values = self._parse_expression(child, f"{prefix}_{idx}")
            children.append(child_tree)
            expression_values.update(child_values)
        return {
            "connector": expression.connector,
            "children": children,
        }, expression_values

    @staticmethod
    def _rename_filter_params(
        filters: list[tuple[str, str, list[str]]], values: dict[str, Any], prefix: str
    ) -> tuple[list[tuple[str, str, list[str]]], dict[str, Any]]:
        renamed_values: dict[str, Any] = {}
        renamed_filters = []
        for column, operator, params in filters:
            renamed_params = []
            for param in params:
                renamed = f"{prefix}__{param}"
                renamed_params.append(renamed)
                renamed_values[renamed] = values[param]
            renamed_filters.append((column, operator, renamed_params))
        return renamed_filters, renamed_values

    @staticmethod
    def _split_filter_key(key: str) -> tuple[str, str]:
        if "__" not in key:
            return key, "eq"
        column, operator = key.rsplit("__", 1)
        if operator not in FILTER_OPERATORS:
            return key, "eq"
        return column, operator
