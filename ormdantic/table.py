"""Thin Python table facade over Rust-owned table handles."""

from __future__ import annotations

import importlib
from enum import Enum
from typing import Any, Generic

from ormdantic.engine import NativeResult
from ormdantic.events import EventRegistry
from ormdantic.expressions import (
    AssignmentExpression,
    OrderExpression,
    ProjectionExpression,
    QueryExpression,
    SelectExpressionQuery,
    SerializableExpression,
    UpdateExpressionQuery,
    select_query,
    update_query,
)
from ormdantic.loaders import LoaderOption, loader_depth
from ormdantic.models import Map, OrmTable, Result
from ormdantic.serializer import OrmSerializer
from ormdantic.types import ModelType
from ormdantic.values import py_type_to_sql

_ormdantic: Any = importlib.import_module("ormdantic._ormdantic")


class Order(Enum):
    """Sort direction for table queries."""

    asc = "asc"
    desc = "desc"


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

    async def select(
        self,
        *projections: ProjectionExpression | SerializableExpression,
        query: SelectExpressionQuery | None = None,
        where: QueryExpression | None = None,
        group_by: list[SerializableExpression] | None = None,
        having: QueryExpression | None = None,
        order_by: list[OrderExpression] | None = None,
        limit: int | None = None,
        offset: int | None = None,
        distinct: bool = False,
    ) -> NativeResult:
        """Execute a typed projection query and return raw projected rows."""
        if query is None:
            query = select_query(
                self.tablename,
                *projections,
                where=where,
                group_by=group_by or (),
                having=having,
                order_by=order_by or (),
                limit=limit,
                offset=offset,
                distinct=distinct,
            )
        elif projections:
            raise ValueError("pass either query= or projection arguments, not both")
        payload = query.to_query_payload()
        if payload["table"] != self.tablename:
            raise ValueError(
                f"typed query targets table '{payload['table']}', not '{self.tablename}'"
            )
        result = self._rust_handle.select_expression(payload)
        return NativeResult(
            columns=list(result["columns"]),
            rows=[tuple(row) for row in result["rows"]],
        )

    async def update_where(
        self,
        *assignments: AssignmentExpression,
        query: UpdateExpressionQuery | None = None,
        where: QueryExpression | None = None,
    ) -> NativeResult:
        """Execute a typed UPDATE expression query."""
        if query is None:
            query = update_query(self.tablename, *assignments, where=where)
        elif assignments:
            raise ValueError("pass either query= or assignment arguments, not both")
        payload = query.to_query_payload()
        if payload["table"] != self.tablename:
            raise ValueError(
                f"typed update targets table '{payload['table']}', not '{self.tablename}'"
            )
        result = self._rust_handle.update_expression(payload)
        return NativeResult(
            columns=list(result["columns"]),
            rows=[tuple(row) for row in result["rows"]],
        )

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

    @staticmethod
    def _normalize_where(where: dict[str, Any] | None) -> dict[str, Any]:
        if where is None:
            return {}
        return where

    def _compile_where(
        self, where: dict[str, Any] | QueryExpression | None
    ) -> tuple[Any, dict[str, Any]]:
        """Normalize public Python filter inputs into the Rust runtime payload."""
        if isinstance(where, QueryExpression):
            raw_where: Any = where.to_filter_tree()
        else:
            raw_where = self._normalize_where(where)
        normalized = _ormdantic.normalize_filters(raw_where)
        return normalized["filters"], dict(normalized["values"])
