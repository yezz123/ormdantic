"""Thin Python table facade over Rust-owned table handles."""

from __future__ import annotations

import importlib
import logging
from dataclasses import dataclass
from enum import Enum
from time import perf_counter
from typing import Any, Generic, Literal

from pydantic import BaseModel

from ormdantic.engine import NativeResult
from ormdantic.errors import (
    HydrationError,
    QueryCompilationError,
    QueryExecutionError,
    RelationshipLoadingError,
    classify_native_error,
    redact_parameter_values,
)
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
from ormdantic.expressions import (
    column as expr_column,
)
from ormdantic.expressions import (
    count as count_expr,
)
from ormdantic.loaders import LoaderOption, path_parts
from ormdantic.models import Map, OrmTable, Result
from ormdantic.serializer import OrmSerializer
from ormdantic.types import ModelType
from ormdantic.values import py_type_to_sql

_ormdantic: Any = importlib.import_module("ormdantic._ormdantic")
DEFAULT_SELECTIN_BATCH_SIZE = 500
QUERY_LOGGER = logging.getLogger("ormdantic.query")


class Order(Enum):
    """Sort direction for table queries."""

    asc = "asc"
    desc = "desc"


@dataclass(frozen=True)
class _ResolvedLoadPlan:
    depth: int
    paths: tuple[str, ...] | None
    selectin_paths: tuple[str, ...] = ()
    options: tuple[LoaderOption, ...] = ()
    use_selectin: bool = False


class Table(Generic[ModelType]):
    """User-facing table handle backed by a Rust `PyTableHandle`."""

    def __init__(
        self,
        *,
        table_data: OrmTable[ModelType],
        table_map: Map,
        rust_handle: Any,
        events: EventRegistry,
        runtime: Any | None = None,
        connection: str | None = None,
        debug: bool = False,
        log_queries: bool = False,
    ) -> None:
        self._table_data = table_data
        self._table_map = table_map
        self._rust_handle = rust_handle
        self._events = events
        self._runtime = runtime
        self._connection = connection
        self._debug = debug
        self._log_queries = log_queries
        self.tablename = table_data.tablename
        self.columns = table_data.columns

    async def find_one(
        self,
        pk: Any,
        depth: int = 0,
        load: list[LoaderOption] | None = None,
    ) -> ModelType | None:
        """Find a model by primary key."""
        load_plan = self._resolve_load_plan(depth, load)
        if load_plan.paths:
            joined_filters, joined_order_by, joined_values = (
                self._joined_loader_query_parts(load_plan)
            )
            joined_values[self._table_data.pk] = py_type_to_sql(self._table_map, pk)
            result = await self._execute_rust(
                "select_one",
                lambda: self._rust_handle.find_one_with_paths(
                    joined_values,
                    list(load_plan.paths),
                    joined_filters,
                    joined_order_by,
                ),
                parameters=joined_values,
                context={"load_paths": list(load_plan.paths)},
            )
        else:
            primary_key = py_type_to_sql(self._table_map, pk)
            values = {self._table_data.pk: primary_key}
            result = await self._execute_rust(
                "select_one",
                lambda: self._rust_handle.find_one(primary_key, load_plan.depth),
                parameters=values,
                compile_query=(
                    lambda: (
                        self._compile_select_pk_query()
                        if load_plan.depth == 0
                        else None
                    )
                ),
                context={"depth": load_plan.depth},
            )
        model = await self._deserialize(
            result,
            is_array=False,
            depth=load_plan.depth,
            load_paths=load_plan.paths,
            load_options=load_plan.options,
        )
        if model is not None and load_plan.selectin_paths:
            await self._load_selectin_graph([model], load_plan)
        return model

    async def find_many(
        self,
        where: dict[str, Any] | QueryExpression | None = None,
        order_by: list[str | OrderExpression] | None = None,
        order: Order = Order.asc,
        limit: int = 0,
        offset: int = 0,
        depth: int = 0,
        load: list[LoaderOption] | None = None,
    ) -> Result[ModelType]:
        """Find many model instances."""
        load_plan = self._resolve_load_plan(depth, load)
        if self._requires_expression_select(where, order_by):
            return await self._find_many_expression(
                where=where if isinstance(where, QueryExpression) else None,
                order_by=order_by or [],
                order=order,
                limit=limit,
                offset=offset,
                load_plan=load_plan,
            )
        filters, values = self._compile_where(where)
        legacy_order_by = self._legacy_order_columns(order_by)
        if load_plan.paths:
            joined_filters, joined_order_by, joined_values = (
                self._joined_loader_query_parts(load_plan)
            )
            values.update(joined_values)
            result = await self._execute_rust(
                "select_many",
                lambda: self._rust_handle.find_many_with_paths(
                    filters,
                    values,
                    legacy_order_by,
                    order.value,
                    limit or None,
                    offset or None,
                    list(load_plan.paths),
                    joined_filters,
                    joined_order_by,
                ),
                parameters=values,
                context={
                    "limit": limit or None,
                    "offset": offset or None,
                    "load_paths": list(load_plan.paths),
                },
            )
            data = (
                await self._deserialize(
                    result,
                    is_array=True,
                    depth=load_plan.depth,
                    load_paths=load_plan.paths,
                    load_options=load_plan.options,
                )
                or []
            )
            if load_plan.selectin_paths:
                await self._load_selectin_graph(data, load_plan)
        else:
            result = await self._execute_rust(
                "select_many",
                lambda: self._rust_handle.find_many(
                    filters,
                    values,
                    legacy_order_by,
                    order.value,
                    limit or None,
                    offset or None,
                    load_plan.depth,
                ),
                parameters=values,
                compile_query=lambda: self._compile_find_many_query(
                    filters,
                    legacy_order_by,
                    order.value,
                    limit or None,
                    offset or None,
                ),
                context={
                    "limit": limit or None,
                    "offset": offset or None,
                    "depth": load_plan.depth,
                },
            )
            data = (
                await self._deserialize(
                    result,
                    is_array=True,
                    depth=load_plan.depth,
                    load_paths=load_plan.paths,
                    load_options=load_plan.options,
                )
                or []
            )
            if load_plan.selectin_paths:
                await self._load_selectin_graph(data, load_plan)
        return Result(offset=offset, limit=limit, data=data)

    async def insert(self, model_instance: ModelType) -> ModelType:
        """Insert a model instance."""
        await self._events.dispatch(
            "before_create", model=model_instance, table=self._table_data
        )
        await self._events.dispatch(
            "before_insert", model=model_instance, table=self._table_data
        )
        payload = self._payload(model_instance, mode="insert")
        await self._execute_rust(
            "insert",
            lambda: self._rust_handle.insert(payload),
            parameters=payload,
            compile_query=lambda: self._compile_insert_query(payload),
        )
        await self._events.dispatch(
            "after_insert", model=model_instance, table=self._table_data
        )
        await self._events.dispatch(
            "after_create", model=model_instance, table=self._table_data
        )
        return model_instance

    async def update(self, model_instance: ModelType) -> ModelType:
        """Update a model instance."""
        await self._events.dispatch(
            "before_update", model=model_instance, table=self._table_data
        )
        payload = self._payload(model_instance, mode="update")
        await self._execute_rust(
            "update",
            lambda: self._rust_handle.update(payload),
            parameters=payload,
            compile_query=lambda: self._compile_update_query(payload),
        )
        await self._events.dispatch(
            "after_update", model=model_instance, table=self._table_data
        )
        return model_instance

    async def upsert(self, model_instance: ModelType) -> ModelType:
        """Insert or update a model instance."""
        await self._events.dispatch(
            "before_upsert", model=model_instance, table=self._table_data
        )
        payload = self._payload(model_instance, mode="upsert")
        await self._execute_rust(
            "upsert",
            lambda: self._rust_handle.upsert(payload),
            parameters=payload,
            compile_query=lambda: self._compile_upsert_query(payload),
        )
        await self._events.dispatch(
            "after_upsert", model=model_instance, table=self._table_data
        )
        return model_instance

    async def delete(self, pk: Any) -> bool:
        """Delete a model by primary key."""
        await self._events.dispatch("before_delete", pk=pk, table=self._table_data)
        primary_key = py_type_to_sql(self._table_map, pk)
        await self._execute_rust(
            "delete",
            lambda: self._rust_handle.delete(primary_key),
            parameters={self._table_data.pk: primary_key},
            compile_query=self._compile_delete_query,
        )
        await self._events.dispatch("after_delete", pk=pk, table=self._table_data)
        return True

    async def count(
        self, where: dict[str, Any] | QueryExpression | None = None, depth: int = 0
    ) -> int:
        """Count records matching an optional filter."""
        if isinstance(where, QueryExpression) and not where.supports_legacy_filters():
            result = await self.select(count_expr(), where=where)
            return int(result.scalar())
        filters, values = self._compile_where(where)
        result = await self._execute_rust(
            "count",
            lambda: self._rust_handle.count(filters, values),
            parameters=values,
            compile_query=lambda: self._compile_count_query(filters),
        )
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
        result = await self._execute_rust(
            "select",
            lambda: self._rust_handle.select_expression(payload),
            parameters=dict(payload.get("values") or {}),
            compile_query=lambda: self._compile_typed_select_query(payload),
        )
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
        result = await self._execute_rust(
            "update",
            lambda: self._rust_handle.update_expression(payload),
            parameters=dict(payload.get("values") or {}),
            compile_query=lambda: self._compile_typed_update_query(payload),
        )
        return NativeResult(
            columns=list(result["columns"]),
            rows=[tuple(row) for row in result["rows"]],
        )

    async def _find_many_expression(
        self,
        *,
        where: QueryExpression | None,
        order_by: list[str | OrderExpression],
        order: Order,
        limit: int,
        offset: int,
        load_plan: _ResolvedLoadPlan,
    ) -> Result[ModelType]:
        if load_plan.depth > 0 or load_plan.paths:
            data = await self._find_many_expression_by_primary_keys(
                where=where,
                order_by=order_by,
                order=order,
                limit=limit,
                offset=offset,
                load_plan=load_plan,
            )
            return Result(offset=offset, limit=limit, data=data)

        query = select_query(
            self.tablename,
            *(
                expr_column(name).as_(f"{self.tablename}\\{name}")
                for name in self._table_data.columns
            ),
            where=where,
            order_by=self._expression_order_by(order_by, order),
            limit=limit or None,
            offset=offset or None,
        )
        payload = query.to_query_payload()
        result = await self._execute_rust(
            "select",
            lambda: self._rust_handle.select_expression(payload),
            parameters=dict(payload.get("values") or {}),
            compile_query=lambda: self._compile_typed_select_query(payload),
            context={"limit": limit or None, "offset": offset or None},
        )
        data = (
            await self._deserialize(
                result,
                is_array=True,
                depth=0,
                load_paths=load_plan.paths,
                load_options=load_plan.options,
            )
            or []
        )
        if load_plan.selectin_paths:
            await self._load_selectin_graph(data, load_plan)
        return Result(offset=offset, limit=limit, data=data)

    async def _find_many_expression_by_primary_keys(
        self,
        *,
        where: QueryExpression | None,
        order_by: list[str | OrderExpression],
        order: Order,
        limit: int,
        offset: int,
        load_plan: _ResolvedLoadPlan,
    ) -> list[ModelType]:
        query = select_query(
            self.tablename,
            expr_column(self._table_data.pk),
            where=where,
            order_by=self._expression_order_by(order_by, order),
            limit=limit or None,
            offset=offset or None,
        )
        payload = query.to_query_payload()
        result = await self._execute_rust(
            "select",
            lambda: self._rust_handle.select_expression(payload),
            parameters=dict(payload.get("values") or {}),
            compile_query=lambda: self._compile_typed_select_query(payload),
            context={"limit": limit or None, "offset": offset or None},
        )
        native_result = NativeResult(
            columns=list(result["columns"]),
            rows=[tuple(row) for row in result["rows"]],
        )
        primary_keys = [row[0] for row in native_result]
        if not primary_keys:
            return []

        filters, values = self._compile_where(
            expr_column(self._table_data.pk).in_(primary_keys)
        )
        if load_plan.paths:
            joined_filters, joined_order_by, joined_values = (
                self._joined_loader_query_parts(load_plan)
            )
            values.update(joined_values)
            loaded = await self._execute_rust(
                "select_many",
                lambda: self._rust_handle.find_many_with_paths(
                    filters,
                    values,
                    [],
                    Order.asc.value,
                    None,
                    None,
                    list(load_plan.paths),
                    joined_filters,
                    joined_order_by,
                ),
                parameters=values,
                context={"load_paths": list(load_plan.paths)},
            )
            data = (
                await self._deserialize(
                    loaded,
                    is_array=True,
                    depth=load_plan.depth,
                    load_paths=load_plan.paths,
                    load_options=load_plan.options,
                )
                or []
            )
        else:
            loaded = await self._execute_rust(
                "select_many",
                lambda: self._rust_handle.find_many(
                    filters,
                    values,
                    [],
                    Order.asc.value,
                    None,
                    None,
                    load_plan.depth,
                ),
                parameters=values,
                compile_query=lambda: self._compile_find_many_query(
                    filters,
                    [],
                    Order.asc.value,
                    None,
                    None,
                ),
                context={"depth": load_plan.depth},
            )
            data = (
                await self._deserialize(
                    loaded,
                    is_array=True,
                    depth=load_plan.depth,
                    load_paths=load_plan.paths,
                    load_options=load_plan.options,
                )
                or []
            )
        if load_plan.selectin_paths:
            await self._load_selectin_graph(data, load_plan)
        return self._order_by_primary_key_sequence(data, primary_keys)

    async def _deserialize(
        self,
        result: dict[str, Any],
        *,
        is_array: bool,
        depth: int,
        load_paths: tuple[str, ...] | None = None,
        load_options: tuple[LoaderOption, ...] = (),
    ) -> Any:
        native_result = NativeResult(
            columns=list(result["columns"]),
            rows=[tuple(row) for row in result["rows"]],
        )
        payload = {
            "operation": "hydrate",
            "table": self._table_data,
            "table_name": self.tablename,
            "model": self._table_data.model,
            "model_name": self._table_data.model.__name__,
            "row_count": len(native_result._rows),
            "is_array": is_array,
            "depth": depth,
            "load_paths": list(load_paths or ()),
        }
        await self._events.dispatch("before_hydration", **payload)
        started = perf_counter()
        try:
            hydrated = OrmSerializer[ModelType | None](
                table_data=self._table_data,
                table_map=self._table_map,
                result_set=native_result,
                is_array=is_array,
                depth=depth,
                load_paths=load_paths,
                load_options=load_options,
            ).deserialize()
        except Exception as exc:
            duration_ms = self._duration_ms(started)
            context = self._context("hydrate", row_count=len(native_result._rows))
            error = classify_native_error(
                exc,
                default=HydrationError,
                message=f"hydration failed for table '{self.tablename}'",
                context=context,
            )
            await self._events.dispatch(
                "after_hydration",
                **payload,
                duration_ms=duration_ms,
                error=error,
            )
            raise error from exc
        duration_ms = self._duration_ms(started)
        await self._events.dispatch(
            "after_hydration",
            **payload,
            duration_ms=duration_ms,
            error=None,
        )
        return hydrated

    async def _load_selectin_graph(
        self, roots: list[ModelType], load_plan: _ResolvedLoadPlan
    ) -> None:
        if not roots or not load_plan.selectin_paths:
            return
        if self._runtime is None:
            raise RuntimeError(
                "select-in relationship loading requires an initialized runtime"
            )
        identity_map: dict[tuple[type[Any], str], Any] = {}
        for root in roots:
            self._remember_identity(root, self._table_data, identity_map)
        option_by_path = {
            option.path.replace(".", "/"): option for option in load_plan.options
        }
        await self._load_selectin_tree(
            roots,
            self._table_data,
            self._path_tree(load_plan.selectin_paths),
            (),
            identity_map,
            option_by_path,
            set(load_plan.paths or ()),
        )

    async def _load_selectin_tree(
        self,
        parents: list[Any],
        table_data: OrmTable[Any],
        path_tree: dict[str, Any],
        path_prefix: tuple[str, ...],
        identity_map: dict[tuple[type[Any], str], Any],
        option_by_path: dict[str, LoaderOption],
        joined_paths: set[str],
    ) -> None:
        if not parents:
            return
        for field_name, subtree in path_tree.items():
            relationship = table_data.relationships[field_name]
            related_table = self._table_map.name_to_data[relationship.foreign_table]
            path = (*path_prefix, field_name)
            option = option_by_path.get(self._slash_path(path))
            if self._joined_path_contains(joined_paths, path):
                related = self._loaded_relationship_values(
                    parents, field_name, related_table
                )
                related = [
                    self._remember_identity(model, related_table, identity_map)
                    for model in related
                ]
            else:
                try:
                    related = await self._selectin_load_relationship(
                        parents,
                        table_data,
                        field_name,
                        relationship.back_references,
                        related_table,
                        option,
                        identity_map,
                    )
                except Exception as exc:
                    context = self._context(
                        "relationship_load",
                        relationship=".".join(path),
                        source_table=table_data.tablename,
                        target_table=related_table.tablename,
                    )
                    error = RelationshipLoadingError(
                        "relationship loading failed for "
                        f"'{table_data.model.__name__}.{field_name}'",
                        context=context,
                        cause=exc,
                    )
                    raise error from exc
            if subtree:
                await self._load_selectin_tree(
                    related,
                    related_table,
                    subtree,
                    path,
                    identity_map,
                    option_by_path,
                    joined_paths,
                )

    async def _selectin_load_relationship(
        self,
        parents: list[Any],
        table_data: OrmTable[Any],
        field_name: str,
        back_reference: str | None,
        related_table: OrmTable[Any],
        option: LoaderOption | None,
        identity_map: dict[tuple[type[Any], str], Any],
    ) -> list[Any]:
        related_handle = self._related_table(related_table)
        if back_reference is not None:
            parent_ids = self._unique_values(
                getattr(parent, table_data.pk) for parent in parents
            )
            if not parent_ids:
                for parent in parents:
                    object.__setattr__(parent, field_name, [])
                return []
            children = []
            for batch in self._selectin_batches(parent_ids, option):
                where = self._selectin_where(back_reference, batch, option)
                children.extend((await related_handle.find_many(where=where)).data)
            children = [
                self._remember_identity(child, related_table, identity_map)
                for child in children
            ]
            children_by_parent: dict[str, list[Any]] = {}
            for child in children:
                children_by_parent.setdefault(
                    str(getattr(child, back_reference)), []
                ).append(child)
            assigned: list[Any] = []
            for parent in parents:
                value = children_by_parent.get(str(getattr(parent, table_data.pk)), [])
                value = self._filter_and_order_relationship(value, option)
                object.__setattr__(parent, field_name, value)
                assigned.extend(value)
            return self._unique_models(assigned, related_table)

        foreign_keys = self._unique_values(
            self._foreign_key_value(getattr(parent, field_name, None))
            for parent in parents
        )
        if not foreign_keys:
            for parent in parents:
                object.__setattr__(parent, field_name, None)
            return []
        related_rows = []
        for batch in self._selectin_batches(foreign_keys, option):
            where = self._selectin_where(related_table.pk, batch, option)
            related_rows.extend((await related_handle.find_many(where=where)).data)
        related_by_pk = {
            str(getattr(related, related_table.pk)): self._remember_identity(
                related, related_table, identity_map
            )
            for related in related_rows
        }
        scalar_assigned: list[Any] = []
        for parent in parents:
            key = self._foreign_key_value(getattr(parent, field_name, None))
            related_value = related_by_pk.get(str(key)) if key is not None else None
            related_value = self._filter_and_order_relationship(related_value, option)
            object.__setattr__(parent, field_name, related_value)
            if related_value is not None:
                scalar_assigned.append(related_value)
        return self._unique_models(scalar_assigned, related_table)

    def _related_table(self, table_data: OrmTable[Any]) -> "Table[Any]":
        if self._runtime is None:
            raise RuntimeError(
                "select-in relationship loading requires an initialized runtime"
            )
        return Table(
            table_data=table_data,
            table_map=self._table_map,
            rust_handle=self._runtime.table(table_data.model.__name__),
            events=self._events,
            runtime=self._runtime,
            connection=self._connection,
            debug=self._debug,
            log_queries=self._log_queries,
        )

    @staticmethod
    def _joined_path_contains(joined_paths: set[str], path: tuple[str, ...]) -> bool:
        current = "/".join(path)
        return any(
            joined == current or joined.startswith(f"{current}/")
            for joined in joined_paths
        )

    @staticmethod
    def _loaded_relationship_values(
        parents: list[Any], field_name: str, related_table: OrmTable[Any]
    ) -> list[Any]:
        related: list[Any] = []
        for parent in parents:
            value = getattr(parent, field_name, None)
            if isinstance(value, list):
                related.extend(item for item in value if isinstance(item, BaseModel))
            elif isinstance(value, BaseModel):
                related.append(value)
        unique: dict[tuple[type[Any], str], Any] = {}
        for item in related:
            primary_key = getattr(item, related_table.pk, id(item))
            unique.setdefault((type(item), str(primary_key)), item)
        return list(unique.values())

    @staticmethod
    def _path_tree(load_paths: tuple[str, ...]) -> dict[str, Any]:
        tree: dict[str, Any] = {}
        for path in load_paths:
            node = tree
            for part in path.replace(".", "/").split("/"):
                if part:
                    node = node.setdefault(part, {})
        return tree

    @staticmethod
    def _selectin_where(
        key_column: str, values: list[Any], option: LoaderOption | None
    ) -> dict[str, Any]:
        where = {f"{key_column}__in": values}
        if option and option.filter_by:
            where.update(option.filter_by)
        return where

    @staticmethod
    def _foreign_key_value(value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, BaseModel):
            return None
        return value

    @staticmethod
    def _unique_values(values: Any) -> list[Any]:
        unique: dict[str, Any] = {}
        for value in values:
            if value is not None:
                unique.setdefault(str(value), value)
        return list(unique.values())

    def _selectin_batches(
        self, values: list[Any], option: LoaderOption | None
    ) -> list[list[Any]]:
        batch_size = self._selectin_batch_size(option)
        return [
            values[index : index + batch_size]
            for index in range(0, len(values), batch_size)
        ]

    def _selectin_batch_size(self, option: LoaderOption | None) -> int:
        requested = option.batch_size if option and option.batch_size else None
        requested = requested or DEFAULT_SELECTIN_BATCH_SIZE
        max_bind_parameters = self._max_bind_parameters()
        if max_bind_parameters is None:
            return requested
        reserved_filter_binds = (
            len(option.filter_by) if option and option.filter_by else 0
        )
        backend_limit = max(1, max_bind_parameters - reserved_filter_binds)
        return min(requested, backend_limit)

    def _max_bind_parameters(self) -> int | None:
        get_max_bind_parameters = getattr(
            self._rust_handle, "max_bind_parameters", None
        )
        if get_max_bind_parameters is None:
            return None
        value = get_max_bind_parameters()
        return int(value) if value is not None else None

    @staticmethod
    def _remember_identity(
        model: Any,
        table_data: OrmTable[Any],
        identity_map: dict[tuple[type[Any], str], Any],
    ) -> Any:
        key = (table_data.model, str(getattr(model, table_data.pk)))
        existing = identity_map.get(key)
        if existing is not None:
            return existing
        identity_map[key] = model
        return model

    @staticmethod
    def _unique_models(models: list[Any], table_data: OrmTable[Any]) -> list[Any]:
        unique: dict[str, Any] = {}
        for model in models:
            unique.setdefault(str(getattr(model, table_data.pk)), model)
        return list(unique.values())

    def _filter_and_order_relationship(
        self, value: Any, option: LoaderOption | None
    ) -> Any:
        if option is None:
            return value
        if isinstance(value, list):
            items = [
                item for item in value if self._matches_loader_filter(item, option)
            ]
            for column in reversed(option.order_by):
                descending = column.startswith("-")
                key = column[1:] if descending else column
                items.sort(
                    key=lambda item: (
                        getattr(item, key, None) is None,
                        getattr(item, key, None),
                    ),
                    reverse=descending,
                )
            return items
        if value is None:
            return None
        return value if self._matches_loader_filter(value, option) else None

    @staticmethod
    def _matches_loader_filter(value: Any, option: LoaderOption) -> bool:
        for column, expected in (option.filter_by or {}).items():
            actual = getattr(value, column, None)
            if actual != expected and str(actual) != str(expected):
                return False
        return True

    def _joined_loader_query_parts(
        self, load_plan: _ResolvedLoadPlan
    ) -> tuple[
        list[tuple[str, list[tuple[str, str, list[str]]]]],
        list[tuple[str, str, str]],
        dict[str, Any],
    ]:
        joined_paths = set(load_plan.paths or ())
        filters: list[tuple[str, list[tuple[str, str, list[str]]]]] = []
        order_by: list[tuple[str, str, str]] = []
        values: dict[str, Any] = {}
        for index, option in enumerate(load_plan.options):
            path = option.path.replace(".", "/")
            if path not in joined_paths:
                continue
            table_alias = f"{self.tablename}/{path}"
            filter_specs = []
            for column, value in (option.filter_by or {}).items():
                param = f"loader_{index}__{column}"
                values[param] = py_type_to_sql(self._table_map, value)
                filter_specs.append((column, "eq", [param]))
            if filter_specs:
                filters.append((table_alias, filter_specs))
            for column in option.order_by:
                direction = "desc" if column.startswith("-") else "asc"
                order_by.append(
                    (
                        table_alias,
                        column[1:] if column.startswith("-") else column,
                        direction,
                    )
                )
        return filters, order_by, values

    async def _execute_rust(
        self,
        operation: str,
        call: Any,
        *,
        parameters: dict[str, Any] | None = None,
        compile_query: Any | None = None,
        context: dict[str, Any] | None = None,
    ) -> Any:
        error_context = self._context(operation, **(context or {}))
        debug_payload = self._debug_payload(
            operation,
            parameters=parameters,
            compile_query=compile_query,
            context=error_context,
        )
        payload = self._event_payload(error_context, debug_payload)
        await self._events.dispatch("before_execute", **payload)
        started = perf_counter()
        try:
            result = call()
        except Exception as exc:
            duration_ms = self._duration_ms(started)
            native_error = classify_native_error(
                exc,
                default=QueryExecutionError,
                message=f"{operation} failed for table '{self.tablename}'",
                context={**error_context, **debug_payload},
            )
            await self._events.dispatch(
                "after_execute",
                **payload,
                duration_ms=duration_ms,
                row_count=None,
                error=native_error,
            )
            self._log_query(
                payload,
                duration_ms=duration_ms,
                row_count=None,
                error=native_error,
            )
            raise native_error from exc
        duration_ms = self._duration_ms(started)
        row_count = self._row_count(result)
        await self._events.dispatch(
            "after_execute",
            **payload,
            duration_ms=duration_ms,
            row_count=row_count,
            error=None,
        )
        self._log_query(
            payload,
            duration_ms=duration_ms,
            row_count=row_count,
            error=None,
        )
        return result

    def _debug_payload(
        self,
        operation: str,
        *,
        parameters: dict[str, Any] | None,
        compile_query: Any | None,
        context: dict[str, Any],
    ) -> dict[str, Any]:
        if not self._debug:
            return {"debug": False}
        compiled: dict[str, Any] | None = None
        if compile_query is not None:
            try:
                compiled = compile_query()
            except Exception as exc:
                raise QueryCompilationError(
                    f"{operation} compilation failed for table '{self.tablename}'",
                    context=context,
                    cause=exc,
                ) from exc
        bind_names = list((compiled or {}).get("params") or [])
        if not bind_names and parameters is not None:
            bind_names = list(parameters)
        return {
            "debug": True,
            "sql": (compiled or {}).get("sql"),
            "bind_names": bind_names,
            "parameters": redact_parameter_values(parameters, bind_names=bind_names),
        }

    def _event_payload(
        self,
        context: dict[str, Any],
        debug_payload: dict[str, Any],
    ) -> dict[str, Any]:
        payload = {
            "operation": context["operation"],
            "table": self._table_data,
            "table_name": self.tablename,
            "model": self._table_data.model,
            "model_name": self._table_data.model.__name__,
            "backend": context["backend"],
        }
        for key, value in context.items():
            if key not in {"operation", "table", "model", "backend"}:
                payload[key] = value
        payload.update(debug_payload)
        return payload

    def _context(self, operation: str, **extra: Any) -> dict[str, Any]:
        return {
            "operation": operation,
            "table": self.tablename,
            "model": self._table_data.model.__name__,
            "backend": self._backend(),
            **{key: value for key, value in extra.items() if value is not None},
        }

    def _backend(self) -> str:
        if not self._connection:
            return "unknown"
        scheme = self._connection.split("://", 1)[0].split("+", 1)[0].lower()
        if scheme == "postgres":
            return "postgresql"
        return scheme

    @staticmethod
    def _duration_ms(started: float) -> float:
        return (perf_counter() - started) * 1000

    @staticmethod
    def _row_count(result: Any) -> int | None:
        if isinstance(result, dict) and isinstance(result.get("rows"), list):
            return len(result["rows"])
        return None

    def _log_query(
        self,
        payload: dict[str, Any],
        *,
        duration_ms: float,
        row_count: int | None,
        error: BaseException | None,
    ) -> None:
        if not self._log_queries:
            return
        status = "error" if error is not None else "ok"
        QUERY_LOGGER.info(
            "ormdantic query %s operation=%s table=%s duration_ms=%.3f rows=%s",
            status,
            payload["operation"],
            payload["table_name"],
            duration_ms,
            row_count,
            extra={
                "ormdantic": {
                    **payload,
                    "duration_ms": duration_ms,
                    "row_count": row_count,
                    "error": str(error) if error is not None else None,
                }
            },
        )

    def _qualified_table_name(self) -> str:
        if self._table_data.schema_name is None:
            return self.tablename
        return f"{self._table_data.schema_name}.{self.tablename}"

    def _flat_aliases(self) -> list[str]:
        return [f"{self.tablename}\\{column}" for column in self._table_data.columns]

    def _compile_select_pk_query(self) -> dict[str, Any] | None:
        if self._connection is None:
            return None
        return _ormdantic.compile_select_pk(
            self._connection,
            self._qualified_table_name(),
            self._table_data.pk,
            list(self._table_data.columns),
            self._flat_aliases(),
        )

    def _compile_find_many_query(
        self,
        filters: Any,
        order_by: list[str],
        order_direction: str,
        limit: int | None,
        offset: int | None,
    ) -> dict[str, Any] | None:
        if self._connection is None or not isinstance(filters, list):
            return None
        return _ormdantic.compile_find_many(
            self._connection,
            self._qualified_table_name(),
            list(self._table_data.columns),
            filters,
            order_by,
            order_direction,
            limit,
            offset,
            self._flat_aliases(),
        )

    def _compile_count_query(self, filters: Any) -> dict[str, Any] | None:
        if self._connection is None or not isinstance(filters, list):
            return None
        return _ormdantic.compile_count(
            self._connection,
            self._qualified_table_name(),
            filters,
        )

    def _compile_insert_query(self, payload: dict[str, Any]) -> dict[str, Any] | None:
        if self._connection is None:
            return None
        return _ormdantic.compile_insert(
            self._connection,
            self._qualified_table_name(),
            list(payload),
        )

    def _compile_update_query(self, payload: dict[str, Any]) -> dict[str, Any] | None:
        if self._connection is None:
            return None
        columns = [column for column in payload if column != self._table_data.pk]
        if not columns:
            return None
        return _ormdantic.compile_update(
            self._connection,
            self._qualified_table_name(),
            self._table_data.pk,
            columns,
        )

    def _compile_upsert_query(self, payload: dict[str, Any]) -> dict[str, Any] | None:
        if self._connection is None:
            return None
        return _ormdantic.compile_upsert(
            self._connection,
            self._qualified_table_name(),
            self._table_data.pk,
            list(payload),
        )

    def _compile_delete_query(self) -> dict[str, Any] | None:
        if self._connection is None:
            return None
        return _ormdantic.compile_delete_pk(
            self._connection,
            self._qualified_table_name(),
            self._table_data.pk,
        )

    def _compile_typed_select_query(
        self, payload: dict[str, Any]
    ) -> dict[str, Any] | None:
        if self._connection is None:
            return None
        return _ormdantic.compile_typed_expression_query(self._connection, payload)

    def _compile_typed_update_query(
        self, payload: dict[str, Any]
    ) -> dict[str, Any] | None:
        if self._connection is None:
            return None
        return _ormdantic.compile_typed_update_query(self._connection, payload)

    def _payload(
        self,
        model_instance: ModelType,
        *,
        mode: Literal["insert", "update", "upsert"],
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {}
        for column in self._table_data.columns:
            options = self._table_data.column_options.get(column)
            if options is not None and options.computed is not None:
                continue
            value = getattr(model_instance, column)
            if (
                mode in {"insert", "upsert"}
                and options is not None
                and (options.server_default is not None or options.autoincrement)
                and value is None
            ):
                continue
            if options is not None and options.has_identity:
                if mode in {"insert", "upsert"} and (
                    options.identity_always or value is None
                ):
                    continue
                if mode == "update" and column != self._table_data.pk:
                    continue
            payload[column] = py_type_to_sql(self._table_map, value)
        return payload

    @staticmethod
    def _normalize_where(where: dict[str, Any] | None) -> dict[str, Any]:
        if where is None:
            return {}
        return where

    @staticmethod
    def _requires_expression_select(
        where: dict[str, Any] | QueryExpression | None,
        order_by: list[str | OrderExpression] | None,
    ) -> bool:
        if isinstance(where, QueryExpression) and not where.supports_legacy_filters():
            return True
        return any(isinstance(item, OrderExpression) for item in order_by or [])

    @staticmethod
    def _legacy_order_columns(
        order_by: list[str | OrderExpression] | None,
    ) -> list[str]:
        return [item for item in order_by or [] if isinstance(item, str)]

    @staticmethod
    def _expression_order_by(
        order_by: list[str | OrderExpression],
        order: Order,
    ) -> list[OrderExpression]:
        expressions = []
        for item in order_by:
            if isinstance(item, OrderExpression):
                expressions.append(item)
                continue
            if item.startswith("-"):
                expressions.append(expr_column(item[1:]).desc())
                continue
            if order is Order.desc:
                expressions.append(expr_column(item).desc())
            else:
                expressions.append(expr_column(item).asc())
        return expressions

    def _order_by_primary_key_sequence(
        self, data: list[ModelType], primary_keys: list[Any]
    ) -> list[ModelType]:
        order = {str(pk): index for index, pk in enumerate(primary_keys)}
        return sorted(
            data,
            key=lambda model: order.get(
                str(getattr(model, self._table_data.pk)), len(order)
            ),
        )

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

    def _resolve_load_plan(
        self, depth: int, load: list[LoaderOption] | None
    ) -> _ResolvedLoadPlan:
        if depth < 0:
            raise ValueError("relationship depth must be greater than or equal to 0")
        if not load:
            return _ResolvedLoadPlan(depth=depth, paths=None)

        option_by_path: dict[tuple[str, ...], LoaderOption] = {}
        for option in load:
            parts = self._validate_loader_path(option)
            existing = option_by_path.get(parts)
            if existing is not None and existing.strategy != option.strategy:
                dotted_path = ".".join(parts)
                raise ValueError(
                    f"loader path '{dotted_path}' has conflicting strategies "
                    f"'{existing.strategy}' and '{option.strategy}'"
                )
            terminal_table = self._table_for_loader_path(parts)
            self._validate_loader_modifiers(option, terminal_table)
            option_by_path[parts] = option

        joined_paths = {
            self._slash_path(parts)
            for parts, option in option_by_path.items()
            if option.strategy == "joined"
        }
        selectin_paths = {
            self._slash_path(parts)
            for parts, option in option_by_path.items()
            if option.strategy == "selectin"
        }
        eager_options = [
            option
            for option in option_by_path.values()
            if option.strategy in {"joined", "selectin"}
        ]
        disabled_paths = {
            self._slash_path(parts)
            for parts, option in option_by_path.items()
            if option.strategy in {"lazy", "noload"}
        }

        for eager_path in joined_paths | selectin_paths:
            for disabled_path in disabled_paths:
                if eager_path == disabled_path or eager_path.startswith(
                    f"{disabled_path}/"
                ):
                    raise ValueError(
                        "loader path "
                        f"'{eager_path.replace('/', '.')}' cannot be eager loaded "
                        f"because '{disabled_path.replace('/', '.')}' is disabled"
                    )

        joined_paths = set(joined_paths)
        if depth > 0:
            joined_paths.update(self._expand_depth_paths(depth))

        if disabled_paths:
            joined_paths = {
                path
                for path in joined_paths
                if not any(
                    path == disabled or path.startswith(f"{disabled}/")
                    for disabled in disabled_paths
                )
            }
            selectin_paths = {
                path
                for path in selectin_paths
                if not any(
                    path == disabled or path.startswith(f"{disabled}/")
                    for disabled in disabled_paths
                )
            }

        if not joined_paths and not selectin_paths:
            return _ResolvedLoadPlan(depth=0, paths=None)
        options = tuple(
            option
            for option in option_by_path.values()
            if option.strategy in {"joined", "selectin"}
            and (option.filter_by or option.order_by)
        )
        return _ResolvedLoadPlan(
            depth=0,
            paths=tuple(sorted(joined_paths)) or None,
            selectin_paths=tuple(sorted(selectin_paths)),
            options=options,
            use_selectin=bool(eager_options)
            and depth == 0
            and all(option.strategy == "selectin" for option in eager_options),
        )

    def _validate_loader_path(self, option: LoaderOption) -> tuple[str, ...]:
        parts = path_parts(option.path)
        current = self._table_data
        for part in parts:
            relationship = current.relationships.get(part)
            if relationship is None:
                available = ", ".join(sorted(current.relationships)) or "none"
                raise ValueError(
                    f"invalid loader path '{option.path}': "
                    f"'{current.model.__name__}.{part}' is not a relationship; "
                    f"available relationships: {available}"
                )
            current = self._table_map.name_to_data[relationship.foreign_table]
        return parts

    def _table_for_loader_path(self, parts: tuple[str, ...]) -> OrmTable[Any]:
        current = self._table_data
        for part in parts:
            relationship = current.relationships[part]
            current = self._table_map.name_to_data[relationship.foreign_table]
        return current

    @staticmethod
    def _validate_loader_modifiers(
        option: LoaderOption, terminal_table: OrmTable[Any]
    ) -> None:
        if option.strategy in {"lazy", "noload"} and (
            option.filter_by or option.order_by
        ):
            raise ValueError(
                f"loader path '{option.path}' cannot define filtering or ordering "
                f"with strategy '{option.strategy}'"
            )
        available_columns = set(terminal_table.columns)
        for column in option.filter_by or {}:
            if column not in available_columns:
                Table._raise_invalid_loader_column(option, terminal_table, column)
        for column in option.order_by:
            normalized = column[1:] if column.startswith("-") else column
            if normalized not in available_columns:
                Table._raise_invalid_loader_column(option, terminal_table, normalized)

    @staticmethod
    def _raise_invalid_loader_column(
        option: LoaderOption, terminal_table: OrmTable[Any], column: str
    ) -> None:
        available = ", ".join(sorted(terminal_table.columns)) or "none"
        raise ValueError(
            f"invalid loader option for path '{option.path}': "
            f"'{column}' is not a column on {terminal_table.model.__name__}; "
            f"available columns: {available}"
        )

    def _expand_depth_paths(self, depth: int) -> set[str]:
        paths: set[str] = set()

        def walk(
            table: OrmTable[Any],
            remaining_depth: int,
            prefix: tuple[str, ...],
            stack: tuple[str, ...],
        ) -> None:
            if remaining_depth <= 0:
                return
            for field_name, relationship in table.relationships.items():
                path = (*prefix, field_name)
                paths.add(self._slash_path(path))
                related = self._table_map.name_to_data[relationship.foreign_table]
                if related.tablename in stack:
                    continue
                walk(
                    related,
                    remaining_depth - 1,
                    path,
                    (*stack, related.tablename),
                )

        walk(self._table_data, depth, (), (self._table_data.tablename,))
        return paths

    @staticmethod
    def _slash_path(path: tuple[str, ...]) -> str:
        return "/".join(path)
