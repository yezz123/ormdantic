"""Thin Python table facade over Rust-owned table handles."""

from __future__ import annotations

import importlib
from dataclasses import dataclass
from enum import Enum
from typing import Any, Generic

from pydantic import BaseModel

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
from ormdantic.loaders import LoaderOption, path_parts
from ormdantic.models import Map, OrmTable, Result
from ormdantic.serializer import OrmSerializer
from ormdantic.types import ModelType
from ormdantic.values import py_type_to_sql

_ormdantic: Any = importlib.import_module("ormdantic._ormdantic")


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
    ) -> None:
        self._table_data = table_data
        self._table_map = table_map
        self._rust_handle = rust_handle
        self._events = events
        self._runtime = runtime
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
            result = self._rust_handle.find_one_with_paths(
                joined_values,
                list(load_plan.paths),
                joined_filters,
                joined_order_by,
            )
        else:
            result = self._rust_handle.find_one(
                py_type_to_sql(self._table_map, pk), load_plan.depth
            )
        model = self._deserialize(
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
        order_by: list[str] | None = None,
        order: Order = Order.asc,
        limit: int = 0,
        offset: int = 0,
        depth: int = 0,
        load: list[LoaderOption] | None = None,
    ) -> Result[ModelType]:
        """Find many model instances."""
        load_plan = self._resolve_load_plan(depth, load)
        filters, values = self._compile_where(where)
        if load_plan.paths:
            joined_filters, joined_order_by, joined_values = (
                self._joined_loader_query_parts(load_plan)
            )
            values.update(joined_values)
            result = self._rust_handle.find_many_with_paths(
                filters,
                values,
                order_by or [],
                order.value,
                limit or None,
                offset or None,
                list(load_plan.paths),
                joined_filters,
                joined_order_by,
            )
            data = (
                self._deserialize(
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
            result = self._rust_handle.find_many(
                filters,
                values,
                order_by or [],
                order.value,
                limit or None,
                offset or None,
                load_plan.depth,
            )
            data = (
                self._deserialize(
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
        return OrmSerializer[ModelType | None](
            table_data=self._table_data,
            table_map=self._table_map,
            result_set=native_result,
            is_array=is_array,
            depth=depth,
            load_paths=load_paths,
            load_options=load_options,
        ).deserialize()

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
            else:
                related = await self._selectin_load_relationship(
                    parents,
                    table_data,
                    field_name,
                    relationship.back_references,
                    related_table,
                    option,
                    identity_map,
                )
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
            where = self._selectin_where(back_reference, parent_ids, option)
            children = (await related_handle.find_many(where=where)).data
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
        where = self._selectin_where(related_table.pk, foreign_keys, option)
        related_rows = (await related_handle.find_many(where=where)).data
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
