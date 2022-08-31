"""Handle table interactions for a model."""


import asyncio
import json
import re
from types import NoneType
from typing import Any, Generic, Type, get_args
from uuid import UUID

import pydantic
from pydantic import BaseModel
from pypika import Field, Order, Query, Table
from pypika.queries import QueryBuilder
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession
from sqlalchemy.orm import sessionmaker

from ormdantic.generator._query import OrmQuery
from ormdantic.handler import TableName_From_Model
from ormdantic.models import Map, OrmTable, Relationship, Result
from ormdantic.types import ModelType


class PydanticSQLCRUDGenerator(Generic[ModelType]):
    """Provides DB CRUD methods and table information for a model."""

    def __init__(
        self,
        tablename: str,
        engine: AsyncEngine,
        table_map: Map,  # type: ignore
    ) -> None:

        self.tablename = tablename
        self._engine = engine
        self._table_map = table_map
        self._field_to_column: dict[Any, str] = {}

    async def find_one(self, pk: Any, depth: int = 0) -> ModelType | None:
        """Find a model instance by primary key."""
        return await self._find_one(self.tablename, pk, depth)

    async def find_many(
        self,
        where: dict[str, Any] | None = None,
        order_by: list[str] | None = None,
        order: Order = Order.asc,
        limit: int = 0,
        offset: int = 0,
        depth: int = 0,
    ) -> Result[ModelType]:
        """Find many model instances."""
        query = self._get_find_many_query(
            self.tablename, where, order_by, order, limit, offset, depth
        )
        result = await self._execute_query(query)
        return Result(
            offset=offset,
            limit=limit,
            data=[self._model_from_row_mapping(row._mapping) for row in result],
        )

    async def insert(self, model_instance: ModelType) -> ModelType:
        """Insert a model instance."""
        await self._execute_query(
            OrmQuery(model_instance, self._table_map).get_insert_query()
        )
        return model_instance

    async def update(self, model_instance: ModelType) -> ModelType:
        """Update a record.

        :param `model_instance``: Model representing record to update.
        :return: The updated model.
        """
        await self._execute_query(
            OrmQuery(model_instance, self._table_map).get_update_queries()
        )
        return model_instance

    async def upsert(self, model_instance: ModelType) -> ModelType:
        """Insert a record if it does not exist, else update it.

        :param `model_instance``: Model representing record to insert or update.
        :return: The inserted or updated model.
        """

        await self._execute_query(
            OrmQuery(model_instance, self._table_map).get_upsert_query()
        )
        return model_instance

    async def delete(self, pk: Any) -> bool:
        """Delete a model instance by primary key."""
        table = Table(self.tablename)
        await self._execute_query(
            Query.from_(table)
            .where(table.field(self._table_map.name_to_data[self.tablename].pk) == pk)
            .delete()
        )
        return True

    async def _find_one(
        self, tablename: str, pk: Any, depth: int = 0
    ) -> ModelType | None:
        table_data = self._table_map.name_to_data[tablename]
        table = Table(tablename)
        query, columns = self._build_joins(
            Query.from_(table),
            table_data,
            depth,
            self._columns(table_data, depth),
        )
        query = query.where(
            table.field(table_data.pk) == self._py_type_to_sql(pk)
        ).select(*columns)
        result = await self._execute_query(query)
        try:

            model_instance = self._model_from_row_mapping(
                next(result)._mapping, tablename=tablename
            )
            model_instance = await self._populate_many_relations(
                table_data, model_instance, depth
            )
            return model_instance
        except StopIteration:
            return None

    async def _populate_many_relations(
        self, table_data: OrmTable, model_instance: ModelType, depth: int  # type: ignore
    ) -> ModelType:
        if depth <= 0:
            return model_instance
        depth -= 1
        for column, relation in table_data.relationships.items():
            if not relation.back_references:
                continue
            pk = model_instance.__dict__[table_data.pk]
            models = await self._find_many_relation(table_data, pk, relation, depth)
            models = await asyncio.gather(
                *[
                    self._populate_many_relations(
                        self._table_map.name_to_data[relation.foreign_table],
                        model,
                        depth,
                    )
                    for model in models  # type: ignore
                ]
            )
            model_instance.__setattr__(column, models)
        # If depth is exhausted back out to here to skip needless loops.
        if depth <= 0:
            return model_instance
        # For each field, populate the many relationships of that field.
        for tablename, data in self._table_map.name_to_data.items():
            for column in table_data.model.__fields__:
                if type(model := model_instance.__dict__.get(column)) == data.model:
                    model = await self._populate_many_relations(data, model, depth)
                    model_instance.__setattr__(column, model)
        return model_instance

    async def _find_many_relation(
        self, table_data: OrmTable, pk: Any, relation: Relationship, depth: int  # type: ignore
    ) -> list[ModelType] | None:
        table = Table(table_data.tablename)
        foreign_table = Table(relation.foreign_table)
        foreign_table_data = self._table_map.name_to_data[relation.foreign_table]
        many_result = await self._find_otm(
            table_data, foreign_table_data, relation, table, foreign_table, pk, depth
        )
        return [
            self._model_from_row_mapping(
                row._mapping, tablename=foreign_table_data.tablename
            )
            for row in many_result
        ]

    async def _find_otm(
        self,
        table_data: OrmTable,  # type: ignore
        foreign_table_data: OrmTable,  # type: ignore
        relation: Relationship,
        table: Table,
        foreign_table: Table,
        pk: Any,
        depth: int,
    ) -> Any:
        query = (
            Query.from_(table)
            .left_join(foreign_table)
            .on(
                foreign_table.field(relation.back_references)
                == table.field(table_data.pk)
            )
            .where(table.field(table_data.pk) == pk)
            .select(foreign_table.field(foreign_table_data.pk))
        )
        result = await self._execute_query(query)
        many_query = self._get_find_many_query(
            foreign_table_data.tablename, depth=depth
        ).where(
            foreign_table.field(foreign_table_data.pk).isin([it[0] for it in result])
        )
        return await self._execute_query(many_query)  # pragma: no cover

    def _get_find_many_query(
        self,
        tablename: str,
        where: dict[str, Any] | None = None,
        order_by: list[str] | None = None,
        order: Order = Order.asc,
        limit: int = 0,
        offset: int = 0,
        depth: int = 0,
    ) -> QueryBuilder:
        """
        Get a query to find many models of a table.
        """
        table = Table(tablename)
        where = where or {}
        order_by = order_by or []
        pydantic_table = self._table_map.name_to_data[tablename]
        query, columns = self._build_joins(
            Query.from_(table),
            pydantic_table,
            depth,
            self._columns(pydantic_table, depth),
        )
        for field, value in where.items():
            query = query.where(table.field(field) == value)
        query = query.orderby(*order_by, order=order).select(*columns)
        if limit:
            query = query.limit(limit)
        if offset:
            query = query.offset(offset)
        return query

    async def _execute_query(self, query: QueryBuilder) -> Any:
        async_session = sessionmaker(
            self._engine, expire_on_commit=False, class_=AsyncSession
        )
        async with async_session() as session:
            async with session.begin():
                result = await session.execute(text(str(query)))
            await session.commit()
        await self._engine.dispose()
        return result

    def _build_joins(
        self,
        query: QueryBuilder,
        table_data: OrmTable,  # type: ignore
        depth: int,
        columns: list[Field],
        table_tree: str | None = None,
    ) -> tuple[QueryBuilder, list[Field]]:
        if depth <= 0:
            return query, columns
        if not (
            relationships := self._table_map.name_to_data[
                table_data.tablename
            ].relationships
        ):
            return query, columns
        depth -= 1
        table_tree = table_tree or table_data.tablename
        pypika_table: Table = Table(table_data.tablename)
        if table_data.tablename != table_tree:
            pypika_table = pypika_table.as_(table_tree)
        # For each related table, add join to query.
        for field_name, relation in relationships.items():
            if relation.back_references is not None:
                continue
            relation_name = f"{table_tree}/{field_name}"
            rel_table = Table(relation.foreign_table).as_(relation_name)
            query = query.left_join(rel_table).on(
                pypika_table.field(field_name)
                == rel_table.field(
                    self._table_map.name_to_data[relation.foreign_table].pk
                )
            )
            columns.extend(
                [
                    rel_table.field(c).as_(f"{relation_name}//{depth}//{c}")
                    for c in self._table_map.name_to_data[
                        relation.foreign_table
                    ].columns
                ]
            )
            # Add joins of relations of this table to query.
            query, new_cols = self._build_joins(
                query,
                self._table_map.name_to_data[relation.foreign_table],
                depth,
                columns,
                relation_name,
            )
            columns.extend([c for c in new_cols if c not in columns])
        return query, columns

    def _model_from_row_mapping(
        self,
        row_mapping: dict[str, Any],
        model_type: Type[ModelType] | None = None,
        table_tree: str | None = None,
        tablename: str | None = None,
    ) -> ModelType:
        tablename = tablename or self.tablename
        model_type = model_type or self._table_map.name_to_data[tablename].model
        table_tree = table_tree or tablename
        py_type = {}
        table_data = self._table_map.name_to_data[
            TableName_From_Model(model_type, self._table_map)
        ]
        for column, value in row_mapping.items():
            if not column.startswith(f"{table_tree}//"):
                # This must be a column somewhere else in the tree.
                continue
            groups = re.match(rf"{re.escape(table_tree)}//(\d+)//(.*)", column)
            depth = int(groups[1])  # type: ignore
            column_name = groups[2]  # type: ignore
            if column_name in table_data.relationships:
                if value is None:
                    # No further depth has been found.
                    continue
                foreign_table = self._table_map.name_to_data[
                    table_data.relationships[column_name].foreign_table
                ]
                if depth <= 0:
                    py_type[column_name] = self._sql_pk_to_py_pk_type(
                        model_type, column_name, column, row_mapping
                    )
                else:
                    py_type[column_name] = self._model_from_row_mapping(
                        row_mapping={
                            k.removeprefix(f"{table_tree}/"): v
                            for k, v in row_mapping.items()
                            if not k.startswith(f"{table_tree}//")
                        },
                        model_type=foreign_table.model,
                        table_tree=column_name,
                    )
            else:
                py_type[column_name] = self._sql_type_to_py(
                    model_type, column_name, value
                )
        return model_type.construct(**py_type)

    def _tablename_from_model_instance(self, model: BaseModel) -> str:
        return [
            k
            for k, v in self._table_map.name_to_data.items()
            if isinstance(model, v.model)
        ][0]

    def _py_type_to_sql(self, value: Any) -> Any:
        if self._engine.name != "postgres" and isinstance(value, UUID):
            return str(value)
        if isinstance(value, (dict, list)):
            return json.dumps(value)
        if isinstance(value, BaseModel) and type(value) in [
            it.model for it in self._table_map.name_to_data.values()
        ]:
            tablename = self._tablename_from_model_instance(value)
            return self._py_type_to_sql(
                value.__dict__[self._table_map.name_to_data[tablename].pk]
            )
        return value.json() if isinstance(value, BaseModel) else value

    def _sql_pk_to_py_pk_type(self, model_type: Type[ModelType], field_name: str, column: str, row_mapping: dict) -> Any:  # type: ignore
        type_ = None
        for arg in get_args(model_type.__fields__[field_name].type_):
            if arg in self._table_map.name_to_data.values() or arg is NoneType:
                continue  # pragma: no cover
            type_ = arg
        return type_(row_mapping[column]) if type_ else row_mapping[column]

    @staticmethod
    def _columns(table_data: OrmTable, depth: int) -> list[Field]:  # type: ignore
        table = Table(table_data.tablename)
        return [
            table.field(c).as_(f"{table_data.tablename}//{depth}//{c}")
            for c in table_data.columns
        ]

    @staticmethod
    def _sql_type_to_py(model: Type[ModelType], column: str, value: Any) -> Any:
        if value is None:
            return None
        if model.__fields__[column].type_ in [dict, list]:
            return json.loads(value)
        if issubclass(model.__fields__[column].type_, pydantic.BaseModel):
            return model.__fields__[column].type_(**json.loads(value))
        return model.__fields__[column].type_(value)
