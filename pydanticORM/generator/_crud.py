"""Generate Python CRUD methods for a model."""
import asyncio
import json
import re
from types import NoneType  # type: ignore
from typing import Any, Generic, Type, get_args
from uuid import UUID

import pydantic
from pydantic import BaseModel
from pydantic.generics import GenericModel
from pypika import Field, Order, Query, Table
from pypika.queries import QueryBuilder
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession
from sqlalchemy.orm import sessionmaker

from pydanticORM.handler import TableName_From_Model
from pydanticORM.table import PydanticTableMeta, Relation, RelationType, Result
from pydanticORM.types import ModelType


class PydanticSQLCRUDGenerator(Generic[ModelType]):
    """Provides Database CRUD methods for a model type."""

    def __init__(
        self,
        tablename: str,
        engine: AsyncEngine,
        schema: dict[str, PydanticTableMeta],  # type: ignore
    ) -> None:

        self._tablename = tablename
        self._engine = engine
        self._schema = schema
        self._field_to_column: dict[Any, str] = {}

    async def find_one(self, pk: Any, depth: int = 0) -> ModelType | None:  # type: ignore
        return await self._find_one(self._tablename, pk, depth)

    async def find_many(
        self,
        where: dict[str, Any] | None = None,  # type: ignore
        order_by: list[str] | None = None,  # type: ignore
        order: Order = Order.asc,
        limit: int = 0,
        offset: int = 0,
        depth: int = 0,
    ) -> Result[ModelType]:
        query = self._get_find_many_query(
            self._tablename, where, order_by, order, limit, offset, depth
        )
        result = await self._execute(query)
        return Result(
            offset=offset,
            limit=limit,
            data=[self._model_from_row_mapping(row._mapping) for row in result],
        )

    async def insert(
        self, model_instance: ModelType, upsert_relations: bool = True
    ) -> ModelType:
        return await self._insert(model_instance, self._tablename, upsert_relations)

    async def update(
        self, model_instance: ModelType, upsert_relations: bool = True
    ) -> ModelType:
        return await self._update(model_instance, self._tablename, upsert_relations)

    async def upsert(
        self, model_instance: ModelType, upsert_relations: bool = True
    ) -> ModelType:
        return await self._upsert(model_instance, self._tablename, upsert_relations)

    async def delete(self, pk: Any) -> bool:
        table = Table(self._tablename)
        await self._execute(
            Query.from_(table)
            .where(table.field(self._schema[self._tablename].pk) == pk)
            .delete()
        )
        return True

    async def _find_one(
        self, tablename: str, pk: Any, depth: int = 0
    ) -> ModelType | None:  # type: ignore
        table_data = self._schema[tablename]
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
        result = await self._execute(query)
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

    async def _insert(  # type: ignore
        self, model_instance: ModelType, tablename: str, upsert_relations
    ):
        table_data = self._schema[tablename]
        table = Table(tablename)
        if upsert_relations:
            await self._upsert_relations(model_instance, table_data)
        values = [
            self._py_type_to_sql(model_instance.__dict__[c]) for c in table_data.columns
        ]
        await self._execute(
            Query.into(table).columns(*table_data.columns).insert(*values)
        )
        return model_instance

    async def _update(
        self, model_instance: ModelType, tablename: str, upsert_relations: bool = True
    ) -> ModelType:
        table_data = self._schema[tablename]
        table = Table(tablename)
        if upsert_relations:
            await self._upsert_relations(model_instance, table_data)
        values = [
            self._py_type_to_sql(model_instance.__dict__[c]) for c in table_data.columns
        ]
        query = Query.update(table)
        for i, column in enumerate(table_data.columns):
            query = query.set(column, values[i])
        pk = model_instance.__dict__[table_data.pk]
        query = query.where(table.field(table_data.pk) == self._py_type_to_sql(pk))
        await self._execute(query)
        return model_instance

    async def _upsert(
        self, model_instance: ModelType, tablename: str, upsert_relations: bool
    ) -> ModelType:
        if model := (
            await self._find_one(
                tablename, model_instance.__dict__[self._schema[tablename].pk]
            )
        ):
            return (
                model if model == model_instance else await self.update(model_instance)
            )
        return await self._insert(model_instance, tablename, upsert_relations)

    async def _upsert_relations(  # type: ignore
        self, model_instance: ModelType, table_data: PydanticTableMeta  # type: ignore
    ):
        for column, relation in table_data.relationships.items():
            if relation.relation_type == RelationType.MANY_TO_MANY:
                print(relation)
            elif rel_model := model_instance.__dict__.get(column):
                tablename = TableName_From_Model(type(rel_model), self._schema)
                await self._upsert(rel_model, tablename, True)

    async def _populate_many_relations(
        self, table_data: PydanticTableMeta, model_instance: ModelType, depth: int  # type: ignore
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
                        self._schema[relation.foreign_table], model, depth
                    )
                    for model in models  # type: ignore
                ]
            )
            model_instance.__setattr__(column, models)
        # If depth is exhausted back out to here to skip needless loops.
        if depth <= 0:
            return model_instance
        # For each field, populate the many relationships of that field.
        for tablename, data in self._schema.items():
            for column in table_data.columns:
                if type(model := model_instance.__dict__.get(column)) == data.model:
                    model = await self._populate_many_relations(data, model, depth)
                    model_instance.__setattr__(column, model)
        return model_instance

    async def _find_many_relation(
        self, table_data: PydanticTableMeta, pk: Any, relation: Relation, depth: int  # type: ignore
    ) -> list[ModelType] | None:  # type: ignore
        table = Table(table_data.name)
        foreign_table = Table(relation.foreign_table)
        foreign_table_data = self._schema[relation.foreign_table]
        if relation.relation_type == RelationType.ONE_TO_MANY:
            many_result = await self._find_otm(
                table_data,
                foreign_table_data,
                relation,
                table,
                foreign_table,
                pk,
                depth,
            )
        else:
            many_result = await self._find_mtm(
                table_data,
                foreign_table_data,
                relation,
                table,
                foreign_table,
                pk,
                depth,
            )
        return [
            self._model_from_row_mapping(
                row._mapping, tablename=foreign_table_data.name
            )
            for row in many_result
        ]

    async def _find_otm(
        self,
        table_data: PydanticTableMeta,  # type: ignore
        foreign_table_data: PydanticTableMeta,  # type: ignore
        relation: Relation,
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
        result = await self._execute(query)
        many_query = self._get_find_many_query(
            foreign_table_data.name, depth=depth
        ).where(
            foreign_table.field(foreign_table_data.pk).isin([it[0] for it in result])
        )
        return await self._execute(many_query)

    async def _find_mtm(
        self,
        table_data: PydanticTableMeta,  # type: ignore
        foreign_table_data: PydanticTableMeta,  # type: ignore
        relation: Relation,
        table: Table,
        foreign_table: Table,
        pk: Any,
        depth: int,
    ) -> Any:
        mtm_table = Table(relation.m2m_table)
        if relation.foreign_table == table_data.name:
            mtm_field_a = f"{table_data.name}_a"
            mtm_field_b = f"{relation.foreign_table}_b"
        else:
            mtm_field_a = table_data.name
            mtm_field_b = relation.foreign_table
        query = (
            Query.from_(table)
            .left_join(mtm_table)
            .on(
                mtm_table.field(mtm_field_a)
                == table.field(self._schema[table_data.name].pk)
            )
            .left_join(foreign_table)
            .on(
                mtm_table.field(mtm_field_b)
                == foreign_table.field(foreign_table_data.pk)
            )
            .where(table.field(table_data.pk) == pk)
            .select(foreign_table.field(foreign_table_data.pk))
        )
        result = await self._execute(query)
        many_query = self._get_find_many_query(
            foreign_table_data.name, depth=depth
        ).where(
            foreign_table.field(foreign_table_data.pk).isin([it[0] for it in result])
        )
        return await self._execute(many_query)

    def _get_find_many_query(
        self,
        tablename: str,
        where: dict[str, Any] | None = None,  # type: ignore
        order_by: list[str] | None = None,  # type: ignore
        order: Order = Order.asc,
        limit: int = 0,
        offset: int = 0,
        depth: int = 0,
    ) -> QueryBuilder:
        table = Table(tablename)
        where = where or {}
        order_by = order_by or []
        pydantic_table = self._schema[tablename]
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

    async def _execute(self, query: QueryBuilder) -> Any:
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
        table_data: PydanticTableMeta,  # type: ignore
        depth: int,
        columns: list[Field],
        table_tree: str | None = None,  # type: ignore
    ) -> tuple[QueryBuilder, list[Field]]:
        if depth <= 0:
            return query, columns
        if not (relationships := self._schema[table_data.name].relationships):
            return query, columns
        depth -= 1
        table_tree = table_tree or table_data.name
        pypika_table: Table = Table(table_data.name)
        if table_data.name != table_tree:
            pypika_table = pypika_table.as_(table_tree)
        # For each related table, add join to query.
        for field_name, relation in relationships.items():
            if relation.back_references is not None:
                continue
            relation_name = f"{table_tree}/{field_name}"
            rel_table = Table(relation.foreign_table).as_(relation_name)
            query = query.left_join(rel_table).on(
                pypika_table.field(field_name)
                == rel_table.field(self._schema[relation.foreign_table].pk)
            )
            columns.extend(
                [
                    rel_table.field(c).as_(f"{relation_name}//{depth}//{c}")
                    for c in self._schema[relation.foreign_table].columns
                ]
            )
            # Add joins of relations of this table to query.
            query, new_cols = self._build_joins(
                query,
                self._schema[relation.foreign_table],
                depth,
                columns,
                relation_name,
            )
            columns.extend([c for c in new_cols if c not in columns])
        return query, columns

    def _model_from_row_mapping(
        self,
        row_mapping: dict[str, Any],
        model_type: Type[ModelType] | None = None,  # type: ignore
        table_tree: str | None = None,  # type: ignore
        tablename: str | None = None,  # type: ignore
    ) -> ModelType:
        tablename = tablename or self._tablename
        model_type = model_type or self._schema[tablename].model
        table_tree = table_tree or tablename
        py_type = {}
        table_data = self._schema[TableName_From_Model(model_type, self._schema)]
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
                foreign_table = self._schema[
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
        return [k for k, v in self._schema.items() if isinstance(model, v.model)][0]

    def _py_type_to_sql(self, value: Any) -> Any:
        if self._engine.name != "postgres" and isinstance(value, UUID):
            return str(value)
        if isinstance(value, (dict, list)):
            return json.dumps(value)
        if isinstance(value, BaseModel) and type(value) in [
            it.model for it in self._schema.values()
        ]:
            tablename = self._tablename_from_model_instance(value)
            return self._py_type_to_sql(value.__dict__[self._schema[tablename].pk])
        if isinstance(value, BaseModel):
            return value.json()
        return value

    def _sql_pk_to_py_pk_type(
        self,
        model_type: Type[ModelType],
        field_name: str,
        column: str,
        row_mapping: dict,  # type: ignore
    ) -> Any:
        type_ = None
        for arg in get_args(model_type.__fields__[field_name].type_):
            if arg in self._schema.values() or arg is NoneType:
                continue
            type_ = arg
        if type_:
            return type_(row_mapping[column])
        return row_mapping[column]

    @staticmethod
    def _columns(table_data: PydanticTableMeta, depth: int) -> list[Field]:  # type: ignore
        table = Table(table_data.name)
        return [
            table.field(c).as_(f"{table_data.name}//{depth}//{c}")
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
