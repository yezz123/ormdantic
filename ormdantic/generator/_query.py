import json
from typing import Any
from uuid import UUID

from pydantic import BaseModel
from pypika import PostgreSQLQuery, Query, Table
from pypika.dialects import PostgreSQLQueryBuilder
from pypika.queries import QueryBuilder

from ormdantic.handler import Model_Instance
from ormdantic.models import Map, Relationship, RelationType
from ormdantic.types import ModelType


class OrmQuery:
    """Build SQL queries for model CRUD operations."""

    def __init__(
        self,
        model: ModelType,
        table_map: Map,
        depth: int = 1,
        processed_models: list[ModelType] | None = None,
        query: Query | PostgreSQLQuery | None = None,
    ) -> None:
        self._depth = depth
        self._model = model
        # PostgreSQLQuery works for SQLite and PostgreSQL.
        self._query: QueryBuilder | PostgreSQLQueryBuilder | Query | PostgreSQLQuery = (
            query or PostgreSQLQuery
        )
        self._table_map = table_map
        self._processed_models = processed_models or []
        self._table_data = self._table_map.model_to_data[type(self._model)]
        self._table = Table(self._table_data.tablename)

    def get_insert_queries(self) -> list[QueryBuilder | PostgreSQLQueryBuilder]:
        """Get queries to insert model tree."""
        return self._get_inserts_or_upserts(is_upsert=False)

    def get_upsert_queries(self) -> list[QueryBuilder | PostgreSQLQueryBuilder]:
        """Get queries to upsert model tree."""
        return self._get_inserts_or_upserts(is_upsert=True)

    def get_find_one_query(self, populate_back_references: bool = False) -> Query:
        """pass"""

    def get_find_many_query(self, populate_back_references: bool = False) -> Query:
        """pass"""

    def get_update_queries(self) -> list[QueryBuilder | PostgreSQLQueryBuilder]:
        """Get queries to update model tree."""
        if self._model in self._processed_models:
            return []
        self._query = self._query.update(self._table)
        for column, value in self._get_columns_and_values().items():
            self._query = self._query.set(column, value)
        self._query = self._query.where(
            self._table.field(self._table_data.pk)
            == self._model.__dict__[self._table_data.pk]
        )
        queries = [self._query]
        if self._depth > 1:
            queries.extend(self._get_relation_upserts())
        return queries

    def get_patch_queries(self) -> list[QueryBuilder | PostgreSQLQueryBuilder]:
        """pass"""

    def get_delete_queries(self) -> list[QueryBuilder | PostgreSQLQueryBuilder]:
        """Get Delete Queries for the given model"""
        # TODO: For each mtm, delete any mappings to this record.

    def _get_inserts_or_upserts(
        self, is_upsert: bool
    ) -> list[QueryBuilder | PostgreSQLQueryBuilder]:
        if self._model in self._processed_models:
            return []
        col_to_value = self._get_columns_and_values()
        self._query = (
            self._query.into(self._table)
            .columns(*self._model.__fields__)
            .insert(*col_to_value.values())
        )

        if is_upsert and isinstance(self._query, PostgreSQLQueryBuilder):
            self._query = self._query.on_conflict(self._table_data.pk)
            for column, value in col_to_value.items():
                self._query = self._query.do_update(self._table.field(column), value)
        queries = [self._query]
        if self._depth > 1:
            queries.extend(self._get_relation_upserts())
        return queries

    def _get_relation_upserts(self) -> list[QueryBuilder | PostgreSQLQueryBuilder]:
        queries = []
        for col, rel in self._table_data.relationships.items():
            relation_value = self._model.__dict__[col]
            if not relation_value:
                continue
            if isinstance(relation_value, list):
                for model in relation_value:
                    queries.extend(
                        OrmQuery(
                            model=model,
                            table_map=self._table_map,
                            depth=self._depth - 1,
                            processed_models=self._processed_models + [self._model],
                        ).get_upsert_queries()
                    )
                    if rel.relationship_type == RelationType.MANY_TO_MANY:
                        queries.append(self._get_mtm_upsert(rel, model))

            else:
                queries.extend(
                    OrmQuery(
                        model=relation_value,
                        table_map=self._table_map,
                        depth=self._depth - 1,
                        processed_models=self._processed_models + [self._model],
                    ).get_upsert_queries()
                )
        return queries

    def _get_mtm_upsert(
        self, relation: Relationship, rel_model: ModelType
    ) -> QueryBuilder | PostgreSQLQueryBuilder:
        table_data = self._table_map.model_to_data[type(self._model)]
        r_data = self._table_map.model_to_data[type(rel_model)]
        table = Table(relation.mtm_data.tablename)
        col_a = table.field(relation.mtm_data.table_a_column)
        col_b = table.field(relation.mtm_data.table_b_column)
        return (
            PostgreSQLQuery.into(table)
            .columns(col_a, col_b)
            .insert(self._model.__dict__[table_data.pk], rel_model.__dict__[r_data.pk])
            .on_conflict(col_a, col_b)
            .do_update(col_a)
            .do_update(col_b)
        )

    def _get_columns_and_values(self):
        return {
            column: self._py_type_to_sql(self._model.__dict__[column])
            for column in self._model.__fields__
        }

    def _py_type_to_sql(self, value: Any) -> Any:
        if isinstance(value, UUID):
            return str(value)
        if isinstance(value, (dict, list)):
            return json.dumps(value)
        if (
            isinstance(value, BaseModel)
            and type(value) in self._table_map.model_to_data
        ):
            tablename = Model_Instance(value, self._table_map)
            return self._py_type_to_sql(
                value.__dict__[self._table_map.name_to_data[tablename].pk]
            )

        return value.json() if isinstance(value, BaseModel) else value
