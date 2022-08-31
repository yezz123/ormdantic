import json
from typing import Any
from uuid import UUID

from pydantic import BaseModel
from pypika import PostgreSQLQuery, Query, Table
from pypika.dialects import PostgreSQLQueryBuilder
from pypika.queries import QueryBuilder

from ormdantic.handler import Model_Instance
from ormdantic.models import Map
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
        self._query = query or PostgreSQLQuery
        self._table_map = table_map
        self._processed_models = processed_models or []

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
        """pass"""

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
        table_data = self._table_map.model_to_data[type(self._model)]
        col_to_value = {
            query: self._py_type_to_sql(self._model.__dict__[query])
            for query in table_data.columns
        }
        table = Table(table_data.tablename)
        self._query = (
            self._query.into(table)
            .columns(*table_data.columns)
            .insert(*col_to_value.values())
        )

        if is_upsert and isinstance(self._query, PostgreSQLQueryBuilder):
            self._query = self._query.on_conflict(table_data.pk)
            for column, value in col_to_value.items():
                self._query = self._query.do_update(table.field(column), value)
        queries = [self._query]
        if self._depth <= 1:
            return queries
        for col, rel in table_data.relationships.items():
            rel_value = self._model.__dict__[col]
            if not rel_value:
                continue
            if isinstance(rel_value, list):
                for model in rel_value:
                    queries.extend(
                        OrmQuery(
                            model=model,
                            table_map=self._table_map,
                            depth=self._depth - 1,
                            processed_models=self._processed_models + [self._model],
                        ).get_upsert_queries()
                    )
                    queries.extend(self._get_mtm_upsert())
            else:
                queries.extend(
                    OrmQuery(
                        model=model,
                        model=rel_value,
                        table_map=self._table_map,
                        depth=self._depth - 1,
                        processed_models=self._processed_models + [self._model],
                    ).get_upsert_queries()
                )
        return queries

    def _get_mtm_upsert(
        self, relation: ModelType
    ) -> QueryBuilder | PostgreSQLQueryBuilder:
        pass
        # TODO Get mtm table.

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
