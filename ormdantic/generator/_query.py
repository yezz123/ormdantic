"""Build Rust-backed write queries from Pydantic model instances."""

from ormdantic.generator._rust_query import (
    RustQuery,
    bind_compiled_query,
    compile_insert,
    compile_update,
    compile_upsert,
)
from ormdantic.handler import py_type_to_sql
from ormdantic.models import Map
from ormdantic.types import ModelType


class OrmQuery:
    """Build SQL queries for model CRUD operations."""

    def __init__(
        self,
        model: ModelType,
        table_map: Map,
        dialect: str = "sqlite",
    ) -> None:
        self._model = model
        self._table_map = table_map
        self._table_data = self._table_map.model_to_data[type(self._model)]
        self._dialect = dialect

    def get_insert_query(self) -> RustQuery:
        """Get queries to insert model tree."""
        columns_and_values = self._get_columns_and_values()
        return bind_compiled_query(
            compile_insert(
                dialect=self._dialect,
                table=self._table_data.tablename,
                columns=list(columns_and_values),
            ),
            columns_and_values,
        )

    def get_upsert_query(self) -> RustQuery:
        """Get queries to upsert model tree."""
        columns_and_values = self._get_columns_and_values()
        return bind_compiled_query(
            compile_upsert(
                dialect=self._dialect,
                table=self._table_data.tablename,
                primary_key=self._table_data.pk,
                columns=list(columns_and_values),
            ),
            columns_and_values,
        )

    def get_update_queries(self) -> RustQuery:
        """Get queries to update model tree."""
        columns_and_values = self._get_columns_and_values()
        return bind_compiled_query(
            compile_update(
                dialect=self._dialect,
                table=self._table_data.tablename,
                primary_key=self._table_data.pk,
                columns=list(columns_and_values),
            ),
            columns_and_values,
        )

    def _get_columns_and_values(self):  # type: ignore
        return {
            column: py_type_to_sql(self._table_map, self._model.__dict__[column])
            for column in self._table_data.columns
        }
