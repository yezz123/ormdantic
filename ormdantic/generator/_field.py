"""Module for building queries from field data."""
from typing import Any

from pypika.queries import Query, QueryBuilder, Table

from ormdantic.models import Map, OrmTable


class OrmField:
    """Build SQL queries from field information."""

    def __init__(self, table_data: OrmTable, table_map: Map | None = None) -> None:  # type: ignore
        """Build CRUD queries from tablename and field info.

        :param table_data: Meta data of target table for SQL script.
        :param table_map: Map of tablenames and models.
        """
        self._table_data = table_data
        self._table_map = table_map
        self._table = Table(table_data.tablename)

    def get_find_one_query(self, depth: int = 1) -> QueryBuilder:
        """pass"""

    def get_find_many_query(self, depth: int = 1) -> QueryBuilder:
        """pass"""

    def get_delete_query(self, pk: Any) -> QueryBuilder:
        """Get a delete query."""
        return (
            Query.from_(self._table)
            .where(self._table.field(self._table_data.pk) == pk)
            .delete()
        )
