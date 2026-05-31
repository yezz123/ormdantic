"""Module providing OrmTableGenerator."""

from ormdantic.engine import NativeEngine
from ormdantic.generator._rust_schema import compile_create_table_sql
from ormdantic.models import Map


class OrmTableGenerator:
    def __init__(
        self,
        connection: str,
        table_map: Map,
    ) -> None:
        """Initialize OrmTableGenerator."""
        self._connection = connection
        self._table_map = table_map
        self._tables: list[str] = []
        self._native_engine = NativeEngine(connection)

    async def init(self) -> None:
        """Generate database tables with Rust DDL."""
        for tablename, table_data in self._table_map.name_to_data.items():
            self._tables.append(tablename)
            for sql in compile_create_table_sql(
                self._table_map, table_data.tablename, self._connection
            ):
                await self._native_engine.execute(sql, ())
