"""Module providing OrmTableGenerator."""

from sqlalchemy import MetaData
from sqlalchemy.ext.asyncio import AsyncEngine

from ormdantic.engine import NativeEngine
from ormdantic.generator._rust_schema import compile_create_table_sql
from ormdantic.models import Map, OrmTable


class OrmTableGenerator:
    def __init__(
        self,
        engine: AsyncEngine,
        metadata: MetaData,
        table_map: Map,
    ) -> None:
        """Initialize OrmTableGenerator."""
        self._engine = engine
        self._metadata = metadata
        self._table_map = table_map
        self._tables: list[str] = []
        self._native_engine = NativeEngine(str(engine.url))

    async def init(self) -> None:
        """Generate database tables with Rust DDL."""
        for tablename, table_data in self._table_map.name_to_data.items():
            self._tables.append(tablename)
            for sql in compile_create_table_sql(
                self._table_map, table_data.tablename, self._engine.name
            ):
                await self._native_engine.execute(sql, ())
