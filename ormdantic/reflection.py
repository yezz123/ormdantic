"""Database reflection helpers."""

from __future__ import annotations

from typing import Any


def _validate_sqlite_identifier(identifier: str) -> str:
    if not identifier.replace("_", "").isalnum():
        raise ValueError(f"invalid SQLite identifier '{identifier}'")
    return identifier


class Inspector:
    """Small runtime inspector backed by native SQL queries."""

    def __init__(self, database: Any) -> None:
        self._database = database

    async def table_names(self) -> list[str]:
        """Return table names for supported dialects."""
        if self._database._connection.startswith("sqlite"):
            result = await self._database._native_engine.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name",
                (),
            )
            return [row[0] for row in result]
        raise NotImplementedError("reflection is currently implemented for SQLite")

    async def columns(self, table: str) -> list[dict[str, Any]]:
        """Return column metadata for a table."""
        if self._database._connection.startswith("sqlite"):
            table = _validate_sqlite_identifier(table)
            result = await self._database._native_engine.execute(
                f"SELECT cid, name, type, [notnull], dflt_value, pk FROM pragma_table_info('{table}')",
                (),
            )
            return [
                {
                    "name": row[1],
                    "type": row[2],
                    "nullable": not bool(row[3]),
                    "default": row[4],
                    "primary_key": bool(row[5]),
                }
                for row in result
            ]
        raise NotImplementedError("reflection is currently implemented for SQLite")

    async def indexes(self, table: str) -> list[dict[str, Any]]:
        """Return index metadata for a table."""
        if self._database._connection.startswith("sqlite"):
            table = _validate_sqlite_identifier(table)
            result = await self._database._native_engine.execute(
                f"SELECT name, [unique] FROM pragma_index_list('{table}')",
                (),
            )
            return [
                {"name": row[0], "unique": bool(row[1])}
                for row in result
                if row[0] is not None
                and not str(row[0]).startswith("sqlite_autoindex_")
            ]
        raise NotImplementedError("reflection is currently implemented for SQLite")

    async def foreign_keys(self, table: str) -> list[dict[str, Any]]:
        """Return foreign key metadata for a table."""
        if self._database._connection.startswith("sqlite"):
            table = _validate_sqlite_identifier(table)
            result = await self._database._native_engine.execute(
                f"SELECT [table], [from], [to] FROM pragma_foreign_key_list('{table}')",
                (),
            )
            return [{"table": row[0], "from": row[1], "to": row[2]} for row in result]
        raise NotImplementedError("reflection is currently implemented for SQLite")
