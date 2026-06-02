"""Database reflection helpers."""

from __future__ import annotations

from typing import Any


class Inspector:
    """Small runtime inspector backed by the Rust runtime."""

    def __init__(self, database: Any) -> None:
        self._database = database

    async def table_names(self) -> list[str]:
        """Return table names for supported dialects."""
        return list(self._database._ensure_runtime().table_names())

    async def columns(self, table: str) -> list[dict[str, Any]]:
        """Return column metadata for a table."""
        return list(self._database._ensure_runtime().columns(table))

    async def indexes(self, table: str) -> list[dict[str, Any]]:
        """Return index metadata for a table."""
        return list(self._database._ensure_runtime().indexes(table))

    async def foreign_keys(self, table: str) -> list[dict[str, Any]]:
        """Return foreign key metadata for a table."""
        return list(self._database._ensure_runtime().foreign_keys(table))
