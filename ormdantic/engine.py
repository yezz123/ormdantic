"""Async Python wrapper around Ormdantic's native Rust execution layer."""

from __future__ import annotations

import asyncio
import importlib
from dataclasses import dataclass
from typing import Any, Iterator

try:
    _ormdantic: Any | None = importlib.import_module("ormdantic._ormdantic")
except ImportError:  # pragma: no cover - exercised when extension is not built
    _ormdantic = None


def runtime_capabilities() -> dict[str, bool]:
    """Return the database runtimes compiled into the Rust extension."""
    if _ormdantic is None or not hasattr(_ormdantic, "runtime_capabilities"):
        return {
            "sqlite": False,
            "postgresql": False,
            "mysql": False,
            "mariadb": False,
            "mssql": False,
            "oracle": False,
        }
    return dict(_ormdantic.runtime_capabilities())


@dataclass
class NativeCursor:
    """Cursor-like metadata returned with native query results."""

    description: list[tuple[str]]


class NativeResult:
    """Result wrapper compatible with the serializer's cursor/row contract."""

    def __init__(self, columns: list[str], rows: list[tuple[Any, ...]]) -> None:
        """Create a result from column names and native rows."""
        self.cursor = NativeCursor([(column,) for column in columns])
        self._rows = rows

    def __iter__(self) -> Iterator[tuple[Any, ...]]:
        """Iterate over result rows."""
        return iter(self._rows)

    def scalar(self) -> Any:
        """Return the first column from the first row, if present."""
        if not self._rows or not self._rows[0]:
            return None
        return self._rows[0][0]


class NativeEngine:
    """Async facade over a persistent Rust `PyNativeConnection`."""

    def __init__(self, url: str) -> None:
        """Open a native connection for a database URL."""
        if _ormdantic is None or not hasattr(_ormdantic, "PyNativeConnection"):
            raise RuntimeError(
                "Ormdantic vNext requires the Rust extension for native execution. "
                "Install the package with maturin or reinstall the wheel."
            )
        self.url = url
        self._connection = _ormdantic.PyNativeConnection(url)

    async def execute(self, sql: str, values: tuple[Any, ...]) -> NativeResult:
        """Execute SQL with ordered bind values on the native connection."""
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(None, self._execute_sync, sql, list(values))
        return NativeResult(
            columns=list(result["columns"]),
            rows=[tuple(row) for row in result["rows"]],
        )

    def _execute_sync(self, sql: str, values: list[Any]) -> dict[str, Any]:
        """Run the blocking Rust execution call in a worker thread."""
        assert _ormdantic is not None
        return self._connection.execute(sql, values)

    def transaction(self) -> NativeTransaction:
        """Create an async transaction context manager."""
        return NativeTransaction(self)

    async def begin(self) -> None:
        """Begin a transaction on the native connection."""
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._connection.begin)

    async def commit(self) -> None:
        """Commit the active transaction."""
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._connection.commit)

    async def rollback(self) -> None:
        """Roll back the active transaction."""
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._connection.rollback)


class NativeTransaction:
    """Async context manager that commits or rolls back a native transaction."""

    def __init__(self, engine: NativeEngine) -> None:
        """Create a transaction bound to a native engine."""
        self._engine = engine

    async def __aenter__(self) -> NativeTransaction:
        """Begin the transaction and return the context manager."""
        await self._engine.begin()
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        """Commit on success and roll back on error."""
        if exc_type is None:
            await self._engine.commit()
        else:
            await self._engine.rollback()
