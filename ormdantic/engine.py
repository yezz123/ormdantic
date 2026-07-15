"""Async Python wrapper around Ormdantic's native Rust execution layer."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from types import TracebackType
from typing import Any, Iterator

from typing_extensions import Self

from ormdantic._native import (
    import_native_extension,
    raise_native_extension_unavailable,
    unavailable_runtime_capabilities,
)
from ormdantic.errors import (
    DatabaseConnectionError,
    NativeExtensionError,
    QueryExecutionError,
    TransactionError,
    classify_native_error,
)

try:
    _ormdantic: Any | None = import_native_extension(
        context="runtime capability diagnostics",
        required_symbols=("runtime_capabilities",),
    )
except NativeExtensionError:  # pragma: no cover - exercised when extension is not built
    _ormdantic = None


def runtime_capabilities() -> dict[str, bool]:
    """Return the database runtimes compiled into the Rust extension."""
    if _ormdantic is None or not hasattr(_ormdantic, "runtime_capabilities"):
        return unavailable_runtime_capabilities()
    return dict(_ormdantic.runtime_capabilities())


@dataclass
class NativeCursor:
    """Cursor-like metadata returned with native query results."""

    description: list[tuple[str]]


class NativeResult:
    """Result wrapper compatible with the serializer's cursor/row contract."""

    def __init__(
        self,
        columns: list[str],
        rows: list[tuple[Any, ...]],
        rowcount: int | None = None,
    ) -> None:
        """Create a result from column names and native rows."""
        self.cursor = NativeCursor([(column,) for column in columns])
        self._rows = rows
        self.rowcount = rowcount

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
            raise_native_extension_unavailable(
                context="native execution",
                required_symbols=("PyNativeConnection",),
            )
        self.url = url
        try:
            self._connection = _ormdantic.PyNativeConnection(url)
        except Exception as exc:
            error = classify_native_error(
                exc,
                default=DatabaseConnectionError,
                message="native database connection failed",
                context={"operation": "connect", "backend": _backend(url)},
            )
            raise error from exc

    async def execute(self, sql: str, values: tuple[Any, ...]) -> NativeResult:
        """Execute SQL with ordered bind values on the native connection."""
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(None, self._execute_sync, sql, list(values))
        return NativeResult(
            columns=list(result["columns"]),
            rows=[tuple(row) for row in result["rows"]],
            rowcount=result.get("rowcount"),
        )

    def _execute_sync(self, sql: str, values: list[Any]) -> dict[str, Any]:
        """Run the blocking Rust execution call in a worker thread."""
        assert _ormdantic is not None
        try:
            return self._connection.execute(sql, values)
        except Exception as exc:
            error = classify_native_error(
                exc,
                default=QueryExecutionError,
                message="native SQL execution failed",
                context={
                    "operation": "execute",
                    "backend": _backend(self.url),
                    "sql": sql,
                },
            )
            raise error from exc

    def transaction(self) -> NativeTransaction:
        """Create an async transaction context manager."""
        return NativeTransaction(self)

    async def begin(self) -> None:
        """Begin a transaction on the native connection."""
        loop = asyncio.get_running_loop()
        try:
            await loop.run_in_executor(None, self._connection.begin)
        except Exception as exc:
            error = classify_native_error(
                exc,
                default=TransactionError,
                message="native transaction begin failed",
                context={"operation": "begin", "backend": _backend(self.url)},
            )
            raise error from exc

    async def commit(self) -> None:
        """Commit the active transaction."""
        loop = asyncio.get_running_loop()
        try:
            await loop.run_in_executor(None, self._connection.commit)
        except Exception as exc:
            error = classify_native_error(
                exc,
                default=TransactionError,
                message="native transaction commit failed",
                context={"operation": "commit", "backend": _backend(self.url)},
            )
            raise error from exc

    async def rollback(self) -> None:
        """Roll back the active transaction."""
        loop = asyncio.get_running_loop()
        try:
            await loop.run_in_executor(None, self._connection.rollback)
        except Exception as exc:
            error = classify_native_error(
                exc,
                default=TransactionError,
                message="native transaction rollback failed",
                context={"operation": "rollback", "backend": _backend(self.url)},
            )
            raise error from exc


class NativeTransaction:
    """Async context manager that commits or rolls back a native transaction."""

    def __init__(self, engine: NativeEngine) -> None:
        """Create a transaction bound to a native engine."""
        self._engine = engine

    async def __aenter__(self) -> Self:
        """Begin the transaction and return the context manager."""
        await self._engine.begin()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        """Commit on success and roll back on error."""
        if exc_type is None:
            await self._engine.commit()
        else:
            await self._engine.rollback()


def _backend(url: str) -> str:
    scheme = url.split("://", 1)[0].split("+", 1)[0].lower()
    if scheme == "postgres":
        return "postgresql"
    return scheme
