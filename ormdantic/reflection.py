"""Database reflection helpers."""

from __future__ import annotations

from time import perf_counter
from typing import Any

from ormdantic.errors import ReflectionError, classify_native_error


class Inspector:
    """Small runtime inspector backed by the Rust runtime."""

    def __init__(self, database: Any) -> None:
        self._database = database

    async def table_names(self) -> list[str]:
        """Return table names for supported dialects."""
        return await self._reflect(
            "table_names",
            None,
            lambda: list(self._database._ensure_runtime().table_names()),
        )

    async def columns(self, table: str) -> list[dict[str, Any]]:
        """Return column metadata for a table."""
        return await self._reflect(
            "columns",
            table,
            lambda: list(self._database._ensure_runtime().columns(table)),
        )

    async def indexes(self, table: str) -> list[dict[str, Any]]:
        """Return index metadata for a table."""
        return await self._reflect(
            "indexes",
            table,
            lambda: list(self._database._ensure_runtime().indexes(table)),
        )

    async def foreign_keys(self, table: str) -> list[dict[str, Any]]:
        """Return foreign key metadata for a table."""
        return await self._reflect(
            "foreign_keys",
            table,
            lambda: list(self._database._ensure_runtime().foreign_keys(table)),
        )

    async def _reflect(self, operation: str, table: str | None, call: Any) -> Any:
        context = self._database._context(
            "reflection", reflection=operation, table=table
        )
        payload = {
            "database": self._database,
            "operation": "reflection",
            "reflection": operation,
            "table_name": table,
            "backend": context["backend"],
        }
        await self._database._events.dispatch("before_reflection", **payload)
        started = perf_counter()
        try:
            result = call()
        except Exception as exc:
            duration_ms = (perf_counter() - started) * 1000
            error = classify_native_error(
                exc,
                default=ReflectionError,
                message=f"reflection failed for {operation}",
                context=context,
            )
            await self._database._events.dispatch(
                "after_reflection",
                **payload,
                duration_ms=duration_ms,
                error=error,
            )
            raise error from exc
        await self._database._events.dispatch(
            "after_reflection",
            **payload,
            duration_ms=(perf_counter() - started) * 1000,
            row_count=len(result) if hasattr(result, "__len__") else None,
            error=None,
        )
        return result
