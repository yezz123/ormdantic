from __future__ import annotations

import asyncio
import importlib
from dataclasses import dataclass
from typing import Any, Iterator

try:
    _ormdantic: Any | None = importlib.import_module("ormdantic._ormdantic")
except ImportError:  # pragma: no cover - exercised when extension is not built
    _ormdantic = None


@dataclass
class NativeCursor:
    description: list[tuple[str]]


class NativeResult:
    def __init__(self, columns: list[str], rows: list[tuple[Any, ...]]) -> None:
        self.cursor = NativeCursor([(column,) for column in columns])
        self._rows = rows

    def __iter__(self) -> Iterator[tuple[Any, ...]]:
        return iter(self._rows)

    def scalar(self) -> Any:
        if not self._rows or not self._rows[0]:
            return None
        return self._rows[0][0]


class NativeEngine:
    def __init__(self, url: str) -> None:
        if _ormdantic is None or not hasattr(_ormdantic, "execute_native"):
            raise RuntimeError(
                "Ormdantic vNext requires the Rust extension for native execution. "
                "Install the package with maturin or reinstall the wheel."
            )
        self.url = url

    async def execute(self, sql: str, values: tuple[Any, ...]) -> NativeResult:
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(
            None, self._execute_sync, sql, list(values)
        )
        return NativeResult(
            columns=list(result["columns"]),
            rows=[tuple(row) for row in result["rows"]],
        )

    def _execute_sync(self, sql: str, values: list[Any]) -> dict[str, Any]:
        return _ormdantic.execute_native(self.url, sql, values)
