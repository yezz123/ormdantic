from __future__ import annotations

from pathlib import Path

import pytest

from ormdantic.engine import NativeEngine


@pytest.mark.asyncio
async def test_native_engine_executes_sqlite_queries(tmp_path: Path) -> None:
    engine = NativeEngine(f"sqlite:///{tmp_path / 'engine.sqlite3'}")

    await engine.execute(
        "CREATE TABLE flavors (id TEXT PRIMARY KEY, name TEXT)",
        (),
    )
    await engine.execute(
        "INSERT INTO flavors (id, name) VALUES (?, ?)",
        ("1", "mocha"),
    )
    result = await engine.execute(
        "SELECT id, name FROM flavors WHERE id = ?",
        ("1",),
    )

    assert result.cursor.description == [("id",), ("name",)]
    assert list(result) == [("1", "mocha")]
    assert result.scalar() == "1"
