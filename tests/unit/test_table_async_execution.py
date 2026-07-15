from __future__ import annotations

import asyncio
import time

from pydantic import BaseModel

from ormdantic.engine import NativeEngine
from ormdantic.events import EventRegistry
from ormdantic.models import Map, OrmTable
from ormdantic.table import Table


class Item(BaseModel):
    id: str


class SlowHandle:
    def count(self, filters: object, values: object) -> dict[str, object]:
        time.sleep(0.05)
        return {"columns": ["count"], "rows": [[1]], "rowcount": None}


async def test_table_native_call_does_not_block_event_loop() -> None:
    table_data = OrmTable[Item](
        model=Item,
        tablename="items",
        pk="id",
        columns=["id"],
        indexed=[],
        unique=[],
        unique_constraints=[],
        relationships={},
        back_references={},
    )
    table_map = Map(name_to_data={"items": table_data}, model_to_data={})
    table_map.model_to_data = {Item: table_data}
    table = Table[Item](
        table_data=table_data,
        table_map=table_map,
        rust_handle=SlowHandle(),
        events=EventRegistry(),
    )
    ticks = 0
    started = time.monotonic()

    async def heartbeat() -> None:
        nonlocal ticks
        deadline = started + 0.05
        while time.monotonic() < deadline:
            ticks += 1
            await asyncio.sleep(0.001)

    count, _ = await asyncio.gather(table.count(), heartbeat())

    assert count == 1
    assert ticks >= 20


async def test_native_sqlite_io_releases_python_for_heartbeat(tmp_path) -> None:
    engine = NativeEngine(f"sqlite:///{tmp_path / 'heartbeat.sqlite3'}")
    ticks = 0
    started = time.monotonic()

    async def heartbeat() -> None:
        nonlocal ticks
        deadline = started + 0.05
        while time.monotonic() < deadline:
            ticks += 1
            await asyncio.sleep(0.001)

    query = """
        WITH RECURSIVE counter(value) AS (
            SELECT 1
            UNION ALL
            SELECT value + 1 FROM counter WHERE value < 500000
        )
        SELECT SUM(value) FROM counter
    """
    result, _ = await asyncio.gather(engine.execute(query, ()), heartbeat())

    assert result.scalar() == 125000250000
    assert ticks >= 20
