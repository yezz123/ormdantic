from __future__ import annotations

import asyncio
import threading
import time
from typing import Any

from pydantic import BaseModel

from ormdantic.engine import NativeEngine
from ormdantic.events import EventRegistry
from ormdantic.models import Map, OrmTable
from ormdantic.table import Table


class Item(BaseModel):
    id: str


class SlowHandle:
    execution_thread_id: int | None = None

    def count(self, filters: object, values: object) -> dict[str, object]:
        self.execution_thread_id = threading.get_ident()
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
    handle = SlowHandle()
    table = Table[Item](
        table_data=table_data,
        table_map=table_map,
        rust_handle=handle,
        events=EventRegistry(),
    )
    event_loop_thread_id = threading.get_ident()

    count = await table.count()

    assert handle.execution_thread_id is not None
    assert handle.execution_thread_id != event_loop_thread_id
    assert count == 1


async def test_native_sqlite_io_releases_python_while_query_is_running(
    tmp_path,
) -> None:
    native_started = threading.Event()
    native_finished = threading.Event()

    class ObservedNativeEngine(NativeEngine):
        def _execute_sync(self, sql: str, values: list[Any]) -> dict[str, Any]:
            native_started.set()
            try:
                return super()._execute_sync(sql, values)
            finally:
                native_finished.set()

    engine = ObservedNativeEngine(f"sqlite:///{tmp_path / 'heartbeat.sqlite3'}")

    query = """
        WITH RECURSIVE counter(value) AS (
            SELECT 1
            UNION ALL
            SELECT value + 1 FROM counter WHERE value < 5000000
        )
        SELECT SUM(value) FROM counter
    """
    query_task = asyncio.create_task(engine.execute(query, ()))

    assert await asyncio.to_thread(native_started.wait, 5)
    assert not native_finished.is_set()

    result = await asyncio.wait_for(query_task, 30)

    assert result.scalar() == 12500002500000
