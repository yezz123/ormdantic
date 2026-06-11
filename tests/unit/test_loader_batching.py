from __future__ import annotations

from pydantic import BaseModel

from ormdantic.events import EventRegistry
from ormdantic.loaders import selectinload
from ormdantic.models import Map, OrmTable
from ormdantic.table import DEFAULT_SELECTIN_BATCH_SIZE, Table


class BatchModel(BaseModel):
    id: int
    kind: str = "keep"


class BindLimitHandle:
    def __init__(self, limit: int | None) -> None:
        self.limit = limit

    def max_bind_parameters(self) -> int | None:
        return self.limit


def table_for_bind_limit(limit: int | None) -> Table[BatchModel]:
    table_data = OrmTable[BatchModel](
        model=BatchModel,
        tablename="batch_model",
        pk="id",
        indexed=[],
        unique=[],
        unique_constraints=[],
        columns=["id", "kind"],
        relationships={},
        back_references={},
    )
    return Table(
        table_data=table_data,
        table_map=Map(),
        rust_handle=BindLimitHandle(limit),
        events=EventRegistry(),
    )


def test_selectin_batches_cap_requested_size_to_backend_bind_limit() -> None:
    table = table_for_bind_limit(3)
    option = selectinload("children").filter(kind="keep").batched(10)

    assert table._selectin_batches([1, 2, 3, 4, 5], option) == [
        [1, 2],
        [3, 4],
        [5],
    ]


def test_selectin_batches_keep_requested_size_without_backend_limit() -> None:
    table = table_for_bind_limit(None)
    option = selectinload("children").batched(DEFAULT_SELECTIN_BATCH_SIZE + 1)

    assert table._selectin_batches(list(range(3)), option) == [[0, 1, 2]]
