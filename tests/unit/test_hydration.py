from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterator
from uuid import UUID, uuid4

from pydantic import BaseModel, Field

from ormdantic.generator._hydration import hydrate_flat_payload
from ormdantic.generator._serializer import OrmSerializer
from ormdantic.models import Map, OrmTable


class HydratedFlavor(BaseModel):
    id: UUID = Field(default_factory=uuid4)
    name: str
    strength: int


@dataclass
class FakeCursor:
    description: list[tuple[str]]


class FakeResult:
    def __init__(self, columns: list[str], rows: list[tuple[Any, ...]]) -> None:
        self.cursor = FakeCursor([(column,) for column in columns])
        self._rows = rows

    def __iter__(self) -> Iterator[tuple[Any, ...]]:
        return iter(self._rows)


def flavor_table() -> OrmTable[HydratedFlavor]:
    return OrmTable[HydratedFlavor](
        model=HydratedFlavor,
        tablename="hydrated_flavors",
        pk="id",
        indexed=[],
        unique=[],
        unique_constraints=[],
        columns=["id", "name", "strength"],
        relationships={},
        back_references={},
    )


def test_hydrate_flat_payload_deduplicates_array_rows_by_pk() -> None:
    flavor_id = str(uuid4())

    payload = hydrate_flat_payload(
        tablename="hydrated_flavors",
        pk="id",
        columns=[
            "hydrated_flavors\\id",
            "hydrated_flavors\\name",
            "hydrated_flavors\\strength",
        ],
        rows=[
            (flavor_id, "mocha", 1),
            (flavor_id, "duplicate", 2),
        ],
        is_array=True,
    )

    assert payload == [{"id": flavor_id, "name": "mocha", "strength": 1}]


def test_hydrate_flat_payload_returns_single_record() -> None:
    flavor_id = str(uuid4())

    payload = hydrate_flat_payload(
        tablename="hydrated_flavors",
        pk="id",
        columns=[
            "hydrated_flavors\\id",
            "hydrated_flavors\\name",
            "hydrated_flavors\\strength",
        ],
        rows=[(flavor_id, "mocha", 1)],
        is_array=False,
    )

    assert payload == {"id": flavor_id, "name": "mocha", "strength": 1}


def test_serializer_uses_flat_hydration_path_for_array_results() -> None:
    table = flavor_table()
    rows = [
        (str(uuid4()), "mocha", 1),
        (str(uuid4()), "vanilla", 2),
    ]
    result = FakeResult(
        [
            "hydrated_flavors\\id",
            "hydrated_flavors\\name",
            "hydrated_flavors\\strength",
        ],
        rows,
    )

    hydrated = OrmSerializer[list[HydratedFlavor]](
        table_data=table,
        table_map=Map(name_to_data={table.tablename: table}, model_to_data={}),
        result_set=result,
        is_array=True,
        depth=0,
    ).deserialize()

    assert hydrated == [
        HydratedFlavor(id=rows[0][0], name="mocha", strength=1),
        HydratedFlavor(id=rows[1][0], name="vanilla", strength=2),
    ]
