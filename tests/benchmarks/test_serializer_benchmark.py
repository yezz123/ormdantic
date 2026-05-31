from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterator
from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel, Field

from ormdantic.generator._serializer import OrmSerializer
from ormdantic.models import Map, OrmTable, Relationship

pytest.importorskip("pytest_benchmark")


class _BenchFlavor(BaseModel):
    id: UUID = Field(default_factory=uuid4)
    name: str
    strength: int


class _BenchCoffee(BaseModel):
    id: UUID = Field(default_factory=uuid4)
    name: str
    flavor: _BenchFlavor | UUID


class _BenchOne(BaseModel):
    id: UUID = Field(default_factory=uuid4)
    name: str
    many: list[_BenchMany] = Field(default_factory=list)


class _BenchMany(BaseModel):
    id: UUID = Field(default_factory=uuid4)
    one: _BenchOne | UUID


_BenchOne.model_rebuild()
_BenchMany.model_rebuild()


@dataclass
class _FakeCursor:
    description: list[tuple[str]]


class _FakeResult:
    def __init__(self, columns: list[str], rows: list[tuple[Any, ...]]) -> None:
        self.cursor = _FakeCursor([(column,) for column in columns])
        self._rows = rows

    def __iter__(self) -> Iterator[tuple[Any, ...]]:
        return iter(self._rows)


def _flat_table() -> OrmTable[_BenchFlavor]:
    return OrmTable[_BenchFlavor](
        model=_BenchFlavor,
        tablename="bench_flavors",
        pk="id",
        indexed=[],
        unique=[],
        unique_constraints=[],
        columns=["id", "name", "strength"],
        relationships={},
        back_references={},
    )


def _coffee_table() -> OrmTable[_BenchCoffee]:
    return OrmTable[_BenchCoffee](
        model=_BenchCoffee,
        tablename="bench_coffees",
        pk="id",
        indexed=[],
        unique=[],
        unique_constraints=[],
        columns=["id", "name", "flavor"],
        relationships={"flavor": Relationship(foreign_table="bench_flavors")},
        back_references={},
    )


def _one_table() -> OrmTable[_BenchOne]:
    return OrmTable[_BenchOne](
        model=_BenchOne,
        tablename="bench_ones",
        pk="id",
        indexed=[],
        unique=[],
        unique_constraints=[],
        columns=["id", "name"],
        relationships={
            "many": Relationship(
                foreign_table="bench_many", back_references="one"
            )
        },
        back_references={"many": "one"},
    )


def _many_table() -> OrmTable[_BenchMany]:
    return OrmTable[_BenchMany](
        model=_BenchMany,
        tablename="bench_many",
        pk="id",
        indexed=[],
        unique=[],
        unique_constraints=[],
        columns=["id", "one"],
        relationships={"one": Relationship(foreign_table="bench_ones")},
        back_references={},
    )


def _flat_rows(row_count: int) -> list[tuple[Any, ...]]:
    return [
        (str(uuid4()), f"flavor-{index}", index)
        for index in range(row_count)
    ]


def _joined_rows(row_count: int) -> list[tuple[Any, ...]]:
    return [
        (
            str(uuid4()),
            f"coffee-{index}",
            str(uuid4()),
            f"flavor-{index}",
            index,
        )
        for index in range(row_count)
    ]


def _one_to_many_rows(parent_count: int, children_per_parent: int) -> list[tuple[Any, ...]]:
    rows = []
    for parent_index in range(parent_count):
        parent_id = str(uuid4())
        for child_index in range(children_per_parent):
            rows.append(
                (
                    parent_id,
                    f"one-{parent_index}",
                    str(uuid4()),
                    parent_id,
                )
            )
    return rows


def _deserialize_flat(rows: list[tuple[Any, ...]]) -> list[_BenchFlavor]:
    table = _flat_table()
    result = _FakeResult(
        ["bench_flavors\\id", "bench_flavors\\name", "bench_flavors\\strength"],
        rows,
    )
    return OrmSerializer[list[_BenchFlavor]](
        table_data=table,
        table_map=Map(name_to_data={table.tablename: table}, model_to_data={}),
        result_set=result,
        is_array=True,
        depth=0,
    ).deserialize()


def _deserialize_joined(rows: list[tuple[Any, ...]]) -> list[_BenchCoffee]:
    flavor = _flat_table()
    coffee = _coffee_table()
    result = _FakeResult(
        [
            "bench_coffees\\id",
            "bench_coffees\\name",
            "bench_coffees/flavor\\id",
            "bench_coffees/flavor\\name",
            "bench_coffees/flavor\\strength",
        ],
        rows,
    )
    return OrmSerializer[list[_BenchCoffee]](
        table_data=coffee,
        table_map=Map(
            name_to_data={coffee.tablename: coffee, flavor.tablename: flavor},
            model_to_data={},
        ),
        result_set=result,
        is_array=True,
        depth=1,
    ).deserialize()


def _deserialize_one_to_many(rows: list[tuple[Any, ...]]) -> list[_BenchOne]:
    one = _one_table()
    many = _many_table()
    result = _FakeResult(
        [
            "bench_ones\\id",
            "bench_ones\\name",
            "bench_ones/many\\id",
            "bench_ones/many\\one",
        ],
        rows,
    )
    return OrmSerializer[list[_BenchOne]](
        table_data=one,
        table_map=Map(
            name_to_data={one.tablename: one, many.tablename: many},
            model_to_data={},
        ),
        result_set=result,
        is_array=True,
        depth=1,
    ).deserialize()


@pytest.mark.parametrize("row_count", [1, 1_000, 10_000])
def test_flat_serializer_benchmark(benchmark: Any, row_count: int) -> None:
    rows = _flat_rows(row_count)

    result = benchmark(_deserialize_flat, rows)

    assert len(result) == row_count


def test_joined_serializer_benchmark(benchmark: Any) -> None:
    rows = _joined_rows(1_000)

    result = benchmark(_deserialize_joined, rows)

    assert len(result) == 1_000
    assert isinstance(result[0].flavor, _BenchFlavor)


def test_one_to_many_serializer_benchmark(benchmark: Any) -> None:
    rows = _one_to_many_rows(parent_count=100, children_per_parent=10)

    result = benchmark(_deserialize_one_to_many, rows)

    assert len(result) == 100
    assert len(result[0].many) == 10
