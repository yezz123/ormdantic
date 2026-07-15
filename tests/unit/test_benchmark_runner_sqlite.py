from __future__ import annotations

import pytest

from benchmark.backends import resolve_backend
from benchmark.cases import case_matrix
from benchmark.charts import ORMDANTIC
from benchmark.config import build_config
from benchmark.runner import (
    _measure_case,
    _ormdantic_item_index_sql,
    _relationship_loader_plan,
    _serialization_operation,
)


@pytest.mark.asyncio
async def test_ormdantic_update_case_validates_updated_rows() -> None:
    config = build_config(
        profile="smoke",
        backend="sqlite",
        rows=20,
        write_rows=20,
        lookup_count=5,
        iterations=1,
        warmups=0,
        batch_size=10,
        category="cat-3",
    )
    case = next(case for case in case_matrix() if case.name == "orm update filtered")

    measurement = await _measure_case(
        config=config,
        backend=resolve_backend("sqlite"),
        case=case,
        rows=case.rows(
            read_rows=config.rows,
            write_rows=config.write_rows,
            lookup_count=config.lookup_count,
        ),
        orm_name=ORMDANTIC,
        expected=case.expected(
            read_rows=config.rows,
            write_rows=config.write_rows,
            lookup_count=config.lookup_count,
            category=config.category,
        ),
    )

    assert measurement.validation == {"expected": 2, "actual": 2}


@pytest.mark.asyncio
async def test_ormdantic_insert_case_uses_bulk_table_api(monkeypatch) -> None:
    async def reject_single_insert(*args, **kwargs):
        raise AssertionError("benchmark used the single-row insert path")

    monkeypatch.setattr("ormdantic.table.Table.insert", reject_single_insert)
    config = build_config(
        profile="smoke",
        backend="sqlite",
        rows=6,
        write_rows=6,
        lookup_count=2,
        iterations=1,
        warmups=0,
        batch_size=3,
    )
    case = next(case for case in case_matrix() if case.name == "orm insert models")

    measurement = await _measure_case(
        config=config,
        backend=resolve_backend("sqlite"),
        case=case,
        rows=6,
        orm_name=ORMDANTIC,
        expected=6,
    )

    assert measurement.validation == {"expected": 6, "actual": 6}


@pytest.mark.asyncio
async def test_ormdantic_upsert_case_uses_bulk_table_api(monkeypatch) -> None:
    async def reject_single_upsert(*args, **kwargs):
        raise AssertionError("benchmark used the single-row upsert path")

    monkeypatch.setattr("ormdantic.table.Table.upsert", reject_single_upsert)
    config = build_config(
        profile="smoke",
        backend="sqlite",
        rows=20,
        write_rows=20,
        lookup_count=5,
        iterations=1,
        warmups=0,
        batch_size=10,
    )
    case = next(case for case in case_matrix() if case.name == "orm upsert mixed")

    measurement = await _measure_case(
        config=config,
        backend=resolve_backend("sqlite"),
        case=case,
        rows=5,
        orm_name=ORMDANTIC,
        expected=5,
    )

    assert measurement.validation == {"expected": 5, "actual": 5}


@pytest.mark.asyncio
async def test_ormdantic_delete_case_uses_set_based_table_api(monkeypatch) -> None:
    async def reject_single_delete(*args, **kwargs):
        raise AssertionError("benchmark used the single-row delete path")

    monkeypatch.setattr("ormdantic.table.Table.delete", reject_single_delete)
    config = build_config(
        profile="smoke",
        backend="sqlite",
        rows=20,
        write_rows=20,
        lookup_count=5,
        iterations=1,
        warmups=0,
        batch_size=10,
        category="cat-3",
    )
    case = next(case for case in case_matrix() if case.name == "orm delete filtered")

    measurement = await _measure_case(
        config=config,
        backend=resolve_backend("sqlite"),
        case=case,
        rows=20,
        orm_name=ORMDANTIC,
        expected=18,
    )

    assert measurement.validation == {"expected": 18, "actual": 18}


def test_ormdantic_mysql_item_index_uses_prefix_length() -> None:
    assert _ormdantic_item_index_sql("mysql") == [
        "CREATE INDEX ormdantic_bench_items_category_idx ON ormdantic_bench_items (category(24))",
        "CREATE INDEX ormdantic_bench_items_score_idx ON ormdantic_bench_items (score)",
    ]


def test_relationship_cases_have_distinct_loader_plans() -> None:
    assert _relationship_loader_plan("one-to-many relationship loading") == "children"
    assert _relationship_loader_plan("nested relationship loading") == "children.leaves"
    assert _relationship_loader_plan("hydrate relationship results") == "joined-depth"


@pytest.mark.asyncio
async def test_serialization_payload_setup_happens_before_timing(monkeypatch) -> None:
    setup_calls: list[int] = []
    serialization_calls: list[int] = []

    def prepare_payloads(orm_name: str, count: int) -> list[dict[str, str]]:
        setup_calls.append(count)
        return [{"id": str(index)} for index in range(count)]

    def serialize_payloads(
        orm_name: str, inputs: list[dict[str, str]]
    ) -> list[dict[str, str]]:
        serialization_calls.append(len(inputs))
        return inputs

    monkeypatch.setattr(
        "benchmark.runner._simple_serialization_inputs",
        prepare_payloads,
        raising=False,
    )
    monkeypatch.setattr(
        "benchmark.runner._serialize_simple_inputs",
        serialize_payloads,
        raising=False,
    )
    config = build_config(
        profile="smoke",
        backend="sqlite",
        rows=10,
        write_rows=10,
        lookup_count=3,
        iterations=1,
        warmups=0,
    )

    operation = _serialization_operation(config, "serialize simple payloads", ORMDANTIC)

    assert setup_calls == [3]
    assert serialization_calls == []
    await operation.run()
    assert setup_calls == [3]
    assert serialization_calls == [3]
