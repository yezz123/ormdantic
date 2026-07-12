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


def test_ormdantic_mysql_item_index_uses_prefix_length() -> None:
    assert _ormdantic_item_index_sql("mysql") == [
        "CREATE INDEX ormdantic_bench_items_category_idx ON ormdantic_bench_items (category(24))",
        "CREATE INDEX ormdantic_bench_items_score_idx ON ormdantic_bench_items (score)",
    ]


def test_relationship_cases_have_distinct_loader_plans() -> None:
    assert _relationship_loader_plan("one-to-many relationship loading") == "children"
    assert _relationship_loader_plan("nested relationship loading") == "children.leaves"
    assert _relationship_loader_plan("hydrate relationship results") == "joined-depth"
