from __future__ import annotations

import pytest

from benchmark.backends import resolve_backend
from benchmark.cases import case_matrix
from benchmark.config import build_config
from benchmark.runner import (
    _measure_case_group,
    _median_absolute_deviation,
    _rotated_order,
)


def test_rotated_order_alternates_first_orm_by_round() -> None:
    orms = ("ormdantic", "sqlalchemy", "sqlmodel")

    assert _rotated_order(orms, 0) == orms
    assert _rotated_order(orms, 1) == ("sqlalchemy", "sqlmodel", "ormdantic")
    assert _rotated_order(orms, 2) == ("sqlmodel", "ormdantic", "sqlalchemy")


def test_median_absolute_deviation_is_robust_to_outlier() -> None:
    assert _median_absolute_deviation((1.0, 2.0, 100.0)) == 1.0


@pytest.mark.asyncio
async def test_group_measurement_rotates_orm_order_each_round(monkeypatch) -> None:
    calls: list[str] = []

    async def fake_measure_once(*, orm_name, **kwargs):
        calls.append(orm_name)
        return 1.0, 2.0

    monkeypatch.setattr("benchmark.runner._measure_once", fake_measure_once)
    config = build_config(
        profile="smoke",
        backend="sqlite",
        iterations=2,
        warmups=0,
    )
    case = next(case for case in case_matrix() if case.name == "count all rows")

    measurements = await _measure_case_group(
        config=config,
        backend=resolve_backend("sqlite"),
        case=case,
        rows=config.rows,
        orm_names=("ormdantic", "sqlalchemy", "sqlmodel"),
        expected=config.rows,
        allow_missing=False,
        progress=None,
    )

    assert calls == [
        "ormdantic",
        "sqlalchemy",
        "sqlmodel",
        "sqlalchemy",
        "sqlmodel",
        "ormdantic",
    ]
    assert measurements[0].order_positions == (0, 2)
