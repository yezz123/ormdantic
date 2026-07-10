from __future__ import annotations

from benchmark.charts import ORMDANTIC, SQLALCHEMY, SQLMODEL
from benchmark.ormdantic_vs_sqlalchemy import (
    BenchmarkConfig,
    benchmark_case_names,
    benchmark_orm_names,
)


def test_huge_profile_uses_million_row_read_and_write_workload() -> None:
    config = BenchmarkConfig.for_profile("huge")

    assert config.rows >= 1_000_000
    assert config.write_rows >= 1_000_000
    assert config.lookup_count >= 10_000
    assert config.iterations == 1
    assert config.warmups == 0
    assert "write insert batches" in benchmark_case_names(config)
    assert "scalar projection read" in benchmark_case_names(config)


def test_benchmark_includes_ormdantic_sqlalchemy_and_sqlmodel() -> None:
    assert benchmark_orm_names() == (ORMDANTIC, SQLALCHEMY, SQLMODEL)
