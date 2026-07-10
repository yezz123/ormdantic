from __future__ import annotations

from typing import Any

import pytest
from typing_extensions import Self

from benchmark import ormdantic_vs_sqlalchemy as benchmark_module
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


@pytest.mark.asyncio
async def test_ormdantic_write_case_wraps_batches_in_one_transaction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    instances: list[FakeNativeEngine] = []

    class FakeNativeEngine:
        def __init__(self, url: str) -> None:
            self.url = url
            self.calls: list[tuple[str, Any]] = []
            instances.append(self)

        def transaction(self) -> Self:
            return self

        async def __aenter__(self) -> Self:
            self.calls.append(("begin", None))
            return self

        async def __aexit__(self, *args: object) -> None:
            self.calls.append(("commit", None))

        async def execute(self, sql: str, values: tuple[Any, ...]) -> None:
            self.calls.append(("execute", len(values)))

    monkeypatch.setattr(benchmark_module, "NativeEngine", FakeNativeEngine)

    config = BenchmarkConfig(write_rows=5, batch_size=2, iterations=1, warmups=0)
    operation = await benchmark_module._ormdantic_write_case(config)()
    try:
        await operation.run()
    finally:
        await operation.cleanup()

    calls = instances[0].calls
    assert calls[0] == ("begin", None)
    assert calls[-1] == ("commit", None)
    assert calls[1:-1] == [("execute", 10), ("execute", 10), ("execute", 5)]
