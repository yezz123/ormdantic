from __future__ import annotations

from benchmark.backends import resolve_backend
from benchmark.charts import BenchmarkMeasurement
from benchmark.config import build_config
from benchmark.runner import build_result_payload


def test_result_payload_contains_reproducibility_metadata() -> None:
    config = build_config(profile="smoke", backend="postgres")
    backend = resolve_backend(
        "postgres",
        env={"ORMDANTIC_BENCH_POSTGRES_URL": "postgresql://user:secret@localhost/app"},
    )
    measurement = BenchmarkMeasurement(
        backend="postgres",
        profile="smoke",
        case="count all rows",
        rows=1_000,
        orm="ormdantic",
        median_ms=1.25,
        samples_ms=(1.25,),
        mad_ms=0.0,
        setup_ms=0.5,
        order_positions=(0,),
        validation={"expected": 1_000, "actual": 1_000},
    )

    payload = build_result_payload(
        config=config,
        backend=backend,
        measurements=[measurement],
        git_commit="abc123",
        git_dirty=True,
        server_version="PostgreSQL 16",
        docker_image="postgres:16",
        cases=("count all rows",),
        orms=("ormdantic",),
    )

    assert payload["schema_version"] == 2
    assert payload["metadata"]["git_commit"] == "abc123"
    assert payload["metadata"]["git_dirty"] is True
    assert payload["metadata"]["database_url"] == "postgresql://user:***@localhost/app"
    assert payload["metadata"]["server_version"] == "PostgreSQL 16"
    assert payload["metadata"]["docker_image"] == "postgres:16"
    assert payload["metadata"]["materialized"] is True
    assert payload["metadata"]["planner_scale"] is False
    assert payload["metadata"]["runner"] == "benchmark.runner"
    assert payload["metadata"]["timing_order"] == "rotating-per-round"
    assert isinstance(payload["metadata"]["dependency_versions"], dict)
    assert payload["config"]["profile"] == "smoke"
    assert payload["config"]["cases"] == ["count all rows"]
    assert payload["config"]["orms"] == ["ormdantic"]
    assert payload["measurements"][0]["backend"] == "postgres"
    assert payload["measurements"][0]["setup_ms"] == 0.5
    assert payload["measurements"][0]["mad_ms"] == 0.0
    assert payload["measurements"][0]["order_positions"] == [0]
    assert payload["measurements"][0]["validation"] == {
        "expected": 1_000,
        "actual": 1_000,
    }
    assert payload["measurements"][0]["comparable"] is True


def test_skipped_measurements_record_reason() -> None:
    measurement = BenchmarkMeasurement(
        backend="mysql",
        profile="smoke",
        case="nested relationship loading",
        rows=12,
        orm="sqlmodel",
        median_ms=None,
        samples_ms=(),
        skip_reason="SQLModel dependency is not installed",
    )

    assert (
        measurement.as_dict()["skip_reason"] == "SQLModel dependency is not installed"
    )
    assert measurement.as_dict()["median_ms"] is None
