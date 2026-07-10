from __future__ import annotations

from pathlib import Path

from benchmark.charts import BenchmarkMeasurement, write_chart_bundle


def test_write_chart_bundle_creates_svg_charts_and_summary(tmp_path: Path) -> None:
    measurements = [
        BenchmarkMeasurement(
            case="filtered read",
            rows=10_000,
            orm="ormdantic",
            median_ms=10.0,
            samples_ms=(9.8, 10.0, 10.2),
        ),
        BenchmarkMeasurement(
            case="filtered read",
            rows=10_000,
            orm="sqlalchemy",
            median_ms=25.0,
            samples_ms=(24.8, 25.0, 25.2),
        ),
        BenchmarkMeasurement(
            case="point lookup batch",
            rows=1_000,
            orm="ormdantic",
            median_ms=18.0,
            samples_ms=(17.5, 18.0, 18.4),
        ),
        BenchmarkMeasurement(
            case="point lookup batch",
            rows=1_000,
            orm="sqlalchemy",
            median_ms=36.0,
            samples_ms=(35.2, 36.0, 36.6),
        ),
    ]

    artifacts = write_chart_bundle(measurements, tmp_path)

    latency_svg = artifacts.latency_svg.read_text()
    speedup_svg = artifacts.speedup_svg.read_text()
    summary_csv = artifacts.summary_csv.read_text()

    assert latency_svg.startswith("<svg")
    assert "Ormdantic vs SQLAlchemy median latency" in latency_svg
    assert "filtered read" in latency_svg
    assert "SQLAlchemy" in latency_svg
    assert speedup_svg.startswith("<svg")
    assert "Ormdantic speedup over SQLAlchemy" in speedup_svg
    assert "2.50x" in speedup_svg
    assert "2.00x" in speedup_svg
    assert summary_csv.splitlines()[0] == (
        "case,rows,ormdantic_median_ms,sqlalchemy_median_ms,ormdantic_speedup"
    )
    assert "filtered read,10000,10.000,25.000,2.500" in summary_csv
