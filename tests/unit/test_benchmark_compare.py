from __future__ import annotations

import json
from pathlib import Path

import pytest

from benchmark.compare import compare_results, main


def _payload(
    *, ormdantic_ms: float, sqlalchemy_ms: float, sqlmodel_ms: float
) -> dict[str, object]:
    return {
        "schema_version": 2,
        "metadata": {"git_commit": "a" * 40, "backend": "sqlite"},
        "config": {"profile": "ci"},
        "measurements": [
            {
                "backend": "sqlite",
                "profile": "ci",
                "case": "count all rows",
                "rows": 10_000,
                "orm": "ormdantic",
                "median_ms": ormdantic_ms,
                "samples_ms": [ormdantic_ms] * 7,
            },
            {
                "backend": "sqlite",
                "profile": "ci",
                "case": "count all rows",
                "rows": 10_000,
                "orm": "sqlalchemy",
                "median_ms": sqlalchemy_ms,
                "samples_ms": [sqlalchemy_ms] * 7,
            },
            {
                "backend": "sqlite",
                "profile": "ci",
                "case": "count all rows",
                "rows": 10_000,
                "orm": "sqlmodel",
                "median_ms": sqlmodel_ms,
                "samples_ms": [sqlmodel_ms] * 7,
            },
        ],
    }


def test_compare_aligns_base_head_and_competitors() -> None:
    base = _payload(ormdantic_ms=10.0, sqlalchemy_ms=18.0, sqlmodel_ms=25.0)
    head = _payload(ormdantic_ms=8.0, sqlalchemy_ms=16.0, sqlmodel_ms=24.0)

    report = compare_results(base, head)

    row = report.rows[0]
    assert row.ormdantic_ms == 8.0
    assert row.ormdantic_vs_sqlalchemy == 2.0
    assert row.ormdantic_vs_sqlmodel == 3.0
    assert row.head_vs_base == 1.25
    assert row.ormdantic_vs_sqlalchemy_ci == pytest.approx((2.0, 2.0))
    assert row.ormdantic_vs_sqlmodel_ci == pytest.approx((3.0, 3.0))
    assert row.head_vs_base_ci == pytest.approx((1.25, 1.25))
    assert report.geometric_mean_vs_sqlalchemy == 2.0
    assert report.geometric_mean_vs_sqlmodel == pytest.approx(3.0)


def test_compare_accepts_legacy_base_without_schema_version() -> None:
    base = _payload(ormdantic_ms=10.0, sqlalchemy_ms=18.0, sqlmodel_ms=25.0)
    base.pop("schema_version")
    head = _payload(ormdantic_ms=8.0, sqlalchemy_ms=16.0, sqlmodel_ms=24.0)

    report = compare_results(base, head)

    assert report.rows[0].head_vs_base == 1.25


def test_compare_aligns_legacy_base_profile_with_equivalent_head_case() -> None:
    base = _payload(ormdantic_ms=10.0, sqlalchemy_ms=18.0, sqlmodel_ms=25.0)
    base.pop("schema_version")
    base["config"]["profile"] = "smoke"
    for measurement in base["measurements"]:
        measurement["profile"] = "smoke"
    head = _payload(ormdantic_ms=8.0, sqlalchemy_ms=16.0, sqlmodel_ms=24.0)

    report = compare_results(base, head)

    assert report.rows[0].head_vs_base == 1.25


def test_compare_excludes_diagnostic_cases_from_geometric_mean() -> None:
    base = _payload(ormdantic_ms=10.0, sqlalchemy_ms=20.0, sqlmodel_ms=30.0)
    head = _payload(ormdantic_ms=10.0, sqlalchemy_ms=20.0, sqlmodel_ms=30.0)
    for payload in (base, head):
        diagnostics = []
        for measurement in payload["measurements"]:
            diagnostic = dict(measurement)
            diagnostic["case"] = "serialize simple payloads"
            diagnostic["comparable"] = False
            if diagnostic["orm"] == "ormdantic":
                diagnostic["median_ms"] = 10.0
            else:
                diagnostic["median_ms"] = 1.0
            diagnostics.append(diagnostic)
        payload["measurements"].extend(diagnostics)

    report = compare_results(base, head)

    assert len(report.rows) == 2
    assert report.rows[1].comparable is False
    assert report.geometric_mean_vs_sqlalchemy == 2.0
    assert report.geometric_mean_vs_sqlmodel == pytest.approx(3.0)


def test_cli_skips_regression_gate_for_legacy_base(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    base = _payload(ormdantic_ms=10.0, sqlalchemy_ms=18.0, sqlmodel_ms=25.0)
    base.pop("schema_version")
    head = _payload(ormdantic_ms=20.0, sqlalchemy_ms=18.0, sqlmodel_ms=25.0)
    head["metadata"]["runner_version"] = "cross-db-v2"
    base_path = tmp_path / "base.json"
    head_path = tmp_path / "head.json"
    output_dir = tmp_path / "report"
    base_path.write_text(json.dumps(base), encoding="utf-8")
    head_path.write_text(json.dumps(head), encoding="utf-8")

    exit_code = main(
        [
            "--base",
            str(base_path),
            "--head",
            str(head_path),
            "--output-dir",
            str(output_dir),
            "--fail-regression",
            "0.10",
        ]
    )

    assert exit_code == 0
    assert "regression gate skipped" in capsys.readouterr().out
    assert (output_dir / "report.md").is_file()


def test_cli_enforces_regression_gate_for_matching_protocols(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    base = _payload(ormdantic_ms=10.0, sqlalchemy_ms=18.0, sqlmodel_ms=25.0)
    head = _payload(ormdantic_ms=20.0, sqlalchemy_ms=18.0, sqlmodel_ms=25.0)
    base["metadata"]["runner_version"] = "cross-db-v2"
    head["metadata"]["runner_version"] = "cross-db-v2"
    base_path = tmp_path / "base.json"
    head_path = tmp_path / "head.json"
    output_dir = tmp_path / "report"
    base_path.write_text(json.dumps(base), encoding="utf-8")
    head_path.write_text(json.dumps(head), encoding="utf-8")

    exit_code = main(
        [
            "--base",
            str(base_path),
            "--head",
            str(head_path),
            "--output-dir",
            str(output_dir),
            "--fail-regression",
            "0.10",
        ]
    )

    assert exit_code == 1
    assert "regression gate skipped" not in capsys.readouterr().out
    assert (output_dir / "report.md").is_file()
