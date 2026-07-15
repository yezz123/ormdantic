from __future__ import annotations

from benchmark.compare import ComparisonReport, ComparisonRow
from benchmark.report import write_pr_report


def test_pr_report_writes_escaped_svg_markdown_csv_and_json(tmp_path) -> None:
    report = ComparisonReport(
        base_commit="a" * 40,
        head_commit="b" * 40,
        backend="sqlite",
        profile="ci",
        rows=(
            ComparisonRow(
                case='<script>alert("x")</script>',
                rows=10_000,
                ormdantic_ms=8.0,
                sqlalchemy_ms=16.0,
                sqlmodel_ms=24.0,
                ormdantic_vs_sqlalchemy=2.0,
                ormdantic_vs_sqlmodel=3.0,
                head_vs_base=1.25,
            ),
        ),
        geometric_mean_vs_sqlalchemy=2.0,
        geometric_mean_vs_sqlmodel=3.0,
    )

    artifacts = write_pr_report(report, tmp_path)

    svg = artifacts.svg.read_text(encoding="utf-8")
    assert "<script>" not in svg
    assert "&lt;script&gt;" in svg
    assert "Ormdantic vs SQLAlchemy 2.00x" in svg
    assert "<!-- ormdantic-benchmark-report -->" in artifacts.markdown.read_text(
        encoding="utf-8"
    )
    assert "ormdantic_vs_sqlmodel" in artifacts.csv.read_text(encoding="utf-8")
    assert '"head_commit"' in artifacts.json.read_text(encoding="utf-8")
