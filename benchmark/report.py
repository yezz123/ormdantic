from __future__ import annotations

import csv
import html
import json
from dataclasses import dataclass
from io import StringIO
from pathlib import Path

from benchmark.compare import ComparisonReport

REPORT_MARKER = "<!-- ormdantic-benchmark-report -->"


@dataclass(frozen=True)
class ReportArtifacts:
    svg: Path
    markdown: Path
    csv: Path
    json: Path


def write_pr_report(report: ComparisonReport, output_dir: Path) -> ReportArtifacts:
    """Write deterministic pull-request benchmark artifacts."""
    output_dir.mkdir(parents=True, exist_ok=True)
    artifacts = ReportArtifacts(
        svg=output_dir / "report.svg",
        markdown=output_dir / "report.md",
        csv=output_dir / "report.csv",
        json=output_dir / "comparison.json",
    )
    artifacts.svg.write_text(_svg(report), encoding="utf-8")
    artifacts.markdown.write_text(_markdown(report), encoding="utf-8")
    artifacts.csv.write_text(_csv(report), encoding="utf-8")
    artifacts.json.write_text(
        json.dumps(report.as_dict(), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return artifacts


def _svg(report: ComparisonReport) -> str:
    width = 1280
    top = 150
    row_height = 58
    height = top + len(report.rows) * row_height + 90
    max_latency = max(
        max(row.ormdantic_ms, row.sqlalchemy_ms, row.sqlmodel_ms) for row in report.rows
    )
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}" role="img">',
        "<style>text{font-family:Inter,ui-sans-serif,system-ui,sans-serif}</style>",
        '<rect width="100%" height="100%" rx="20" fill="#0b1220"/>',
        _text(42, 52, "Ormdantic pull request benchmark", 28, "#f8fafc", 700),
        _text(
            42,
            86,
            f"{report.backend} · {report.profile} · lower latency is better",
            15,
            "#94a3b8",
            500,
        ),
        _text(
            42,
            119,
            _summary_text(report),
            17,
            "#5eead4",
            700,
        ),
    ]
    colors = (
        ("Ormdantic", "#14b8a6"),
        ("SQLAlchemy", "#64748b"),
        ("SQLModel", "#8b5cf6"),
    )
    for index, row in enumerate(report.rows):
        y = top + index * row_height
        suffix = " · diagnostic" if not row.comparable else ""
        label = f"{row.case} ({row.rows:,}){suffix}"
        parts.append(_text(42, y + 20, label, 14, "#e2e8f0", 600))
        values = (row.ormdantic_ms, row.sqlalchemy_ms, row.sqlmodel_ms)
        for offset, ((name, color), value) in enumerate(
            zip(colors, values, strict=True)
        ):
            x = 400 + offset * 275
            bar_width = max(2.0, value / max_latency * 150)
            parts.append(
                f'<rect x="{x}" y="{y + 3}" width="{bar_width:.2f}" height="18" rx="4" fill="{color}"/>'
            )
            parts.append(
                _text(
                    x + bar_width + 8, y + 17, f"{name} {value:.2f} ms", 12, color, 600
                )
            )
        delta = (
            "new" if row.head_vs_base is None else f"base/head {row.head_vs_base:.2f}x"
        )
        parts.append(_text(42, y + 43, delta, 12, "#94a3b8", 500))
    parts.append("</svg>\n")
    return "\n".join(parts)


def _markdown(report: ComparisonReport) -> str:
    lines = [
        REPORT_MARKER,
        "## Ormdantic benchmark report",
        "",
        f"**{report.backend} / {report.profile}:** {_markdown_summary(report)}",
        "",
        "| Case | Ormdantic | vs SQLAlchemy | vs SQLModel | Base/head | Scope |",
        "| --- | ---: | ---: | ---: | ---: | --- |",
    ]
    for row in report.rows:
        base = "new" if row.head_vs_base is None else f"{row.head_vs_base:.2f}x"
        lines.append(
            f"| {row.case.replace('|', '&#124;')} | {row.ormdantic_ms:.3f} ms | "
            f"{_ratio_with_ci(row.ormdantic_vs_sqlalchemy, row.ormdantic_vs_sqlalchemy_ci)} | "
            f"{_ratio_with_ci(row.ormdantic_vs_sqlmodel, row.ormdantic_vs_sqlmodel_ci)} | "
            f"{_ratio_with_ci(row.head_vs_base, row.head_vs_base_ci) if row.head_vs_base is not None else base} | "
            f"{'comparable' if row.comparable else 'diagnostic'} |"
        )
    return "\n".join(lines) + "\n"


def _csv(report: ComparisonReport) -> str:
    output = StringIO()
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(
        [
            "case",
            "rows",
            "ormdantic_ms",
            "sqlalchemy_ms",
            "sqlmodel_ms",
            "ormdantic_vs_sqlalchemy",
            "ormdantic_vs_sqlalchemy_ci_low",
            "ormdantic_vs_sqlalchemy_ci_high",
            "ormdantic_vs_sqlmodel",
            "ormdantic_vs_sqlmodel_ci_low",
            "ormdantic_vs_sqlmodel_ci_high",
            "head_vs_base",
            "head_vs_base_ci_low",
            "head_vs_base_ci_high",
            "comparable",
        ]
    )
    for row in report.rows:
        writer.writerow(
            [
                row.case,
                row.rows,
                f"{row.ormdantic_ms:.6f}",
                f"{row.sqlalchemy_ms:.6f}",
                f"{row.sqlmodel_ms:.6f}",
                f"{row.ormdantic_vs_sqlalchemy:.6f}",
                _ci_value(row.ormdantic_vs_sqlalchemy_ci, 0),
                _ci_value(row.ormdantic_vs_sqlalchemy_ci, 1),
                f"{row.ormdantic_vs_sqlmodel:.6f}",
                _ci_value(row.ormdantic_vs_sqlmodel_ci, 0),
                _ci_value(row.ormdantic_vs_sqlmodel_ci, 1),
                "" if row.head_vs_base is None else f"{row.head_vs_base:.6f}",
                _ci_value(row.head_vs_base_ci, 0),
                _ci_value(row.head_vs_base_ci, 1),
                str(row.comparable).lower(),
            ]
        )
    return output.getvalue()


def _summary_text(report: ComparisonReport) -> str:
    sqlalchemy = report.geometric_mean_vs_sqlalchemy
    sqlmodel = report.geometric_mean_vs_sqlmodel
    if sqlalchemy is None or sqlmodel is None:
        return "Diagnostic cases only · no comparable geometric mean"
    return f"Ormdantic vs SQLAlchemy {sqlalchemy:.2f}x  ·  vs SQLModel {sqlmodel:.2f}x"


def _markdown_summary(report: ComparisonReport) -> str:
    sqlalchemy = report.geometric_mean_vs_sqlalchemy
    sqlmodel = report.geometric_mean_vs_sqlmodel
    if sqlalchemy is None or sqlmodel is None:
        return "diagnostic cases only; no comparable geometric mean."
    return (
        f"Ormdantic is {sqlalchemy:.2f}x vs SQLAlchemy and "
        f"{sqlmodel:.2f}x vs SQLModel (geometric mean of comparable cases)."
    )


def _ratio_with_ci(value: float | None, bounds: tuple[float, float] | None) -> str:
    if value is None:
        return "new"
    if bounds is None:
        return f"{value:.2f}x"
    return f"{value:.2f}x ({bounds[0]:.2f}–{bounds[1]:.2f})"


def _ci_value(bounds: tuple[float, float] | None, index: int) -> str:
    return "" if bounds is None else f"{bounds[index]:.6f}"


def _text(
    x: float,
    y: float,
    value: str,
    size: int,
    color: str,
    weight: int,
) -> str:
    return (
        f'<text x="{x}" y="{y}" font-size="{size}" fill="{color}" '
        f'font-weight="{weight}">{html.escape(value)}</text>'
    )
