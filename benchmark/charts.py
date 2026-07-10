from __future__ import annotations

import csv
import html
from dataclasses import dataclass
from io import StringIO
from pathlib import Path
from typing import Iterable

ORMDANTIC = "ormdantic"
SQLALCHEMY = "sqlalchemy"


@dataclass(frozen=True)
class BenchmarkMeasurement:
    """One timed ORM benchmark result."""

    case: str
    rows: int
    orm: str
    median_ms: float
    samples_ms: tuple[float, ...]

    def as_dict(self) -> dict[str, object]:
        """Return a JSON-serializable representation."""
        return {
            "case": self.case,
            "rows": self.rows,
            "orm": self.orm,
            "median_ms": self.median_ms,
            "samples_ms": list(self.samples_ms),
        }


@dataclass(frozen=True)
class ChartArtifacts:
    """Paths written by `write_chart_bundle`."""

    latency_svg: Path
    speedup_svg: Path
    summary_csv: Path


@dataclass(frozen=True)
class _SummaryRow:
    case: str
    rows: int
    ormdantic_median_ms: float
    sqlalchemy_median_ms: float

    @property
    def speedup(self) -> float:
        if self.ormdantic_median_ms <= 0:
            return 0.0
        return self.sqlalchemy_median_ms / self.ormdantic_median_ms


def write_chart_bundle(
    measurements: Iterable[BenchmarkMeasurement],
    output_dir: Path,
) -> ChartArtifacts:
    """Write SVG charts and a CSV summary for paired ORM measurements."""
    rows = _summary_rows(tuple(measurements))
    output_dir.mkdir(parents=True, exist_ok=True)

    latency_svg = output_dir / "ormdantic-vs-sqlalchemy-latency.svg"
    speedup_svg = output_dir / "ormdantic-vs-sqlalchemy-speedup.svg"
    summary_csv = output_dir / "ormdantic-vs-sqlalchemy-summary.csv"

    latency_svg.write_text(_latency_svg(rows), encoding="utf-8")
    speedup_svg.write_text(_speedup_svg(rows), encoding="utf-8")
    summary_csv.write_text(_summary_csv(rows), encoding="utf-8")

    return ChartArtifacts(
        latency_svg=latency_svg,
        speedup_svg=speedup_svg,
        summary_csv=summary_csv,
    )


def _summary_rows(
    measurements: tuple[BenchmarkMeasurement, ...],
) -> list[_SummaryRow]:
    grouped: dict[tuple[str, int], dict[str, BenchmarkMeasurement]] = {}
    order: list[tuple[str, int]] = []
    for measurement in measurements:
        key = (measurement.case, measurement.rows)
        if key not in grouped:
            grouped[key] = {}
            order.append(key)
        grouped[key][measurement.orm.lower()] = measurement

    rows = []
    for key in order:
        pair = grouped[key]
        if ORMDANTIC not in pair or SQLALCHEMY not in pair:
            continue
        rows.append(
            _SummaryRow(
                case=key[0],
                rows=key[1],
                ormdantic_median_ms=pair[ORMDANTIC].median_ms,
                sqlalchemy_median_ms=pair[SQLALCHEMY].median_ms,
            )
        )
    if not rows:
        raise ValueError(
            "benchmark chart output requires paired ormdantic and sqlalchemy results"
        )
    return rows


def _summary_csv(rows: list[_SummaryRow]) -> str:
    buffer = StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(
        [
            "case",
            "rows",
            "ormdantic_median_ms",
            "sqlalchemy_median_ms",
            "ormdantic_speedup",
        ]
    )
    for row in rows:
        writer.writerow(
            [
                row.case,
                row.rows,
                f"{row.ormdantic_median_ms:.3f}",
                f"{row.sqlalchemy_median_ms:.3f}",
                f"{row.speedup:.3f}",
            ]
        )
    return buffer.getvalue()


def _latency_svg(rows: list[_SummaryRow]) -> str:
    width = 1120
    left = 300
    chart_width = 650
    top = 120
    group_height = 92
    height = top + len(rows) * group_height + 86
    max_ms = max(max(row.ormdantic_median_ms, row.sqlalchemy_median_ms) for row in rows)
    max_ms = max(max_ms, 1.0)
    parts = [_svg_header(width, height)]
    parts.extend(
        [
            _text(40, 52, "Ormdantic vs SQLAlchemy median latency", 28, "#111827", 700),
            _text(
                40,
                84,
                "Lower is better. Values are median wall time in milliseconds.",
                15,
                "#4b5563",
            ),
            _legend(left, 82, "Ormdantic", "#0f766e"),
            _legend(left + 160, 82, "SQLAlchemy", "#6b7280"),
        ]
    )
    for index, row in enumerate(rows):
        y = top + index * group_height
        label = f"{row.case} ({row.rows:,} rows)"
        parts.append(_text(40, y + 28, label, 15, "#111827", 600))
        parts.append(
            _bar(
                left,
                y + 8,
                row.ormdantic_median_ms / max_ms * chart_width,
                24,
                "#0f766e",
            )
        )
        parts.append(
            _bar(
                left,
                y + 42,
                row.sqlalchemy_median_ms / max_ms * chart_width,
                24,
                "#6b7280",
            )
        )
        parts.append(
            _text(
                left + row.ormdantic_median_ms / max_ms * chart_width + 12,
                y + 26,
                f"{row.ormdantic_median_ms:.2f} ms",
                14,
                "#0f766e",
                700,
            )
        )
        parts.append(
            _text(
                left + row.sqlalchemy_median_ms / max_ms * chart_width + 12,
                y + 60,
                f"{row.sqlalchemy_median_ms:.2f} ms",
                14,
                "#374151",
                700,
            )
        )
    parts.append("</svg>\n")
    return "\n".join(parts)


def _speedup_svg(rows: list[_SummaryRow]) -> str:
    width = 1120
    left = 300
    chart_width = 650
    top = 118
    group_height = 72
    height = top + len(rows) * group_height + 80
    max_speedup = max(max(row.speedup for row in rows), 1.0)
    parts = [_svg_header(width, height)]
    parts.extend(
        [
            _text(40, 52, "Ormdantic speedup over SQLAlchemy", 28, "#111827", 700),
            _text(
                40,
                84,
                "Values above 1.00x mean Ormdantic completed the measured case faster.",
                15,
                "#4b5563",
            ),
            _text(left, 104, "1.00x parity", 13, "#6b7280", 600),
            (
                f'<line x1="{left}" y1="{top - 12}" x2="{left}" y2="{height - 58}" '
                'stroke="#9ca3af" stroke-width="1" stroke-dasharray="5 5"/>'
            ),
        ]
    )
    for index, row in enumerate(rows):
        y = top + index * group_height
        label = f"{row.case} ({row.rows:,} rows)"
        speedup = row.speedup
        color = "#0f766e" if speedup >= 1.0 else "#b45309"
        bar_width = speedup / max_speedup * chart_width
        parts.append(_text(40, y + 28, label, 15, "#111827", 600))
        parts.append(_bar(left, y + 8, bar_width, 28, color))
        parts.append(
            _text(
                left + bar_width + 12,
                y + 28,
                f"{speedup:.2f}x",
                15,
                color,
                700,
            )
        )
    parts.append("</svg>\n")
    return "\n".join(parts)


def _svg_header(width: int, height: int) -> str:
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
        f'viewBox="0 0 {width} {height}" role="img">'
        "\n"
        "<style>"
        "text{font-family:Inter,ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,"
        "'Segoe UI',sans-serif;letter-spacing:0}"
        "</style>"
        f'\n<rect width="{width}" height="{height}" fill="#ffffff"/>'
    )


def _text(
    x: float,
    y: float,
    value: str,
    size: int,
    fill: str,
    weight: int = 400,
) -> str:
    return (
        f'<text x="{x:.1f}" y="{y:.1f}" font-size="{size}" '
        f'font-weight="{weight}" fill="{fill}">{html.escape(value)}</text>'
    )


def _bar(x: float, y: float, width: float, height: float, fill: str) -> str:
    return (
        f'<rect x="{x:.1f}" y="{y:.1f}" width="{max(width, 2):.1f}" '
        f'height="{height:.1f}" rx="5" fill="{fill}"/>'
    )


def _legend(x: float, y: float, label: str, fill: str) -> str:
    return (
        f'<rect x="{x:.1f}" y="{y - 13:.1f}" width="18" height="12" rx="3" '
        f'fill="{fill}"/>{_text(x + 26, y, label, 14, "#374151", 600)}'
    )
