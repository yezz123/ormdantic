from __future__ import annotations

import csv
import html
from dataclasses import dataclass
from io import StringIO
from pathlib import Path
from typing import Iterable

ORMDANTIC = "ormdantic"
SQLALCHEMY = "sqlalchemy"
SQLMODEL = "sqlmodel"
ORM_ORDER = (ORMDANTIC, SQLALCHEMY, SQLMODEL)
ORM_LABELS = {
    ORMDANTIC: "Ormdantic",
    SQLALCHEMY: "SQLAlchemy",
    SQLMODEL: "SQLModel",
}
ORM_COLORS = {
    ORMDANTIC: "#0f766e",
    SQLALCHEMY: "#64748b",
    SQLMODEL: "#7c3aed",
}


@dataclass(frozen=True)
class BenchmarkMeasurement:
    """One timed ORM benchmark result."""

    case: str
    rows: int
    orm: str
    median_ms: float | None
    samples_ms: tuple[float, ...]
    backend: str = ""
    profile: str = ""
    setup_ms: float | None = None
    validation: dict[str, object] | None = None
    skip_reason: str | None = None

    def as_dict(self) -> dict[str, object]:
        """Return a JSON-serializable representation."""
        payload: dict[str, object] = {
            "case": self.case,
            "rows": self.rows,
            "orm": self.orm,
            "median_ms": self.median_ms,
            "samples_ms": list(self.samples_ms),
        }
        if self.backend:
            payload["backend"] = self.backend
        if self.profile:
            payload["profile"] = self.profile
        if self.setup_ms is not None:
            payload["setup_ms"] = self.setup_ms
        if self.validation is not None:
            payload["validation"] = self.validation
        if self.skip_reason is not None:
            payload["skip_reason"] = self.skip_reason
        return payload


@dataclass(frozen=True)
class ChartArtifacts:
    """Paths written by `write_chart_bundle`."""

    latency_svg: Path
    speedup_svg: Path
    summary_csv: Path


@dataclass(frozen=True)
class _SummaryRow:
    backend: str
    profile: str
    case: str
    rows: int
    medians_ms: dict[str, float]

    def median(self, orm: str) -> float | None:
        return self.medians_ms.get(orm)

    def ormdantic_speedup(self, other: str) -> float | None:
        baseline = self.median(ORMDANTIC)
        competitor = self.median(other)
        if baseline is None or competitor is None or baseline <= 0:
            return None
        return competitor / baseline


def write_chart_bundle(
    measurements: Iterable[BenchmarkMeasurement],
    output_dir: Path,
    *,
    backend: str | None = None,
    profile: str | None = None,
) -> ChartArtifacts:
    """Write transparent SVG charts and a CSV summary."""
    rows = _summary_rows(tuple(measurements))
    output_dir.mkdir(parents=True, exist_ok=True)

    latency_svg = output_dir / "ormdantic-orm-benchmark-latency.svg"
    speedup_svg = output_dir / "ormdantic-orm-benchmark-speedup.svg"
    summary_csv = output_dir / "ormdantic-orm-benchmark-summary.csv"

    latency_svg.write_text(
        _latency_svg(rows, backend=backend, profile=profile), encoding="utf-8"
    )
    speedup_svg.write_text(
        _speedup_svg(rows, backend=backend, profile=profile), encoding="utf-8"
    )
    summary_csv.write_text(_summary_csv(rows), encoding="utf-8")

    return ChartArtifacts(
        latency_svg=latency_svg,
        speedup_svg=speedup_svg,
        summary_csv=summary_csv,
    )


def _summary_rows(
    measurements: tuple[BenchmarkMeasurement, ...],
) -> list[_SummaryRow]:
    grouped: dict[tuple[str, str, str, int], dict[str, BenchmarkMeasurement]] = {}
    order: list[tuple[str, str, str, int]] = []
    for measurement in measurements:
        if measurement.median_ms is None:
            continue
        key = (
            measurement.backend,
            measurement.profile,
            measurement.case,
            measurement.rows,
        )
        if key not in grouped:
            grouped[key] = {}
            order.append(key)
        grouped[key][measurement.orm.lower()] = measurement

    rows = []
    for key in order:
        pair = grouped[key]
        if ORMDANTIC not in pair:
            continue
        rows.append(
            _SummaryRow(
                backend=key[0],
                profile=key[1],
                case=key[2],
                rows=key[3],
                medians_ms={
                    orm: pair[orm].median_ms
                    for orm in ORM_ORDER
                    if orm in pair and pair[orm].median_ms is not None
                },
            )
        )
    if not rows:
        raise ValueError("benchmark chart output requires ormdantic results")
    return rows


def _summary_csv(rows: list[_SummaryRow]) -> str:
    buffer = StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(
        [
            "backend",
            "profile",
            "case",
            "rows",
            "ormdantic_median_ms",
            "sqlalchemy_median_ms",
            "sqlmodel_median_ms",
            "ormdantic_vs_sqlalchemy_speedup",
            "ormdantic_vs_sqlmodel_speedup",
        ]
    )
    for row in rows:
        writer.writerow(
            [
                row.backend,
                row.profile,
                row.case,
                row.rows,
                _format_number(row.median(ORMDANTIC)),
                _format_number(row.median(SQLALCHEMY)),
                _format_number(row.median(SQLMODEL)),
                _format_number(row.ormdantic_speedup(SQLALCHEMY)),
                _format_number(row.ormdantic_speedup(SQLMODEL)),
            ]
        )
    return buffer.getvalue()


def _latency_svg(
    rows: list[_SummaryRow],
    *,
    backend: str | None = None,
    profile: str | None = None,
) -> str:
    width = 1240
    left = 330
    chart_width = 720
    top = 134
    bar_height = 18
    bar_gap = 8
    group_height = 102
    height = top + len(rows) * group_height + 70
    max_ms = max(median for row in rows for median in row.medians_ms.values())
    max_ms = max(max_ms, 1.0)
    title = _latency_title(rows, backend=backend, profile=profile)
    parts = [_svg_header(width, height)]
    parts.extend(
        [
            _text(
                40,
                48,
                title,
                27,
                "#111827",
                700,
            ),
            _text(
                40,
                80,
                "Lower is better. Bars use median wall time; labels show exact milliseconds.",
                15,
                "#4b5563",
            ),
            _legend(left, 102),
        ]
    )
    for index, row in enumerate(rows):
        y = top + index * group_height
        parts.append(
            _text(40, y + 33, f"{row.case} ({row.rows:,} rows)", 15, "#111827", 700)
        )
        for orm_index, orm in enumerate(ORM_ORDER):
            median = row.median(orm)
            if median is None:
                continue
            bar_y = y + orm_index * (bar_height + bar_gap)
            bar_width = median / max_ms * chart_width
            parts.append(_bar(left, bar_y, bar_width, bar_height, ORM_COLORS[orm]))
            parts.append(
                _text(
                    left + bar_width + 10,
                    bar_y + 15,
                    f"{ORM_LABELS[orm]} {median:.2f} ms",
                    13,
                    ORM_COLORS[orm],
                    700,
                )
            )
    parts.append("</svg>\n")
    return "\n".join(parts)


def _speedup_svg(
    rows: list[_SummaryRow],
    *,
    backend: str | None = None,
    profile: str | None = None,
) -> str:
    width = 1240
    left = 330
    chart_width = 720
    top = 130
    bar_height = 22
    group_height = 88
    height = top + len(rows) * group_height + 68
    speedups = [
        speedup
        for row in rows
        for speedup in (
            row.ormdantic_speedup(SQLALCHEMY),
            row.ormdantic_speedup(SQLMODEL),
        )
        if speedup is not None
    ]
    max_speedup = max(max(speedups) if speedups else 1.0, 1.0)
    title = _speedup_title(rows, backend=backend, profile=profile)
    parts = [_svg_header(width, height)]
    parts.extend(
        [
            _text(
                40,
                48,
                title,
                27,
                "#111827",
                700,
            ),
            _text(
                40,
                80,
                "Higher is better. 1.00x is parity; values above 1.00x favor Ormdantic.",
                15,
                "#4b5563",
            ),
            (
                f'<line x1="{left}" y1="{top - 14}" x2="{left}" y2="{height - 48}" '
                'stroke="#94a3b8" stroke-width="1" stroke-dasharray="5 5"/>'
            ),
            _text(left, 106, "1.00x", 13, "#64748b", 700),
        ]
    )
    for index, row in enumerate(rows):
        y = top + index * group_height
        parts.append(
            _text(40, y + 32, f"{row.case} ({row.rows:,} rows)", 15, "#111827", 700)
        )
        for offset, orm in enumerate((SQLALCHEMY, SQLMODEL)):
            speedup = row.ormdantic_speedup(orm)
            if speedup is None:
                continue
            color = "#0f766e" if speedup >= 1.0 else "#b45309"
            bar_y = y + offset * (bar_height + 10)
            bar_width = speedup / max_speedup * chart_width
            parts.append(_bar(left, bar_y, bar_width, bar_height, color))
            parts.append(
                _text(
                    left + bar_width + 10,
                    bar_y + 17,
                    f"vs {ORM_LABELS[orm]} {speedup:.2f}x",
                    13,
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
    )


def _format_number(value: float | None) -> str:
    if value is None:
        return ""
    return f"{value:.3f}"


def _latency_title(
    rows: list[_SummaryRow],
    *,
    backend: str | None,
    profile: str | None,
) -> str:
    backend_label, profile_label = _scope_labels(rows, backend=backend, profile=profile)
    if backend_label and profile_label:
        return f"{backend_label} {profile_label} profile median latency"
    return "Ormdantic, SQLAlchemy, and SQLModel median latency"


def _speedup_title(
    rows: list[_SummaryRow],
    *,
    backend: str | None,
    profile: str | None,
) -> str:
    backend_label, profile_label = _scope_labels(rows, backend=backend, profile=profile)
    if backend_label and profile_label:
        return f"{backend_label} {profile_label} profile Ormdantic speedup"
    return "Ormdantic speedup over SQLAlchemy and SQLModel"


def _scope_labels(
    rows: list[_SummaryRow],
    *,
    backend: str | None,
    profile: str | None,
) -> tuple[str | None, str | None]:
    row_backend = rows[0].backend if rows and rows[0].backend else None
    row_profile = rows[0].profile if rows and rows[0].profile else None
    backend_name = backend or row_backend
    profile_name = profile or row_profile
    return (
        _backend_label(backend_name) if backend_name else None,
        profile_name if profile_name else None,
    )


def _backend_label(backend: str) -> str:
    labels = {
        "sqlite": "SQLite",
        "postgres": "PostgreSQL",
        "postgresql": "PostgreSQL",
        "mysql": "MySQL",
    }
    return labels.get(backend, backend.title())


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
        f'height="{height:.1f}" rx="4" fill="{fill}"/>'
    )


def _legend(x: float, y: float) -> str:
    parts = []
    cursor = x
    for orm in ORM_ORDER:
        parts.append(
            f'<rect x="{cursor:.1f}" y="{y - 13:.1f}" width="18" height="12" '
            f'rx="3" fill="{ORM_COLORS[orm]}"/>'
        )
        parts.append(_text(cursor + 26, y, ORM_LABELS[orm], 14, "#374151", 700))
        cursor += 155
    return "".join(parts)
