from __future__ import annotations

import argparse
import hashlib
import json
import math
import random
import statistics
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from benchmark.charts import ORMDANTIC, SQLALCHEMY, SQLMODEL
from benchmark.schema import RESULT_SCHEMA_VERSION


@dataclass(frozen=True)
class ComparisonRow:
    """One aligned base/head case with current competitor ratios."""

    case: str
    rows: int
    ormdantic_ms: float
    sqlalchemy_ms: float
    sqlmodel_ms: float
    ormdantic_vs_sqlalchemy: float
    ormdantic_vs_sqlmodel: float
    head_vs_base: float | None
    comparable: bool = True
    ormdantic_vs_sqlalchemy_ci: tuple[float, float] | None = None
    ormdantic_vs_sqlmodel_ci: tuple[float, float] | None = None
    head_vs_base_ci: tuple[float, float] | None = None


@dataclass(frozen=True)
class ComparisonReport:
    """Validated benchmark comparison used by every report renderer."""

    base_commit: str
    head_commit: str
    backend: str
    profile: str
    rows: tuple[ComparisonRow, ...]
    geometric_mean_vs_sqlalchemy: float | None
    geometric_mean_vs_sqlmodel: float | None

    def as_dict(self) -> dict[str, object]:
        return {
            "schema_version": RESULT_SCHEMA_VERSION,
            "base_commit": self.base_commit,
            "head_commit": self.head_commit,
            "backend": self.backend,
            "profile": self.profile,
            "rows": [asdict(row) for row in self.rows],
            "geometric_mean_vs_sqlalchemy": self.geometric_mean_vs_sqlalchemy,
            "geometric_mean_vs_sqlmodel": self.geometric_mean_vs_sqlmodel,
        }


def compare_results(
    base: dict[str, object], head: dict[str, object]
) -> ComparisonReport:
    """Validate and align base/head results without inventing missing values."""
    _validate_payload(base, "base")
    _validate_payload(head, "head")
    base_measurements = _measurement_map(base)
    head_measurements = _measurement_map(head)
    base_samples = _sample_map(base)
    head_samples = _sample_map(head)
    comparability = _case_comparability(head)
    rows = []
    for key in _case_order(head):
        backend, profile, case, row_count = key
        current = {
            orm: head_measurements.get((*key, orm))
            for orm in (ORMDANTIC, SQLALCHEMY, SQLMODEL)
        }
        if any(value is None for value in current.values()):
            continue
        ormdantic_ms = current[ORMDANTIC]
        sqlalchemy_ms = current[SQLALCHEMY]
        sqlmodel_ms = current[SQLMODEL]
        assert ormdantic_ms is not None
        assert sqlalchemy_ms is not None
        assert sqlmodel_ms is not None
        base_ormdantic = _base_median(
            base_measurements,
            key,
            ORMDANTIC,
            allow_profile_fallback=base.get("schema_version") is None,
        )
        ormdantic_samples = head_samples.get((*key, ORMDANTIC), ())
        sqlalchemy_samples = head_samples.get((*key, SQLALCHEMY), ())
        sqlmodel_samples = head_samples.get((*key, SQLMODEL), ())
        base_ormdantic_samples = _base_samples(
            base_samples,
            key,
            ORMDANTIC,
            allow_profile_fallback=base.get("schema_version") is None,
        )
        rows.append(
            ComparisonRow(
                case=case,
                rows=row_count,
                ormdantic_ms=ormdantic_ms,
                sqlalchemy_ms=sqlalchemy_ms,
                sqlmodel_ms=sqlmodel_ms,
                ormdantic_vs_sqlalchemy=sqlalchemy_ms / ormdantic_ms,
                ormdantic_vs_sqlmodel=sqlmodel_ms / ormdantic_ms,
                head_vs_base=(
                    base_ormdantic / ormdantic_ms
                    if base_ormdantic is not None
                    else None
                ),
                comparable=comparability.get(key, True),
                ormdantic_vs_sqlalchemy_ci=_bootstrap_ratio_bounds(
                    sqlalchemy_samples,
                    ormdantic_samples,
                    seed_material=f"{key}:sqlalchemy",
                ),
                ormdantic_vs_sqlmodel_ci=_bootstrap_ratio_bounds(
                    sqlmodel_samples,
                    ormdantic_samples,
                    seed_material=f"{key}:sqlmodel",
                ),
                head_vs_base_ci=_bootstrap_ratio_bounds(
                    base_ormdantic_samples,
                    ormdantic_samples,
                    seed_material=f"{key}:base-head",
                ),
            )
        )
    if not rows:
        raise ValueError("comparison requires at least one complete ORM case")
    metadata = _mapping(head.get("metadata"), "head metadata")
    config = _mapping(head.get("config"), "head config")
    return ComparisonReport(
        base_commit=str(
            _mapping(base.get("metadata"), "base metadata").get("git_commit", "")
        ),
        head_commit=str(metadata.get("git_commit", "")),
        backend=str(metadata.get("backend", rows and _case_order(head)[0][0])),
        profile=str(config.get("profile", rows and _case_order(head)[0][1])),
        rows=tuple(rows),
        geometric_mean_vs_sqlalchemy=_geometric_mean(
            row.ormdantic_vs_sqlalchemy for row in rows if row.comparable
        ),
        geometric_mean_vs_sqlmodel=_geometric_mean(
            row.ormdantic_vs_sqlmodel for row in rows if row.comparable
        ),
    )


def load_result(path: Path) -> dict[str, object]:
    if path.stat().st_size > 10_000_000:
        raise ValueError(f"benchmark result exceeds 10 MB: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"benchmark result must be an object: {path}")
    return payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Compare base and head ORM benchmarks")
    parser.add_argument("--base", required=True, type=Path)
    parser.add_argument("--head", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--fail-regression", type=float, default=0.10)
    args = parser.parse_args(argv)
    from benchmark.report import write_pr_report

    report = compare_results(load_result(args.base), load_result(args.head))
    write_pr_report(report, args.output_dir)
    regression_limit = 1 / (1 + args.fail_regression)
    return int(
        any(
            row.comparable
            and row.head_vs_base is not None
            and row.head_vs_base < regression_limit
            and row.head_vs_base_ci is not None
            and row.head_vs_base_ci[1] < 1.0
            for row in report.rows
        )
    )


def _validate_payload(payload: dict[str, object], label: str) -> None:
    version = payload.get("schema_version")
    if version != RESULT_SCHEMA_VERSION and not (label == "base" and version is None):
        raise ValueError(f"{label} benchmark schema must be {RESULT_SCHEMA_VERSION}")
    measurements = payload.get("measurements")
    if not isinstance(measurements, list) or len(measurements) > 500:
        raise ValueError(f"{label} measurements must be a list of at most 500 items")


def _measurement_map(
    payload: dict[str, object],
) -> dict[tuple[str, str, str, int, str], float]:
    mapped = {}
    for raw in payload["measurements"]:  # type: ignore[index]
        measurement = _mapping(raw, "measurement")
        median = measurement.get("median_ms")
        if median is None or measurement.get("skip_reason") is not None:
            continue
        if isinstance(median, bool) or not isinstance(median, (int, float)):
            raise ValueError("median_ms must be a finite number")
        median = float(median)
        if not math.isfinite(median) or median <= 0:
            raise ValueError("median_ms must be finite and greater than zero")
        case = str(measurement.get("case", ""))
        if not case or len(case) > 160:
            raise ValueError("case labels must contain 1 to 160 characters")
        row_count = measurement.get("rows")
        if isinstance(row_count, bool) or not isinstance(row_count, int):
            raise ValueError("rows must be an integer")
        key = (
            str(measurement.get("backend", "")),
            str(measurement.get("profile", "")),
            case,
            row_count,
            str(measurement.get("orm", "")).lower(),
        )
        mapped[key] = median
    return mapped


def _sample_map(
    payload: dict[str, object],
) -> dict[tuple[str, str, str, int, str], tuple[float, ...]]:
    mapped = {}
    for raw in payload["measurements"]:  # type: ignore[index]
        measurement = _mapping(raw, "measurement")
        if (
            measurement.get("median_ms") is None
            or measurement.get("skip_reason") is not None
        ):
            continue
        raw_samples = measurement.get("samples_ms")
        if (
            not isinstance(raw_samples, list)
            or not raw_samples
            or len(raw_samples) > 100
        ):
            raise ValueError("samples_ms must contain 1 to 100 measurements")
        samples = []
        for sample in raw_samples:
            if isinstance(sample, bool) or not isinstance(sample, (int, float)):
                raise ValueError("samples_ms values must be finite numbers")
            value = float(sample)
            if not math.isfinite(value) or value <= 0:
                raise ValueError(
                    "samples_ms values must be finite and greater than zero"
                )
            samples.append(value)
        row_count = measurement.get("rows")
        if not isinstance(row_count, int) or isinstance(row_count, bool):
            raise ValueError("rows must be an integer")
        key = (
            str(measurement.get("backend", "")),
            str(measurement.get("profile", "")),
            str(measurement.get("case", "")),
            row_count,
            str(measurement.get("orm", "")).lower(),
        )
        mapped[key] = tuple(samples)
    return mapped


def _base_median(
    measurements: dict[tuple[str, str, str, int, str], float],
    key: tuple[str, str, str, int],
    orm: str,
    *,
    allow_profile_fallback: bool,
) -> float | None:
    exact = measurements.get((*key, orm))
    if exact is not None or not allow_profile_fallback:
        return exact
    backend, _profile, case, rows = key
    candidates = [
        median
        for (
            candidate_backend,
            _,
            candidate_case,
            candidate_rows,
            candidate_orm,
        ), median in measurements.items()
        if candidate_backend == backend
        and candidate_case == case
        and candidate_rows == rows
        and candidate_orm == orm
    ]
    return candidates[0] if len(candidates) == 1 else None


def _base_samples(
    samples: dict[tuple[str, str, str, int, str], tuple[float, ...]],
    key: tuple[str, str, str, int],
    orm: str,
    *,
    allow_profile_fallback: bool,
) -> tuple[float, ...]:
    exact = samples.get((*key, orm))
    if exact is not None or not allow_profile_fallback:
        return exact or ()
    backend, _profile, case, rows = key
    candidates = [
        values
        for (
            candidate_backend,
            _,
            candidate_case,
            candidate_rows,
            candidate_orm,
        ), values in samples.items()
        if candidate_backend == backend
        and candidate_case == case
        and candidate_rows == rows
        and candidate_orm == orm
    ]
    return candidates[0] if len(candidates) == 1 else ()


def _case_order(payload: dict[str, object]) -> list[tuple[str, str, str, int]]:
    order = []
    seen = set()
    for raw in payload["measurements"]:  # type: ignore[index]
        measurement = _mapping(raw, "measurement")
        row_count = measurement.get("rows")
        if not isinstance(row_count, int) or isinstance(row_count, bool):
            continue
        key = (
            str(measurement.get("backend", "")),
            str(measurement.get("profile", "")),
            str(measurement.get("case", "")),
            row_count,
        )
        if key not in seen:
            seen.add(key)
            order.append(key)
    return order


def _case_comparability(
    payload: dict[str, object],
) -> dict[tuple[str, str, str, int], bool]:
    comparability: dict[tuple[str, str, str, int], bool] = {}
    for raw in payload["measurements"]:  # type: ignore[index]
        measurement = _mapping(raw, "measurement")
        row_count = measurement.get("rows")
        if not isinstance(row_count, int) or isinstance(row_count, bool):
            continue
        comparable = measurement.get("comparable", True)
        if not isinstance(comparable, bool):
            raise ValueError("comparable must be a boolean")
        key = (
            str(measurement.get("backend", "")),
            str(measurement.get("profile", "")),
            str(measurement.get("case", "")),
            row_count,
        )
        existing = comparability.setdefault(key, comparable)
        if existing != comparable:
            raise ValueError(f"inconsistent comparable flag for case {key[2]!r}")
    return comparability


def _mapping(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{label} must be an object")
    return value


def _geometric_mean(values: Any) -> float | None:
    materialized = list(values)
    if not materialized:
        return None
    return math.exp(sum(math.log(value) for value in materialized) / len(materialized))


def _bootstrap_ratio_bounds(
    numerator: tuple[float, ...],
    denominator: tuple[float, ...],
    *,
    seed_material: str,
    iterations: int = 2_000,
) -> tuple[float, float] | None:
    if not numerator or not denominator:
        return None
    seed = int.from_bytes(
        hashlib.sha256(seed_material.encode("utf-8")).digest()[:8], "big"
    )
    generator = random.Random(seed)
    ratios = []
    for _ in range(iterations):
        numerator_median = statistics.median(
            generator.choice(numerator) for _ in numerator
        )
        denominator_median = statistics.median(
            generator.choice(denominator) for _ in denominator
        )
        ratios.append(numerator_median / denominator_median)
    ratios.sort()
    low_index = math.floor(0.025 * (iterations - 1))
    high_index = math.ceil(0.975 * (iterations - 1))
    return ratios[low_index], ratios[high_index]


if __name__ == "__main__":
    raise SystemExit(main())
