from __future__ import annotations

import argparse
import json
import platform
import sys
from datetime import datetime, timezone
from pathlib import Path

from benchmark.charts import BenchmarkMeasurement, write_chart_bundle
from benchmark.ormdantic_vs_sqlalchemy import (
    BenchmarkConfig,
    run_benchmarks_sync,
)

DEFAULT_RESULTS = Path("benchmark/results/ormdantic-vs-sqlalchemy.json")
DEFAULT_CHARTS = Path("benchmark/charts")
DEFAULT_DOCS_CHARTS = Path("docs/assets/benchmarks")


def main(argv: list[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    config = BenchmarkConfig(
        rows=args.rows,
        lookup_count=args.lookup_count,
        iterations=args.iterations,
        warmups=args.warmups,
        category=args.category,
    )

    try:
        measurements = run_benchmarks_sync(
            config,
            progress=lambda message: print(f"running {message}", flush=True),
        )
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(_payload(config, measurements), indent=2) + "\n",
        encoding="utf-8",
    )

    charts = write_chart_bundle(measurements, Path(args.charts_dir))
    print(f"wrote {output}")
    print(f"wrote {charts.latency_svg}")
    print(f"wrote {charts.speedup_svg}")
    print(f"wrote {charts.summary_csv}")

    if args.docs_charts_dir:
        docs_charts = write_chart_bundle(measurements, Path(args.docs_charts_dir))
        docs_charts.summary_csv.unlink(missing_ok=True)
        print(f"wrote {docs_charts.latency_svg}")
        print(f"wrote {docs_charts.speedup_svg}")

    print()
    print(charts.summary_csv.read_text(encoding="utf-8").strip())
    return 0


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run Ormdantic vs SQLAlchemy async SQLite benchmarks.",
    )
    parser.add_argument("--rows", type=int, default=20_000)
    parser.add_argument("--lookup-count", type=int, default=1_000)
    parser.add_argument("--iterations", type=int, default=5)
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--category", default="cat-3")
    parser.add_argument("--output", default=str(DEFAULT_RESULTS))
    parser.add_argument("--charts-dir", default=str(DEFAULT_CHARTS))
    parser.add_argument(
        "--docs-charts-dir",
        default=str(DEFAULT_DOCS_CHARTS),
        help="write docs-ready SVG copies; pass an empty string to skip",
    )
    return parser


def _payload(
    config: BenchmarkConfig,
    measurements: list[BenchmarkMeasurement],
) -> dict[str, object]:
    return {
        "metadata": {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "python": sys.version.split()[0],
            "platform": platform.platform(),
            "runner": "benchmark.ormdantic_vs_sqlalchemy",
            "notes": [
                "SQLite file databases are created in a temporary directory per sample.",
                "Rows are seeded outside the timed section for every measured case.",
                "Validation queries run after the timed section and before cleanup.",
            ],
        },
        "config": {
            "rows": config.rows,
            "lookup_count": config.lookup_count,
            "iterations": config.iterations,
            "warmups": config.warmups,
            "category": config.category,
        },
        "measurements": [measurement.as_dict() for measurement in measurements],
    }


if __name__ == "__main__":
    raise SystemExit(main())
