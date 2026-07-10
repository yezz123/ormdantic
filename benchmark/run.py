from __future__ import annotations

import argparse
import json
import platform
import sys
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

from benchmark.charts import BenchmarkMeasurement, write_chart_bundle
from benchmark.ormdantic_vs_sqlalchemy import (
    BenchmarkConfig,
    run_benchmarks_sync,
)

DEFAULT_RESULTS_ROOT = Path("benchmark/results")
DEFAULT_CHARTS_ROOT = Path("benchmark/charts")
DEFAULT_DOCS_CHARTS_ROOT = Path("docs/assets/benchmarks")


def main(argv: list[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    config = _config_from_args(args)

    try:
        measurements = run_benchmarks_sync(
            config,
            progress=lambda message: print(f"running {message}", flush=True),
        )
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    output = Path(
        args.output or DEFAULT_RESULTS_ROOT / f"{config.profile}-orm-benchmark.json"
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(_payload(config, measurements), indent=2) + "\n",
        encoding="utf-8",
    )

    charts_dir = Path(args.charts_dir or DEFAULT_CHARTS_ROOT / config.profile)
    charts = write_chart_bundle(measurements, charts_dir)
    print(f"wrote {output}")
    print(f"wrote {charts.latency_svg}")
    print(f"wrote {charts.speedup_svg}")
    print(f"wrote {charts.summary_csv}")

    if args.docs_charts_dir != "":
        docs_dir = Path(
            args.docs_charts_dir or DEFAULT_DOCS_CHARTS_ROOT / config.profile
        )
        docs_charts = write_chart_bundle(measurements, docs_dir)
        docs_charts.summary_csv.unlink(missing_ok=True)
        print(f"wrote {docs_charts.latency_svg}")
        print(f"wrote {docs_charts.speedup_svg}")

    print()
    print(charts.summary_csv.read_text(encoding="utf-8").strip())
    return 0


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run Ormdantic, SQLAlchemy, and SQLModel SQLite benchmarks.",
    )
    parser.add_argument("--profile", choices=["default", "huge"], default="default")
    parser.add_argument("--rows", type=int)
    parser.add_argument("--write-rows", type=int)
    parser.add_argument("--lookup-count", type=int)
    parser.add_argument("--iterations", type=int)
    parser.add_argument("--warmups", type=int)
    parser.add_argument("--batch-size", type=int)
    parser.add_argument("--category")
    parser.add_argument("--output")
    parser.add_argument("--charts-dir")
    parser.add_argument(
        "--docs-charts-dir",
        default=None,
        help="write docs-ready SVG copies; pass an empty string to skip",
    )
    return parser


def _config_from_args(args: argparse.Namespace) -> BenchmarkConfig:
    config = BenchmarkConfig.for_profile(args.profile)
    overrides = {
        "rows": args.rows,
        "write_rows": args.write_rows,
        "lookup_count": args.lookup_count,
        "iterations": args.iterations,
        "warmups": args.warmups,
        "batch_size": args.batch_size,
        "category": args.category,
    }
    return replace(
        config,
        **{key: value for key, value in overrides.items() if value is not None},
    )


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
            "write_rows": config.write_rows,
            "lookup_count": config.lookup_count,
            "iterations": config.iterations,
            "warmups": config.warmups,
            "batch_size": config.batch_size,
            "category": config.category,
            "profile": config.profile,
        },
        "measurements": [measurement.as_dict() for measurement in measurements],
    }


if __name__ == "__main__":
    raise SystemExit(main())
