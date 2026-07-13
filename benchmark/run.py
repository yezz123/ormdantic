from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from benchmark.backends import resolve_backend
from benchmark.charts import write_chart_bundle
from benchmark.config import (
    PROFILES,
    SUPPORTED_BACKENDS,
    BenchmarkConfigurationError,
    build_config,
)
from benchmark.runner import (
    backend_server_version,
    build_result_payload,
    run_from_config,
)

DEFAULT_RESULTS_ROOT = Path("benchmark/results")
DEFAULT_CHARTS_ROOT = Path("benchmark/charts")
DEFAULT_DOCS_CHARTS_ROOT = Path("docs/assets/benchmarks")


def main(argv: list[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    try:
        config = build_config(
            profile=args.profile,
            backend=args.backend,
            rows=args.rows,
            write_rows=args.write_rows,
            lookup_count=args.lookup_count,
            iterations=args.iterations,
            warmups=args.warmups,
            batch_size=args.batch_size,
            category=args.category,
            planner_scale=args.planner_scale,
            i_understand_this_may_be_expensive=args.i_understand_this_may_be_expensive,
        )
    except BenchmarkConfigurationError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    backend = resolve_backend(config.backend)
    try:
        measurements = run_from_config(
            config,
            allow_missing=args.allow_missing,
            progress=lambda message: print(f"running {message}", flush=True),
        )
    except Exception as exc:
        print(str(exc), file=sys.stderr)
        return 2

    server_version = None
    if not config.planner_scale:
        try:
            import asyncio

            server_version = asyncio.run(backend_server_version(backend))
        except Exception as exc:
            if not args.allow_missing:
                print(str(exc), file=sys.stderr)
                return 2

    output = Path(
        args.output
        or DEFAULT_RESULTS_ROOT
        / f"{config.backend}-{config.profile}-orm-benchmark.json"
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(
            build_result_payload(
                config=config,
                backend=backend,
                measurements=measurements,
                server_version=server_version,
            ),
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    charts_dir = Path(
        args.charts_dir or DEFAULT_CHARTS_ROOT / config.profile / config.backend
    )
    try:
        charts = write_chart_bundle(
            measurements,
            charts_dir,
            backend=config.backend,
            profile=config.profile,
        )
    except ValueError as exc:
        print(f"skipped chart output: {exc}")
        charts = None

    print(f"wrote {output}")
    if charts is not None:
        print(f"wrote {charts.latency_svg}")
        print(f"wrote {charts.speedup_svg}")
        print(f"wrote {charts.summary_csv}")

        if args.docs_charts_dir != "":
            docs_dir = Path(
                args.docs_charts_dir
                or DEFAULT_DOCS_CHARTS_ROOT / config.profile / config.backend
            )
            docs_charts = write_chart_bundle(
                measurements,
                docs_dir,
                backend=config.backend,
                profile=config.profile,
            )
            docs_charts.summary_csv.unlink(missing_ok=True)
            print(f"wrote {docs_charts.latency_svg}")
            print(f"wrote {docs_charts.speedup_svg}")

        print()
        print(charts.summary_csv.read_text(encoding="utf-8").strip())
    return 0


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run Ormdantic, SQLAlchemy, and SQLModel benchmarks across SQLite, "
            "PostgreSQL, or MySQL."
        ),
    )
    parser.add_argument("--backend", choices=SUPPORTED_BACKENDS, default="sqlite")
    parser.add_argument(
        "--profile",
        choices=tuple(PROFILES) + ("huge",),
        default="default",
    )
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
    parser.add_argument(
        "--allow-missing",
        action="store_true",
        help="record unsupported or unavailable combinations as skipped measurements",
    )
    parser.add_argument(
        "--planner-scale",
        action="store_true",
        help="label billion-scale artifacts as planner-scale rather than materialized",
    )
    parser.add_argument(
        "--i-understand-this-may-be-expensive",
        action="store_true",
        help="required for the billion profile",
    )
    return parser


if __name__ == "__main__":
    raise SystemExit(main())
