#!/usr/bin/env python3
"""Combine Python Cobertura XML and Rust LCOV line coverage totals."""

from __future__ import annotations

import argparse
import json
import sys
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Sequence


@dataclass(frozen=True)
class CoverageTotals:
    label: str
    covered: int
    valid: int

    @property
    def percent(self) -> float:
        if self.valid == 0:
            return 100.0
        return self.covered / self.valid * 100.0


def read_cobertura_line_totals(path: Path) -> CoverageTotals:
    root = ET.parse(path).getroot()
    return CoverageTotals(
        label="python",
        covered=int(root.attrib["lines-covered"]),
        valid=int(root.attrib["lines-valid"]),
    )


def read_lcov_line_totals(path: Path) -> CoverageTotals:
    summary_covered = 0
    summary_valid = 0
    current_file = ""
    line_hits: dict[tuple[str, int], int] = {}
    for line in path.read_text().splitlines():
        if line.startswith("SF:"):
            current_file = line[3:]
        elif line.startswith("DA:"):
            line_number, _, hit_count = line[3:].partition(",")
            key = (current_file, int(line_number))
            line_hits[key] = line_hits.get(key, 0) + int(hit_count)
        elif line.startswith("LH:"):
            summary_covered += int(line[3:])
        elif line.startswith("LF:"):
            summary_valid += int(line[3:])
    if line_hits:
        return CoverageTotals(
            label="rust",
            covered=sum(1 for hits in line_hits.values() if hits > 0),
            valid=len(line_hits),
        )
    covered = summary_covered
    valid = summary_valid
    return CoverageTotals(label="rust", covered=covered, valid=valid)


def combine_totals(totals: Iterable[CoverageTotals]) -> CoverageTotals:
    items = list(totals)
    return CoverageTotals(
        label="combined",
        covered=sum(item.covered for item in items),
        valid=sum(item.valid for item in items),
    )


def _format_text(totals: Sequence[CoverageTotals], fail_under: float | None) -> str:
    lines = [
        f"{item.label}: {item.covered}/{item.valid} lines ({item.percent:.2f}%)"
        for item in totals
    ]
    if fail_under is not None:
        lines.append(f"fail-under: {fail_under:.2f}%")
    return "\n".join(lines)


def _format_json(totals: Sequence[CoverageTotals], fail_under: float | None) -> str:
    return json.dumps(
        {
            "totals": {
                item.label: {
                    "covered": item.covered,
                    "valid": item.valid,
                    "percent": round(item.percent, 4),
                }
                for item in totals
            },
            "fail_under": fail_under,
        },
        indent=2,
        sort_keys=True,
    )


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Combine Python coverage.xml and Rust LCOV line coverage."
    )
    parser.add_argument(
        "--python",
        type=Path,
        default=Path("coverage.xml"),
        help="Path to coverage.py Cobertura XML output.",
    )
    parser.add_argument(
        "--rust",
        type=Path,
        default=Path("target/coverage/rust.lcov"),
        help="Path to cargo llvm-cov LCOV output.",
    )
    parser.add_argument(
        "--fail-under",
        type=float,
        default=None,
        help="Exit with status 1 when combined line coverage is below this percent.",
    )
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Output format.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    python = read_cobertura_line_totals(args.python)
    rust = read_lcov_line_totals(args.rust)
    combined = combine_totals([python, rust])
    totals = [python, rust, combined]

    if args.format == "json":
        print(_format_json(totals, args.fail_under))
    else:
        print(_format_text(totals, args.fail_under))

    if args.fail_under is not None and combined.percent < args.fail_under:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
