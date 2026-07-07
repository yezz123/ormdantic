from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest


def _load_coverage_combined() -> object:
    script = Path(__file__).resolve().parents[2] / "scripts" / "coverage_combined.py"
    spec = importlib.util.spec_from_file_location("coverage_combined", script)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_combines_python_cobertura_and_rust_lcov_line_totals(tmp_path: Path) -> None:
    coverage_combined = _load_coverage_combined()
    python_xml = tmp_path / "coverage.xml"
    rust_lcov = tmp_path / "rust.lcov"
    python_xml.write_text(
        '<coverage lines-valid="10" lines-covered="9" '
        'branches-valid="4" branches-covered="2" />'
    )
    rust_lcov.write_text(
        "\n".join(
            [
                "TN:",
                "SF:/repo/rust/src/lib.rs",
                "LF:20",
                "LH:19",
                "end_of_record",
                "SF:/repo/rust/src/driver.rs",
                "LF:5",
                "LH:3",
                "end_of_record",
            ]
        )
    )

    python = coverage_combined.read_cobertura_line_totals(python_xml)
    rust = coverage_combined.read_lcov_line_totals(rust_lcov)
    combined = coverage_combined.combine_totals([python, rust])

    assert python.covered == 9
    assert python.valid == 10
    assert rust.covered == 22
    assert rust.valid == 25
    assert combined.covered == 31
    assert combined.valid == 35
    assert combined.percent == pytest.approx(88.5714285714)


def test_rust_lcov_totals_prefer_unique_da_lines_over_summary_fields(
    tmp_path: Path,
) -> None:
    coverage_combined = _load_coverage_combined()
    rust_lcov = tmp_path / "rust.lcov"
    rust_lcov.write_text(
        "\n".join(
            [
                "TN:",
                "SF:/repo/rust/src/ddl.rs",
                "DA:10,3",
                "DA:11,0",
                "DA:12,1",
                "LH:1",
                "LF:30",
                "end_of_record",
                "SF:/repo/rust/src/driver.rs",
                "DA:7,0",
                "DA:8,5",
                "LH:5",
                "LF:100",
                "end_of_record",
            ]
        )
    )

    rust = coverage_combined.read_lcov_line_totals(rust_lcov)

    assert rust.covered == 3
    assert rust.valid == 5


def test_combined_coverage_cli_enforces_fail_under(tmp_path: Path) -> None:
    coverage_combined = _load_coverage_combined()
    python_xml = tmp_path / "coverage.xml"
    rust_lcov = tmp_path / "rust.lcov"
    python_xml.write_text('<coverage lines-valid="2" lines-covered="2" />')
    rust_lcov.write_text("SF:/repo/rust/src/lib.rs\nLF:2\nLH:1\nend_of_record\n")

    assert (
        coverage_combined.main(
            [
                "--python",
                str(python_xml),
                "--rust",
                str(rust_lcov),
                "--fail-under",
                "80",
            ]
        )
        == 1
    )
    assert (
        coverage_combined.main(
            [
                "--python",
                str(python_xml),
                "--rust",
                str(rust_lcov),
                "--fail-under",
                "75",
            ]
        )
        == 0
    )
