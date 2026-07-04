#!/usr/bin/env python
"""Smoke-test an installed Ormdantic distribution."""

from __future__ import annotations

import importlib
import tempfile
from pathlib import Path
from typing import Any

REQUIRED_NATIVE_SYMBOLS = (
    "PyDatabase",
    "PyNativeConnection",
    "execute_native",
    "runtime_capabilities",
)
REQUIRED_CAPABILITY_KEYS = (
    "sqlite",
    "postgresql",
    "mysql",
    "mariadb",
    "mssql",
    "oracle",
)


def validate_native_runtime(
    native: Any,
    capabilities: dict[str, bool],
) -> None:
    """Validate native runtime symbols and driver diagnostics."""
    missing_symbols = [
        symbol for symbol in REQUIRED_NATIVE_SYMBOLS if not hasattr(native, symbol)
    ]
    if missing_symbols:
        raise RuntimeError(
            "Installed native extension is missing required symbol(s): "
            + ", ".join(missing_symbols)
        )

    missing_capabilities = [
        driver for driver in REQUIRED_CAPABILITY_KEYS if driver not in capabilities
    ]
    if missing_capabilities:
        raise RuntimeError(
            "Installed runtime diagnostics are missing driver key(s): "
            + ", ".join(missing_capabilities)
        )
    if not capabilities["sqlite"]:
        raise RuntimeError("Installed native runtime does not report SQLite support.")


def run_sqlite_smoke(native: Any, directory: Path) -> None:
    """Execute a SQLite query through the installed native extension."""
    database = directory / "ormdantic-smoke.sqlite3"
    result = native.execute_native(
        f"sqlite:///{database}",
        "SELECT 1 AS ok",
        [],
    )
    if result.get("columns") != ["ok"] or result.get("rows") != [[1]]:
        raise RuntimeError(f"Unexpected SQLite smoke-test result: {result!r}")


def main() -> None:
    """Run import, diagnostics, and basic SQLite execution checks."""
    import ormdantic
    from ormdantic import runtime_capabilities

    native = importlib.import_module("ormdantic._ormdantic")
    capabilities = runtime_capabilities()
    validate_native_runtime(native, capabilities)
    with tempfile.TemporaryDirectory(prefix="ormdantic-smoke-") as directory:
        run_sqlite_smoke(native, Path(directory))
    print(
        "Ormdantic smoke test passed: "
        f"version={ormdantic.__version__}, capabilities={capabilities}"
    )


if __name__ == "__main__":
    main()
