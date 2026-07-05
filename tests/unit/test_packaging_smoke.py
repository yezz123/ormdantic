from __future__ import annotations

import importlib.util
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest


def _load_smoke_module() -> Any:
    root = Path(__file__).resolve().parents[2]
    script = root / "scripts" / "smoke_installed_package.py"
    spec = importlib.util.spec_from_file_location("smoke_installed_package", script)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_smoke_validation_accepts_runtime_with_required_symbols() -> None:
    smoke = _load_smoke_module()
    native = SimpleNamespace(
        PyDatabase=object,
        PyNativeConnection=object,
        execute_native=lambda *_args: {"columns": ["ok"], "rows": [[1]]},
        runtime_capabilities=lambda: {},
    )

    smoke.validate_native_runtime(
        native,
        {
            "sqlite": True,
            "postgresql": True,
            "mysql": True,
            "mariadb": True,
            "mssql": True,
            "oracle": True,
        },
    )


def test_smoke_validation_rejects_missing_sqlite_runtime() -> None:
    smoke = _load_smoke_module()
    native = SimpleNamespace(
        PyDatabase=object,
        PyNativeConnection=object,
        execute_native=lambda *_args: {"columns": ["ok"], "rows": [[1]]},
        runtime_capabilities=lambda: {},
    )

    with pytest.raises(RuntimeError, match="SQLite"):
        smoke.validate_native_runtime(
            native,
            {
                "sqlite": False,
                "postgresql": True,
                "mysql": True,
                "mariadb": True,
                "mssql": True,
                "oracle": True,
            },
        )


def test_sqlite_smoke_executes_basic_select(tmp_path: Path) -> None:
    smoke = _load_smoke_module()
    calls: list[tuple[str, str, list[object]]] = []

    def execute_native(url: str, sql: str, params: list[object]) -> dict[str, object]:
        calls.append((url, sql, params))
        return {"columns": ["ok"], "rows": [[1]]}

    smoke.run_sqlite_smoke(SimpleNamespace(execute_native=execute_native), tmp_path)

    assert calls == [
        (
            f"sqlite:///{tmp_path / 'ormdantic-smoke.sqlite3'}",
            "SELECT 1 AS ok",
            [],
        )
    ]
