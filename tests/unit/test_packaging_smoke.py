from __future__ import annotations

import importlib.util
import subprocess
import sys
import tarfile
from email.parser import Parser
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


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


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


def test_sdist_contains_declared_license_file_and_project_readme(
    tmp_path: Path,
) -> None:
    root = _repo_root()
    dist_dir = tmp_path / "dist"
    subprocess.run(
        [sys.executable, "-m", "maturin", "sdist", "--out", str(dist_dir)],
        cwd=root,
        check=True,
    )

    sdist = next(dist_dir.glob("ormdantic-*.tar.gz"))
    root_readme = (root / "README.md").read_text(encoding="utf-8")

    with tarfile.open(sdist, "r:gz") as archive:
        names = set(archive.getnames())
        package_root = sdist.name.removesuffix(".tar.gz")
        pkg_info_member = archive.extractfile(f"{package_root}/PKG-INFO")
        assert pkg_info_member is not None
        pkg_info = pkg_info_member.read().decode("utf-8")

    metadata = Parser().parsestr(pkg_info)
    assert f"{package_root}/LICENSE" in names
    assert f"{package_root}/README.md" in names
    assert metadata.get_payload().rstrip() == root_readme.rstrip()
    assert "# ormdantic-py" not in metadata.get_payload()


def test_release_workflow_checks_crates_io_not_local_workspace() -> None:
    workflow = (_repo_root() / ".github" / "workflows" / "release.yml").read_text(
        encoding="utf-8"
    )

    assert 'cargo info --registry crates-io "${crate}@${VERSION}"' in workflow
    assert 'cargo info "${crate}@${VERSION}"' not in workflow
