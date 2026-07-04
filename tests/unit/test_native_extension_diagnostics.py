from __future__ import annotations

from types import SimpleNamespace

import pytest


def test_native_extension_import_failure_includes_rebuild_guidance(monkeypatch) -> None:
    from ormdantic._native import import_native_extension
    from ormdantic.errors import NativeExtensionError

    def fail_import(name: str) -> object:
        assert name == "ormdantic._ormdantic"
        raise ImportError("dynamic module does not define module export function")

    monkeypatch.setattr("ormdantic._native.importlib.import_module", fail_import)

    with pytest.raises(NativeExtensionError) as exc_info:
        import_native_extension(
            context="schema compilation",
            required_symbols=("validate_schema_tables",),
        )

    message = str(exc_info.value)
    assert "ormdantic._ormdantic" in message
    assert "schema compilation" in message
    assert "uv run --group dev maturin develop" in message
    assert "pip install --force-reinstall ormdantic" in message
    assert exc_info.value.context["extension_module"] == "ormdantic._ormdantic"
    assert exc_info.value.context["context"] == "schema compilation"
    assert exc_info.value.context["missing_symbols"] == ["validate_schema_tables"]


def test_native_extension_missing_symbols_are_reported(monkeypatch) -> None:
    from ormdantic._native import import_native_extension
    from ormdantic.errors import NativeExtensionError

    monkeypatch.setattr(
        "ormdantic._native.importlib.import_module",
        lambda name: SimpleNamespace(runtime_capabilities=lambda: {}),
    )

    with pytest.raises(NativeExtensionError) as exc_info:
        import_native_extension(
            context="native execution",
            required_symbols=("runtime_capabilities", "PyNativeConnection"),
        )

    assert "missing required symbol(s): PyNativeConnection" in str(exc_info.value)
    assert exc_info.value.context["missing_symbols"] == ["PyNativeConnection"]


def test_unavailable_runtime_capabilities_reports_all_drivers_false() -> None:
    from ormdantic._native import unavailable_runtime_capabilities

    assert unavailable_runtime_capabilities() == {
        "sqlite": False,
        "postgresql": False,
        "mysql": False,
        "mariadb": False,
        "mssql": False,
        "oracle": False,
    }
