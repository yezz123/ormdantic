from __future__ import annotations

import importlib
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest

_MISSING = object()
_APPLICATION_MODULES = (
    "examples.todo_app.app.main",
    "examples.todo_app.app.routes",
    "examples.todo_app.app.service",
    "examples.todo_app.app.schemas",
    "examples.todo_app.app.models",
    "examples.todo_app.app.database",
)


@pytest.fixture(scope="module")
def todo_app_modules() -> SimpleNamespace:
    """Import the example against a deterministic URL without leaking module state."""
    module_names = (
        "examples.todo_app.app.models",
        "examples.todo_app.app.database",
    )
    previous_modules: dict[str, ModuleType] = {
        name: module
        for name in module_names
        if (module := sys.modules.get(name)) is not None
    }
    app_package = importlib.import_module("examples.todo_app.app")
    previous_attributes = {
        name.rsplit(".", 1)[-1]: getattr(
            app_package,
            name.rsplit(".", 1)[-1],
            _MISSING,
        )
        for name in module_names
    }

    try:
        with pytest.MonkeyPatch.context() as monkeypatch:
            monkeypatch.setenv("APP_ENV", "test")
            monkeypatch.setenv("DATABASE_URL", "sqlite:///:memory:")
            for name in module_names:
                sys.modules.pop(name, None)

            models = importlib.import_module("examples.todo_app.app.models")
            database = importlib.import_module("examples.todo_app.app.database")
            yield SimpleNamespace(models=models, database=database)
    finally:
        for name in module_names:
            sys.modules.pop(name, None)
        sys.modules.update(previous_modules)
        for name, previous_attribute in previous_attributes.items():
            if previous_attribute is _MISSING:
                if hasattr(app_package, name):
                    delattr(app_package, name)
            else:
                setattr(app_package, name, previous_attribute)


@pytest.fixture
async def service_context(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> SimpleNamespace:
    """Initialize the complete example against an isolated SQLite database."""
    previous = {
        name: module
        for name in _APPLICATION_MODULES
        if (module := sys.modules.get(name)) is not None
    }
    monkeypatch.setenv("APP_ENV", "test")
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{tmp_path / 'todo.sqlite3'}")
    for name in _APPLICATION_MODULES:
        sys.modules.pop(name, None)

    service_module = importlib.import_module("examples.todo_app.app.service")
    await service_module.db.init()
    try:
        yield SimpleNamespace(
            module=service_module,
            service=service_module.TodoService(service_module.db),
            schemas=importlib.import_module("examples.todo_app.app.schemas"),
            models=importlib.import_module("examples.todo_app.app.models"),
        )
    finally:
        for name in _APPLICATION_MODULES:
            sys.modules.pop(name, None)
        sys.modules.update(previous)
