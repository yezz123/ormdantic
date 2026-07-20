from __future__ import annotations

import importlib
import os
import sys
from pathlib import Path

import pytest
import yaml

from ormdantic import Ormdantic

EXAMPLE_ROOT = Path(__file__).resolve().parents[1]


def test_compose_orders_database_migrations_and_api_safely() -> None:
    compose = yaml.safe_load((EXAMPLE_ROOT / "docker-compose.yml").read_text())
    services = compose["services"]

    assert set(services) == {"postgres", "migrate", "api"}
    assert services["postgres"]["healthcheck"]
    assert services["migrate"]["depends_on"]["postgres"]["condition"] == (
        "service_healthy"
    )
    assert services["api"]["depends_on"]["migrate"]["condition"] == (
        "service_completed_successfully"
    )
    assert services["api"]["healthcheck"]
    assert all(
        "/var/run/docker.sock" not in str(service.get("volumes", []))
        for service in services.values()
    )


def test_container_runs_the_local_wheel_as_a_non_root_user() -> None:
    dockerfile = (EXAMPLE_ROOT / "Dockerfile").read_text()

    assert "uv build --wheel" in dockerfile
    assert "COPY --from=builder" in dockerfile
    assert "USER app" in dockerfile
    assert 'CMD ["uvicorn", "app.main:app"' in dockerfile


@pytest.mark.skipif(
    not os.getenv("ORMDANTIC_TODO_POSTGRES_URL"),
    reason="set ORMDANTIC_TODO_POSTGRES_URL for the live PostgreSQL contract",
)
async def test_postgresql_migrations_and_service_contract(monkeypatch) -> None:
    url = os.environ["ORMDANTIC_TODO_POSTGRES_URL"]
    manager = Ormdantic(url).migrations
    migrations = EXAMPLE_ROOT / "migrations" / "postgresql"
    await manager.apply_directory(migrations)

    module_names = (
        "examples.todo_app.app.service",
        "examples.todo_app.app.schemas",
        "examples.todo_app.app.models",
        "examples.todo_app.app.database",
    )
    monkeypatch.setenv("APP_ENV", "test")
    monkeypatch.setenv("DATABASE_URL", url)
    for name in module_names:
        sys.modules.pop(name, None)

    service_module = importlib.import_module("examples.todo_app.app.service")
    schemas = importlib.import_module("examples.todo_app.app.schemas")
    await service_module.db.init()
    service = service_module.TodoService(service_module.db)

    project = await service.create_project(schemas.ProjectCreate(name="Release"))
    todo = await service.create_todo(
        project.id,
        schemas.TodoCreate(title="Ship PostgreSQL example", priority=1),
    )
    page = await service.list_todos(
        project_id=project.id,
        limit=10,
        offset=0,
    )

    assert page.items == [todo]
    assert await manager.applied_revisions() == [
        "0001_create_projects_and_todos",
        "0002_add_todo_due_date",
    ]
