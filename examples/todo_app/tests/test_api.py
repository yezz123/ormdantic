from __future__ import annotations

import importlib

import pytest
from httpx import ASGITransport, AsyncClient

from examples.todo_app.app.errors import DatabaseUnavailable, ResourceConflict


@pytest.fixture
async def api_context(service_context):
    main = importlib.import_module("examples.todo_app.app.main")
    app = main.create_app(service_context.module.db)
    async with app.router.lifespan_context(app):
        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://testserver",
        ) as client:
            yield client, app


async def test_openapi_exposes_the_documented_operations(api_context) -> None:
    client, _app = api_context
    schema = (await client.get("/openapi.json")).json()

    assert set(schema["paths"]) == {
        "/health",
        "/projects",
        "/projects/{project_id}",
        "/projects/{project_id}/todos",
        "/todos",
        "/todos/{todo_id}",
    }
    assert schema["info"] == {
        "title": "Ormdantic Todo API",
        "version": "0.1.0",
    }


async def test_health_is_ready_and_redacted(api_context) -> None:
    client, _app = api_context

    response = await client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ready", "database": "sqlite"}
    assert "password" not in response.text.casefold()


async def test_project_endpoints_cover_create_list_and_get(api_context) -> None:
    client, _app = api_context
    created = await client.post("/projects", json={"name": "  Launch  "})

    assert created.status_code == 201
    project = created.json()
    assert project["name"] == "Launch"

    listing = await client.get("/projects", params={"limit": 10, "offset": 0})
    fetched = await client.get(f"/projects/{project['id']}")

    assert listing.status_code == 200
    assert listing.json()["items"] == [project]
    assert listing.json()["total"] == 1
    assert fetched.json() == project


async def test_todo_endpoints_cover_crud_filters_and_loaded_project(
    api_context,
) -> None:
    client, _app = api_context
    project = (await client.post("/projects", json={"name": "Launch"})).json()
    created = await client.post(
        f"/projects/{project['id']}/todos",
        json={"title": "  Write release notes  ", "priority": 2},
    )

    assert created.status_code == 201
    todo = created.json()
    assert todo["project_id"] == project["id"]
    assert todo["project"] is None

    listing = await client.get(
        "/todos",
        params={
            "project_id": project["id"],
            "status": "pending",
            "priority": 2,
            "search": "RELEASE",
            "limit": 10,
            "offset": 0,
        },
    )
    loaded = await client.get(f"/todos/{todo['id']}")
    updated = await client.patch(
        f"/todos/{todo['id']}",
        json={"status": "completed", "description": "Published"},
    )
    deleted = await client.delete(f"/todos/{todo['id']}")

    assert listing.status_code == 200
    assert listing.json()["total"] == 1
    assert loaded.json()["project"] == project
    assert updated.status_code == 200
    assert updated.json()["status"] == "completed"
    assert updated.json()["description"] == "Published"
    assert deleted.status_code == 204
    assert deleted.content == b""


async def test_validation_and_not_found_responses_are_stable(api_context) -> None:
    client, _app = api_context

    invalid = await client.post("/projects", json={"name": " "})
    missing = await client.get("/projects/123e4567-e89b-12d3-a456-426614174999")

    assert invalid.status_code == 422
    assert missing.status_code == 404
    assert missing.json()["error"]["code"] == "not_found"


async def test_domain_conflict_and_database_errors_are_safely_mapped(
    api_context,
) -> None:
    client, app = api_context
    original = app.state.todo_service

    class FailingService:
        async def create_project(self, _payload):
            raise ResourceConflict("Project", "postgresql://admin:secret@db")

        async def get_project(self, _project_id):
            raise DatabaseUnavailable()

    app.state.todo_service = FailingService()
    try:
        conflict = await client.post("/projects", json={"name": "Launch"})
        unavailable = await client.get("/projects/123e4567-e89b-12d3-a456-426614174999")
    finally:
        app.state.todo_service = original

    assert conflict.status_code == 409
    assert conflict.json()["error"]["code"] == "conflict"
    assert "secret" not in conflict.text
    assert unavailable.status_code == 503
    assert unavailable.json()["error"]["code"] == "database_unavailable"
