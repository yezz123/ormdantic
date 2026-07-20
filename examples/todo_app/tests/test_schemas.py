import json
from datetime import datetime, timedelta, timezone
from uuid import UUID

import pytest
from pydantic import ValidationError

from examples.todo_app.app import schemas
from examples.todo_app.app.models import Project, Todo, TodoStatus

PROJECT_ID = "123e4567-e89b-12d3-a456-426614174000"
TODO_ID = "123e4567-e89b-12d3-a456-426614174001"
CREATED_AT = datetime(2030, 1, 2, 12, tzinfo=timezone.utc)
UPDATED_AT = datetime(2030, 1, 3, 12, tzinfo=timezone.utc)


def test_project_create_trims_name() -> None:
    project = schemas.ProjectCreate(name="  Launch  ")

    assert project.name == "Launch"


@pytest.mark.parametrize("name", ["", "   ", "x" * 121])
def test_project_create_rejects_invalid_name_boundaries(name: str) -> None:
    with pytest.raises(ValidationError):
        schemas.ProjectCreate(name=name)


def test_todo_create_trims_title_and_allows_boundary_length() -> None:
    todo = schemas.TodoCreate(title=f"  {'x' * 200}  ")

    assert todo.title == "x" * 200


@pytest.mark.parametrize("title", ["", "   ", "x" * 201])
def test_todo_create_rejects_invalid_title_boundaries(title: str) -> None:
    with pytest.raises(ValidationError):
        schemas.TodoCreate(title=title)


def test_todo_create_enforces_description_limit() -> None:
    with pytest.raises(ValidationError):
        schemas.TodoCreate(title="Example", description="x" * 4001)


@pytest.mark.parametrize("priority", [0, 6])
def test_todo_create_rejects_priority_outside_bounds(priority: int) -> None:
    with pytest.raises(ValidationError):
        schemas.TodoCreate(title="Example", priority=priority)


def test_todo_create_defaults_priority_and_forbids_status() -> None:
    todo = schemas.TodoCreate(title="Example")

    assert todo.priority == 3
    with pytest.raises(ValidationError, match="status"):
        schemas.TodoCreate(title="Example", status="completed")


def test_todo_update_distinguishes_omitted_fields_from_explicit_null() -> None:
    omitted = schemas.TodoUpdate()
    explicit_null = schemas.TodoUpdate(description=None, due_at=None)

    assert omitted.model_fields_set == set()
    assert omitted.model_dump(exclude_unset=True) == {}
    assert explicit_null.model_fields_set == {"description", "due_at"}
    assert explicit_null.model_dump(exclude_unset=True) == {
        "description": None,
        "due_at": None,
    }


@pytest.mark.parametrize("title", [None, "", "   "])
def test_todo_update_rejects_null_or_blank_supplied_title(
    title: str | None,
) -> None:
    with pytest.raises(ValidationError):
        schemas.TodoUpdate(title=title)


def test_todo_update_validates_status_priority_and_extra_fields() -> None:
    update = schemas.TodoUpdate(status="completed", priority=5)

    assert update.status is TodoStatus.completed
    assert update.priority == 5
    for invalid in (
        {"status": "blocked"},
        {"priority": 0},
        {"priority": 6},
        {"unexpected": "value"},
    ):
        with pytest.raises(ValidationError):
            schemas.TodoUpdate(**invalid)


@pytest.mark.parametrize("field", ["title", "status", "priority"])
def test_todo_update_rejects_explicit_null_for_non_nullable_fields(
    field: str,
) -> None:
    with pytest.raises(ValidationError):
        schemas.TodoUpdate(**{field: None})


def test_todo_update_openapi_omits_but_does_not_nullable_patch_fields() -> None:
    schema = schemas.TodoUpdate.model_json_schema()

    assert not ({"title", "status", "priority"} & set(schema.get("required", [])))
    for field in ("title", "status", "priority"):
        field_schema = schema["properties"][field]
        assert "null" not in json.dumps(field_schema)
        assert "default" not in field_schema


@pytest.mark.parametrize("schema_type_name", ["TodoCreate", "TodoUpdate"])
def test_todo_request_rejects_naive_due_at(schema_type_name: str) -> None:
    schema_type = getattr(schemas, schema_type_name)
    with pytest.raises(ValidationError, match="timezone-aware"):
        schema_type(title="Example", due_at=datetime(2030, 1, 2, 14))


@pytest.mark.parametrize("schema_type_name", ["TodoCreate", "TodoUpdate"])
def test_todo_request_normalizes_offset_due_at_to_utc(
    schema_type_name: str,
) -> None:
    schema_type = getattr(schemas, schema_type_name)
    due_at = datetime(
        2030,
        1,
        2,
        15,
        tzinfo=timezone(timedelta(hours=1)),
    )

    request = schema_type(title="Example", due_at=due_at)

    assert request.due_at == datetime(2030, 1, 2, 14, tzinfo=timezone.utc)
    assert request.due_at.tzinfo is timezone.utc


def test_project_response_validates_a_persisted_project() -> None:
    project = Project(
        id="{123E4567-E89B-12D3-A456-426614174000}",
        name="Launch",
        created_at=CREATED_AT,
    )

    response = schemas.ProjectResponse.model_validate(project)

    assert response.model_dump() == {
        "id": PROJECT_ID,
        "name": "Launch",
        "created_at": CREATED_AT,
    }


def test_response_identifiers_accept_native_uuids_and_publish_uuid_format() -> None:
    project_uuid = UUID(PROJECT_ID)
    todo_uuid = UUID(TODO_ID)

    project = schemas.ProjectResponse(
        id=project_uuid,
        name="Launch",
        created_at=CREATED_AT,
    )
    todo = schemas.TodoResponse(
        id=todo_uuid,
        project_id=project_uuid,
        title="Ship",
        description=None,
        status=TodoStatus.pending,
        priority=3,
        due_at=None,
        created_at=CREATED_AT,
        updated_at=UPDATED_AT,
    )

    assert project.id == PROJECT_ID
    assert todo.id == TODO_ID
    assert todo.project_id == PROJECT_ID
    assert schemas.ProjectResponse.model_json_schema()["properties"]["id"] == {
        "description": "Canonical project UUID.",
        "format": "uuid",
        "title": "Id",
        "type": "string",
    }
    todo_properties = schemas.TodoResponse.model_json_schema()["properties"]
    assert todo_properties["id"]["format"] == "uuid"
    assert todo_properties["project_id"]["format"] == "uuid"


def _todo(project: Project | str) -> Todo:
    return Todo(
        id=TODO_ID,
        project=project,
        title="Ship it",
        description="Release the example",
        status=TodoStatus.in_progress,
        priority=4,
        due_at=UPDATED_AT,
        created_at=CREATED_AT,
        updated_at=UPDATED_AT,
    )


def test_todo_response_converts_a_shallow_project_relationship() -> None:
    response = schemas.TodoResponse.from_model(_todo(PROJECT_ID))

    assert response.model_dump() == {
        "id": TODO_ID,
        "project_id": PROJECT_ID,
        "title": "Ship it",
        "description": "Release the example",
        "status": TodoStatus.in_progress,
        "priority": 4,
        "due_at": UPDATED_AT,
        "created_at": CREATED_AT,
        "updated_at": UPDATED_AT,
        "project": None,
    }


def test_todo_response_converts_a_loaded_project_relationship() -> None:
    project = Project(id=PROJECT_ID, name="Launch", created_at=CREATED_AT)

    response = schemas.TodoResponse.from_model(_todo(project))

    assert response.project_id == PROJECT_ID
    assert response.project == schemas.ProjectResponse.model_validate(project)


def test_page_schemas_validate_bounds_and_preserve_exact_item_shapes() -> None:
    project = schemas.ProjectResponse.model_validate(
        Project(id=PROJECT_ID, name="Launch", created_at=CREATED_AT)
    )
    todo = schemas.TodoResponse.from_model(_todo(PROJECT_ID))

    project_page = schemas.ProjectPage(items=[project], total=1, limit=100, offset=0)
    todo_page = schemas.TodoPage(items=[todo], total=1, limit=1, offset=2)

    assert project_page.model_dump()["items"] == [project.model_dump()]
    assert todo_page.model_dump()["items"] == [todo.model_dump()]
    for page_type, invalid in (
        (schemas.ProjectPage, {"total": -1, "limit": 1, "offset": 0}),
        (schemas.ProjectPage, {"total": 0, "limit": 0, "offset": 0}),
        (schemas.TodoPage, {"total": 0, "limit": 101, "offset": 0}),
        (schemas.TodoPage, {"total": 0, "limit": 1, "offset": -1}),
    ):
        with pytest.raises(ValidationError):
            page_type(items=[], **invalid)


def test_todo_response_json_uses_enum_values_and_iso_timestamps_only() -> None:
    payload = json.loads(
        schemas.TodoResponse.from_model(_todo(PROJECT_ID)).model_dump_json()
    )

    assert payload == {
        "id": TODO_ID,
        "project_id": PROJECT_ID,
        "title": "Ship it",
        "description": "Release the example",
        "status": "in_progress",
        "priority": 4,
        "due_at": "2030-01-03T12:00:00Z",
        "created_at": "2030-01-02T12:00:00Z",
        "updated_at": "2030-01-03T12:00:00Z",
        "project": None,
    }
    assert set(payload) == {
        "id",
        "project_id",
        "title",
        "description",
        "status",
        "priority",
        "due_at",
        "created_at",
        "updated_at",
        "project",
    }
