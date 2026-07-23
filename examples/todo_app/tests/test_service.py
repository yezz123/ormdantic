from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

import pytest

from examples.todo_app.app.errors import ResourceNotFound


async def test_project_crud_and_stable_pagination(service_context) -> None:
    context = service_context
    first = await context.service.create_project(
        context.schemas.ProjectCreate(name="First")
    )
    second = await context.service.create_project(
        context.schemas.ProjectCreate(name="Second")
    )

    page = await context.service.list_projects(limit=1, offset=1)

    assert page.total == 2
    assert page.limit == 1
    assert page.offset == 1
    assert [item.id for item in page.items] == [second.id]
    assert await context.service.get_project(first.id) == first


async def test_missing_project_raises_stable_domain_error(service_context) -> None:
    missing = str(uuid4())

    with pytest.raises(ResourceNotFound) as caught:
        await service_context.service.get_project(missing)

    assert caught.value.context == {"resource": "Project", "identifier": missing}


async def test_create_todo_requires_an_existing_project(service_context) -> None:
    with pytest.raises(ResourceNotFound):
        await service_context.service.create_todo(
            str(uuid4()),
            service_context.schemas.TodoCreate(title="Missing parent"),
        )

    assert await service_context.module.db[service_context.models.Todo].count() == 0


async def test_create_get_and_load_todo_project(service_context) -> None:
    project = await service_context.service.create_project(
        service_context.schemas.ProjectCreate(name="Launch")
    )
    todo = await service_context.service.create_todo(
        project.id,
        service_context.schemas.TodoCreate(title="Ship", priority=4),
    )

    shallow = await service_context.service.get_todo(todo.id, load_project=False)
    loaded = await service_context.service.get_todo(todo.id, load_project=True)

    assert shallow.project == project.id
    assert loaded.project == project


async def test_list_todos_composes_filters_search_and_pagination(
    service_context,
) -> None:
    context = service_context
    project = await context.service.create_project(
        context.schemas.ProjectCreate(name="Launch")
    )
    other = await context.service.create_project(
        context.schemas.ProjectCreate(name="Other")
    )
    first = await context.service.create_todo(
        project.id,
        context.schemas.TodoCreate(title="Write release notes", priority=2),
    )
    await context.service.create_todo(
        project.id,
        context.schemas.TodoCreate(title="Publish package", priority=4),
    )
    await context.service.create_todo(
        other.id,
        context.schemas.TodoCreate(title="Write unrelated", priority=2),
    )

    page = await context.service.list_todos(
        project_id=project.id,
        status=context.models.TodoStatus.pending,
        priority=2,
        search="RELEASE",
        limit=10,
        offset=0,
    )

    assert page.total == 1
    assert [item.id for item in page.items] == [first.id]


async def test_update_todo_preserves_omitted_fields_and_advances_timestamp(
    service_context,
) -> None:
    context = service_context
    project = await context.service.create_project(
        context.schemas.ProjectCreate(name="Launch")
    )
    todo = await context.service.create_todo(
        project.id,
        context.schemas.TodoCreate(title="Draft", description="Keep", priority=2),
    )
    before = todo.updated_at

    updated = await context.service.update_todo(
        todo.id,
        context.schemas.TodoUpdate(title="Published", status="completed"),
    )

    assert updated.title == "Published"
    assert updated.status is context.models.TodoStatus.completed
    assert updated.description == "Keep"
    assert updated.priority == 2
    assert updated.updated_at >= before
    assert updated.updated_at.tzinfo is timezone.utc


async def test_update_todo_can_clear_nullable_fields(service_context) -> None:
    context = service_context
    project = await context.service.create_project(
        context.schemas.ProjectCreate(name="Launch")
    )
    todo = await context.service.create_todo(
        project.id,
        context.schemas.TodoCreate(
            title="Draft",
            description="Clear",
            due_at=datetime(2030, 1, 1, tzinfo=timezone.utc),
        ),
    )

    updated = await context.service.update_todo(
        todo.id,
        context.schemas.TodoUpdate(description=None, due_at=None),
    )

    assert updated.description is None
    assert updated.due_at is None


async def test_delete_todo_and_missing_todo_errors(service_context) -> None:
    context = service_context
    project = await context.service.create_project(
        context.schemas.ProjectCreate(name="Launch")
    )
    todo = await context.service.create_todo(
        project.id,
        context.schemas.TodoCreate(title="Remove"),
    )

    await context.service.delete_todo(todo.id)

    assert await context.module.db[context.models.Todo].count() == 0
    with pytest.raises(ResourceNotFound):
        await context.service.get_todo(todo.id)
    with pytest.raises(ResourceNotFound):
        await context.service.update_todo(
            todo.id, context.schemas.TodoUpdate(title="Missing")
        )
    with pytest.raises(ResourceNotFound):
        await context.service.delete_todo(todo.id)
