"""HTTP routes for the Todo reference application."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Path, Query, Request, Response, status

from .models import TodoStatus
from .schemas import (
    ProjectCreate,
    ProjectPage,
    ProjectResponse,
    TodoCreate,
    TodoPage,
    TodoResponse,
    TodoUpdate,
    UUIDString,
)
from .service import TodoService

router = APIRouter()

PageLimit = Annotated[int, Query(ge=1, le=100)]
PageOffset = Annotated[int, Query(ge=0)]
ResourceId = Annotated[UUIDString, Path()]


def get_service(request: Request) -> TodoService:
    """Return the application-scoped service used by an endpoint."""
    return request.app.state.todo_service


Service = Annotated[TodoService, Depends(get_service)]


@router.get("/health", tags=["operations"])
async def health(request: Request) -> dict[str, str]:
    """Report readiness without exposing a database URL or credentials."""
    return {
        "status": "ready",
        "database": request.app.state.database_dialect,
    }


@router.post(
    "/projects",
    response_model=ProjectResponse,
    status_code=status.HTTP_201_CREATED,
    tags=["projects"],
)
async def create_project(
    payload: ProjectCreate,
    service: Service,
) -> ProjectResponse:
    """Create a project."""
    project = await service.create_project(payload)
    return ProjectResponse.model_validate(project)


@router.get("/projects", response_model=ProjectPage, tags=["projects"])
async def list_projects(
    service: Service,
    limit: PageLimit = 50,
    offset: PageOffset = 0,
) -> ProjectPage:
    """Return a stable, bounded page of projects."""
    page = await service.list_projects(limit=limit, offset=offset)
    return ProjectPage(
        items=[ProjectResponse.model_validate(project) for project in page.items],
        total=page.total,
        limit=page.limit,
        offset=page.offset,
    )


@router.get(
    "/projects/{project_id}",
    response_model=ProjectResponse,
    tags=["projects"],
)
async def get_project(
    project_id: ResourceId,
    service: Service,
) -> ProjectResponse:
    """Return one project by its canonical UUID."""
    project = await service.get_project(project_id)
    return ProjectResponse.model_validate(project)


@router.post(
    "/projects/{project_id}/todos",
    response_model=TodoResponse,
    status_code=status.HTTP_201_CREATED,
    tags=["todos"],
)
async def create_todo(
    project_id: ResourceId,
    payload: TodoCreate,
    service: Service,
) -> TodoResponse:
    """Create a todo in an existing project."""
    todo = await service.create_todo(project_id, payload)
    return TodoResponse.from_model(todo)


@router.get("/todos", response_model=TodoPage, tags=["todos"])
async def list_todos(
    service: Service,
    project_id: Annotated[UUIDString | None, Query()] = None,
    todo_status: Annotated[TodoStatus | None, Query(alias="status")] = None,
    priority: Annotated[int | None, Query(ge=1, le=5)] = None,
    search: Annotated[str | None, Query(max_length=200)] = None,
    limit: PageLimit = 50,
    offset: PageOffset = 0,
) -> TodoPage:
    """Filter and paginate todo items."""
    page = await service.list_todos(
        project_id=project_id,
        status=todo_status,
        priority=priority,
        search=search,
        limit=limit,
        offset=offset,
    )
    return TodoPage(
        items=[TodoResponse.from_model(todo) for todo in page.items],
        total=page.total,
        limit=page.limit,
        offset=page.offset,
    )


@router.get("/todos/{todo_id}", response_model=TodoResponse, tags=["todos"])
async def get_todo(todo_id: ResourceId, service: Service) -> TodoResponse:
    """Return one todo with its project relationship loaded."""
    return TodoResponse.from_model(await service.get_todo(todo_id))


@router.patch("/todos/{todo_id}", response_model=TodoResponse, tags=["todos"])
async def update_todo(
    todo_id: ResourceId,
    payload: TodoUpdate,
    service: Service,
) -> TodoResponse:
    """Apply a validated partial update."""
    return TodoResponse.from_model(await service.update_todo(todo_id, payload))


@router.delete(
    "/todos/{todo_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    tags=["todos"],
)
async def delete_todo(todo_id: ResourceId, service: Service) -> Response:
    """Delete one todo."""
    await service.delete_todo(todo_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)
