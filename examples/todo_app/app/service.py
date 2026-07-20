"""Application use cases for projects and todo items."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Generic, TypeVar

from ormdantic import Ormdantic, QueryExpression, column, selectinload

from .database import db
from .errors import ResourceNotFound
from .models import Project, Todo, TodoStatus, utc_now
from .schemas import ProjectCreate, TodoCreate, TodoUpdate

ModelT = TypeVar("ModelT")


@dataclass(frozen=True)
class Page(Generic[ModelT]):
    """A stable slice of matching domain models."""

    items: list[ModelT]
    total: int
    limit: int
    offset: int


class TodoService:
    """Coordinate Todo application persistence operations."""

    def __init__(self, database: Ormdantic) -> None:
        self.db = database

    async def create_project(self, payload: ProjectCreate) -> Project:
        """Create a Project from validated input."""
        return await self.db[Project].insert(Project(name=payload.name))

    async def list_projects(self, *, limit: int, offset: int) -> Page[Project]:
        """List Projects in stable creation order."""
        table = self.db[Project]
        result = await table.find_many(
            order_by=["created_at", "id"],
            limit=limit,
            offset=offset,
        )
        return Page(
            items=result.data,
            total=await table.count(),
            limit=limit,
            offset=offset,
        )

    async def get_project(self, project_id: str) -> Project:
        """Return a Project or raise a stable domain error."""
        project = await self.db[Project].find_one(project_id)
        if project is None:
            raise ResourceNotFound("Project", project_id)
        return project

    async def create_todo(self, project_id: str, payload: TodoCreate) -> Todo:
        """Create a Todo only when its owning Project exists."""
        async with self.db.transaction():
            project = await self.get_project(project_id)
            todo = Todo(
                project=project.id,
                title=payload.title,
                description=payload.description,
                priority=payload.priority,
                due_at=payload.due_at,
            )
            return await self.db[Todo].insert(todo)

    async def list_todos(
        self,
        *,
        project_id: str | None = None,
        status: TodoStatus | None = None,
        priority: int | None = None,
        search: str | None = None,
        limit: int,
        offset: int,
        load_project: bool = False,
    ) -> Page[Todo]:
        """List Todos using composed typed filters."""
        clauses: list[QueryExpression] = []
        if project_id is not None:
            clauses.append(column("project").eq(project_id))
        if status is not None:
            clauses.append(column("status").eq(status.value))
        if priority is not None:
            clauses.append(column("priority").eq(priority))
        if search is not None and search.strip():
            clauses.append(column("title").ilike(f"%{search.strip()}%"))

        where: QueryExpression | None = None
        for clause in clauses:
            where = clause if where is None else where & clause

        table = self.db[Todo]
        loaders = [selectinload("project")] if load_project else None
        result = await table.find_many(
            where=where,
            order_by=["created_at", "id"],
            limit=limit,
            offset=offset,
            load=loaders,
        )
        return Page(
            items=result.data,
            total=await table.count(where),
            limit=limit,
            offset=offset,
        )

    async def get_todo(self, todo_id: str, *, load_project: bool = True) -> Todo:
        """Return one Todo with an optional explicit Project load."""
        loaders = [selectinload("project")] if load_project else None
        todo = await self.db[Todo].find_one(todo_id, load=loaders)
        if todo is None:
            raise ResourceNotFound("Todo", todo_id)
        return todo

    async def update_todo(self, todo_id: str, payload: TodoUpdate) -> Todo:
        """Apply supplied PATCH fields and update the modification time."""
        current = await self.get_todo(todo_id, load_project=False)
        values = current.model_dump()
        values.update(payload.model_dump(exclude_unset=True))
        values["updated_at"] = utc_now()
        updated = Todo.model_validate(values)
        return await self.db[Todo].update(updated)

    async def delete_todo(self, todo_id: str) -> None:
        """Delete an existing Todo."""
        await self.get_todo(todo_id, load_project=False)
        await self.db[Todo].delete(todo_id)


service = TodoService(db)
