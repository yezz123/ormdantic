"""Validated request and response shapes for the Todo API."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Annotated, Any
from uuid import UUID

from pydantic import (
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    WithJsonSchema,
    field_validator,
)
from pydantic.json_schema import SkipJsonSchema

from .models import Project, Todo, TodoStatus


def _canonical_uuid(value: str | UUID) -> str:
    if isinstance(value, UUID):
        return str(value)
    try:
        return str(UUID(value))
    except (AttributeError, TypeError, ValueError) as error:
        raise ValueError("value must be a valid UUID") from error


UUIDString = Annotated[
    str,
    BeforeValidator(_canonical_uuid),
    WithJsonSchema({"type": "string", "format": "uuid"}),
]


def _aware_to_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("datetime must be timezone-aware")
    return value.astimezone(timezone.utc)


def _patch_schema(schema: dict[str, Any]) -> None:
    properties = schema.get("properties", {})
    for name in ("title", "status", "priority"):
        field_schema = properties.get(name)
        if isinstance(field_schema, dict):
            field_schema.pop("default", None)


class ProjectCreate(BaseModel):
    """Input accepted when creating a project."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    name: str = Field(
        min_length=1,
        max_length=120,
        description="Human-readable project name.",
    )


class ProjectResponse(BaseModel):
    """Public representation of a persisted project."""

    model_config = ConfigDict(from_attributes=True)

    id: UUIDString = Field(description="Canonical project UUID.")
    name: str = Field(description="Human-readable project name.")
    created_at: datetime = Field(description="UTC project creation time.")

    @field_validator("created_at")
    @classmethod
    def normalize_created_at(cls, value: datetime) -> datetime:
        """Require and normalize the project creation time."""
        return _aware_to_utc(value)


class TodoCreate(BaseModel):
    """Input accepted when creating a todo item."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    title: str = Field(
        min_length=1,
        max_length=200,
        description="Short todo title.",
    )
    description: str | None = Field(
        default=None,
        max_length=4000,
        description="Optional todo details.",
    )
    priority: int = Field(
        default=3,
        ge=1,
        le=5,
        description="Priority from 1 (highest) to 5 (lowest).",
    )
    due_at: datetime | None = Field(
        default=None,
        description="Optional timezone-aware due time, normalized to UTC.",
    )

    @field_validator("due_at")
    @classmethod
    def normalize_due_at(cls, value: datetime | None) -> datetime | None:
        """Require supplied due times to be aware and normalize them to UTC."""
        return None if value is None else _aware_to_utc(value)


class TodoUpdate(BaseModel):
    """Optional fields accepted when partially updating a todo item."""

    model_config = ConfigDict(
        extra="forbid",
        str_strip_whitespace=True,
        json_schema_extra=_patch_schema,
    )

    title: str | SkipJsonSchema[None] = Field(
        default=None,
        min_length=1,
        max_length=200,
        description="Replacement title when supplied.",
    )
    description: str | None = Field(
        default=None,
        max_length=4000,
        description="Replacement details; null clears them.",
    )
    status: TodoStatus | SkipJsonSchema[None] = Field(
        default=None,
        description="Replacement lifecycle status.",
    )
    priority: int | SkipJsonSchema[None] = Field(
        default=None,
        ge=1,
        le=5,
        description="Replacement priority from 1 to 5.",
    )
    due_at: datetime | None = Field(
        default=None,
        description="Replacement due time; null clears it.",
    )

    @field_validator("title", "status", "priority", mode="before")
    @classmethod
    def reject_null_non_nullable_patch_field(cls, value: Any) -> Any:
        """Allow omission but reject null for fields that cannot be cleared."""
        if value is None:
            raise ValueError("field cannot be null")
        return value

    @field_validator("due_at")
    @classmethod
    def normalize_due_at(cls, value: datetime | None) -> datetime | None:
        """Require non-null due times to be aware and normalize them to UTC."""
        return None if value is None else _aware_to_utc(value)


class TodoResponse(BaseModel):
    """Stable public representation of a persisted todo item."""

    id: UUIDString = Field(description="Canonical todo UUID.")
    project_id: UUIDString = Field(description="Canonical UUID of the owning project.")
    title: str = Field(description="Todo title.")
    description: str | None = Field(description="Optional todo details.")
    status: TodoStatus = Field(description="Current lifecycle status.")
    priority: int = Field(ge=1, le=5, description="Priority from 1 to 5.")
    due_at: datetime | None = Field(description="Optional UTC due time.")
    created_at: datetime = Field(description="UTC creation time.")
    updated_at: datetime = Field(description="UTC last-update time.")
    project: ProjectResponse | None = Field(
        default=None,
        description="Loaded project data when the relationship was expanded.",
    )

    @field_validator("created_at", "updated_at")
    @classmethod
    def normalize_required_timestamp(cls, value: datetime) -> datetime:
        """Require response timestamps to be aware and normalize them to UTC."""
        return _aware_to_utc(value)

    @field_validator("due_at")
    @classmethod
    def normalize_optional_timestamp(cls, value: datetime | None) -> datetime | None:
        """Normalize an optional due time to UTC."""
        return None if value is None else _aware_to_utc(value)

    @classmethod
    def from_model(cls, todo: Todo) -> TodoResponse:
        """Convert a persisted todo with a shallow or loaded project relation."""
        relation = todo.project
        if isinstance(relation, Project):
            project_id = relation.id
            project = ProjectResponse.model_validate(relation)
        else:
            project_id = relation
            project = None

        return cls(
            id=todo.id,
            project_id=project_id,
            title=todo.title,
            description=todo.description,
            status=todo.status,
            priority=todo.priority,
            due_at=todo.due_at,
            created_at=todo.created_at,
            updated_at=todo.updated_at,
            project=project,
        )


class ProjectPage(BaseModel):
    """A bounded page of projects."""

    items: list[ProjectResponse]
    total: int = Field(ge=0, description="Total matching projects.")
    limit: int = Field(ge=1, le=100, description="Maximum returned items.")
    offset: int = Field(ge=0, description="Number of matching items skipped.")


class TodoPage(BaseModel):
    """A bounded page of todo items."""

    items: list[TodoResponse]
    total: int = Field(ge=0, description="Total matching todo items.")
    limit: int = Field(ge=1, le=100, description="Maximum returned items.")
    offset: int = Field(ge=0, description="Number of matching items skipped.")
