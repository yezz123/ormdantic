"""Persisted models for the Todo example."""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import overload
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator

from ormdantic import TableColumn

from .database import db


def utc_now() -> datetime:
    """Return the current time as a timezone-aware UTC datetime."""
    return datetime.now(timezone.utc)


def _uuid_string() -> str:
    return str(uuid4())


def _canonical_uuid(value: str) -> str:
    try:
        return str(UUID(value))
    except (AttributeError, TypeError, ValueError) as error:
        raise ValueError("value must be a valid UUID") from error


@overload
def _aware_to_utc(value: datetime) -> datetime: ...


@overload
def _aware_to_utc(value: None) -> None: ...


def _aware_to_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("datetime must be timezone-aware")
    return value.astimezone(timezone.utc)


class TodoStatus(str, Enum):
    """Lifecycle states persisted for a todo item."""

    pending = "pending"
    in_progress = "in_progress"
    completed = "completed"


@db.table("project", pk="id", indexed=["name"])
class Project(BaseModel):
    """A named collection of todo items."""

    model_config = ConfigDict(str_strip_whitespace=True, validate_default=True)

    id: str = Field(default_factory=_uuid_string)
    name: str = Field(min_length=1, max_length=120)
    created_at: datetime = Field(default_factory=utc_now)

    @field_validator("id")
    @classmethod
    def normalize_id(cls, value: str) -> str:
        """Validate and canonicalize the persisted identifier."""
        return _canonical_uuid(value)

    @field_validator("created_at")
    @classmethod
    def normalize_created_at(cls, value: datetime) -> datetime:
        """Require the creation time to be aware and normalized to UTC."""
        return _aware_to_utc(value)


@db.table(
    "todo",
    pk="id",
    indexed=["title"],
    column_options={
        "project": TableColumn(
            foreign_key_name="todo_project_fk",
            on_delete="cascade",
        )
    },
)
class Todo(BaseModel):
    """A prioritized unit of work belonging to a project."""

    model_config = ConfigDict(str_strip_whitespace=True, validate_default=True)

    id: str = Field(default_factory=_uuid_string)
    project: Project | str
    title: str = Field(min_length=1, max_length=200)
    description: str | None = None
    status: TodoStatus = TodoStatus.pending
    priority: int = Field(default=3, ge=1, le=5)
    due_at: datetime | None = None
    created_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime = Field(default_factory=utc_now)

    @field_validator("id")
    @classmethod
    def normalize_id(cls, value: str) -> str:
        """Validate and canonicalize the persisted identifier."""
        return _canonical_uuid(value)

    @field_validator("project")
    @classmethod
    def normalize_project(cls, value: Project | str) -> Project | str:
        """Validate shallow project identifiers while preserving nested models."""
        if isinstance(value, Project):
            return value
        return _canonical_uuid(value)

    @field_validator("created_at", "updated_at", "due_at")
    @classmethod
    def normalize_timestamp(cls, value: datetime | None) -> datetime | None:
        """Require persisted timestamps to be aware and normalized to UTC."""
        return _aware_to_utc(value)
