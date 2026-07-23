"""Immutable state snapshots shared by playground services and views."""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import datetime
from enum import Enum
from pathlib import Path

from ormdantic.migrations import (
    MigrationHistoryEntry,
    SchemaDiff,
    SchemaSnapshot,
)
from ormdantic.playground.diagnostics import Diagnostic, redact_text


class RefreshStatus(str, Enum):
    """Lifecycle state for schema refreshes."""

    IDLE = "idle"
    RUNNING = "running"
    HEALTHY = "healthy"
    PARTIAL = "partial"
    ERROR = "error"
    PAUSED = "paused"


@dataclass(frozen=True)
class SchemaState:
    """The most recent model/live snapshots and derived drift."""

    model_snapshot: SchemaSnapshot | None = None
    live_snapshot: SchemaSnapshot | None = None
    diff: SchemaDiff | None = None
    forward_sql: tuple[str, ...] = ()
    rollback_sql: tuple[str, ...] = ()
    refreshed_at: datetime | None = None
    stale: bool = False
    diagnostics: tuple[Diagnostic, ...] = ()

    @property
    def ready_for_generation(self) -> bool:
        """Whether a fresh, complete, non-empty migration plan is available."""
        return (
            not self.stale
            and self.model_snapshot is not None
            and self.live_snapshot is not None
            and bool(self.forward_sql)
        )


@dataclass(frozen=True)
class ArtifactSummary:
    """Render-safe metadata for one migration artifact."""

    path: Path
    revision: str | None
    status: str
    format: str
    destructive: bool = False
    unsafe: bool = False
    valid: bool = True


@dataclass(frozen=True)
class MigrationState:
    """Migration files and durable history known to the playground."""

    artifacts: tuple[ArtifactSummary, ...] = ()
    history: tuple[MigrationHistoryEntry, ...] = ()
    current_revision: str | None = None
    dirty: bool = False
    selected_path: Path | None = None


@dataclass(frozen=True)
class OperationState:
    """The currently executing or most recently completed operation."""

    name: str | None = None
    target: str | None = None
    running: bool = False
    message: str | None = None


@dataclass(frozen=True)
class PlaygroundState:
    """Complete immutable application state."""

    environment: str
    connection_label: str | None = None
    dialect: str | None = None
    generation: int = 0
    status: RefreshStatus = RefreshStatus.IDLE
    watcher_paused: bool = False
    schema: SchemaState = field(default_factory=SchemaState)
    migrations: MigrationState = field(default_factory=MigrationState)
    operation: OperationState = field(default_factory=OperationState)
    diagnostics: tuple[Diagnostic, ...] = ()

    def __post_init__(self) -> None:
        if self.connection_label is not None:
            object.__setattr__(
                self,
                "connection_label",
                redact_text(self.connection_label),
            )


@dataclass(frozen=True)
class RefreshResult:
    """One generation of refresh work ready for publication."""

    generation: int
    status: RefreshStatus
    schema: SchemaState
    migrations: MigrationState | None = None
    connection_label: str | None = None
    dialect: str | None = None
    diagnostics: tuple[Diagnostic, ...] = ()


def accept_refresh(
    state: PlaygroundState,
    result: RefreshResult,
) -> PlaygroundState:
    """Publish a refresh unless a newer generation already won the race."""
    if result.generation < state.generation:
        return state
    changes: dict[str, object] = {
        "generation": result.generation,
        "status": result.status,
        "schema": result.schema,
        "connection_label": result.connection_label,
        "dialect": result.dialect,
        "diagnostics": result.diagnostics,
    }
    if result.migrations is not None:
        changes["migrations"] = result.migrations
    return replace(state, **changes)
