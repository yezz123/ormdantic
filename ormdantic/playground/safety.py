"""Pure preflight and confirmation policy for playground actions."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from ormdantic.migrations import MigrationPlan
from ormdantic.playground.config import EnvironmentConfig


class Risk(str, Enum):
    """Mutation risk presented to users and confirmation policy."""

    READ_ONLY = "read_only"
    WRITE = "write"
    DESTRUCTIVE = "destructive"
    HISTORY_REWRITE = "history_rewrite"


@dataclass(frozen=True)
class ActionRequest:
    """Immutable identity and reviewed SQL for one requested action."""

    action: str
    environment: str
    database_name: str
    target: str
    risk: Risk
    sql: tuple[str, ...] = ()
    destructive_sql: tuple[str, ...] = ()
    artifact_checksum: str | None = None
    reviewed_generation: int | None = None


@dataclass(frozen=True)
class PreflightContext:
    """Current state against which an action review is validated."""

    connected: bool
    target_imported: bool
    dialect: str | None
    artifact_dialect: str | None
    history_readable: bool
    history_dirty: bool
    artifact_valid: bool
    checksum_valid: bool
    dependencies_valid: bool
    revision_state_valid: bool
    rollback_available: bool
    snapshot_current: bool
    operations_supported: bool
    operation_running: bool
    editor_valid: bool
    editor_dirty: bool
    sql_present: bool
    destructive_reviewed: bool
    artifact_checksum: str | None
    generation: int


@dataclass(frozen=True)
class SafetyDecision:
    """Result of current preflight and confirmation validation."""

    allowed: bool
    phrase: str | None
    reasons: tuple[str, ...] = ()


def classify_plan(plan: MigrationPlan) -> Risk:
    """Classify migration SQL using authoritative plan metadata."""
    if plan.has_destructive_operations:
        return Risk.DESTRUCTIVE
    if plan.operations:
        return Risk.WRITE
    return Risk.READ_ONLY


def evaluate_action(
    request: ActionRequest,
    environment: EnvironmentConfig,
    context: PreflightContext,
    *,
    confirmed: bool = False,
    confirmation: str | None = None,
) -> SafetyDecision:
    """Evaluate preflight and the non-cacheable confirmation for one action."""
    reasons = list(_preflight_reasons(request, environment, context))
    phrase = _confirmation_phrase(request, environment)
    if request.risk is not Risk.READ_ONLY:
        if not confirmed:
            reasons.append(
                f"Confirm {request.action} for {request.environment}:{request.target}"
            )
        if phrase is not None and confirmation != phrase:
            reasons.append(f"Type the exact confirmation phrase: {phrase}")
    return SafetyDecision(
        allowed=not reasons,
        phrase=phrase,
        reasons=tuple(reasons),
    )


def _preflight_reasons(
    request: ActionRequest,
    environment: EnvironmentConfig,
    context: PreflightContext,
) -> tuple[str, ...]:
    reasons: list[str] = []
    if request.environment != environment.name:
        reasons.append("selected environment changed after review")
    if not context.artifact_valid:
        reasons.append("artifact is invalid")
    if not context.editor_valid:
        reasons.append("editor document is invalid")
    if request.risk is Risk.READ_ONLY:
        return tuple(reasons)

    database_action = request.action in {
        "apply",
        "rollback",
        "repair",
        "squash",
    }
    if database_action and not context.connected:
        reasons.append("database is not connected")
    if database_action and not context.target_imported:
        reasons.append("model target did not import")
    if (
        database_action
        and context.dialect is not None
        and context.artifact_dialect is not None
        and _dialect(context.dialect) != _dialect(context.artifact_dialect)
    ):
        reasons.append("artifact dialect does not match the live database dialect")
    if database_action and not context.history_readable:
        reasons.append("migration history is not readable")
    if database_action and context.history_dirty and request.action != "repair":
        reasons.append("migration history is dirty")
    if not context.checksum_valid:
        reasons.append("artifact checksum is invalid")
    if not context.dependencies_valid:
        reasons.append("artifact dependencies are invalid")
    if not context.revision_state_valid:
        reasons.append("revision state is not legal for this action")
    if request.action == "rollback" and not context.rollback_available:
        reasons.append("rollback SQL is unavailable")
    if not context.snapshot_current:
        reasons.append("schema review is stale")
    if not context.operations_supported:
        reasons.append("migration contains an unsupported operation")
    if context.operation_running:
        reasons.append("another operation is running")
    if context.editor_dirty:
        reasons.append("save unsaved editor changes before execution")
    if database_action and not context.sql_present:
        reasons.append("there is no SQL to execute")
    if request.risk is Risk.DESTRUCTIVE and not context.destructive_reviewed:
        reasons.append("destructive SQL was not reviewed")
    if (
        request.artifact_checksum is not None
        and request.artifact_checksum != context.artifact_checksum
    ):
        reasons.append("artifact changed after review")
    if (
        request.reviewed_generation is not None
        and request.reviewed_generation != context.generation
    ):
        reasons.append("live schema changed after review")
    return tuple(reasons)


def _confirmation_phrase(
    request: ActionRequest,
    environment: EnvironmentConfig,
) -> str | None:
    typed = (
        environment.production
        or environment.safety == "typed"
        or request.risk is Risk.HISTORY_REWRITE
    )
    if environment.production and request.risk is Risk.DESTRUCTIVE:
        return " ".join(
            (
                request.environment,
                request.database_name,
                request.target,
                str(len(request.destructive_sql)),
            )
        )
    if typed:
        return f"{request.database_name} {request.target}"
    return None


def _dialect(value: str) -> str:
    scheme = value.split("://", 1)[0].split("+", 1)[0].casefold()
    return {"postgres": "postgresql", "sqlserver": "mssql"}.get(scheme, scheme)
