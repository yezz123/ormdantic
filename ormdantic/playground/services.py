"""Framework-neutral schema refresh services."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from ormdantic import Ormdantic
from ormdantic.migrations import (
    MigrationHistoryEntry,
    MigrationPlan,
    SchemaSnapshot,
)
from ormdantic.playground.config import (
    DatabaseUrlSource,
    EffectiveConfig,
    resolve_database_url,
)
from ormdantic.playground.diagnostics import Diagnostic, Severity
from ormdantic.playground.inspection import InspectionResult, inspect_models
from ormdantic.playground.state import (
    MigrationState,
    RefreshResult,
    RefreshStatus,
    SchemaState,
)

UTC = timezone.utc


@dataclass(frozen=True)
class DatabaseRefresh:
    """Live database data collected by a blocking worker."""

    live_snapshot: SchemaSnapshot
    history: tuple[MigrationHistoryEntry, ...] = ()
    current_revision: str | None = None
    dirty: bool = False
    diagnostics: tuple[Diagnostic, ...] = ()


class RefreshService:
    """Coordinate isolated model imports with threaded database reflection."""

    def __init__(
        self,
        *,
        inspector: Callable[..., Any] = inspect_models,
        url_resolver: Callable[..., DatabaseUrlSource] = resolve_database_url,
        reflector: Callable[[str], DatabaseRefresh] | None = None,
        planner: Callable[[str, SchemaSnapshot, SchemaSnapshot], MigrationPlan]
        | None = None,
        thread_runner: Callable[..., Any] = asyncio.to_thread,
    ) -> None:
        self._inspector = inspector
        self._url_resolver = url_resolver
        self._reflector = reflector or reflect_database
        self._planner = planner or plan_migration
        self._thread_runner = thread_runner

    async def refresh(
        self,
        config: EffectiveConfig,
        *,
        generation: int,
        previous: SchemaState | None = None,
    ) -> RefreshResult:
        """Build one refresh generation without blocking the UI loop."""
        target = config.project.target
        if target is None:  # guarded by config loading; retained for typed callers
            raise ValueError("project target is required")
        model_task = asyncio.create_task(
            self._inspector(target, cwd=config.root),
            name=f"ormdantic-model-refresh-{generation}",
        )
        diagnostics: list[Diagnostic] = []
        resolved: DatabaseUrlSource | None = None
        database_task: asyncio.Task[DatabaseRefresh] | None = None
        try:
            resolved = self._url_resolver(config.environment)
        except Exception as exc:
            diagnostics.append(
                Diagnostic.create(
                    Severity.ERROR,
                    "database.url_missing",
                    str(exc),
                    hint="Configure the environment variable or dotenv file.",
                )
            )
        else:
            database_task = asyncio.create_task(
                self._thread_runner(self._reflector, resolved.value),
                name=f"ormdantic-database-refresh-{generation}",
            )

        model_result: InspectionResult | None = None
        database_result: DatabaseRefresh | None = None
        model_value = await _task_result(model_task)
        if isinstance(model_value, Exception):
            diagnostics.append(
                Diagnostic.create(
                    Severity.ERROR,
                    "model.inspection_failed",
                    str(model_value),
                    hint="Fix the model target and refresh.",
                )
            )
        else:
            model_result = model_value
            diagnostics.extend(model_result.diagnostics)

        if database_task is not None:
            database_value = await _task_result(database_task)
            if isinstance(database_value, Exception):
                diagnostics.append(
                    Diagnostic.create(
                        Severity.ERROR,
                        "database.reflection_failed",
                        str(database_value),
                        hint="Check connectivity and database permissions.",
                    )
                )
            else:
                database_result = database_value
                diagnostics.extend(database_result.diagnostics)

        fresh_model_snapshot = (
            model_result.snapshot if model_result is not None else None
        )
        fresh_live_snapshot = (
            database_result.live_snapshot if database_result is not None else None
        )
        plan: MigrationPlan | None = None
        if (
            fresh_model_snapshot is not None
            and fresh_live_snapshot is not None
            and resolved is not None
        ):
            try:
                plan = await self._thread_runner(
                    self._planner,
                    resolved.value,
                    fresh_live_snapshot,
                    fresh_model_snapshot,
                )
            except Exception as exc:
                diagnostics.append(
                    Diagnostic.create(
                        Severity.ERROR,
                        "schema.plan_failed",
                        str(exc),
                        hint="Review dialect support for the reported schema change.",
                    )
                )

        available = sum(
            item is not None for item in (fresh_model_snapshot, fresh_live_snapshot)
        )
        has_errors = any(item.severity is Severity.ERROR for item in diagnostics)
        if available == 2 and not has_errors:
            status = RefreshStatus.HEALTHY
        elif available:
            status = RefreshStatus.PARTIAL
        else:
            status = RefreshStatus.ERROR

        model_snapshot = fresh_model_snapshot or (
            previous.model_snapshot if previous is not None else None
        )
        live_snapshot = fresh_live_snapshot or (
            previous.live_snapshot if previous is not None else None
        )

        schema = SchemaState(
            model_snapshot=model_snapshot,
            live_snapshot=live_snapshot,
            diff=plan.diff
            if plan is not None
            else (previous.diff if previous else None),
            forward_sql=(
                tuple(operation.sql for operation in plan.operations)
                if plan
                else (previous.forward_sql if previous else ())
            ),
            rollback_sql=(
                tuple(operation.sql for operation in plan.rollback_operations)
                if plan
                else (previous.rollback_sql if previous else ())
            ),
            refreshed_at=datetime.now(UTC),
            stale=status is not RefreshStatus.HEALTHY,
            diagnostics=tuple(diagnostics),
        )
        migrations = MigrationState(
            history=database_result.history if database_result else (),
            current_revision=(
                database_result.current_revision if database_result else None
            ),
            dirty=database_result.dirty if database_result else False,
        )
        return RefreshResult(
            generation=generation,
            status=status,
            schema=schema,
            migrations=migrations,
            connection_label=resolved.label if resolved is not None else None,
            dialect=_dialect_name(resolved.value) if resolved is not None else None,
            diagnostics=tuple(diagnostics),
        )


async def _task_result(task: asyncio.Task[Any]) -> Any:
    try:
        return await task
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        return exc


def reflect_database(
    url: str,
    *,
    database_factory: Callable[[str], Any] = Ormdantic,
    history_exists: Callable[[SchemaSnapshot], bool] | None = None,
) -> DatabaseRefresh:
    """Reflect a live schema and read existing migration history in a worker."""
    database = database_factory(url)
    live_snapshot = database.migrations.live_snapshot()
    if history_exists is not None:
        has_history = history_exists(live_snapshot)
    else:
        has_history = bool(asyncio.run(database.migrations.history_table_exists()))
    if not has_history:
        return DatabaseRefresh(live_snapshot=live_snapshot)
    try:
        history = tuple(asyncio.run(database.migrations.history()))
    except Exception as exc:
        return DatabaseRefresh(
            live_snapshot=live_snapshot,
            diagnostics=(
                Diagnostic.create(
                    Severity.ERROR,
                    "database.history_failed",
                    str(exc),
                    hint="The schema is available; inspect migration history permissions.",
                ),
            ),
        )
    current = next(
        (
            entry.revision
            for entry in reversed(history)
            if entry.status == "applied" and not entry.dirty
        ),
        None,
    )
    return DatabaseRefresh(
        live_snapshot=live_snapshot,
        history=history,
        current_revision=current,
        dirty=any(entry.dirty for entry in history),
    )


def plan_migration(
    url: str,
    before: SchemaSnapshot,
    after: SchemaSnapshot,
) -> MigrationPlan:
    """Generate drift SQL using the existing migration manager."""
    return Ormdantic(url).migrations.generate_plan(before, after, dialect=url)


def _dialect_name(url: str) -> str:
    scheme = url.split("://", 1)[0].split("+", 1)[0].casefold()
    aliases = {
        "postgres": "postgresql",
        "sqlserver": "mssql",
    }
    return aliases.get(scheme, scheme)
