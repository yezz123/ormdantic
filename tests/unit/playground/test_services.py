from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

from ormdantic.migrations import MigrationHistoryEntry, MigrationPlan, SchemaSnapshot
from ormdantic.playground.config import (
    DatabaseUrlSource,
    EffectiveConfig,
    EnvironmentConfig,
    ProjectConfig,
)
from ormdantic.playground.diagnostics import Diagnostic, Severity
from ormdantic.playground.inspection import InspectionError, InspectionResult
from ormdantic.playground.services import (
    DatabaseRefresh,
    RefreshService,
    reflect_database,
)
from ormdantic.playground.state import RefreshStatus, SchemaState


def effective_config(tmp_path: Path) -> EffectiveConfig:
    return EffectiveConfig(
        path=tmp_path / "ormdantic.toml",
        root=tmp_path,
        project=ProjectConfig(
            target="app:db",
            migrations_dir=tmp_path / "migrations",
        ),
        environment=EnvironmentConfig(
            name="development",
            url_env="DATABASE_URL",
            env_file=tmp_path / ".env",
        ),
    )


async def test_refresh_runs_model_inspection_and_reflection_concurrently(
    tmp_path: Path,
) -> None:
    model_started = asyncio.Event()
    reflection_started = asyncio.Event()
    snapshot = SchemaSnapshot.empty()

    async def inspector(target: str, *, cwd: Path) -> InspectionResult:
        assert target == "app:db"
        assert cwd == tmp_path
        model_started.set()
        await asyncio.wait_for(reflection_started.wait(), 0.5)
        return InspectionResult(snapshot=snapshot)

    def reflector(url: str) -> DatabaseRefresh:
        assert url == "sqlite:///app.sqlite3"
        return DatabaseRefresh(live_snapshot=snapshot)

    async def thread_runner(function: Any, *args: Any) -> Any:
        reflection_started.set()
        await asyncio.wait_for(model_started.wait(), 0.5)
        return function(*args)

    service = RefreshService(
        inspector=inspector,
        url_resolver=lambda _environment: DatabaseUrlSource(
            "sqlite:///app.sqlite3", "DATABASE_URL"
        ),
        reflector=reflector,
        thread_runner=thread_runner,
        planner=lambda _url, _before, _after: MigrationPlan(),
    )

    result = await service.refresh(effective_config(tmp_path), generation=7)

    assert result.generation == 7
    assert result.status is RefreshStatus.HEALTHY
    assert result.schema.model_snapshot is snapshot
    assert result.schema.live_snapshot is snapshot


async def test_refresh_invokes_database_work_through_thread_runner(
    tmp_path: Path,
) -> None:
    calls: list[str] = []
    snapshot = SchemaSnapshot.empty()

    async def inspector(target: str, *, cwd: Path) -> InspectionResult:
        return InspectionResult(snapshot=snapshot)

    def reflector(url: str) -> DatabaseRefresh:
        calls.append(f"reflect:{url}")
        return DatabaseRefresh(live_snapshot=snapshot)

    async def thread_runner(function: Any, *args: Any) -> Any:
        calls.append("thread")
        return function(*args)

    service = RefreshService(
        inspector=inspector,
        url_resolver=lambda _environment: DatabaseUrlSource("sqlite:///db", "env"),
        reflector=reflector,
        thread_runner=thread_runner,
        planner=lambda _url, _before, _after: MigrationPlan(),
    )

    await service.refresh(effective_config(tmp_path), generation=1)

    assert calls[:2] == ["thread", "reflect:sqlite:///db"]


async def test_refresh_keeps_model_schema_when_database_is_unavailable(
    tmp_path: Path,
) -> None:
    snapshot = SchemaSnapshot.empty()

    async def inspector(target: str, *, cwd: Path) -> InspectionResult:
        return InspectionResult(snapshot=snapshot)

    def reflector(url: str) -> DatabaseRefresh:
        raise ConnectionError("postgresql://user:secret@localhost/app refused")

    service = RefreshService(
        inspector=inspector,
        url_resolver=lambda _environment: DatabaseUrlSource(
            "postgresql://user:secret@localhost/app", "DATABASE_URL"
        ),
        reflector=reflector,
        planner=lambda _url, _before, _after: MigrationPlan(),
    )

    result = await service.refresh(effective_config(tmp_path), generation=2)

    assert result.status is RefreshStatus.PARTIAL
    assert result.schema.model_snapshot is snapshot
    assert result.schema.live_snapshot is None
    assert any(item.code == "database.reflection_failed" for item in result.diagnostics)
    assert "secret" not in repr(result)


async def test_refresh_keeps_live_schema_when_model_import_fails(
    tmp_path: Path,
) -> None:
    live = SchemaSnapshot.empty()
    import_diagnostic = Diagnostic.create(
        Severity.ERROR,
        "model.import_failed",
        "ImportError: broken models",
    )

    async def inspector(target: str, *, cwd: Path) -> InspectionResult:
        return InspectionResult(
            snapshot=None,
            diagnostics=(import_diagnostic,),
            error=InspectionError("ImportError", "broken models"),
        )

    service = RefreshService(
        inspector=inspector,
        url_resolver=lambda _environment: DatabaseUrlSource("sqlite:///db", "env"),
        reflector=lambda _url: DatabaseRefresh(live_snapshot=live),
    )

    result = await service.refresh(effective_config(tmp_path), generation=3)

    assert result.status is RefreshStatus.PARTIAL
    assert result.schema.model_snapshot is None
    assert result.schema.live_snapshot is live
    assert import_diagnostic in result.diagnostics


async def test_refresh_builds_drift_sql_and_history(tmp_path: Path) -> None:
    model = SchemaSnapshot.empty()
    live = SchemaSnapshot.empty()
    history = (MigrationHistoryEntry(revision="001_initial"),)

    async def inspector(target: str, *, cwd: Path) -> InspectionResult:
        return InspectionResult(snapshot=model)

    plan = MigrationPlan()
    plan.operations.append(type("Operation", (), {"sql": "CREATE TABLE users"})())
    plan.rollback_operations.append(
        type("Operation", (), {"sql": "DROP TABLE users"})()
    )

    service = RefreshService(
        inspector=inspector,
        url_resolver=lambda _environment: DatabaseUrlSource(
            "postgresql://localhost/app", "DATABASE_URL"
        ),
        reflector=lambda _url: DatabaseRefresh(
            live_snapshot=live,
            history=history,
            current_revision="001_initial",
        ),
        planner=lambda _url, before, after: plan,
    )

    result = await service.refresh(effective_config(tmp_path), generation=4)

    assert result.schema.forward_sql == ("CREATE TABLE users",)
    assert result.schema.rollback_sql == ("DROP TABLE users",)
    assert result.migrations is not None
    assert result.migrations.history == history
    assert result.migrations.current_revision == "001_initial"
    assert result.dialect == "postgresql"


def test_reflect_database_keeps_snapshot_when_history_query_fails() -> None:
    live = SchemaSnapshot.empty()

    class FakeMigrations:
        def live_snapshot(self) -> SchemaSnapshot:
            return live

        async def history(self) -> list[MigrationHistoryEntry]:
            raise RuntimeError("history unavailable")

    class FakeDatabase:
        def __init__(self, url: str) -> None:
            self.url = url
            self.migrations = FakeMigrations()

    result = reflect_database(
        "sqlite:///db",
        database_factory=FakeDatabase,
        history_exists=lambda _snapshot: True,
    )

    assert result.live_snapshot is live
    assert result.history == ()
    assert result.diagnostics[0].code == "database.history_failed"


def test_reflect_database_checks_history_without_creating_it() -> None:
    live = SchemaSnapshot.empty()
    calls: list[str] = []

    class FakeMigrations:
        def live_snapshot(self) -> SchemaSnapshot:
            return live

        async def history_table_exists(self) -> bool:
            calls.append("exists")
            return False

        async def history(self) -> list[MigrationHistoryEntry]:
            raise AssertionError("history must not be read when its table is absent")

    class FakeDatabase:
        def __init__(self, url: str) -> None:
            self.migrations = FakeMigrations()

    result = reflect_database("sqlite:///db", database_factory=FakeDatabase)

    assert result.live_snapshot is live
    assert result.history == ()
    assert calls == ["exists"]


async def test_refresh_preserves_previous_valid_snapshots_when_both_sources_fail(
    tmp_path: Path,
) -> None:
    previous_model = SchemaSnapshot.empty()
    previous_live = SchemaSnapshot.empty()

    async def inspector(target: str, *, cwd: Path) -> InspectionResult:
        return InspectionResult(
            snapshot=None,
            error=InspectionError("ImportError", "broken"),
        )

    def reflector(url: str) -> DatabaseRefresh:
        raise ConnectionError("offline")

    service = RefreshService(
        inspector=inspector,
        url_resolver=lambda _environment: DatabaseUrlSource("sqlite:///db", "env"),
        reflector=reflector,
    )

    result = await service.refresh(
        effective_config(tmp_path),
        generation=8,
        previous=SchemaState(
            model_snapshot=previous_model,
            live_snapshot=previous_live,
        ),
    )

    assert result.status is RefreshStatus.ERROR
    assert result.schema.model_snapshot is previous_model
    assert result.schema.live_snapshot is previous_live
    assert result.schema.stale is True
