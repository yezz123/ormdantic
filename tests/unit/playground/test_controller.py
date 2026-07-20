from __future__ import annotations

import asyncio
from dataclasses import replace
from pathlib import Path

from ormdantic.migrations import (
    MigrationArtifact,
    MigrationHistoryEntry,
    MigrationOperation,
    MigrationPlan,
    SchemaSnapshot,
)
from ormdantic.playground.config import (
    EffectiveConfig,
    EnvironmentConfig,
    ProjectConfig,
)
from ormdantic.playground.controller import PlaygroundController
from ormdantic.playground.safety import PreflightContext
from ormdantic.playground.state import (
    MigrationState,
    RefreshResult,
    RefreshStatus,
    SchemaState,
)


def config(tmp_path: Path, *, safety: str = "confirm") -> EffectiveConfig:
    return EffectiveConfig(
        path=tmp_path / "ormdantic.toml",
        root=tmp_path,
        project=ProjectConfig(
            target="app:db",
            migrations_dir=tmp_path / "migrations",
        ),
        environment=EnvironmentConfig(
            name="development",
            safety=safety,  # type: ignore[arg-type]
        ),
    )


def write_artifact(tmp_path: Path) -> Path:
    migration = MigrationArtifact.from_plan(
        "001_users",
        MigrationPlan(
            operations=[MigrationOperation(sql="CREATE TABLE users (id INTEGER)")],
            rollback_operations=[
                MigrationOperation(sql="DROP TABLE users", destructive=True)
            ],
        ),
        SchemaSnapshot.empty(),
        SchemaSnapshot.empty(),
        dialect="sqlite",
    )
    path = tmp_path / "migrations" / "001_users.toml"
    migration.write(path)
    return path


def preflight(**changes: object) -> PreflightContext:
    values: dict[str, object] = {
        "connected": True,
        "target_imported": True,
        "dialect": "sqlite",
        "artifact_dialect": "sqlite",
        "history_readable": True,
        "history_dirty": False,
        "artifact_valid": True,
        "checksum_valid": True,
        "dependencies_valid": True,
        "revision_state_valid": True,
        "rollback_available": True,
        "snapshot_current": True,
        "operations_supported": True,
        "operation_running": False,
        "editor_valid": True,
        "editor_dirty": False,
        "sql_present": True,
        "destructive_reviewed": True,
        "artifact_checksum": None,
        "generation": 0,
    }
    values.update(changes)
    return PreflightContext(**values)  # type: ignore[arg-type]


class FakeRefreshService:
    def __init__(self, events: list[str] | None = None) -> None:
        self.events = events if events is not None else []

    async def refresh(
        self,
        config: EffectiveConfig,
        *,
        generation: int,
        previous: SchemaState,
    ) -> RefreshResult:
        self.events.append("refresh")
        return RefreshResult(
            generation=generation,
            status=RefreshStatus.HEALTHY,
            schema=SchemaState(
                model_snapshot=SchemaSnapshot.empty(),
                live_snapshot=SchemaSnapshot.empty(),
            ),
            connection_label="DATABASE_URL",
            dialect="sqlite",
        )


class FakeOperations:
    def __init__(
        self,
        events: list[str] | None = None,
        error: Exception | None = None,
        root: Path | None = None,
    ) -> None:
        self.events = events if events is not None else []
        self.error = error
        self.root = root

    async def apply(self, document: object, request: object, decision: object) -> bool:
        self.events.append("apply")
        if self.error is not None:
            raise self.error
        return True

    async def rollback(
        self, document: object, request: object, decision: object
    ) -> bool:
        self.events.append("rollback")
        if self.error is not None:
            raise self.error
        return True

    async def generate(
        self,
        revision: str,
        description: str | None,
        *,
        before: SchemaSnapshot,
        after: SchemaSnapshot,
        depends_on: tuple[str, ...],
    ) -> Path | None:
        self.events.append("generate")
        if self.error is not None:
            raise self.error
        assert self.root is not None
        migration = MigrationArtifact.from_plan(
            revision,
            MigrationPlan(operations=[MigrationOperation(sql="SELECT 1")]),
            before,
            after,
            description=description,
            depends_on=depends_on,
            dialect="sqlite",
        )
        path = self.root / "migrations" / f"{revision}.toml"
        migration.write(path)
        return path

    async def repair(
        self,
        revision: str,
        status: str,
        request: object,
        decision: object,
    ) -> int:
        self.events.append(f"repair:{revision}:{status}")
        return 1

    async def squash(
        self,
        paths: tuple[Path, ...],
        revision: str,
        request: object,
        decision: object,
    ) -> Path:
        self.events.append(f"squash:{revision}:{len(paths)}")
        assert self.root is not None
        migrations = [MigrationArtifact.read(path) for path in paths]
        operations = [
            operation for migration in migrations for operation in migration.operations
        ]
        squashed = MigrationArtifact.from_plan(
            revision,
            MigrationPlan(operations=operations),
            migrations[0].from_snapshot,
            migrations[-1].to_snapshot,
            dialect="sqlite",
        )
        path = self.root / "migrations" / f"{revision}.toml"
        squashed.write(path)
        return path


def test_controller_loads_workspace_into_initial_state(tmp_path: Path) -> None:
    path = write_artifact(tmp_path)

    controller = PlaygroundController(
        config(tmp_path),
        refresh_service=FakeRefreshService(),
        operations=FakeOperations(),
    )

    assert controller.state.environment == "development"
    assert controller.state.migrations.artifacts[0].path == path
    assert controller.state.migrations.artifacts[0].status == "pending"


async def test_refresh_publishes_running_then_healthy_state(tmp_path: Path) -> None:
    write_artifact(tmp_path)
    controller = PlaygroundController(
        config(tmp_path),
        refresh_service=FakeRefreshService(),
        operations=FakeOperations(),
    )
    statuses = []
    controller.subscribe(lambda state: statuses.append(state.status))

    state = await controller.refresh()

    assert statuses == [RefreshStatus.RUNNING, RefreshStatus.HEALTHY]
    assert state.generation == 1
    assert state.schema.model_snapshot is not None
    assert state.migrations.artifacts[0].revision == "001_users"


async def test_concurrent_refreshes_publish_in_serial_generation_order(
    tmp_path: Path,
) -> None:
    started = asyncio.Event()
    release = asyncio.Event()
    generations: list[int] = []

    class BlockingRefreshService(FakeRefreshService):
        async def refresh(
            self,
            config: EffectiveConfig,
            *,
            generation: int,
            previous: SchemaState,
        ) -> RefreshResult:
            generations.append(generation)
            if len(generations) == 1:
                started.set()
                await release.wait()
            return await super().refresh(
                config,
                generation=generation,
                previous=previous,
            )

    controller = PlaygroundController(
        config(tmp_path),
        refresh_service=BlockingRefreshService(),
        operations=FakeOperations(),
    )
    first = asyncio.create_task(controller.refresh())
    await started.wait()
    second = asyncio.create_task(controller.refresh())
    await asyncio.sleep(0)
    assert generations == [1]

    release.set()
    await asyncio.gather(first, second)

    assert generations == [1, 2]
    assert controller.state.generation == 2
    assert controller.state.status is RefreshStatus.HEALTHY


def test_select_and_edit_sql_updates_workspace_without_touching_disk(
    tmp_path: Path,
) -> None:
    path = write_artifact(tmp_path)
    original_source = path.read_text()
    controller = PlaygroundController(
        config(tmp_path),
        refresh_service=FakeRefreshService(),
        operations=FakeOperations(),
    )

    controller.select_artifact(path)
    controller.edit_active_sql(0, "CREATE TABLE accounts (id INTEGER)")

    active = controller.active_document
    assert active is not None
    assert active.dirty is True
    assert "CREATE TABLE accounts" in active.source
    assert path.read_text() == original_source
    assert controller.state.migrations.selected_path == path


async def test_wrong_confirmation_never_calls_migration_operation(
    tmp_path: Path,
) -> None:
    path = write_artifact(tmp_path)
    events: list[str] = []
    controller = PlaygroundController(
        config(tmp_path, safety="typed"),
        refresh_service=FakeRefreshService(events),
        operations=FakeOperations(events),
    )
    controller.select_artifact(path)
    action = controller.build_action_request("apply", database_name="app")
    context = preflight(
        artifact_checksum=action.artifact_checksum,
        generation=controller.state.generation,
    )

    outcome = await controller.execute_action(
        action,
        context,
        confirmed=True,
        confirmation="wrong",
    )

    assert outcome.executed is False
    assert outcome.decision.allowed is False
    assert events == []


async def test_successful_apply_refreshes_before_reporting_completion(
    tmp_path: Path,
) -> None:
    path = write_artifact(tmp_path)
    events: list[str] = []
    controller = PlaygroundController(
        config(tmp_path),
        refresh_service=FakeRefreshService(events),
        operations=FakeOperations(events),
    )
    controller.select_artifact(path)
    action = controller.build_action_request("apply", database_name="app")
    context = preflight(
        artifact_checksum=action.artifact_checksum,
        generation=controller.state.generation,
    )

    outcome = await controller.execute_action(action, context, confirmed=True)

    assert outcome.executed is True
    assert outcome.result is True
    assert events == ["apply", "refresh"]
    assert controller.state.operation.running is False
    assert controller.state.operation.message == "Applied 001_users"


async def test_operation_error_is_redacted_and_keeps_app_usable(
    tmp_path: Path,
) -> None:
    path = write_artifact(tmp_path)
    controller = PlaygroundController(
        config(tmp_path),
        refresh_service=FakeRefreshService(),
        operations=FakeOperations(
            error=RuntimeError("postgresql://admin:super-secret@localhost/app failed")
        ),
    )
    controller.select_artifact(path)
    action = controller.build_action_request("apply", database_name="app")
    context = preflight(
        artifact_checksum=action.artifact_checksum,
        generation=controller.state.generation,
    )

    outcome = await controller.execute_action(action, context, confirmed=True)

    assert outcome.executed is True
    assert outcome.error is not None
    assert "super-secret" not in repr(controller.state)
    assert controller.state.diagnostics[-1].code == "operation.apply_failed"


async def test_generate_uses_fresh_drift_and_selects_new_toml(tmp_path: Path) -> None:
    events: list[str] = []
    controller = PlaygroundController(
        config(tmp_path),
        refresh_service=FakeRefreshService(events),
        operations=FakeOperations(events, root=tmp_path),
    )
    controller.state = replace(
        controller.state,
        schema=SchemaState(
            model_snapshot=SchemaSnapshot.empty(),
            live_snapshot=SchemaSnapshot.empty(),
            forward_sql=("SELECT 1",),
            stale=False,
        ),
    )

    path = await controller.generate_migration("002_generated", "generated here")

    assert path == tmp_path / "migrations" / "002_generated.toml"
    assert controller.state.migrations.selected_path == path
    assert controller.state.operation.message == "Generated 002_generated"
    assert events == ["generate"]


async def test_dirty_history_repair_requires_typed_bound_review(
    tmp_path: Path,
) -> None:
    events: list[str] = []
    controller = PlaygroundController(
        config(tmp_path),
        refresh_service=FakeRefreshService(events),
        operations=FakeOperations(events),
    )
    controller.state = replace(
        controller.state,
        migrations=MigrationState(
            history=(
                MigrationHistoryEntry(
                    revision="001_failed",
                    status="failed",
                    dirty=True,
                ),
            ),
            dirty=True,
        ),
    )
    action = controller.build_repair_request(
        "001_failed",
        database_name="app",
    )
    context = preflight(
        history_dirty=True,
        artifact_checksum=None,
        generation=controller.state.generation,
    )

    blocked = await controller.execute_repair(
        action,
        context,
        status="failed",
        confirmed=True,
        confirmation="wrong",
    )
    repaired = await controller.execute_repair(
        action,
        context,
        status="failed",
        confirmed=True,
        confirmation="app 001_failed",
    )

    assert blocked.executed is False
    assert repaired.executed is True
    assert events == ["repair:001_failed:failed", "refresh"]
    assert controller.state.operation.message == "Repaired 001_failed"


async def test_squash_binds_all_pending_input_checksums(tmp_path: Path) -> None:
    first = write_artifact(tmp_path)
    second_artifact = MigrationArtifact.from_plan(
        "002_accounts",
        MigrationPlan(operations=[MigrationOperation(sql="SELECT 2")]),
        SchemaSnapshot.empty(),
        SchemaSnapshot.empty(),
        dialect="sqlite",
    )
    second = tmp_path / "migrations" / "002_accounts.toml"
    second_artifact.write(second)
    events: list[str] = []
    controller = PlaygroundController(
        config(tmp_path),
        refresh_service=FakeRefreshService(events),
        operations=FakeOperations(events, root=tmp_path),
    )
    action = controller.build_squash_request(
        (first, second),
        "010_squashed",
        database_name="app",
    )
    context = preflight(
        artifact_checksum=action.artifact_checksum,
        generation=controller.state.generation,
    )

    outcome = await controller.execute_squash(
        action,
        context,
        (first, second),
        confirmed=True,
        confirmation="app 010_squashed",
    )

    assert outcome.executed is True
    assert controller.state.migrations.selected_path == (
        tmp_path / "migrations" / "010_squashed.toml"
    )
    assert events == ["squash:010_squashed:2"]
