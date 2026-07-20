from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

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


def configuration(tmp_path: Path) -> EffectiveConfig:
    return EffectiveConfig(
        path=tmp_path / "ormdantic.toml",
        root=tmp_path,
        project=ProjectConfig(
            target="app:db",
            migrations_dir=tmp_path / "migrations",
        ),
        environment=EnvironmentConfig(name="development"),
    )


def make_artifact(revision: str, *, sql: str = "SELECT 1") -> MigrationArtifact:
    return MigrationArtifact.from_plan(
        revision,
        MigrationPlan(
            operations=[MigrationOperation(sql)],
            rollback_operations=[MigrationOperation("DROP TABLE x", destructive=True)],
        ),
        SchemaSnapshot.empty(),
        SchemaSnapshot.empty(),
        dialect="sqlite",
    )


def write_migration(tmp_path: Path, revision: str, *, format: str = "toml") -> Path:
    path = tmp_path / "migrations" / f"{revision}.{format}"
    make_artifact(revision).write(path)
    return path


def context(**updates: object) -> PreflightContext:
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
    values.update(updates)
    return PreflightContext(**values)  # type: ignore[arg-type]


class Refresh:
    async def refresh(
        self,
        _config: EffectiveConfig,
        *,
        generation: int,
        previous: SchemaState,
    ) -> RefreshResult:
        return RefreshResult(
            generation=generation,
            status=RefreshStatus.HEALTHY,
            schema=previous,
        )


class Operations:
    def __init__(self) -> None:
        self.error: Exception | None = None
        self.generate_result: Path | None = None

    async def generate(self, *_args: Any, **_kwargs: Any) -> Path | None:
        if self.error:
            raise self.error
        return self.generate_result

    async def apply(self, *_args: Any, **_kwargs: Any) -> bool:
        if self.error:
            raise self.error
        return True

    async def rollback(self, *_args: Any, **_kwargs: Any) -> bool:
        if self.error:
            raise self.error
        return True

    async def repair(self, *_args: Any, **_kwargs: Any) -> int:
        if self.error:
            raise self.error
        return 1

    async def squash(
        self,
        _paths: tuple[Path, ...],
        revision: str,
        *_args: Any,
    ) -> Path:
        if self.error:
            raise self.error
        assert self.generate_result is not None
        return self.generate_result


def controller(
    tmp_path: Path, operations: Operations | None = None
) -> PlaygroundController:
    return PlaygroundController(
        configuration(tmp_path),
        refresh_service=Refresh(),
        operations=operations or Operations(),
    )


def test_controller_editor_save_convert_draft_reload_and_subscription_edges(
    tmp_path: Path,
) -> None:
    path = write_migration(tmp_path, "001")
    app = controller(tmp_path)
    with pytest.raises(ValueError, match="select"):
        app.edit_active_source("")

    published: list[int] = []
    unsubscribe = app.subscribe(lambda state: published.append(state.generation))
    unsubscribe()
    unsubscribe()

    app.select_artifact(path)
    app.select_artifact(path)
    active = app.active_document
    assert active is not None and active.artifact is not None
    app.edit_active_source(active.artifact.to_toml())
    app.write_active_draft()
    assert app.recover_active_draft().dirty is True
    app.discard_active_draft()
    app.discard_active_draft()

    destination = tmp_path / "migrations" / "001-copy.toml"
    saved = app.save_active(destination=destination)
    assert saved.path == destination
    assert app.reload_workspace().selected_path == destination

    json_path = write_migration(tmp_path, "002", format="json")
    app.reload_workspace()
    app.select_artifact(json_path)
    converted = app.convert_active_to_toml()
    assert converted.path.suffix == ".toml"


def test_controller_build_and_generate_validation_and_failure_paths(
    tmp_path: Path,
) -> None:
    path = write_migration(tmp_path, "001")
    ops = Operations()
    app = controller(tmp_path, ops)
    app.select_artifact(path)

    with pytest.raises(ValueError, match="unsupported"):
        app.build_action_request("archive", database_name="db")
    app.edit_active_source("revision = [")
    with pytest.raises(ValueError, match="invalid"):
        app.build_action_request("apply", database_name="db")

    with pytest.raises(ValueError, match="refresh model"):
        awaitable = app.generate_migration("002")
        awaitable.send(None)


@pytest.mark.asyncio
async def test_generate_no_drift_noop_and_error_are_reported(tmp_path: Path) -> None:
    ops = Operations()
    app = controller(tmp_path, ops)
    snapshot = SchemaSnapshot.empty()
    app.state = replace(
        app.state,
        schema=SchemaState(
            model_snapshot=snapshot,
            live_snapshot=snapshot,
            stale=False,
        ),
    )
    with pytest.raises(ValueError, match="no schema drift"):
        await app.generate_migration("002")

    app.state = replace(
        app.state,
        schema=replace(app.state.schema, forward_sql=("SELECT 1",)),
    )
    assert await app.generate_migration("002") is None
    assert app.state.operation.message == "No migration generated"

    ops.error = RuntimeError("postgresql://u:secret@db failed")
    with pytest.raises(RuntimeError):
        await app.generate_migration("003")
    assert "secret" not in app.state.operation.message


@pytest.mark.asyncio
async def test_repair_request_and_execution_error_paths(tmp_path: Path) -> None:
    ops = Operations()
    app = controller(tmp_path, ops)
    with pytest.raises(ValueError, match="not available"):
        app.build_repair_request("missing", database_name="db")
    app.state = replace(
        app.state,
        migrations=MigrationState(
            history=(MigrationHistoryEntry(revision="001", dirty=False),)
        ),
    )
    with pytest.raises(ValueError, match="not dirty"):
        app.build_repair_request("001", database_name="db")

    app.state = replace(
        app.state,
        migrations=MigrationState(
            history=(MigrationHistoryEntry(revision="001", dirty=True),),
            dirty=True,
        ),
    )
    request = app.build_repair_request("001", database_name="db")
    blocked = await app.execute_repair(
        request,
        context(history_dirty=True),
        status="failed",
        confirmed=False,
        confirmation=None,
    )
    assert blocked.executed is False

    ops.error = RuntimeError("repair failed")
    failed = await app.execute_repair(
        request,
        context(history_dirty=True),
        status="failed",
        confirmed=True,
        confirmation="db 001",
    )
    assert failed.executed is True and failed.error is not None


@pytest.mark.asyncio
async def test_squash_validation_changed_inputs_block_and_execution_failure(
    tmp_path: Path,
) -> None:
    first = write_migration(tmp_path, "001")
    second = write_migration(tmp_path, "002")
    ops = Operations()
    app = controller(tmp_path, ops)
    with pytest.raises(ValueError, match="at least two"):
        app.build_squash_request((first,), "010", database_name="db")
    with pytest.raises(ValueError, match="not in the workspace"):
        app.build_squash_request(
            (first, tmp_path / "missing"), "010", database_name="db"
        )

    app.state = replace(
        app.state,
        migrations=replace(
            app.state.migrations,
            artifacts=tuple(
                replace(item, status="applied")
                for item in app.state.migrations.artifacts
            ),
        ),
    )
    with pytest.raises(ValueError, match="applied"):
        app.build_squash_request((first, second), "010", database_name="db")

    app = controller(tmp_path, ops)
    request = app.build_squash_request((first, second), "010", database_name="db")
    changed = replace(request, artifact_checksum="changed")
    blocked = await app.execute_squash(
        changed,
        context(
            artifact_checksum=changed.artifact_checksum,
            generation=changed.reviewed_generation,
        ),
        (first, second),
        confirmed=True,
        confirmation="db 010",
    )
    assert blocked.executed is False
    assert any("inputs changed" in reason for reason in blocked.decision.reasons)

    ops.error = RuntimeError("squash failed")
    failed = await app.execute_squash(
        request,
        context(
            artifact_checksum=request.artifact_checksum,
            generation=request.reviewed_generation,
        ),
        (first, second),
        confirmed=True,
        confirmation="db 010",
    )
    assert failed.executed is True and failed.error is not None

    app.select_artifact(first)
    app.edit_active_sql(0, "SELECT 2")
    with pytest.raises(ValueError, match="save and validate"):
        app.build_squash_request((first, second), "011", database_name="db")


@pytest.mark.asyncio
async def test_execute_rollback_and_unknown_action_paths(tmp_path: Path) -> None:
    path = write_migration(tmp_path, "001")
    app = controller(tmp_path)
    app.select_artifact(path)
    rollback = app.build_action_request("rollback", database_name="db")
    result = await app.execute_action(
        rollback,
        context(
            artifact_checksum=rollback.artifact_checksum,
            generation=app.state.generation,
        ),
        confirmed=True,
    )
    assert result.executed is True
    assert app.state.operation.message == "Rolled back 001"

    unknown = replace(
        rollback,
        action="unknown",
        reviewed_generation=app.state.generation,
    )
    result = await app.execute_action(
        unknown,
        context(
            artifact_checksum=unknown.artifact_checksum,
            generation=app.state.generation,
        ),
        confirmed=True,
    )
    assert result.error is not None
    assert result.error.code == "operation.unknown_failed"


def test_invalid_workspace_artifact_is_summarized(tmp_path: Path) -> None:
    migrations = tmp_path / "migrations"
    migrations.mkdir()
    (migrations / "bad.toml").write_text("revision = [")
    app = controller(tmp_path)
    summary = app.state.migrations.artifacts[0]
    assert summary.status == "invalid"
    assert summary.valid is False


def test_controller_replace_append_and_save_without_destination(tmp_path: Path) -> None:
    path = write_migration(tmp_path, "001")
    app = controller(tmp_path)
    app.select_artifact(path)
    assert app.save_active().path == path

    active = app.active_document
    assert active is not None
    app.workspace = replace(app.workspace, selected_path=None)
    appended = replace(active, path=tmp_path / "migrations" / "new.toml")
    app._replace_document(appended)
    assert app.active_document is appended


@pytest.mark.asyncio
async def test_squash_preflight_can_block_before_execution(tmp_path: Path) -> None:
    first = write_migration(tmp_path, "001")
    second = write_migration(tmp_path, "002")
    app = controller(tmp_path)
    request = app.build_squash_request((first, second), "010", database_name="db")

    outcome = await app.execute_squash(
        request,
        context(connected=False),
        (first, second),
        confirmed=True,
        confirmation="db 010",
    )

    assert outcome.executed is False
