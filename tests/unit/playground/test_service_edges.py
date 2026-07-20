from __future__ import annotations

import asyncio
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
from ormdantic.playground import diagnostics, operations, services, workspace
from ormdantic.playground.config import (
    DatabaseUrlSource,
    EffectiveConfig,
    EnvironmentConfig,
    ProjectConfig,
)
from ormdantic.playground.inspection import InspectionResult
from ormdantic.playground.safety import ActionRequest, Risk, SafetyDecision
from ormdantic.playground.state import RefreshStatus, SchemaState


def effective(tmp_path: Path, *, target: str | None = "app:db") -> EffectiveConfig:
    return EffectiveConfig(
        path=tmp_path / "ormdantic.toml",
        root=tmp_path,
        project=ProjectConfig(
            target=target,
            migrations_dir=tmp_path / "migrations",
        ),
        environment=EnvironmentConfig(name="development"),
    )


def migration(
    revision: str = "001_initial",
    *,
    destructive: bool = False,
) -> MigrationArtifact:
    return MigrationArtifact.from_plan(
        revision,
        MigrationPlan(
            operations=[
                MigrationOperation(
                    "DROP TABLE users"
                    if destructive
                    else "CREATE TABLE users (id INT)",
                    destructive=destructive,
                )
            ],
            rollback_operations=[
                MigrationOperation("DROP TABLE users", destructive=True)
            ],
        ),
        SchemaSnapshot.empty(),
        SchemaSnapshot.empty(),
        dialect="sqlite",
    )


def action_request(
    artifact: MigrationArtifact,
    action: str,
    risk: Risk,
    *,
    target: str | None = None,
) -> ActionRequest:
    return ActionRequest(
        action=action,
        environment="development",
        database_name="app",
        target=target or artifact.revision,
        risk=risk,
        sql=tuple(operation.sql for operation in artifact.operations),
        artifact_checksum=artifact.checksum,
        reviewed_generation=1,
    )


def test_workspace_rejects_unknown_invalid_and_out_of_range_edits(
    tmp_path: Path,
) -> None:
    artifact = migration()
    path = tmp_path / "001.toml"
    artifact.write(path)
    document = workspace.load_workspace(tmp_path).documents[0]

    with pytest.raises(ValueError, match="not in the workspace"):
        workspace.select_document(workspace.load_workspace(tmp_path), tmp_path / "x")
    invalid = replace(document, artifact=None)
    with pytest.raises(ValueError, match="validation errors"):
        workspace.replace_operation_sql(invalid, index=0, sql="SELECT 1")
    with pytest.raises(IndexError, match="out of range"):
        workspace.replace_operation_sql(document, index=8, sql="SELECT 1")
    with pytest.raises(ValueError, match="invalid migration"):
        workspace.convert_to_toml(invalid, tmp_path / "copy.toml")
    with pytest.raises(ValueError, match="validation errors"):
        workspace.save_document(invalid)


def test_workspace_covers_rollback_json_drafts_and_safe_fallbacks(
    tmp_path: Path,
) -> None:
    artifact = migration()
    json_path = tmp_path / "001.json"
    artifact.write(json_path)
    json_document = workspace.load_workspace(tmp_path).documents[0]
    edited = workspace.replace_operation_sql(
        json_document,
        index=0,
        sql="DROP TABLE changed",
        rollback=True,
    )
    assert edited.format == "json"
    assert "DROP TABLE changed" in edited.source

    draft = workspace.draft_path(
        tmp_path,
        workspace.ArtifactDocument(
            path=tmp_path / "...toml",
            format="toml",
            source="",
            artifact=None,
        ),
    )
    assert draft.name == "untitled.toml"

    toml_source = artifact.to_toml()
    draft.parent.mkdir(parents=True, exist_ok=True)
    json_draft_path = workspace.draft_path(tmp_path, json_document)
    json_draft_path.write_text(toml_source)
    recovered = workspace.recover_draft(tmp_path, json_document)
    assert recovered.format == "toml"
    assert recovered.path.suffix == ".toml"

    workspace.discard_draft(tmp_path, json_document)
    workspace.discard_draft(tmp_path, json_document)
    assert workspace._parse_edited_source(artifact.to_json(), "json").revision == (
        artifact.revision
    )


def test_recursive_diagnostics_redact_sequences_but_preserve_scalars() -> None:
    value = diagnostics.redact_value(
        {
            "items": ["postgresql://u:p@host/db", 3],
            "payload": b"raw",
            "password": "secret",
        }
    )

    assert value["items"] == ("postgresql://u:<redacted>@host/db", 3)
    assert value["payload"] == b"raw"
    assert value["password"] == "<redacted>"


async def test_operation_methods_cover_noop_rollback_repair_squash_and_history(
    tmp_path: Path,
) -> None:
    artifact = migration()
    artifact_path = tmp_path / "migrations" / "001.toml"
    artifact.write(artifact_path)
    document = workspace.load_workspace(artifact_path.parent).documents[0]
    calls: list[str] = []

    async def runner(function: Any, *args: Any) -> Any:
        calls.append(function.__name__ if hasattr(function, "__name__") else "generate")
        if function is operations._rollback_sync:
            return True
        if function is operations._repair_sync:
            return 1
        if function is operations._squash_sync:
            return migration("002_squashed")
        if function is operations._history_sync:
            return (MigrationHistoryEntry(revision="001_initial"),)
        return function()

    manager = operations.MigrationOperations(
        effective(tmp_path),
        url_resolver=lambda _environment: DatabaseUrlSource("sqlite:///db", "env"),
        thread_runner=runner,
        artifact_generator=lambda *_args, **_kwargs: None,
    )
    assert (
        await manager.generate(
            "002_noop",
            None,
            before=SchemaSnapshot.empty(),
            after=SchemaSnapshot.empty(),
        )
        is None
    )
    assert await manager.rollback(
        document,
        action_request(artifact, "rollback", Risk.DESTRUCTIVE),
        SafetyDecision(True, None),
    )
    assert (
        await manager.repair(
            "001_initial",
            "applied",
            action_request(artifact, "repair", Risk.HISTORY_REWRITE),
            SafetyDecision(True, None),
        )
        == 1
    )
    squashed = await manager.squash(
        [artifact_path],
        "002_squashed",
        action_request(
            artifact,
            "squash",
            Risk.HISTORY_REWRITE,
            target="002_squashed",
        ),
        SafetyDecision(True, None),
    )
    assert squashed.name == "002_squashed.toml"
    assert (await manager.history())[0].revision == "001_initial"
    assert {"_rollback_sync", "_repair_sync", "_squash_sync", "_history_sync"} <= set(
        calls
    )


def test_operation_authorization_and_sync_helpers_cover_failures_and_results(
    tmp_path: Path,
) -> None:
    artifact = migration()
    invalid = workspace.ArtifactDocument(
        path=tmp_path / "bad.toml",
        format="toml",
        source="",
        artifact=None,
    )
    with pytest.raises(PermissionError, match="invalid migration"):
        operations._authorized_artifact(
            invalid,
            action_request(artifact, "apply", Risk.WRITE),
            SafetyDecision(True, None),
            "apply",
        )
    with pytest.raises(PermissionError, match="does not match"):
        operations._authorized_request(
            action_request(artifact, "apply", Risk.WRITE),
            SafetyDecision(True, None),
            "rollback",
            artifact.revision,
        )
    with pytest.raises(PermissionError, match="lower than actual"):
        operations._validate_risk(Risk.WRITE, Risk.DESTRUCTIVE)

    class Manager:
        async def apply_artifact(self, *_args: Any, **_kwargs: Any) -> bool:
            return True

        async def rollback_artifact(self, *_args: Any, **_kwargs: Any) -> bool:
            return True

        async def repair(self, *_args: Any, **_kwargs: Any) -> int:
            return 2

        def squash(self, *_args: Any, **_kwargs: Any) -> MigrationArtifact:
            return artifact

        async def history(self) -> list[MigrationHistoryEntry]:
            return [MigrationHistoryEntry(revision="001_initial")]

    class Database:
        def __init__(self, _url: str) -> None:
            self.migrations = Manager()

    assert operations._apply_sync(Database, "sqlite:///db", artifact, False)
    assert operations._rollback_sync(Database, "sqlite:///db", artifact, True)
    assert operations._repair_sync(Database, "sqlite:///db", "001", "applied") == 2
    assert operations._squash_sync(Database, "sqlite:///db", "002", [artifact]) is (
        artifact
    )
    assert operations._history_sync(Database, "sqlite:///db")[0].revision == (
        "001_initial"
    )
    assert (
        operations._generate_artifact(
            "001_empty",
            SchemaSnapshot.empty(),
            SchemaSnapshot.empty(),
            dialect="sqlite",
            description=None,
            depends_on=(),
        )
        is None
    )


async def test_refresh_handles_missing_target_url_model_and_plan_failures(
    tmp_path: Path,
) -> None:
    async def inspector(_target: str, *, cwd: Path) -> InspectionResult:
        assert cwd == tmp_path
        raise RuntimeError("model failed")

    service = services.RefreshService(
        inspector=inspector,
        url_resolver=lambda _environment: (_ for _ in ()).throw(
            RuntimeError("DATABASE_URL missing")
        ),
    )
    with pytest.raises(ValueError, match="target"):
        await service.refresh(effective(tmp_path, target=None), generation=1)
    result = await service.refresh(effective(tmp_path), generation=2)
    assert result.status is RefreshStatus.ERROR
    assert {item.code for item in result.diagnostics} == {
        "database.url_missing",
        "model.inspection_failed",
    }

    snapshot = SchemaSnapshot.empty()

    async def healthy_inspector(_target: str, *, cwd: Path) -> InspectionResult:
        return InspectionResult(snapshot=snapshot)

    plan_failure = services.RefreshService(
        inspector=healthy_inspector,
        url_resolver=lambda _environment: DatabaseUrlSource("sqlite:///db", "env"),
        reflector=lambda _url: services.DatabaseRefresh(snapshot),
        planner=lambda *_args: (_ for _ in ()).throw(RuntimeError("unsupported")),
    )
    planned = await plan_failure.refresh(
        effective(tmp_path),
        generation=3,
        previous=SchemaState(forward_sql=("old",), rollback_sql=("down",)),
    )
    assert planned.status is RefreshStatus.PARTIAL
    assert planned.schema.forward_sql == ("old",)
    assert planned.diagnostics[0].code == "schema.plan_failed"


async def test_task_cancellation_propagates() -> None:
    async def cancelled() -> None:
        raise asyncio.CancelledError

    task = asyncio.create_task(cancelled())
    with pytest.raises(asyncio.CancelledError):
        await services._task_result(task)


def test_reflection_history_current_dirty_dialect_aliases_and_planner(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    snapshot = SchemaSnapshot.empty()
    history = [
        MigrationHistoryEntry(revision="001", status="rolled_back"),
        MigrationHistoryEntry(revision="002", status="applied", dirty=True),
        MigrationHistoryEntry(revision="003", status="applied"),
    ]

    class Manager:
        def live_snapshot(self) -> SchemaSnapshot:
            return snapshot

        async def history(self) -> list[MigrationHistoryEntry]:
            return history

    class Database:
        def __init__(self, _url: str) -> None:
            self.migrations = Manager()

    reflected = services.reflect_database(
        "sqlite:///db",
        database_factory=Database,
        history_exists=lambda _snapshot: True,
    )
    assert reflected.current_revision == "003"
    assert reflected.dirty is True
    assert services._dialect_name("postgres+tls://db") == "postgresql"
    assert services._dialect_name("sqlserver://db") == "mssql"

    expected = MigrationPlan()

    class PlanningManager:
        def generate_plan(
            self, before: Any, after: Any, *, dialect: str
        ) -> MigrationPlan:
            assert (
                before is snapshot and after is snapshot and dialect == "sqlite:///db"
            )
            return expected

    monkeypatch.setattr(
        services,
        "Ormdantic",
        lambda _url: type("Db", (), {"migrations": PlanningManager()})(),
    )
    assert services.plan_migration("sqlite:///db", snapshot, snapshot) is expected
