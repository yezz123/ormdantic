from __future__ import annotations

import asyncio
from pathlib import Path

from ormdantic import Ormdantic
from ormdantic.migrations import (
    MigrationArtifact,
    MigrationOperation,
    MigrationPlan,
    SchemaSnapshot,
)
from ormdantic.playground.config import (
    DatabaseUrlSource,
    EffectiveConfig,
    EnvironmentConfig,
    ProjectConfig,
)
from ormdantic.playground.operations import MigrationOperations
from ormdantic.playground.safety import ActionRequest, Risk, SafetyDecision
from ormdantic.playground.workspace import load_workspace


def playground_config(tmp_path: Path) -> EffectiveConfig:
    return EffectiveConfig(
        path=tmp_path / "ormdantic.toml",
        root=tmp_path,
        project=ProjectConfig(
            target="app:db",
            migrations_dir=tmp_path / "migrations",
        ),
        environment=EnvironmentConfig(name="development"),
    )


def action_request(
    artifact: MigrationArtifact,
    *,
    action: str,
    risk: Risk,
) -> ActionRequest:
    operations = (
        artifact.rollback_operations if action == "rollback" else artifact.operations
    )
    return ActionRequest(
        action=action,
        environment="development",
        database_name="playground",
        target=artifact.revision,
        risk=risk,
        sql=tuple(operation.sql for operation in operations),
        destructive_sql=tuple(
            operation.sql for operation in operations if operation.destructive
        ),
        artifact_checksum=artifact.checksum,
        reviewed_generation=1,
    )


async def test_sqlite_playground_apply_history_and_rollback(tmp_path: Path) -> None:
    database_path = tmp_path / "playground.sqlite3"
    url = f"sqlite:///{database_path}"
    migration = MigrationArtifact.from_plan(
        "001_users",
        MigrationPlan(
            operations=[
                MigrationOperation(
                    sql="CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
                    kind="create_table",
                )
            ],
            rollback_operations=[
                MigrationOperation(
                    sql="DROP TABLE users",
                    kind="drop_table",
                    destructive=True,
                )
            ],
        ),
        SchemaSnapshot.empty(),
        SchemaSnapshot.empty(),
        dialect="sqlite",
    )
    migration_path = tmp_path / "migrations" / "001_users.toml"
    migration.write(migration_path)
    document = load_workspace(migration_path.parent).documents[0]
    loaded = document.artifact
    assert loaded is not None
    operations = MigrationOperations(
        playground_config(tmp_path),
        url_resolver=lambda _environment: DatabaseUrlSource(url, "DATABASE_URL"),
    )

    applied = await operations.apply(
        document,
        action_request(loaded, action="apply", risk=Risk.WRITE),
        SafetyDecision(True, None),
    )
    history = await operations.history()
    live_after_apply = await asyncio.to_thread(
        lambda: Ormdantic(url).migrations.live_snapshot()
    )

    assert applied is True
    assert history[-1].revision == "001_users"
    assert history[-1].status == "applied"
    assert any(table.name == "users" for table in live_after_apply.tables)

    rolled_back = await operations.rollback(
        document,
        action_request(loaded, action="rollback", risk=Risk.DESTRUCTIVE),
        SafetyDecision(True, None),
    )
    history_after_rollback = await operations.history()
    live_after_rollback = await asyncio.to_thread(
        lambda: Ormdantic(url).migrations.live_snapshot()
    )

    assert rolled_back is True
    assert history_after_rollback[-1].revision == "001_users"
    assert history_after_rollback[-1].status == "rolled_back"
    assert all(table.name != "users" for table in live_after_rollback.tables)
