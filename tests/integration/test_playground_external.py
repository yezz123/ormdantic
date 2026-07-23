from __future__ import annotations

import os
import uuid
from pathlib import Path

import pytest

from ormdantic.migrations import (
    MigrationArtifact,
    MigrationOperation,
    MigrationPlan,
    SchemaSnapshot,
)
from ormdantic.playground.config import (
    EffectiveConfig,
    EnvironmentConfig,
    ProjectConfig,
)
from ormdantic.playground.inspection import InspectionResult
from ormdantic.playground.operations import MigrationOperations
from ormdantic.playground.safety import ActionRequest, Risk, SafetyDecision
from ormdantic.playground.services import RefreshService
from ormdantic.playground.workspace import load_workspace

EXTERNAL_DATABASES = (
    ("postgresql", "ORMDANTIC_TEST_POSTGRES_URL", "VARCHAR(255)"),
    ("mysql", "ORMDANTIC_TEST_MYSQL_URL", "VARCHAR(255)"),
    ("mariadb", "ORMDANTIC_TEST_MARIADB_URL", "VARCHAR(255)"),
    ("mssql", "ORMDANTIC_TEST_MSSQL_URL", "NVARCHAR(255)"),
    ("oracle", "ORMDANTIC_TEST_ORACLE_URL", "VARCHAR2(255)"),
)


def request(
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
        environment="external",
        database_name="external",
        target=artifact.revision,
        risk=risk,
        sql=tuple(operation.sql for operation in operations),
        destructive_sql=tuple(
            operation.sql for operation in operations if operation.destructive
        ),
        artifact_checksum=artifact.checksum,
        reviewed_generation=1,
    )


@pytest.mark.parametrize(("dialect", "env_var", "string_type"), EXTERNAL_DATABASES)
async def test_external_playground_refresh_apply_history_and_rollback(
    tmp_path: Path,
    dialect: str,
    env_var: str,
    string_type: str,
) -> None:
    url = os.getenv(env_var) or os.getenv(
        env_var.replace("ORMDANTIC_TEST_", "ORMDANTIC_")
    )
    if not url:
        pytest.skip(f"set {env_var} to run the {dialect} playground contract")
    migrations_dir = tmp_path / "migrations"
    config = EffectiveConfig(
        path=tmp_path / "ormdantic.toml",
        root=tmp_path,
        project=ProjectConfig(target="app:db", migrations_dir=migrations_dir),
        environment=EnvironmentConfig(
            name="external",
            url_env=env_var,
            env_file=None,
        ),
    )

    async def inspector(target: str, *, cwd: Path) -> InspectionResult:
        return InspectionResult(snapshot=SchemaSnapshot.empty())

    refresh = await RefreshService(
        inspector=inspector,
        planner=lambda _url, _before, _after: MigrationPlan(),
    ).refresh(config, generation=1)
    assert refresh.dialect == dialect
    assert refresh.schema.live_snapshot is not None

    token = uuid.uuid4().hex[:10]
    table = f"ormdantic_playground_{token}"
    revision = f"playground_{dialect}_{token}"
    artifact = MigrationArtifact.from_plan(
        revision,
        MigrationPlan(
            operations=[
                MigrationOperation(
                    sql=f"CREATE TABLE {table} (id {string_type} PRIMARY KEY)",
                    kind="create_table",
                )
            ],
            rollback_operations=[
                MigrationOperation(
                    sql=f"DROP TABLE {table}",
                    kind="drop_table",
                    destructive=True,
                )
            ],
        ),
        SchemaSnapshot.empty(),
        SchemaSnapshot.empty(),
        dialect=dialect,
    )
    path = migrations_dir / f"{revision}.toml"
    artifact.write(path)
    document = load_workspace(migrations_dir).documents[0]
    loaded = document.artifact
    assert loaded is not None
    operations = MigrationOperations(config)
    applied = False
    try:
        applied = await operations.apply(
            document,
            request(loaded, action="apply", risk=Risk.WRITE),
            SafetyDecision(True, None),
        )
        assert applied is True
        history = await operations.history()
        assert any(
            entry.revision == revision and entry.status == "applied"
            for entry in history
        )

        with pytest.raises(PermissionError, match="lower than actual"):
            await operations.rollback(
                document,
                request(loaded, action="rollback", risk=Risk.WRITE),
                SafetyDecision(True, None),
            )

        rolled_back = await operations.rollback(
            document,
            request(loaded, action="rollback", risk=Risk.DESTRUCTIVE),
            SafetyDecision(True, None),
        )
        applied = False
        assert rolled_back is True
    finally:
        if applied:
            await operations.rollback(
                document,
                request(loaded, action="rollback", risk=Risk.DESTRUCTIVE),
                SafetyDecision(True, None),
            )
