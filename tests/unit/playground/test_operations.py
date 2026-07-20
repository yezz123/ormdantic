from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pytest

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


def config(tmp_path: Path) -> EffectiveConfig:
    return EffectiveConfig(
        path=tmp_path / "ormdantic.toml",
        root=tmp_path,
        project=ProjectConfig(
            target="app:db",
            migrations_dir=tmp_path / "migrations",
        ),
        environment=EnvironmentConfig(name="development"),
    )


def artifact(revision: str = "001_initial") -> MigrationArtifact:
    return MigrationArtifact.from_plan(
        revision,
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


def request(migration: MigrationArtifact, *, action: str, risk: Risk) -> ActionRequest:
    operations = (
        migration.rollback_operations if action == "rollback" else migration.operations
    )
    destructive = tuple(item.sql for item in operations if item.destructive)
    return ActionRequest(
        action=action,
        environment="development",
        database_name="app",
        target=migration.revision,
        risk=risk,
        sql=tuple(item.sql for item in operations),
        destructive_sql=destructive,
        artifact_checksum=migration.checksum,
        reviewed_generation=1,
    )


async def test_apply_requires_a_bound_allowed_safety_decision(tmp_path: Path) -> None:
    migration = artifact()
    path = tmp_path / "migrations" / "001_initial.toml"
    migration.write(path)
    document = load_workspace(path.parent).documents[0]
    factory_calls: list[str] = []

    def database_factory(url: str) -> object:
        factory_calls.append(url)
        return object()

    operations = MigrationOperations(
        config(tmp_path),
        url_resolver=lambda _environment: DatabaseUrlSource("sqlite:///db", "env"),
        database_factory=database_factory,
    )

    with pytest.raises(PermissionError, match="not authorized"):
        await operations.apply(
            document,
            request(migration, action="apply", risk=Risk.WRITE),
            SafetyDecision(False, None, ("confirmation required",)),
        )

    assert factory_calls == []


async def test_apply_runs_exact_artifact_in_worker_after_authorization(
    tmp_path: Path,
) -> None:
    migration = artifact()
    path = tmp_path / "migrations" / "001_initial.toml"
    migration.write(path)
    document = load_workspace(path.parent).documents[0]
    calls: list[object] = []

    class FakeManager:
        async def apply_artifact(
            self,
            value: MigrationArtifact,
            *,
            allow_destructive: bool,
        ) -> bool:
            calls.extend((value.revision, allow_destructive))
            return True

    class FakeDatabase:
        def __init__(self, url: str) -> None:
            calls.append(url)
            self.migrations = FakeManager()

    async def thread_runner(function: Any, *args: Any) -> Any:
        calls.append("thread")
        return await asyncio.to_thread(function, *args)

    operations = MigrationOperations(
        config(tmp_path),
        url_resolver=lambda _environment: DatabaseUrlSource("sqlite:///db", "env"),
        database_factory=FakeDatabase,
        thread_runner=thread_runner,
    )

    applied = await operations.apply(
        document,
        request(migration, action="apply", risk=Risk.WRITE),
        SafetyDecision(True, None),
    )

    assert applied is True
    assert calls == ["thread", "sqlite:///db", "001_initial", False]


async def test_apply_rejects_a_decision_for_a_different_checksum(
    tmp_path: Path,
) -> None:
    migration = artifact()
    path = tmp_path / "migrations" / "001_initial.toml"
    migration.write(path)
    document = load_workspace(path.parent).documents[0]
    mismatched = request(migration, action="apply", risk=Risk.WRITE)
    mismatched = ActionRequest(
        **{
            **mismatched.__dict__,
            "artifact_checksum": "different",
        }
    )
    operations = MigrationOperations(
        config(tmp_path),
        url_resolver=lambda _environment: DatabaseUrlSource("sqlite:///db", "env"),
    )

    with pytest.raises(PermissionError, match="artifact changed"):
        await operations.apply(
            document,
            mismatched,
            SafetyDecision(True, None),
        )


async def test_generate_writes_canonical_toml_and_skips_noop(tmp_path: Path) -> None:
    empty = SchemaSnapshot.empty()
    generated = artifact("002_users")

    def generator(
        revision: str,
        before: SchemaSnapshot,
        after: SchemaSnapshot,
        *,
        dialect: str,
        description: str | None,
        depends_on: tuple[str, ...],
    ) -> MigrationArtifact | None:
        assert revision == "002_users"
        assert before is empty
        assert after is empty
        assert dialect == "sqlite:///db"
        assert description == "add users"
        assert depends_on == ("001_initial",)
        return generated

    operations = MigrationOperations(
        config(tmp_path),
        url_resolver=lambda _environment: DatabaseUrlSource("sqlite:///db", "env"),
        artifact_generator=generator,
    )

    path = await operations.generate(
        "002_users",
        "add users",
        before=empty,
        after=empty,
        depends_on=("001_initial",),
    )

    assert path == tmp_path / "migrations" / "002_users.toml"
    assert path is not None
    assert MigrationArtifact.read(path).revision == "002_users"
    assert path.read_text().startswith("version = ")


async def test_generate_rejects_a_revision_that_can_escape_directory(
    tmp_path: Path,
) -> None:
    operations = MigrationOperations(
        config(tmp_path),
        url_resolver=lambda _environment: DatabaseUrlSource("sqlite:///db", "env"),
    )

    with pytest.raises(ValueError, match="revision"):
        await operations.generate(
            "../outside",
            None,
            before=SchemaSnapshot.empty(),
            after=SchemaSnapshot.empty(),
        )
