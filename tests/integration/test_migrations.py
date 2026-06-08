from __future__ import annotations

import pytest
from pydantic import BaseModel, Field

from ormdantic import Ormdantic
from ormdantic.cli import main
from ormdantic.migrations import (
    MigrationArtifact,
    MigrationOperation,
    MigrationPlan,
    SchemaSnapshot,
    create_migration_artifact,
    squash_migrations,
)


def test_schema_snapshot_roundtrip_diff_and_dry_run() -> None:
    old_db = Ormdantic("sqlite:///:memory:")

    @old_db.table("flavor", pk="id", indexed=["name"])
    class OldFlavor(BaseModel):
        id: str
        name: str

    old_snapshot = old_db.migrations.snapshot()

    new_db = Ormdantic("sqlite:///:memory:")

    @new_db.table(
        "flavor", pk="id", indexed=["name"], unique_constraints=[["name", "code"]]
    )
    class NewFlavor(BaseModel):
        id: str
        name: str = Field(min_length=2)
        code: str
        strength: int | None = None

    new_snapshot = new_db.migrations.snapshot()
    roundtrip = SchemaSnapshot.from_json(new_snapshot.to_json())

    assert roundtrip.to_dict() == new_snapshot.to_dict()

    diff = new_db.migrations.diff(old_snapshot, roundtrip)
    assert "Added column flavor.code" in diff.summary()
    assert "Added column flavor.strength" in diff.summary()
    assert any(
        change.object_type == "constraint"
        and change.action == "add"
        and change.name == "flavor_unique_0"
        for change in diff.changes
    )
    assert diff.has_unsafe_operations
    assert not diff.has_destructive_operations

    plan = new_db.migrations.generate_plan(old_snapshot, roundtrip, dialect="sqlite")
    assert 'ALTER TABLE "flavor" ADD COLUMN "code" TEXT NOT NULL' in plan.dry_run()
    assert any("ADD CONSTRAINT" in sql for sql in plan.dry_run())
    assert plan.has_unsafe_operations


@pytest.mark.asyncio
async def test_generated_plan_apply_records_once_and_rolls_back(tmp_path) -> None:
    db = Ormdantic(f"sqlite:///{tmp_path / 'generated.sqlite3'}")

    @db.table(pk="id")
    class Flavor(BaseModel):
        id: str
        name: str

    plan = db.migrations.generate_plan(dialect="sqlite")

    assert await db.migrations.apply("001_create_flavor", plan) is True
    assert await db.migrations.apply("001_create_flavor", plan) is False
    assert await db.migrations.applied_revisions() == ["001_create_flavor"]

    assert await db.migrations.rollback("001_create_flavor", plan) is True
    assert await db.migrations.rollback("001_create_flavor", plan) is False
    assert await db.migrations.applied_revisions() == []


@pytest.mark.asyncio
async def test_destructive_migration_requires_explicit_opt_in(tmp_path) -> None:
    db = Ormdantic(f"sqlite:///{tmp_path / 'destructive.sqlite3'}")

    safe_plan = MigrationPlan(
        [MigrationOperation("CREATE TABLE migration_extra (id TEXT)")]
    )
    assert await db.migrations.apply("001", safe_plan) is True

    destructive_plan = MigrationPlan([MigrationOperation("DROP TABLE migration_extra")])
    with pytest.raises(ValueError, match="allow_destructive=True"):
        await db.migrations.apply("002", destructive_plan)

    assert await db.migrations.applied_revisions() == ["001"]
    assert (
        await db.migrations.apply("002", destructive_plan, allow_destructive=True)
        is True
    )
    assert await db.migrations.applied_revisions() == ["001", "002"]


def test_generated_diff_sql_renders_supported_dialects() -> None:
    old_db = Ormdantic("sqlite:///:memory:")

    @old_db.table("flavor", pk="id")
    class OldFlavor(BaseModel):
        id: str
        name: str

    new_db = Ormdantic("sqlite:///:memory:")

    @new_db.table("flavor", pk="id")
    class NewFlavor(BaseModel):
        id: str
        name: str
        rating: int | None = None

    old_snapshot = old_db.migrations.snapshot()
    new_snapshot = new_db.migrations.snapshot()

    expected = {
        "sqlite": ['ALTER TABLE "flavor" ADD COLUMN "rating" INTEGER'],
        "postgresql": ['ALTER TABLE "flavor" ADD COLUMN "rating" INTEGER'],
        "mysql": ["ALTER TABLE `flavor` ADD COLUMN `rating` INTEGER"],
        "mariadb": ["ALTER TABLE `flavor` ADD COLUMN `rating` INTEGER"],
        "mssql": ["ALTER TABLE [flavor] ADD [rating] INTEGER"],
        "oracle": ['ALTER TABLE "flavor" ADD ("rating" INTEGER)'],
    }
    for dialect, sql in expected.items():
        assert (
            new_db.migrations.dry_run(old_snapshot, new_snapshot, dialect=dialect)
            == sql
        )


@pytest.mark.asyncio
async def test_migration_artifacts_apply_directory_and_squash(tmp_path) -> None:
    base = SchemaSnapshot.empty()
    first_db = Ormdantic("sqlite:///:memory:")

    @first_db.table("flavor", pk="id")
    class InitialFlavor(BaseModel):
        id: str
        name: str

    first = first_db.migrations.snapshot()

    second_db = Ormdantic("sqlite:///:memory:")

    @second_db.table("flavor", pk="id")
    class ExpandedFlavor(BaseModel):
        id: str
        name: str
        rating: int | None = None

    second = second_db.migrations.snapshot()
    first_artifact = create_migration_artifact(
        "001_initial", base, first, dialect="sqlite"
    )
    second_artifact = create_migration_artifact(
        "002_rating", first, second, dialect="sqlite"
    )

    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    first_path = migrations_dir / "001_initial.json"
    second_path = migrations_dir / "002_rating.json"
    first_artifact.write(first_path)
    second_artifact.write(second_path)

    roundtrip = MigrationArtifact.read(second_path)
    assert roundtrip.to_dict() == second_artifact.to_dict()

    db = Ormdantic(f"sqlite:///{tmp_path / 'artifact.sqlite3'}")
    assert await db.migrations.apply_directory(migrations_dir) == [
        "001_initial",
        "002_rating",
    ]
    assert await db.migrations.apply_directory(migrations_dir) == []
    assert await db.migrations.applied_revisions() == ["001_initial", "002_rating"]

    squashed = squash_migrations(
        "001_squashed",
        [first_path, second_path],
        dialect="sqlite",
    )
    assert squashed.revision == "001_squashed"
    assert squashed.from_snapshot.to_dict() == base.to_dict()
    assert squashed.to_snapshot.to_dict() == second.to_dict()
    assert any("CREATE TABLE" in sql for sql in squashed.to_plan().dry_run())
    assert not any("ALTER TABLE" in sql for sql in squashed.to_plan().dry_run())


def test_migration_cli_create_preview_and_apply(tmp_path, capsys) -> None:
    old_snapshot = SchemaSnapshot.empty()
    db = Ormdantic("sqlite:///:memory:")

    @db.table("flavor", pk="id")
    class CliFlavor(BaseModel):
        id: str
        name: str

    new_snapshot = db.migrations.snapshot()
    old_path = tmp_path / "old.json"
    new_path = tmp_path / "new.json"
    artifact_path = tmp_path / "001_cli.json"
    old_snapshot.write(old_path)
    new_snapshot.write(new_path)

    assert (
        main(
            [
                "migrations",
                "create",
                "001_cli",
                "--from",
                str(old_path),
                "--to",
                str(new_path),
                "--dialect",
                "sqlite",
                "--out",
                str(artifact_path),
            ]
        )
        == 0
    )
    assert artifact_path.exists()

    assert main(["migrations", "preview", str(artifact_path)]) == 0
    assert "CREATE TABLE" in capsys.readouterr().out

    url = f"sqlite:///{tmp_path / 'cli.sqlite3'}"
    assert main(["migrations", "apply", url, str(artifact_path)]) == 0
    assert "applied" in capsys.readouterr().out
