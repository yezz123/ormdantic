from __future__ import annotations

import pytest

from ormdantic import migrations
from ormdantic._migrations import artifacts
from ormdantic._migrations.artifacts import MigrationArtifact
from ormdantic._migrations.models import (
    ColumnSnapshot,
    MigrationChange,
    MigrationOperation,
    MigrationPlan,
    MigrationWarning,
    SchemaDiff,
    SchemaSnapshot,
    TableSnapshot,
)


def snapshot(table_name: str) -> SchemaSnapshot:
    return SchemaSnapshot(
        tables=[
            TableSnapshot(
                model_key=table_name.title(),
                name=table_name,
                primary_key="id",
                columns=[
                    ColumnSnapshot(
                        "id",
                        "int",
                        nullable=False,
                        primary_key=True,
                    )
                ],
            )
        ]
    )


def artifact(revision: str, before: SchemaSnapshot, after: SchemaSnapshot) -> MigrationArtifact:
    plan = MigrationPlan(
        operations=[
            MigrationOperation(
                "CREATE TABLE flavor (id INTEGER PRIMARY KEY)",
                kind="create_table",
                table="flavor",
            )
        ],
        rollback_operations=[
            MigrationOperation(
                "DROP TABLE flavor",
                kind="drop_table",
                table="flavor",
                destructive=True,
            )
        ],
        diff=SchemaDiff(
            changes=[
                MigrationChange(
                    "add",
                    "table",
                    "flavor",
                    "flavor",
                    "Added table flavor",
                )
            ],
            warnings=[
                MigrationWarning(
                    "unsafe_table_add",
                    "review before applying",
                    "flavor",
                    "flavor",
                )
            ],
        ),
        safety={"dialect": "sqlite", "rollback_available": True},
    )
    return MigrationArtifact.from_plan(
        revision,
        plan,
        before,
        after,
        dialect="sqlite",
        description=f"migration {revision}",
        depends_on=["base"],
        branch_labels=["main"],
        metadata={"source": "unit"},
        created_at="2026-01-01T00:00:00+00:00",
    )


def test_public_migration_facade_re_exports_artifact_helpers() -> None:
    assert migrations.MigrationArtifact is artifacts.MigrationArtifact
    assert migrations._operation_to_dict is artifacts._operation_to_dict
    assert migrations._plan_checksum is artifacts._plan_checksum


def test_artifact_roundtrips_json_toml_and_plan(tmp_path) -> None:
    before = SchemaSnapshot.empty()
    after = snapshot("flavor")
    migration = artifact("001_create_flavor", before, after)

    assert migration.checksum
    assert MigrationArtifact.from_json(migration.to_json()).to_dict() == migration.to_dict()
    assert MigrationArtifact.from_toml(migration.to_toml()).to_dict() == migration.to_dict()
    assert migration.to_plan().dry_run() == [
        "CREATE TABLE flavor (id INTEGER PRIMARY KEY)"
    ]

    json_path = tmp_path / "001_create_flavor.json"
    toml_path = tmp_path / "001_create_flavor.toml"
    migration.write(json_path)
    migration.write(toml_path)

    assert MigrationArtifact.read(json_path).to_dict() == migration.to_dict()
    assert MigrationArtifact.read(toml_path).to_dict() == migration.to_dict()


def test_artifact_checksum_detects_tampering() -> None:
    migration = artifact("001_create_flavor", SchemaSnapshot.empty(), snapshot("flavor"))
    payload = migration.to_dict()
    payload["up"][0]["sql"] = "DROP TABLE flavor"

    with pytest.raises(ValueError, match="checksum mismatch"):
        MigrationArtifact.from_dict(payload)


def test_checksum_canonicalization_ignores_null_values() -> None:
    assert artifacts._artifact_checksum({"revision": "001", "description": None}) == (
        artifacts._artifact_checksum({"revision": "001"})
    )


def test_artifact_file_discovery_and_coercion(tmp_path) -> None:
    first = artifact("001_create_flavor", SchemaSnapshot.empty(), snapshot("flavor"))
    second = artifact("002_noop", first.to_snapshot, first.to_snapshot)
    json_path = tmp_path / "001_create_flavor.json"
    toml_path = tmp_path / "002_noop.toml"
    ignored_path = tmp_path / "notes.txt"
    first.write(json_path)
    second.write(toml_path)
    ignored_path.write_text("ignore me")

    assert artifacts._migration_files(tmp_path, None) == [json_path, toml_path]
    assert artifacts._migration_files(tmp_path, "*.toml") == [toml_path]
    assert artifacts._coerce_artifact(json_path).revision == "001_create_flavor"
    assert artifacts._coerce_artifact(first.to_dict()).revision == "001_create_flavor"


def test_contiguous_artifact_validation() -> None:
    first = artifact("001_create_flavor", SchemaSnapshot.empty(), snapshot("flavor"))
    second = artifact("002_create_roast", first.to_snapshot, snapshot("roast"))
    artifacts._validate_contiguous_artifacts([first, second])

    broken = artifact("003_broken", SchemaSnapshot.empty(), snapshot("supplier"))
    with pytest.raises(ValueError, match="not contiguous"):
        artifacts._validate_contiguous_artifacts([first, broken])
