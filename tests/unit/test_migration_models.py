from __future__ import annotations

import pytest

from ormdantic import migrations
from ormdantic._migrations import documents
from ormdantic._migrations.models import (
    ColumnSnapshot,
    MigrationOperation,
    MigrationPlan,
    SchemaSnapshot,
    TableSnapshot,
)


def test_public_migration_facade_re_exports_model_objects() -> None:
    assert migrations.SchemaSnapshot is SchemaSnapshot
    assert migrations.MigrationPlan is MigrationPlan


def test_schema_snapshot_roundtrips_json_toml_and_runtime(tmp_path) -> None:
    snapshot = SchemaSnapshot(
        tables=[
            TableSnapshot(
                model_key="Flavor",
                name="flavor",
                primary_key="id",
                columns=[
                    ColumnSnapshot(
                        "id",
                        "str",
                        nullable=False,
                        primary_key=True,
                        max_length=64,
                    )
                ],
            )
        ]
    )

    assert SchemaSnapshot.from_json(snapshot.to_json()).to_dict() == snapshot.to_dict()
    assert SchemaSnapshot.from_toml(snapshot.to_toml()).to_dict() == snapshot.to_dict()

    json_path = tmp_path / "snapshot.json"
    toml_path = tmp_path / "snapshot.toml"
    snapshot.write(json_path)
    snapshot.write(toml_path)

    assert SchemaSnapshot.read(json_path).to_runtime() == snapshot.to_runtime()
    assert SchemaSnapshot.read(toml_path).to_runtime() == snapshot.to_runtime()


def test_column_snapshot_coerces_runtime_values() -> None:
    column = ColumnSnapshot.from_runtime(
        ("name", "str", False, False, None, None, 32, True, [("length", ">=", 2)])
    )

    assert column.foreign_table is None
    assert column.max_length == 32
    assert column.checks == [("length", ">=", "2")]


def test_migration_plan_destructive_detection_lives_with_model() -> None:
    plan = MigrationPlan([MigrationOperation("DROP TABLE flavor")])

    assert plan.has_destructive_operations
    assert plan.dry_run() == ["DROP TABLE flavor"]


def test_document_toml_helpers_reject_null_values() -> None:
    assert documents.toml_loads(documents.toml_dumps({"name": "flavor"})) == {
        "name": "flavor"
    }
    with pytest.raises(ValueError, match="TOML does not support null values"):
        documents.toml_value(None)
