from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from ormdantic import migrations
from ormdantic._migrations.models import (
    MIGRATION_STATUS_APPLIED,
    ColumnSnapshot,
    EnumTypeSnapshot,
    MigrationHistoryEntry,
    MigrationOperation,
    MigrationPlan,
    SchemaSnapshot,
    SequenceSnapshot,
    TableSnapshot,
    ViewSnapshot,
)
from ormdantic.errors import MigrationError, NativeExtensionError


class EventRecorder:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []

    async def dispatch(self, event: str, **payload: Any) -> None:
        self.calls.append((event, payload))


class FakeDatabase:
    def __init__(self, url: str = "postgresql://localhost/app") -> None:
        self._connection = url
        self._events = EventRecorder()


def scoped_snapshot() -> SchemaSnapshot:
    return SchemaSnapshot(
        tables=[
            TableSnapshot(
                "Flavor",
                "flavor",
                "id",
                columns=[ColumnSnapshot("id", "int", nullable=False, primary_key=True)],
            )
        ],
        enum_types=[EnumTypeSnapshot("flavor_kind", ["sweet", "bitter"])],
        sequences=[SequenceSnapshot("flavor_id_seq")],
        views=[ViewSnapshot("flavor_view", "SELECT id FROM flavor")],
    )


async def test_migration_manager_autogenerate_schema_rewrites_and_noop_path(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager = migrations.MigrationManager(FakeDatabase())
    before = SchemaSnapshot.empty()
    after = scoped_snapshot()
    monkeypatch.setattr(
        manager,
        "live_snapshot",
        lambda **kwargs: before,
    )
    monkeypatch.setattr(manager, "snapshot", lambda: after)

    out = tmp_path / "001_autogen.json"
    artifact = manager.autogenerate(
        "001_autogen",
        schema="inventory",
        path=out,
        skip_noop=False,
    )

    assert artifact is not None
    assert out.exists()
    assert artifact.to_snapshot.tables[0].schema == "inventory"
    assert artifact.to_snapshot.enum_types[0].schema == "inventory"
    assert artifact.to_snapshot.sequences[0].schema == "inventory"
    assert artifact.to_snapshot.views[0].schema == "inventory"

    created_path = tmp_path / "003_created.json"
    created = manager.create_migration(
        "003_created",
        before,
        after,
        path=created_path,
    )
    assert created.revision == "003_created"
    assert created_path.exists()

    monkeypatch.setattr(
        manager, "live_snapshot", lambda **kwargs: SchemaSnapshot.empty()
    )
    monkeypatch.setattr(manager, "snapshot", lambda: SchemaSnapshot.empty())
    assert manager.autogenerate("002_noop") is None


async def test_migration_manager_file_helpers_directory_dependency_and_squash(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager = migrations.MigrationManager(FakeDatabase("sqlite:///db.sqlite3"))
    artifact_path = tmp_path / "001.json"
    artifact_path.write_text("{}")
    calls: list[tuple[str, object, bool]] = []

    monkeypatch.setattr(
        migrations.MigrationArtifact,
        "read",
        lambda path: SimpleNamespace(revision="002", depends_on=["missing"]),
    )

    async def fake_apply_artifact(
        artifact: object, *, allow_destructive: bool = False
    ) -> bool:
        calls.append(("apply", artifact, allow_destructive))
        return True

    async def fake_rollback_artifact(
        artifact: object, *, allow_destructive: bool = False
    ) -> bool:
        calls.append(("rollback", artifact, allow_destructive))
        return True

    monkeypatch.setattr(manager, "apply_artifact", fake_apply_artifact)
    monkeypatch.setattr(manager, "rollback_artifact", fake_rollback_artifact)

    assert await manager.apply_file(artifact_path, allow_destructive=True)
    assert await manager.rollback_file(artifact_path, allow_destructive=True)
    assert calls[0][0] == "apply"
    assert calls[0][2] is True
    assert calls[1][0] == "rollback"

    async def is_dirty() -> bool:
        return False

    monkeypatch.setattr(manager, "is_dirty", is_dirty)

    async def applied_revisions() -> list[str]:
        return []

    monkeypatch.setattr(manager, "applied_revisions", applied_revisions)
    monkeypatch.setattr(
        migrations, "_migration_files", lambda path, pattern: [artifact_path]
    )
    with pytest.raises(ValueError, match="missing dependencies"):
        await manager.apply_directory(tmp_path)

    async def dirty_history() -> bool:
        return True

    monkeypatch.setattr(manager, "is_dirty", dirty_history)
    with pytest.raises(ValueError, match="history is dirty"):
        await manager.apply_directory(tmp_path)

    written: list[object] = []

    class FakeSquashed:
        def with_checksum(self) -> FakeSquashed:
            return self

        def write(self, path: object) -> None:
            written.append(path)

    fake_squashed = FakeSquashed()
    monkeypatch.setattr(
        migrations,
        "squash_migrations",
        lambda revision, artifacts, dialect=None: fake_squashed,
    )
    squashed_path = tmp_path / "squashed.json"

    assert (
        manager.squash("010_squashed", [artifact_path], path=squashed_path)
        is fake_squashed
    )
    assert written == [squashed_path]


async def test_migration_manager_native_history_and_rollback_error_paths(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = FakeDatabase("sqlite:///db.sqlite3")
    manager = migrations.MigrationManager(database)
    connection = object()
    calls: list[tuple[str, object, str]] = []
    monkeypatch.setattr(
        migrations,
        "_ormdantic",
        SimpleNamespace(PyNativeConnection=lambda url: connection),
    )
    monkeypatch.setattr(
        migrations,
        "_ensure_migration_history_table",
        lambda connection, dialect: calls.append(("ensure", connection, dialect)),
    )

    await manager.ensure_revision_table()
    assert calls == [("ensure", connection, "sqlite")]

    monkeypatch.setattr(migrations, "_is_dirty", lambda connection, dialect: False)
    monkeypatch.setattr(
        migrations,
        "_history_entry",
        lambda connection, dialect, revision: MigrationHistoryEntry(
            revision=revision,
            status=MIGRATION_STATUS_APPLIED,
        ),
    )
    monkeypatch.setattr(
        migrations,
        "_run_migration_operations",
        lambda **kwargs: None,
    )
    plan = MigrationPlan(
        rollback_operations=[MigrationOperation("DROP TABLE flavor")],
    )

    with pytest.raises(MigrationError, match="migration rollback failed"):
        await manager.rollback("001", plan)

    after_events = [
        payload for name, payload in database._events.calls if name == "after_migration"
    ]
    assert isinstance(after_events[-1]["error"], MigrationError)


def test_require_migration_symbol_reports_missing_native_extension(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(migrations, "_ormdantic", None)

    with pytest.raises(NativeExtensionError):
        migrations._require_migration_symbol("PyNativeConnection")
