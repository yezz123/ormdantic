from __future__ import annotations

from pathlib import Path

import pytest

from ormdantic import Ormdantic
from ormdantic.migrations import MigrationArtifact
from ormdantic.playground.config import load_config

EXAMPLE_ROOT = Path(__file__).resolve().parents[1]
REVISIONS = [
    "0001_create_projects_and_todos",
    "0002_add_todo_due_date",
]


@pytest.mark.parametrize("dialect", ["sqlite", "postgresql"])
def test_dialect_migration_chain_is_contiguous_and_checksummed(dialect: str) -> None:
    directory = EXAMPLE_ROOT / "migrations" / dialect
    paths = sorted(directory.glob("*.toml"))

    assert [path.stem for path in paths] == REVISIONS
    artifacts = [MigrationArtifact.read(path) for path in paths]
    assert [artifact.revision for artifact in artifacts] == REVISIONS
    assert [artifact.description for artifact in artifacts] == [
        "Create projects and todos",
        "Add an optional due date to todos",
    ]
    assert artifacts[0].dialect == dialect
    assert artifacts[0].depends_on == []
    assert artifacts[1].dialect == dialect
    assert artifacts[1].depends_on == [REVISIONS[0]]
    assert artifacts[0].to_snapshot.to_dict() == artifacts[1].from_snapshot.to_dict()
    assert any("due_at" in operation.sql for operation in artifacts[1].operations)
    assert artifacts[1].rollback_operations
    if dialect == "postgresql":
        assert any(
            "due_at" in operation.sql for operation in artifacts[1].rollback_operations
        )
    for artifact in artifacts:
        artifact.validate_checksum()


async def test_sqlite_chain_applies_rolls_back_and_reapplies(tmp_path: Path) -> None:
    manager = Ormdantic(f"sqlite:///{tmp_path / 'todo.sqlite3'}").migrations
    directory = EXAMPLE_ROOT / "migrations" / "sqlite"
    second = directory / f"{REVISIONS[1]}.toml"

    assert await manager.apply_directory(directory) == REVISIONS
    assert await manager.apply_directory(directory) == []
    assert await manager.applied_revisions() == REVISIONS
    assert (await manager.current()).revision == REVISIONS[1]  # type: ignore[union-attr]

    assert await manager.rollback_file(second, allow_destructive=True) is True
    assert await manager.applied_revisions() == [REVISIONS[0]]
    assert (await manager.current()).revision == REVISIONS[0]  # type: ignore[union-attr]

    assert await manager.apply_directory(directory) == [REVISIONS[1]]
    assert await manager.is_dirty() is False


def test_migration_source_tampering_is_rejected(tmp_path: Path) -> None:
    source = (
        EXAMPLE_ROOT / "migrations" / "sqlite" / f"{REVISIONS[0]}.toml"
    ).read_text()
    tampered = tmp_path / "tampered.toml"
    tampered.write_text(source.replace("Create projects", "Drop projects", 1))

    with pytest.raises(ValueError, match="checksum mismatch"):
        MigrationArtifact.read(tampered)


def test_playground_configuration_is_strict_and_environment_driven() -> None:
    path = EXAMPLE_ROOT / "ormdantic.toml"
    source = path.read_text()
    development = load_config(path, environment="development")
    production = load_config(path, environment="production")

    assert 'target = "app.database:db"' in source
    assert 'migrations_dir = "migrations/sqlite"' in source
    assert 'url_env = "DATABASE_URL"' in source
    assert 'safety = "typed"' in source
    assert development.environment.safety == "typed"
    assert production.environment.safety == "typed"
