import json
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID

import pytest
from pydantic import ValidationError

from ormdantic import Ormdantic

ROOT = Path(__file__).resolve().parents[3]
PROJECT_ID = "123e4567-e89b-12d3-a456-426614174000"


def _run_fresh_todo_script(script: str, database_url: str) -> object:
    environment = os.environ.copy()
    environment.update(
        {
            "APP_ENV": "development",
            "DATABASE_URL": database_url,
        }
    )
    completed = subprocess.run(
        [sys.executable, "-c", script],
        cwd=ROOT,
        env=environment,
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(completed.stdout)


def _fresh_schema_payload(database_url: str, dialect: str) -> dict[str, object]:
    payload = _run_fresh_todo_script(
        f"""
import json
from examples.todo_app.app.database import db
from ormdantic.migrations import SchemaSnapshot, create_migration_artifact

snapshot = db.migrations.snapshot()
artifact = create_migration_artifact(
    "initial-{dialect}",
    SchemaSnapshot.empty(),
    snapshot,
    dialect={dialect!r},
)
print(json.dumps({{
    "snapshot": snapshot.to_dict(),
    "sql": [operation.sql for operation in artifact.operations],
}}))
""",
        database_url,
    )
    assert isinstance(payload, dict)
    return payload


def test_clean_database_import_registers_all_models() -> None:
    tables = _run_fresh_todo_script(
        """
import json
from examples.todo_app.app.database import db

print(json.dumps(sorted(table.name for table in db.migrations.snapshot().tables)))
""",
        "sqlite:///:memory:",
    )

    assert tables == ["project", "todo"]


def test_clean_models_import_is_circular_import_safe() -> None:
    tables = _run_fresh_todo_script(
        """
import json
from examples.todo_app.app.models import Project, Todo
from examples.todo_app.app.database import db

assert Project.__name__ == "Project"
assert Todo.__name__ == "Todo"
print(json.dumps(sorted(table.name for table in db.migrations.snapshot().tables)))
""",
        "sqlite:///:memory:",
    )

    assert tables == ["project", "todo"]


def test_uuid_defaults_are_valid_and_distinct(
    todo_app_modules: SimpleNamespace,
) -> None:
    project_one = todo_app_modules.models.Project(name="One")
    project_two = todo_app_modules.models.Project(name="Two")
    todo_one = todo_app_modules.models.Todo(project=project_one, title="One")
    todo_two = todo_app_modules.models.Todo(project=project_one, title="Two")

    identifiers = {project_one.id, project_two.id, todo_one.id, todo_two.id}
    assert len(identifiers) == 4
    assert all(str(UUID(identifier)) == identifier for identifier in identifiers)


def test_default_timestamps_are_utc_aware(
    todo_app_modules: SimpleNamespace,
) -> None:
    project = todo_app_modules.models.Project(name="Example")
    todo = todo_app_modules.models.Todo(project=project, title="Example")

    assert todo_app_modules.models.utc_now().utcoffset() == timedelta(0)
    assert project.created_at.utcoffset() == timedelta(0)
    assert todo.created_at.utcoffset() == timedelta(0)
    assert todo.updated_at.utcoffset() == timedelta(0)


def test_due_at_is_normalized_to_utc_and_rejects_naive_values(
    todo_app_modules: SimpleNamespace,
) -> None:
    offset_due_at = datetime(
        2030,
        1,
        2,
        15,
        tzinfo=timezone(timedelta(hours=1)),
    )

    todo = todo_app_modules.models.Todo(
        project=PROJECT_ID,
        title="Example",
        due_at=offset_due_at,
    )

    assert todo.due_at == datetime(2030, 1, 2, 14, tzinfo=timezone.utc)
    assert todo.due_at.tzinfo is timezone.utc
    with pytest.raises(ValidationError, match="timezone-aware"):
        todo_app_modules.models.Todo(
            project=PROJECT_ID,
            title="Example",
            due_at=datetime(2030, 1, 2, 14),
        )


def test_project_name_and_todo_title_are_trimmed(
    todo_app_modules: SimpleNamespace,
) -> None:
    project = todo_app_modules.models.Project(name="  Launch  ")
    todo = todo_app_modules.models.Todo(project=project, title="  Ship it  ")

    assert project.name == "Launch"
    assert todo.title == "Ship it"


@pytest.mark.parametrize("name", ["", "   ", "x" * 121])
def test_project_rejects_invalid_names(
    todo_app_modules: SimpleNamespace,
    name: str,
) -> None:
    with pytest.raises(ValidationError):
        todo_app_modules.models.Project(name=name)


@pytest.mark.parametrize("title", ["", "   ", "x" * 201])
def test_todo_rejects_invalid_titles(
    todo_app_modules: SimpleNamespace,
    title: str,
) -> None:
    with pytest.raises(ValidationError):
        todo_app_modules.models.Todo(project=PROJECT_ID, title=title)


@pytest.mark.parametrize("priority", [0, 6])
def test_todo_rejects_priorities_outside_one_to_five(
    todo_app_modules: SimpleNamespace,
    priority: int,
) -> None:
    with pytest.raises(ValidationError):
        todo_app_modules.models.Todo(
            project=PROJECT_ID,
            title="Example",
            priority=priority,
        )


def test_todo_status_defaults_and_validation(
    todo_app_modules: SimpleNamespace,
) -> None:
    default_todo = todo_app_modules.models.Todo(
        project=PROJECT_ID,
        title="Default",
    )
    completed_todo = todo_app_modules.models.Todo(
        project=PROJECT_ID,
        title="Completed",
        status="completed",
    )

    assert default_todo.status is todo_app_modules.models.TodoStatus.pending
    assert completed_todo.status is todo_app_modules.models.TodoStatus.completed
    assert [status.value for status in todo_app_modules.models.TodoStatus] == [
        "pending",
        "in_progress",
        "completed",
    ]
    with pytest.raises(ValidationError):
        todo_app_modules.models.Todo(
            project=PROJECT_ID,
            title="Invalid",
            status="blocked",
        )


def test_explicit_identifiers_must_be_valid_uuids(
    todo_app_modules: SimpleNamespace,
) -> None:
    with pytest.raises(ValidationError, match="UUID"):
        todo_app_modules.models.Project(id="not-a-uuid", name="Invalid")
    with pytest.raises(ValidationError, match="UUID"):
        todo_app_modules.models.Todo(
            id="not-a-uuid",
            project=PROJECT_ID,
            title="Invalid id",
        )
    with pytest.raises(ValidationError, match="UUID"):
        todo_app_modules.models.Todo(
            project="not-a-uuid",
            title="Invalid project",
        )


def test_explicit_identifier_strings_are_canonicalized(
    todo_app_modules: SimpleNamespace,
) -> None:
    noncanonical = "{123E4567-E89B-12D3-A456-426614174000}"
    canonical = str(UUID(noncanonical))

    project = todo_app_modules.models.Project(id=noncanonical, name="Canonical")
    todo = todo_app_modules.models.Todo(
        id=noncanonical,
        project=noncanonical,
        title="Canonical",
    )

    assert project.id == canonical
    assert todo.id == canonical
    assert todo.project == canonical


def test_project_rejects_naive_created_at(
    todo_app_modules: SimpleNamespace,
) -> None:
    with pytest.raises(ValidationError, match="timezone-aware"):
        todo_app_modules.models.Project(
            name="Naive",
            created_at=datetime(2030, 1, 2, 14),
        )


@pytest.mark.parametrize("field_name", ["created_at", "updated_at", "due_at"])
def test_todo_rejects_naive_timestamps(
    todo_app_modules: SimpleNamespace,
    field_name: str,
) -> None:
    with pytest.raises(ValidationError, match="timezone-aware"):
        todo_app_modules.models.Todo(
            project=PROJECT_ID,
            title="Naive",
            **{field_name: datetime(2030, 1, 2, 14)},
        )


def test_persisted_timestamps_are_normalized_to_utc(
    todo_app_modules: SimpleNamespace,
) -> None:
    offset_time = datetime(
        2030,
        1,
        2,
        15,
        tzinfo=timezone(timedelta(hours=2)),
    )
    expected = datetime(2030, 1, 2, 13, tzinfo=timezone.utc)

    project = todo_app_modules.models.Project(
        name="UTC",
        created_at=offset_time,
    )
    todo = todo_app_modules.models.Todo(
        project=project,
        title="UTC",
        created_at=offset_time,
        updated_at=offset_time,
        due_at=offset_time,
    )

    assert project.created_at == expected
    assert todo.created_at == expected
    assert todo.updated_at == expected
    assert todo.due_at == expected
    assert project.created_at.tzinfo is timezone.utc
    assert todo.created_at.tzinfo is timezone.utc
    assert todo.updated_at.tzinfo is timezone.utc
    assert todo.due_at.tzinfo is timezone.utc


def test_project_and_todo_are_registered_on_module_database(
    todo_app_modules: SimpleNamespace,
) -> None:
    models = todo_app_modules.models
    database = todo_app_modules.database
    registered = {
        table.model_key: table.name
        for table in database.db.migrations.snapshot().tables
    }

    assert isinstance(database.db, Ormdantic)
    assert database.settings.database_url == "sqlite:///:memory:"
    assert registered[models.Project.__name__] == "project"
    assert registered[models.Todo.__name__] == "todo"


def test_snapshot_has_primary_keys_and_expected_indexes(
    todo_app_modules: SimpleNamespace,
) -> None:
    snapshot = todo_app_modules.database.db.migrations.snapshot()
    tables = {table.name: table for table in snapshot.tables}

    assert set(tables) == {"project", "todo"}
    assert tables["project"].primary_key == "id"
    assert tables["todo"].primary_key == "id"
    assert [index.columns for index in tables["project"].indexes] == [["name"]]
    assert [index.columns for index in tables["todo"].indexes] == [["title"]]


def test_snapshot_has_exact_named_project_relationship_foreign_key(
    todo_app_modules: SimpleNamespace,
) -> None:
    snapshot = todo_app_modules.database.db.migrations.snapshot()
    todo = next(table for table in snapshot.tables if table.name == "todo")
    project = next(column for column in todo.columns if column.name == "project")

    assert todo.foreign_key_constraints == []
    assert project.foreign_table == "project"
    assert project.foreign_column == "id"
    assert project.foreign_key_name == "todo_project_fk"
    assert project.on_delete == "cascade"


@pytest.mark.parametrize(
    ("dialect", "database_url"),
    [
        ("sqlite", "sqlite:///:memory:"),
        ("postgresql", "postgresql://todo:todo@localhost/todo"),
    ],
)
def test_todo_ddl_has_one_named_cascading_project_foreign_key(
    dialect: str,
    database_url: str,
) -> None:
    payload = _fresh_schema_payload(database_url, dialect)
    operations = payload["sql"]
    assert isinstance(operations, list)
    sql = " ".join("\n".join(str(operation) for operation in operations).split())

    assert sql.upper().count("FOREIGN KEY") == 1
    assert (
        'CONSTRAINT "todo_project_fk" FOREIGN KEY ("project") '
        'REFERENCES "project" ("id") ON DELETE CASCADE'
    ) in sql


def test_postgresql_module_state_uses_native_todo_status_enum() -> None:
    payload = _fresh_schema_payload(
        "postgresql://todo:todo@localhost/todo",
        "postgresql",
    )
    snapshot = payload["snapshot"]
    operations = payload["sql"]
    assert isinstance(snapshot, dict)
    assert isinstance(operations, list)
    tables = snapshot["tables"]
    assert isinstance(tables, list)
    todo = next(
        table
        for table in tables
        if isinstance(table, dict) and table.get("name") == "todo"
    )
    columns = todo["columns"]
    assert isinstance(columns, list)
    status = next(
        column
        for column in columns
        if isinstance(column, dict) and column.get("name") == "status"
    )
    sql = " ".join("\n".join(str(operation) for operation in operations).split())

    assert snapshot["enum_types"] == [
        {
            "name": "todo_status",
            "values": ["pending", "in_progress", "completed"],
        }
    ]
    assert status["kind"] == "enum:todo_status"
    assert 'CREATE TYPE "todo_status" AS ENUM' in sql
    assert sql.upper().count("FOREIGN KEY") == 1


def test_sqlite_module_state_uses_portable_enum_metadata() -> None:
    payload = _fresh_schema_payload("sqlite:///:memory:", "sqlite")
    snapshot = payload["snapshot"]
    operations = payload["sql"]
    assert isinstance(snapshot, dict)
    assert isinstance(operations, list)
    tables = snapshot["tables"]
    assert isinstance(tables, list)
    todo = next(
        table
        for table in tables
        if isinstance(table, dict) and table.get("name") == "todo"
    )
    columns = todo["columns"]
    assert isinstance(columns, list)
    status = next(
        column
        for column in columns
        if isinstance(column, dict) and column.get("name") == "status"
    )
    sql = " ".join("\n".join(str(operation) for operation in operations).split())

    assert snapshot.get("enum_types", []) == []
    assert status["kind"] == "enum"
    assert "CREATE TYPE" not in sql.upper()
    assert '"status" TEXT' in sql
    assert sql.upper().count("FOREIGN KEY") == 1


def test_model_dump_preserves_supported_relationship_shapes(
    todo_app_modules: SimpleNamespace,
) -> None:
    project = todo_app_modules.models.Project(name="Example")
    nested_todo = todo_app_modules.models.Todo(project=project, title="Nested")
    shallow_todo = todo_app_modules.models.Todo(project=project.id, title="Shallow")

    assert nested_todo.model_dump()["project"] == project.model_dump()
    assert shallow_todo.model_dump()["project"] == project.id
