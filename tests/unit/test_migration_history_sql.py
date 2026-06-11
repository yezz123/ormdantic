from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from ormdantic import migrations
from ormdantic._migrations import history
from ormdantic._migrations.models import MigrationHistoryEntry


class RecordingConnection:
    def __init__(
        self,
        *,
        table_exists: bool,
        existing_columns: set[str] | None = None,
    ) -> None:
        self.table_exists = table_exists
        self.existing_columns = existing_columns or set()
        self.statements: list[str] = []

    def execute(self, sql: str, params: Sequence[Any]) -> dict[str, Any]:
        del params
        self.statements.append(sql)
        if self._is_table_exists_query(sql):
            return {"rows": [[1 if self.table_exists else 0]]}
        column_name = self._column_exists_query(sql)
        if column_name is not None:
            return {"rows": [[1 if column_name in self.existing_columns else 0]]}
        return {"rows": []}

    @staticmethod
    def _is_table_exists_query(sql: str) -> bool:
        normalized = sql.lower()
        return any(
            marker in normalized
            for marker in (
                "sqlite_master",
                "information_schema.tables",
                "sys.tables",
                "user_tables",
            )
        )

    @staticmethod
    def _column_exists_query(sql: str) -> str | None:
        normalized = sql.lower()
        if "information_schema.columns" not in normalized and not any(
            marker in normalized for marker in ("sys.columns", "user_tab_columns")
        ):
            return None
        marker = "column_name = '"
        if marker in normalized:
            return normalized.split(marker, 1)[1].split("'", 1)[0]
        marker = "name = '"
        if marker in normalized:
            return normalized.split(marker, 1)[1].split("'", 1)[0]
        return None


class RowsConnection:
    def __init__(self, rows: list[list[Any]] | None = None) -> None:
        self.rows = rows or []
        self.statements: list[str] = []

    def execute(self, sql: str, params: Sequence[Any]) -> dict[str, Any]:
        del params
        self.statements.append(sql)
        if sql.startswith("SELECT"):
            return {"rows": self.rows}
        return {"rows": []}


def test_public_migration_facade_re_exports_history_helpers() -> None:
    assert migrations.MIGRATION_TABLE == history.MIGRATION_TABLE
    assert migrations.MIGRATION_LOCK_NAME == history.MIGRATION_LOCK_NAME
    assert (
        migrations._migration_table_column_defs is history._migration_table_column_defs
    )
    assert (
        migrations._ensure_migration_history_table
        is history._ensure_migration_history_table
    )


def test_history_column_types_are_key_safe_for_mysql_family() -> None:
    mysql_columns = {
        name: column_type
        for name, column_type, _ in migrations._migration_table_column_defs("mysql")
    }
    mariadb_columns = {
        name: column_type
        for name, column_type, _ in migrations._migration_table_column_defs("mariadb")
    }

    assert mysql_columns["revision"] == "VARCHAR(255) PRIMARY KEY"
    assert mariadb_columns["revision"] == "VARCHAR(255) PRIMARY KEY"
    assert mysql_columns["metadata"] == "TEXT"


def test_history_column_types_are_key_safe_for_enterprise_databases() -> None:
    mssql_columns = {
        name: column_type
        for name, column_type, _ in migrations._migration_table_column_defs("mssql")
    }
    oracle_columns = {
        name: column_type
        for name, column_type, _ in migrations._migration_table_column_defs("oracle")
    }

    assert mssql_columns["revision"] == "NVARCHAR(255) PRIMARY KEY"
    assert mssql_columns["metadata"] == "NVARCHAR(MAX)"
    assert oracle_columns["revision"] == "VARCHAR2(255) PRIMARY KEY"
    assert oracle_columns["metadata"] == "VARCHAR2(4000)"


def test_history_table_create_for_mssql_avoids_if_not_exists() -> None:
    connection = RecordingConnection(table_exists=False)

    migrations._ensure_migration_history_table(connection, "mssql")

    create_table = next(
        statement
        for statement in connection.statements
        if statement.startswith("CREATE TABLE")
    )
    assert "IF NOT EXISTS" not in create_table
    assert "[revision] NVARCHAR(255) PRIMARY KEY" in create_table
    assert not any("ADD COLUMN" in statement for statement in connection.statements)


def test_history_table_ensure_skips_existing_columns_for_postgresql() -> None:
    existing_columns = {
        name for name, _, _ in migrations._migration_table_column_defs("postgresql")
    }
    connection = RecordingConnection(
        table_exists=True,
        existing_columns=existing_columns,
    )

    migrations._ensure_migration_history_table(connection, "postgresql")

    assert not any("ALTER TABLE" in statement for statement in connection.statements)


def test_history_table_upgrade_uses_mssql_add_syntax() -> None:
    connection = RecordingConnection(table_exists=True, existing_columns={"revision"})

    migrations._ensure_migration_history_table(connection, "mssql")

    add_statements = [
        statement for statement in connection.statements if "ALTER TABLE" in statement
    ]
    assert add_statements
    assert all(" ADD [" in statement for statement in add_statements)
    assert not any("ADD COLUMN" in statement for statement in add_statements)


def test_history_table_upgrade_uses_oracle_add_syntax() -> None:
    connection = RecordingConnection(table_exists=True, existing_columns={"revision"})

    migrations._ensure_migration_history_table(connection, "oracle")

    add_statements = [
        statement for statement in connection.statements if "ALTER TABLE" in statement
    ]
    assert add_statements
    assert all(" ADD (" in statement for statement in add_statements)


def test_history_entries_parse_metadata_and_dirty_state() -> None:
    connection = RowsConnection(
        rows=[
            [
                "001",
                "create flavor",
                "abc123",
                "2026-01-01T00:00:00+00:00",
                "12",
                "applied",
                0,
                2,
                "2.0.0",
                '{"phase": "completed"}',
            ],
            ["002", None, None, None, None, "failed", 1, None, None, "not-json"],
        ]
    )

    entries = history._history_entries(connection, "sqlite")

    assert entries[0].metadata == {"phase": "completed"}
    assert not entries[0].dirty
    assert entries[0].execution_time_ms == 12
    assert entries[1].metadata == {"raw": "not-json"}
    assert entries[1].dirty
    assert history._current_entry(connection, "sqlite") == entries[0]
    assert history._is_dirty(connection, "sqlite")


def test_history_entries_reads_oracle_columns_separately() -> None:
    class OracleRowsConnection:
        def __init__(self) -> None:
            self.statements: list[str] = []

        def execute(self, sql: str, params: Sequence[Any]) -> dict[str, Any]:
            del params
            self.statements.append(sql)
            if sql.startswith('SELECT "revision" FROM'):
                return {"rows": [["001"]]}
            values = {
                "description": "create flavor",
                "checksum": "abc123",
                "applied_at": "2026-01-01T00:00:00+00:00",
                "execution_time_ms": "12",
                "status": "applied",
                "dirty": 0,
                "artifact_version": 2,
                "ormdantic_version": "2.0.0",
                "metadata": '{"phase": "completed"}',
            }
            for column, value in values.items():
                if f'"{column}"' in sql:
                    return {"rows": [["001", value]]}
            return {"rows": []}

    connection = OracleRowsConnection()

    entries = history._history_entries(connection, "oracle")

    assert len(connection.statements) == 10
    assert connection.statements[0].startswith('SELECT "revision" FROM')
    assert any(
        'SELECT "revision", "metadata"' in statement
        for statement in connection.statements
    )
    assert any(
        'SELECT "revision", "ormdantic_version"' in statement
        for statement in connection.statements
    )
    assert entries[0].metadata == {"phase": "completed"}
    assert entries[0].artifact_version == 2
    assert entries[0].ormdantic_version == "2.0.0"


def test_history_entries_paginates_oracle_revisions_before_column_reads() -> None:
    class OraclePagedRowsConnection:
        def __init__(self) -> None:
            self.statements: list[str] = []

        def execute(self, sql: str, params: Sequence[Any]) -> dict[str, Any]:
            del params
            self.statements.append(sql)
            if sql.startswith('SELECT "revision" FROM'):
                if "OFFSET 0 ROWS" in sql:
                    return {"rows": [[f"{index:03d}"] for index in range(50)]}
                if "OFFSET 50 ROWS" in sql:
                    return {"rows": [["050"]]}
                return {"rows": []}
            if '"checksum"' not in sql:
                return {"rows": []}
            rows: list[list[str]] = []
            if "'000'" in sql:
                rows.append(["000", "checksum-000"])
            if "'050'" in sql:
                rows.append(["050", "checksum-050"])
            return {"rows": rows}

    connection = OraclePagedRowsConnection()

    entries = history._history_entries(connection, "oracle")

    checksum_statements = [
        statement for statement in connection.statements if '"checksum"' in statement
    ]
    assert len(entries) == 51
    assert entries[0].checksum == "checksum-000"
    assert entries[-1].checksum == "checksum-050"
    assert any("OFFSET 50 ROWS" in statement for statement in connection.statements)
    assert len(checksum_statements) == 2
    assert all(" WHERE " in statement for statement in checksum_statements)


def test_repair_history_skips_unchanged_entries() -> None:
    connection = RowsConnection(
        rows=[
            [
                "001",
                "create flavor",
                "abc123",
                "2026-01-01T00:00:00+00:00",
                "12",
                "applied",
                0,
                2,
                "2.0.0",
                '{"phase": "completed"}',
            ],
            [
                "002",
                "failed flavor",
                "def456",
                "2026-01-01T00:00:01+00:00",
                "8",
                "failed",
                1,
                2,
                "2.0.0",
                '{"phase": "failed"}',
            ],
        ]
    )

    repaired = history._repair_history(
        connection,
        "sqlite",
        revision=None,
        status=None,
        clear_dirty=True,
        checksum=None,
    )

    delete_statements = [
        statement
        for statement in connection.statements
        if statement.startswith("DELETE")
    ]
    insert_statements = [
        statement
        for statement in connection.statements
        if statement.startswith("INSERT")
    ]
    assert repaired == 1
    assert len(delete_statements) == 1
    assert len(insert_statements) == 1
    assert "002" in delete_statements[0]


def test_write_history_entry_serializes_metadata() -> None:
    connection = RowsConnection()

    history._write_history_entry(
        connection,
        "sqlite",
        MigrationHistoryEntry(
            revision="001",
            description="create flavor",
            checksum="abc123",
            applied_at="2026-01-01T00:00:00+00:00",
            execution_time_ms=12,
            metadata={"phase": "completed"},
        ),
    )

    assert connection.statements[0] == (
        'DELETE FROM "ormdantic_migrations" WHERE "revision" = \'001\''
    )
    assert connection.statements[1].startswith('INSERT INTO "ormdantic_migrations"')
    assert '"metadata"' in connection.statements[1]
    assert '"phase": "completed"' in connection.statements[1]
