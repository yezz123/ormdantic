from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from ormdantic import migrations


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
