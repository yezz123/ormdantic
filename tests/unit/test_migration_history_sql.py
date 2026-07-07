from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from ormdantic import migrations
from ormdantic._migrations import history
from ormdantic._migrations.models import MigrationHistoryEntry, MigrationOperation


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


class RaisingConnection(RecordingConnection):
    def __init__(self, *, table_exists: bool, failures: dict[str, Exception]) -> None:
        super().__init__(table_exists=table_exists)
        self.failures = failures

    def execute(self, sql: str, params: Sequence[Any]) -> dict[str, Any]:
        for marker, error in self.failures.items():
            if marker in sql:
                self.statements.append(sql)
                raise error
        return super().execute(sql, params)


class OperationConnection:
    def __init__(
        self,
        *,
        fail_operation: str | None = None,
        fail_rollback: bool = False,
        fail_release: bool = False,
        fail_commit_after: int | None = None,
    ) -> None:
        self.fail_operation = fail_operation
        self.fail_rollback = fail_rollback
        self.fail_release = fail_release
        self.fail_commit_after = fail_commit_after
        self.statements: list[str] = []
        self.begins = 0
        self.commits = 0
        self.rollbacks = 0

    def execute(self, sql: str, params: Sequence[Any]) -> dict[str, Any]:
        del params
        self.statements.append(sql)
        normalized = sql.lower()
        if any(
            marker in normalized
            for marker in (
                "sqlite_master",
                "information_schema.tables",
                "sys.tables",
                "user_tables",
            )
        ):
            return {"rows": [[1]]}
        if any(
            marker in normalized
            for marker in (
                "pragma_table_info",
                "information_schema.columns",
                "sys.columns",
                "user_tab_columns",
            )
        ):
            return {"rows": [[1]]}
        if "get_lock" in normalized:
            return {"rows": [[1]]}
        if "pg_try_advisory_lock" in normalized:
            return {"rows": [[1]]}
        if "release_lock" in normalized and self.fail_release:
            raise RuntimeError("release failed")
        if self.fail_operation is not None and self.fail_operation in sql:
            raise RuntimeError("operation failed")
        return {"rows": []}

    def begin(self) -> None:
        self.begins += 1

    def commit(self) -> None:
        self.commits += 1
        if self.fail_commit_after is not None and self.commits > self.fail_commit_after:
            raise RuntimeError("commit failed")

    def rollback(self) -> None:
        self.rollbacks += 1
        if self.fail_rollback:
            raise RuntimeError("rollback failed")


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


def test_history_table_ensure_tolerates_duplicate_create_and_add_races() -> None:
    create_race = RaisingConnection(
        table_exists=False,
        failures={"CREATE TABLE": RuntimeError("table already exists")},
    )
    migrations._ensure_migration_history_table(create_race, "sqlite")

    assert any(
        statement.startswith("CREATE TABLE") for statement in create_race.statements
    )
    assert any("ALTER TABLE" in statement for statement in create_race.statements)

    add_race = RaisingConnection(
        table_exists=True,
        failures={"ADD COLUMN": RuntimeError("duplicate column name: checksum")},
    )
    migrations._ensure_migration_history_table(add_race, "sqlite")

    assert any("ADD COLUMN" in statement for statement in add_race.statements)


def test_history_table_ensure_reraises_nonduplicate_ddl_errors() -> None:
    connection = RaisingConnection(
        table_exists=False,
        failures={"CREATE TABLE": RuntimeError("permission denied")},
    )

    try:
        migrations._ensure_migration_history_table(connection, "sqlite")
    except RuntimeError as exc:
        assert "permission denied" in str(exc)
    else:  # pragma: no cover - assertion guard
        raise AssertionError("non-duplicate create errors should propagate")


def test_history_table_ensure_reraises_nonduplicate_add_column_errors() -> None:
    connection = RaisingConnection(
        table_exists=True,
        failures={"ADD COLUMN": RuntimeError("permission denied")},
    )

    try:
        migrations._ensure_migration_history_table(connection, "sqlite")
    except RuntimeError as exc:
        assert "permission denied" in str(exc)
    else:  # pragma: no cover - assertion guard
        raise AssertionError("non-duplicate add column errors should propagate")


def test_history_existence_queries_cover_truthy_and_unknown_dialects() -> None:
    truthy = RowsConnection(rows=[["yes"]])

    assert history._migration_history_table_exists(truthy, "sqlite")
    assert history._migration_history_column_exists(truthy, "mysql", "checksum")
    assert any("sqlite_master" in statement for statement in truthy.statements)
    assert any(
        "information_schema.columns" in statement for statement in truthy.statements
    )

    unknown = RowsConnection(rows=[[1]])
    assert not history._migration_history_table_exists(unknown, "unknown")
    assert not history._migration_history_column_exists(unknown, "unknown", "checksum")
    assert unknown.statements == []

    mysql_table = RowsConnection(rows=[["true"]])
    assert history._migration_history_table_exists(mysql_table, "mysql")
    assert "information_schema.tables" in mysql_table.statements[0]


def test_history_duplicate_error_predicates_and_column_sql_edges() -> None:
    assert history._is_duplicate_table_error(
        RuntimeError("ORA-00955: name is already used by an existing object")
    )
    assert history._is_duplicate_table_error(
        RuntimeError("there is already an object named flavors")
    )
    assert not history._is_duplicate_table_error(RuntimeError("permission denied"))
    assert history._is_duplicate_column_error(
        RuntimeError("column names in each table must be unique")
    )
    assert history._is_duplicate_column_error(
        RuntimeError("column flavor specified more than once")
    )
    assert not history._is_duplicate_column_error(RuntimeError("syntax error"))
    assert (
        history._add_migration_history_column_sql(
            "sqlite",
            '"ormdantic_migrations"',
            "status",
            "TEXT",
            "'applied'",
        )
        == 'ALTER TABLE "ormdantic_migrations" ADD COLUMN "status" TEXT DEFAULT \'applied\''
    )


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


def test_oracle_history_entries_cover_empty_and_invalid_metadata_defaults() -> None:
    assert history._history_entries(RowsConnection(), "oracle") == []

    class OracleInvalidMetadataConnection:
        def execute(self, sql: str, params: Sequence[Any]) -> dict[str, Any]:
            del params
            if sql.startswith('SELECT "revision" FROM'):
                return {"rows": [["001"]]}
            if '"metadata"' in sql:
                return {"rows": [["001", "not-json"]]}
            if '"dirty"' in sql:
                return {"rows": [["001", "0"]]}
            return {"rows": []}

    entries = history._history_entries(OracleInvalidMetadataConnection(), "oracle")

    assert entries[0].metadata == {"raw": "not-json"}
    assert entries[0].status == "applied"
    assert entries[0].dirty is False


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


def test_current_entry_and_repair_revision_filters_cover_empty_results() -> None:
    connection = RowsConnection(
        rows=[
            [
                "001",
                None,
                None,
                None,
                None,
                "failed",
                1,
                None,
                None,
                None,
            ]
        ]
    )

    assert history._current_entry(connection, "sqlite") is None
    assert (
        history._repair_history(
            connection,
            "sqlite",
            revision="missing",
            status="applied",
            clear_dirty=True,
            checksum="fixed",
        )
        == 0
    )


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


def test_acquire_migration_lock_success_and_failure_paths() -> None:
    postgres = RowsConnection(rows=[[1]])
    assert history._acquire_migration_lock(postgres, "postgresql") == (
        "SELECT pg_advisory_unlock(hashtext('ormdantic_migration_lock'))"
    )
    assert "pg_try_advisory_lock" in postgres.statements[0]

    mysql = RowsConnection(rows=[[0]])
    try:
        history._acquire_migration_lock(mysql, "mysql")
    except ValueError as exc:
        assert "failed to acquire mysql migration lock" in str(exc)
    else:  # pragma: no cover - assertion guard
        raise AssertionError("failed mysql locks should raise")

    postgres_failed = RowsConnection(rows=[[0]])
    try:
        history._acquire_migration_lock(postgres_failed, "postgresql")
    except ValueError as exc:
        assert "failed to acquire postgres" in str(exc)
    else:  # pragma: no cover - assertion guard
        raise AssertionError("failed postgres locks should raise")

    mysql_success = RowsConnection(rows=[[1]])
    assert history._acquire_migration_lock(mysql_success, "mysql") == (
        "SELECT RELEASE_LOCK('ormdantic:migration:lock')"
    )

    mssql = RowsConnection()
    assert history._acquire_migration_lock(mssql, "mssql") == (
        "EXEC sp_releaseapplock @Resource = 'ormdantic_migration_lock', "
        "@LockOwner = 'Session'"
    )
    assert mssql.statements[0].startswith("EXEC sp_getapplock")
    assert history._acquire_migration_lock(RowsConnection(), "sqlite") is None


def test_run_migration_operations_commits_success_and_records_failed_sqlite() -> None:
    success = OperationConnection()
    history._run_migration_operations(
        connection=success,
        dialect="sqlite",
        revision="001",
        operations=[MigrationOperation("CREATE TABLE flavor (id TEXT)")],
        status="applied",
        description="create flavor",
        checksum="abc123",
        artifact_version=2,
        metadata={"source": "test"},
    )

    assert "BEGIN IMMEDIATE" in success.statements
    assert success.commits == 1
    assert any("CREATE TABLE flavor" in statement for statement in success.statements)
    assert any('"phase": "completed"' in statement for statement in success.statements)

    postgres_success = OperationConnection()
    history._run_migration_operations(
        connection=postgres_success,
        dialect="postgresql",
        revision="001_pg",
        operations=[MigrationOperation("ALTER TABLE flavor ADD name TEXT")],
        status="applied",
        description="alter flavor",
        checksum=None,
        artifact_version=2,
        metadata={},
    )
    assert postgres_success.begins == 1
    assert postgres_success.commits == 1

    failure = OperationConnection(
        fail_operation="ALTER TABLE flavor",
        fail_rollback=True,
    )
    try:
        history._run_migration_operations(
            connection=failure,
            dialect="sqlite",
            revision="002",
            operations=[MigrationOperation("ALTER TABLE flavor ADD bad TEXT")],
            status="applied",
            description=None,
            checksum=None,
            artifact_version=2,
            metadata={},
        )
    except RuntimeError as exc:
        assert "operation failed" in str(exc)
    else:  # pragma: no cover - assertion guard
        raise AssertionError("failing migration operation should propagate")

    assert failure.rollbacks == 1
    assert any('"phase": "failed"' in statement for statement in failure.statements)


def test_run_migration_operations_ignores_release_and_failed_history_commit() -> None:
    release_failure = OperationConnection(fail_release=True)
    history._run_migration_operations(
        connection=release_failure,
        dialect="mysql",
        revision="003",
        operations=[MigrationOperation("ALTER TABLE flavor ADD name TEXT")],
        status="applied",
        description=None,
        checksum=None,
        artifact_version=2,
        metadata={},
    )
    assert any("RELEASE_LOCK" in statement for statement in release_failure.statements)

    commit_failure = OperationConnection(
        fail_operation="ALTER TABLE flavor",
        fail_commit_after=1,
    )
    try:
        history._run_migration_operations(
            connection=commit_failure,
            dialect="oracle",
            revision="004",
            operations=[MigrationOperation("ALTER TABLE flavor ADD bad VARCHAR2(20)")],
            status="applied",
            description=None,
            checksum=None,
            artifact_version=2,
            metadata={},
        )
    except RuntimeError as exc:
        assert "operation failed" in str(exc)
    else:  # pragma: no cover - assertion guard
        raise AssertionError("failing oracle migration should propagate")

    assert commit_failure.commits == 2
