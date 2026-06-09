from __future__ import annotations

from ormdantic._migrations import sql


def test_dialect_name_normalizes_urls_and_aliases() -> None:
    assert sql.dialect_name("postgresql://user:pass@localhost/db") == "postgresql"
    assert sql.dialect_name("postgres://localhost/db") == "postgresql"
    assert sql.dialect_name("sqlite+aiosqlite:///db.sqlite3") == "sqlite"
    assert sql.dialect_name("sqlserver") == "mssql"


def test_quote_ident_uses_backend_style() -> None:
    assert sql.quote_ident("postgresql", 'flavor"name') == '"flavor""name"'
    assert sql.quote_ident("mysql", "flavor`name") == "`flavor``name`"
    assert sql.quote_ident("mssql", "flavor]name") == "[flavor]]name]"


def test_sql_literal_serializes_basic_values() -> None:
    assert sql.sql_literal(None) == "NULL"
    assert sql.sql_literal(True) == "1"
    assert sql.sql_literal("vanilla's") == "'vanilla''s'"
    assert sql.sql_literal({"b": 2, "a": 1}) == '\'{"a": 1, "b": 2}\''


def test_table_filter_and_destructive_detection() -> None:
    assert sql.table_matches_filters("flavor", ["f*"], None)
    assert not sql.table_matches_filters("legacy_flavor", None, ["legacy_*"])
    assert sql.operation_looks_destructive("DROP TABLE flavor")
    assert sql.operation_looks_destructive("ALTER TABLE flavor DROP COLUMN code")
    assert not sql.operation_looks_destructive(
        "ALTER TABLE flavor ADD COLUMN code TEXT"
    )


def test_document_format_defaults_and_validates() -> None:
    assert sql.document_format("migration.toml") == "toml"
    assert sql.document_format("migration") == "json"
    assert sql.document_format("migration.any", "json") == "json"
