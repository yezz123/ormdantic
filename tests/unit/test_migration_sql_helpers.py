from __future__ import annotations

import sys
from types import SimpleNamespace

from ormdantic._migrations import artifacts, documents, sql
from ormdantic._migrations.models import SchemaSnapshot


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
    assert sql.sql_literal([2, 1]) == "'[2, 1]'"
    assert sql.sql_literal(1.5) == "1.5"


def test_table_filter_and_destructive_detection() -> None:
    assert sql.table_matches_filters("flavor", ["f*"], None)
    assert not sql.table_matches_filters("legacy_flavor", None, ["legacy_*"])
    assert sql.operation_looks_destructive("DROP TABLE flavor")
    assert sql.operation_looks_destructive("ALTER TABLE flavor DROP COLUMN code")
    assert sql.operation_looks_destructive("DELETE FROM flavor")
    assert sql.operation_looks_destructive("TRUNCATE TABLE flavor")
    assert not sql.operation_looks_destructive(
        "ALTER TABLE flavor ADD COLUMN code TEXT"
    )
    assert not sql.table_matches_filters("coffee", ["flavor*"], None)


def test_document_format_defaults_and_validates() -> None:
    assert sql.document_format("migration.toml") == "toml"
    assert sql.document_format("migration") == "json"
    assert sql.document_format("migration.any", "json") == "json"
    assert sql.document_format("migration.any", ".toml") == "toml"
    try:
        sql.document_format("migration.yaml")
    except ValueError as exc:
        assert "unsupported" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("expected unsupported format")


def test_query_helpers_normalize_result_shapes() -> None:
    class FakeConnection:
        def __init__(self, result):
            self.result = result

        def execute(self, sql_text, params):
            assert sql_text == "SELECT 1"
            assert params == ["id"]
            return self.result

    assert sql.query_rows(
        FakeConnection({"rows": [(1,), ["two"], object()]}), "SELECT 1", ["id"]
    ) == [
        [1],
        ["two"],
    ]
    assert sql.query_rows(FakeConnection(object()), "SELECT 1", ["id"]) == []
    assert sql.query_rows(FakeConnection({"rows": object()}), "SELECT 1", ["id"]) == []
    assert sql.query_scalar(FakeConnection({"rows": []}), "SELECT 1", ["id"]) is None
    assert sql.query_scalar(FakeConnection({"rows": [()]}), "SELECT 1", ["id"]) is None
    assert sql.query_scalar(FakeConnection({"rows": [(3,)]}), "SELECT 1", ["id"]) == 3

    rust_module = type(
        "RustModule",
        (),
        {"execute_native": staticmethod(lambda *_args: {"rows": [(4,)]})},
    )
    assert sql.query_rows_url(rust_module, "sqlite:///db.sqlite3", "SELECT 1") == [[4]]
    rust_module_bad = type(
        "RustModuleBad",
        (),
        {"execute_native": staticmethod(lambda *_args: {"rows": object()})},
    )
    assert sql.query_rows_url(rust_module_bad, "sqlite:///db.sqlite3", "SELECT 1") == []
    rust_module_not_mapping = type(
        "RustModuleNotMapping",
        (),
        {"execute_native": staticmethod(lambda *_args: object())},
    )
    assert (
        sql.query_rows_url(rust_module_not_mapping, "sqlite:///db.sqlite3", "SELECT 1")
        == []
    )


def test_toml_document_helpers_cover_scalars_nested_values_and_errors() -> None:
    payload = {
        "plain": "dark",
        "needs.dot": True,
        "count": 3,
        "ratio": 1.5,
        "nested": {"value": "ok", "skip": None},
        "items": ["a", 1],
        "skip": None,
    }
    dumped = documents.toml_dumps(payload)
    assert '"needs.dot" = true' in dumped
    assert 'nested = { value = "ok" }' in dumped
    assert documents.toml_loads(dumped.encode())["plain"] == "dark"

    try:
        documents.toml_value(None)
    except ValueError as exc:
        assert "null" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("expected null value error")

    try:
        documents.toml_value(object())
    except TypeError as exc:
        assert "unsupported" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("expected unsupported value error")


def test_artifact_detail_and_toml_fallback_helpers(monkeypatch) -> None:
    artifact = artifacts.MigrationArtifact(
        "001_no_checksum",
        SchemaSnapshot.empty(),
        SchemaSnapshot.empty(),
    )
    artifact.validate_checksum()

    details = artifacts.normalize_change_details(
        {
            "from": {
                "name": "id",
                "kind": "int",
                "nullable": False,
                "primary_key": True,
            },
            "to": {
                "name": "id",
                "kind": "str",
                "nullable": False,
                "primary_key": True,
            },
        }
    )
    assert details["from"]["foreign_table"] is None
    assert details["to"]["max_length"] is None

    monkeypatch.setitem(
        sys.modules,
        "tomli",
        SimpleNamespace(loads=lambda payload: {"fallback": payload}),
    )
    monkeypatch.setattr(documents.sys, "version_info", (3, 10))
    assert documents.toml_loads(b"legacy = true") == {"fallback": "legacy = true"}
