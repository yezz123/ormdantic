from __future__ import annotations

from typing import Any

import pytest

from ormdantic import migrations
from ormdantic._migrations import reflection
from ormdantic._migrations.models import IndexSnapshot, SchemaSnapshot


def test_public_migration_facade_re_exports_reflection_helpers() -> None:
    assert migrations._reflect_schema_snapshot is reflection._reflect_schema_snapshot
    assert migrations._reflect_server_snapshot is reflection._reflect_server_snapshot
    assert migrations._reflect_sqlite_snapshot is reflection._reflect_sqlite_snapshot
    assert migrations._normalize_sqlite_type is reflection._normalize_sqlite_type
    assert migrations._schema_filter is reflection._schema_filter


def test_reflect_schema_snapshot_dispatches_by_normalized_dialect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, str, tuple[str, ...] | None, tuple[str, ...] | None]] = []

    def fake_server_snapshot(
        url: str,
        *,
        dialect: str,
        include_tables: tuple[str, ...] | None,
        exclude_tables: tuple[str, ...] | None,
        schema: str | None,
    ) -> SchemaSnapshot:
        calls.append(("server", dialect, include_tables, exclude_tables))
        assert url == "postgres://localhost/db"
        assert schema == "public"
        return SchemaSnapshot.empty()

    def fake_sqlite_snapshot(
        url: str,
        *,
        include_tables: tuple[str, ...] | None,
        exclude_tables: tuple[str, ...] | None,
        schema: str | None,
    ) -> SchemaSnapshot:
        calls.append(("sqlite", url, include_tables, exclude_tables))
        assert schema is None
        return SchemaSnapshot.empty()

    monkeypatch.setattr(reflection, "_reflect_server_snapshot", fake_server_snapshot)
    monkeypatch.setattr(reflection, "_reflect_sqlite_snapshot", fake_sqlite_snapshot)

    reflection._reflect_schema_snapshot(
        "postgres://localhost/db",
        dialect="postgres://localhost/db",
        include_tables=("flavor",),
        exclude_tables=None,
        schema="public",
    )
    reflection._reflect_schema_snapshot(
        "sqlite:///tmp.db",
        dialect="sqlite+aiosqlite",
        include_tables=None,
        exclude_tables=("legacy_*",),
        schema=None,
    )

    assert calls == [
        ("server", "postgresql", ("flavor",), None),
        ("sqlite", "sqlite:///tmp.db", None, ("legacy_*",)),
    ]


def test_reflect_server_snapshot_combines_reflected_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class RustExtension:
        pass

    monkeypatch.setattr(reflection, "_require_migration_symbol", lambda symbol: RustExtension)
    monkeypatch.setattr(
        reflection,
        "_reflect_server_tables",
        lambda rust, url, dialect, schema: [
            "flavor",
            "legacy_flavor",
            reflection.MIGRATION_TABLE,
        ],
    )
    monkeypatch.setattr(
        reflection,
        "_reflect_server_columns",
        lambda rust, url, dialect, schema, table_names: {
            "flavor": [
                {
                    "name": "id",
                    "kind": "int",
                    "nullable": True,
                    "max_length": None,
                },
                {
                    "name": "code",
                    "kind": "str",
                    "nullable": False,
                    "max_length": 32,
                },
                {
                    "name": "supplier_id",
                    "kind": "int",
                    "nullable": True,
                    "max_length": None,
                },
            ]
        },
    )
    monkeypatch.setattr(
        reflection,
        "_reflect_server_primary_keys",
        lambda rust, url, dialect, schema, table_names: {"flavor": ["id"]},
    )
    monkeypatch.setattr(
        reflection,
        "_reflect_server_unique_constraints",
        lambda rust, url, dialect, schema, table_names: {
            "flavor": [["code"], ["code", "supplier_id"]]
        },
    )
    monkeypatch.setattr(
        reflection,
        "_reflect_server_foreign_keys",
        lambda rust, url, dialect, schema, table_names: {
            "flavor": {"supplier_id": ("supplier", "id")}
        },
    )
    monkeypatch.setattr(
        reflection,
        "_reflect_server_indexes",
        lambda rust, url, dialect, schema, table_names: {
            "flavor": [IndexSnapshot("flavor_code_idx", ["code"], unique=True)]
        },
    )

    snapshot = reflection._reflect_server_snapshot(
        "postgresql://localhost/db",
        dialect="postgresql",
        include_tables=("flavor",),
        exclude_tables=("legacy_*",),
        schema="public",
    )

    assert [table.name for table in snapshot.tables] == ["flavor"]
    table = snapshot.tables[0]
    assert table.primary_key == "id"
    assert table.unique_constraints == [["code", "supplier_id"]]
    assert table.indexes == [IndexSnapshot("flavor_code_idx", ["code"], unique=True)]
    assert table.columns[0].primary_key
    assert not table.columns[0].nullable
    assert table.columns[1].unique
    assert table.columns[2].foreign_table == "supplier"
    assert table.columns[2].foreign_column == "id"


def test_reflection_sql_filters_and_oracle_views() -> None:
    assert reflection._schema_filter("postgresql", None) == "current_schema()"
    assert reflection._schema_filter("mysql", None) == "DATABASE()"
    assert reflection._schema_filter("mssql", None) == "SCHEMA_NAME()"
    assert reflection._schema_filter("oracle", None) == ""
    assert reflection._schema_filter("oracle", "app") == "'APP'"
    assert reflection._table_name_filter(["flavor", "supplier's"], "t.name") == (
        "AND t.name IN ('flavor', 'supplier''s')"
    )
    assert reflection._table_name_filter([], "t.name") == "AND 1 = 0"
    assert reflection._oracle_table_view("app") == "all_tables"
    assert reflection._oracle_table_view(None) == "user_tables"
    assert reflection._oracle_tab_columns_view("app") == "all_tab_cols"
    assert reflection._oracle_constraints_view(None) == "user_constraints"
    assert reflection._oracle_cons_columns_view("app") == "all_cons_columns"
    assert reflection._oracle_indexes_view(None) == "user_indexes"
    assert reflection._oracle_ind_columns_view("app") == "all_ind_columns"
    assert reflection._oracle_owner_filter("app", table_alias="t") == (
        "WHERE t.owner = 'APP'"
    )

    with pytest.raises(ValueError, match="schema filter is not available"):
        reflection._schema_filter("unknown", None)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("YES", True),
        (" y ", True),
        ("TRUE", True),
        ("1", True),
        ("NO", False),
        (0, False),
    ],
)
def test_nullable_from_reflection(value: Any, expected: bool) -> None:
    assert reflection._nullable_from_reflection(value) is expected


def test_reflected_max_length_normalizes_values() -> None:
    assert reflection._reflected_max_length(None) is None
    assert reflection._reflected_max_length(-1) is None
    assert reflection._reflected_max_length("32") == 32


@pytest.mark.parametrize(
    ("dialect", "value", "scale", "expected"),
    [
        ("postgresql", "", None, "str"),
        ("mssql", "UNIQUEIDENTIFIER", None, "uuid"),
        ("postgresql", "JSONB", None, "json"),
        ("postgresql", "BYTEA", None, "bytes"),
        ("mssql", "BIT", None, "bool"),
        ("postgresql", "DOUBLE PRECISION", None, "float"),
        ("postgresql", "BIGSERIAL", None, "int"),
        ("postgresql", "DATE", None, "date"),
        ("mysql", "DATETIME", None, "datetime"),
        ("oracle", "NUMBER", 0, "int"),
        ("oracle", "NUMBER", 2, "decimal"),
        ("postgresql", "VARCHAR", None, "str"),
        ("postgresql", "GEOGRAPHY", None, "geography"),
    ],
)
def test_normalize_reflected_type(
    dialect: str,
    value: str,
    scale: int | None,
    expected: str,
) -> None:
    assert (
        reflection._normalize_reflected_type(dialect, value, scale=scale) == expected
    )


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, "str"),
        ("INTEGER", "int"),
        ("VARCHAR(32)", "str"),
        ("BLOB", "bytes"),
        ("DOUBLE", "float"),
        ("BOOLEAN", "bool"),
        ("NUMERIC", "numeric"),
    ],
)
def test_normalize_sqlite_type(value: Any, expected: str) -> None:
    assert reflection._normalize_sqlite_type(value) == expected
