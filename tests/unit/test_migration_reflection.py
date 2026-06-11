from __future__ import annotations

from typing import Any

import pytest

from ormdantic import migrations
from ormdantic._migrations import reflection
from ormdantic._migrations.models import (
    ColumnSnapshot,
    EnumTypeSnapshot,
    ExclusionConstraintSnapshot,
    ForeignKeyConstraintSnapshot,
    IndexSnapshot,
    NamespaceSnapshot,
    SchemaSnapshot,
    SequenceSnapshot,
    TableCheckSnapshot,
    UniqueConstraintSnapshot,
    ViewSnapshot,
)


def test_public_migration_facade_re_exports_reflection_helpers() -> None:
    assert migrations._reflect_schema_snapshot is reflection._reflect_schema_snapshot
    assert migrations._reflect_server_snapshot is reflection._reflect_server_snapshot
    assert (
        migrations._reflect_server_table_comments
        is reflection._reflect_server_table_comments
    )
    assert (
        migrations._reflect_server_column_comments
        is reflection._reflect_server_column_comments
    )
    assert (
        migrations._reflect_server_table_tablespaces
        is reflection._reflect_server_table_tablespaces
    )
    assert (
        migrations._reflect_server_mysql_table_options
        is reflection._reflect_server_mysql_table_options
    )
    assert (
        migrations._reflect_server_oracle_table_compressions
        is reflection._reflect_server_oracle_table_compressions
    )
    assert (
        migrations._reflect_server_postgres_inherits
        is reflection._reflect_server_postgres_inherits
    )
    assert (
        migrations._reflect_server_postgres_with
        is reflection._reflect_server_postgres_with
    )
    assert (
        migrations._reflect_server_postgres_using
        is reflection._reflect_server_postgres_using
    )
    assert (
        migrations._reflect_server_postgres_unlogged
        is reflection._reflect_server_postgres_unlogged
    )
    assert (
        migrations._reflect_server_postgres_partition_by
        is reflection._reflect_server_postgres_partition_by
    )
    assert (
        migrations._reflect_server_postgres_partitions
        is reflection._reflect_server_postgres_partitions
    )
    assert migrations._reflect_sqlite_snapshot is reflection._reflect_sqlite_snapshot
    assert (
        migrations._reflect_server_check_constraints
        is reflection._reflect_server_check_constraints
    )
    assert (
        migrations._reflect_server_foreign_key_constraints
        is reflection._reflect_server_foreign_key_constraints
    )
    assert (
        migrations._reflect_server_exclusion_constraints
        is reflection._reflect_server_exclusion_constraints
    )
    assert (
        migrations._reflect_server_enum_types is reflection._reflect_server_enum_types
    )
    assert migrations._reflect_server_sequences is reflection._reflect_server_sequences
    assert (
        migrations._reflect_server_namespaces is reflection._reflect_server_namespaces
    )
    assert migrations._reflect_server_views is reflection._reflect_server_views
    assert migrations._reflect_sqlite_views is reflection._reflect_sqlite_views
    assert (
        migrations._normalize_reflected_validated
        is reflection._normalize_reflected_validated
    )
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


def test_normalize_generated_column_checks_moves_reflected_checks_to_columns() -> None:
    columns = [
        ColumnSnapshot("code", "str", nullable=False, primary_key=False),
        ColumnSnapshot("amount", "decimal", nullable=False, primary_key=False),
        ColumnSnapshot("quantity", "int", nullable=False, primary_key=False),
    ]
    reflected_columns, table_checks = reflection._normalize_generated_column_checks(
        "sqlite",
        "flavor",
        columns,
        [
            TableCheckSnapshot(
                "flavor_code_pattern_check",
                "ormdantic_regex_match(code, '^[A-Z]{2}$') = 1",
            ),
            TableCheckSnapshot(
                "flavor_amount_gt_check",
                "ormdantic_decimal_cmp(amount, '0') > 0",
            ),
            TableCheckSnapshot(
                "flavor_quantity_multiple_of_check",
                "ormdantic_decimal_multiple_of(quantity, 5) = 1",
            ),
            TableCheckSnapshot("flavor_quantity_positive_check", "quantity > 0"),
        ],
    )

    checks = {column.name: column.checks for column in reflected_columns}
    assert checks["code"] == [("pattern", "matches", "'^[A-Z]{2}$'")]
    assert checks["amount"] == [("comparison", ">", "0")]
    assert checks["quantity"] == [("multiple_of", "=", "5")]
    assert table_checks == [
        TableCheckSnapshot("flavor_quantity_positive_check", "quantity > 0")
    ]


def test_normalize_generated_column_checks_orders_reflected_checks() -> None:
    columns = [
        ColumnSnapshot("name", "str", nullable=False, primary_key=False),
    ]
    reflected_columns, table_checks = reflection._normalize_generated_column_checks(
        "postgresql",
        "flavor",
        columns,
        [
            TableCheckSnapshot(
                "flavor_name_max_length_check",
                "char_length((name)::text) <= 255",
            ),
            TableCheckSnapshot(
                "flavor_name_min_length_check",
                "char_length((name)::text) >= 2",
            ),
        ],
    )

    assert reflected_columns[0].checks == [
        ("length", ">=", "2"),
        ("length", "<=", "255"),
    ]
    assert table_checks == []


@pytest.mark.parametrize(
    ("dialect", "expression", "expected"),
    [
        ("postgresql", "code ~ '^[A-Z]{2}$'", "'^[A-Z]{2}$'"),
        ("mysql", "`code` REGEXP '^[A-Z]{2}$'", "'^[A-Z]{2}$'"),
        ("mysql", "code REGEXP '^[A-Z]{2}$'", "'^[A-Z]{2}$'"),
        ("oracle", "REGEXP_LIKE(\"code\", '^[A-Z]{2}$')", "'^[A-Z]{2}$'"),
        ("oracle", "REGEXP_LIKE(code, '^[A-Z]{2}$')", "'^[A-Z]{2}$'"),
    ],
)
def test_generated_pattern_check_value_parses_dialect_rewrites(
    dialect: str,
    expression: str,
    expected: str,
) -> None:
    assert (
        reflection._generated_column_check_value(
            dialect,
            "code",
            "pattern",
            "matches",
            expression,
        )
        == expected
    )


@pytest.mark.parametrize(
    ("dialect", "expression", "expected"),
    [
        ("postgresql", "char_length((name)::text) >= 2", "2"),
        ("mysql", "LENGTH(`name`) >= 2", "2"),
        ("mariadb", "octet_length(`name`) <= 255", "255"),
        ("mariadb", "LENGTH(`name`) <= 255", "255"),
        ("mssql", "LEN([name]) <= 255", "255"),
        ("oracle", 'LENGTH("name") <= 255', "255"),
    ],
)
def test_generated_length_check_value_parses_quoted_identifiers(
    dialect: str,
    expression: str,
    expected: str,
) -> None:
    assert (
        reflection._generated_column_check_value(
            dialect,
            "name",
            "length",
            "<=" if "<=" in expression else ">=",
            expression,
        )
        == expected
    )


@pytest.mark.parametrize(
    ("dialect", "expression", "expected"),
    [
        ("postgresql", "MOD(quantity, 5) = 0", "5"),
        ("mysql", "MOD(quantity, 5) = 0", "5"),
        ("oracle", "MOD(quantity, 5) = 0", "5"),
        ("mssql", "quantity % 5 = 0", "5"),
    ],
)
def test_generated_multiple_of_check_value_parses_dialect_rewrites(
    dialect: str,
    expression: str,
    expected: str,
) -> None:
    assert (
        reflection._generated_column_check_value(
            dialect,
            "quantity",
            "multiple_of",
            "=",
            expression,
        )
        == expected
    )


def test_generated_postgres_check_value_normalizes_reflected_casts() -> None:
    assert (
        reflection._generated_column_check_value(
            "postgresql",
            "amount",
            "comparison",
            ">",
            "(amount > (0)::numeric)",
        )
        == "0"
    )
    assert (
        reflection._generated_column_check_value(
            "postgresql",
            "flavor",
            "enum",
            "in",
            "((flavor)::text = ANY (ARRAY['mocha'::text, 'latte'::text]))",
        )
        == "'mocha', 'latte'"
    )
    assert (
        reflection._generated_column_check_value(
            "postgresql",
            "code",
            "pattern",
            "matches",
            "((code)::text ~ '^[A-Z]{2}$'::text)",
        )
        == "'^[A-Z]{2}$'"
    )
    assert (
        reflection._generated_column_check_value(
            "postgresql",
            "quantity",
            "multiple_of",
            "=",
            "MOD((quantity)::numeric, (5)::numeric) = 0",
        )
        == "5"
    )


def test_reflect_sqlite_snapshot_preserves_unique_constraints_and_index_columns(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeRuntime:
        class PyDatabase:
            def __init__(self, url: str, tables: list[object]) -> None:
                assert url == "sqlite:///db.sqlite3"
                assert tables == []

            def table_names(self) -> list[str]:
                return ["flavor"]

            def columns(self, table: str) -> list[dict[str, object]]:
                assert table == "flavor"
                return [
                    {
                        "name": "id",
                        "type": "INTEGER",
                        "nullable": False,
                        "default": None,
                        "primary_key": True,
                    },
                    {
                        "name": "code",
                        "type": "TEXT",
                        "nullable": False,
                        "default": None,
                        "primary_key": False,
                    },
                    {
                        "name": "name",
                        "type": "TEXT",
                        "nullable": False,
                        "default": None,
                        "primary_key": False,
                    },
                    {
                        "name": "name_lower",
                        "type": "TEXT",
                        "nullable": True,
                        "default": None,
                        "primary_key": False,
                    },
                    {
                        "name": "supplier_id",
                        "type": "TEXT",
                        "nullable": False,
                        "default": None,
                        "primary_key": False,
                    },
                    {
                        "name": "origin_id",
                        "type": "TEXT",
                        "nullable": False,
                        "default": None,
                        "primary_key": False,
                    },
                    {
                        "name": "origin_code",
                        "type": "TEXT",
                        "nullable": False,
                        "default": None,
                        "primary_key": False,
                    },
                    {
                        "name": "roaster_id",
                        "type": "TEXT",
                        "nullable": False,
                        "default": None,
                        "primary_key": False,
                    },
                ]

            def foreign_keys(self, table: str) -> list[dict[str, object]]:
                assert table == "flavor"
                return [
                    {
                        "from": "supplier_id",
                        "table": "supplier",
                        "to": "id",
                        "on_delete": "SET NULL",
                        "on_update": "CASCADE",
                        "name": None,
                    },
                    {
                        "from": "origin_id",
                        "table": "origin",
                        "to": "id",
                        "on_delete": "CASCADE",
                        "on_update": "NO ACTION",
                        "name": None,
                    },
                    {
                        "from": "origin_code",
                        "table": "origin",
                        "to": "code",
                        "on_delete": "CASCADE",
                        "on_update": "NO ACTION",
                        "name": None,
                    },
                    {
                        "from": "roaster_id",
                        "table": "roaster",
                        "to": "id",
                        "on_delete": "NO ACTION",
                        "on_update": "NO ACTION",
                        "name": None,
                    },
                ]

            def indexes(
                self,
                table: str,
                include_autoindexes: bool = False,
            ) -> list[dict[str, object]]:
                assert table == "flavor"
                assert include_autoindexes is True
                return [
                    {
                        "name": "sqlite_autoindex_flavor_1",
                        "unique": True,
                        "origin": "pk",
                        "columns": ["id"],
                    },
                    {
                        "name": "sqlite_autoindex_flavor_2",
                        "unique": True,
                        "origin": "u",
                        "columns": ["code"],
                    },
                    {
                        "name": "sqlite_autoindex_flavor_3",
                        "unique": True,
                        "origin": "u",
                        "columns": ["name", "supplier_id"],
                    },
                    {
                        "name": "sqlite_autoindex_flavor_4",
                        "unique": True,
                        "origin": "u",
                        "columns": ["name"],
                    },
                    {
                        "name": "flavor_name_idx",
                        "unique": False,
                        "origin": "c",
                        "columns": ["name"],
                    },
                    {
                        "name": "flavor_name_lower_active_idx",
                        "unique": False,
                        "origin": "c",
                        "columns": ["name"],
                    },
                    {
                        "name": "flavor_lower_active_idx",
                        "unique": False,
                        "origin": "c",
                        "columns": [],
                    },
                ]

        @staticmethod
        def execute_native(
            url: str, sql: str, params: list[object]
        ) -> dict[str, object]:
            assert url == "sqlite:///db.sqlite3"
            assert params == []
            assert "sqlite_master" in sql
            if "type = 'view'" in sql:
                return {
                    "rows": [
                        [
                            "active_flavors",
                            (
                                'CREATE VIEW "active_flavors" AS '
                                'SELECT id, name FROM "flavor" WHERE active = 1;'
                            ),
                        ]
                    ]
                }
            if "type = 'index'" in sql:
                return {
                    "rows": [
                        [
                            "flavor_name_idx",
                            'CREATE INDEX "flavor_name_idx" ON "flavor" ("name")',
                        ],
                        [
                            "flavor_name_lower_active_idx",
                            (
                                'CREATE INDEX "flavor_name_lower_active_idx" '
                                'ON "flavor" ("name", LOWER(name)) '
                                "WHERE active = 1"
                            ),
                        ],
                        [
                            "flavor_lower_active_idx",
                            (
                                'CREATE INDEX "flavor_lower_active_idx" '
                                'ON "flavor" (LOWER(name)) WHERE active = 1'
                            ),
                        ],
                    ]
                }
            return {
                "rows": [
                    [
                        (
                            'CREATE TABLE "flavor" ('
                            '"id" INTEGER PRIMARY KEY ON CONFLICT REPLACE '
                            "AUTOINCREMENT, "
                            '"code" TEXT UNIQUE ON CONFLICT ABORT, '
                            '"name" TEXT COLLATE NOCASE NOT NULL ON CONFLICT FAIL, '
                            '"name_lower" TEXT GENERATED ALWAYS AS '
                            "(LOWER(name)) STORED, "
                            '"supplier_id" TEXT, '
                            '"origin_id" TEXT, '
                            '"origin_code" TEXT, '
                            '"roaster_id" TEXT, '
                            'CONSTRAINT "flavor_unique_0" UNIQUE ("name") '
                            "ON CONFLICT IGNORE, "
                            'CONSTRAINT "flavor_name_supplier_unique" '
                            'UNIQUE ("name", "supplier_id"), '
                            'CONSTRAINT "flavor_name_check" '
                            "CHECK (LENGTH(name) >= 2), "
                            'CONSTRAINT "flavor_code_pattern_check" '
                            "CHECK (ormdantic_regex_match(code, '^[A-Z]{2}$') = 1), "
                            'CONSTRAINT "flavor_origin_fk" '
                            'FOREIGN KEY ("origin_id", "origin_code") '
                            'REFERENCES "origin" ("id", "code") '
                            "ON DELETE CASCADE NOT DEFERRABLE, "
                            'CONSTRAINT "flavor_roaster_fk" '
                            'FOREIGN KEY ("roaster_id") '
                            'REFERENCES "roaster" ("id") MATCH FULL, '
                            'CONSTRAINT "flavor_supplier_fk" '
                            'FOREIGN KEY ("supplier_id") '
                            'REFERENCES "supplier" ("id") '
                            "ON DELETE SET NULL ON UPDATE CASCADE "
                            "DEFERRABLE INITIALLY DEFERRED) "
                            "STRICT, WITHOUT ROWID"
                        )
                    ]
                ]
            }

    monkeypatch.setattr(
        reflection, "_require_migration_symbol", lambda symbol: FakeRuntime
    )

    snapshot = reflection._reflect_sqlite_snapshot(
        "sqlite:///db.sqlite3",
        include_tables=["flavor", "active_*"],
        exclude_tables=None,
        schema=None,
    )

    table = snapshot.tables[0]
    assert table.primary_key == "id"
    assert table.sqlite_strict is True
    assert table.sqlite_without_rowid is True
    assert table.unique_constraints == []
    assert table.named_unique_constraints == [
        UniqueConstraintSnapshot(
            "flavor_name_supplier_unique",
            ["name", "supplier_id"],
        )
    ]
    code = next(column for column in table.columns if column.name == "code")
    assert code.unique is True
    assert code.sqlite_on_conflict_unique == "ABORT"
    assert code.checks == [("pattern", "matches", "'^[A-Z]{2}$'")]
    id_column = next(column for column in table.columns if column.name == "id")
    assert id_column.autoincrement is True
    assert id_column.sqlite_on_conflict_primary_key == "REPLACE"
    name = next(column for column in table.columns if column.name == "name")
    assert name.unique is True
    assert name.collation == "NOCASE"
    assert name.sqlite_on_conflict_not_null == "FAIL"
    assert name.sqlite_on_conflict_unique == "IGNORE"
    name_lower = next(column for column in table.columns if column.name == "name_lower")
    assert name_lower.computed == "LOWER(name)"
    assert name_lower.computed_persisted is True
    supplier_id = next(
        column for column in table.columns if column.name == "supplier_id"
    )
    assert supplier_id.foreign_table == "supplier"
    assert supplier_id.foreign_column == "id"
    assert supplier_id.foreign_key_name == "flavor_supplier_fk"
    assert supplier_id.on_delete == "set_null"
    assert supplier_id.on_update == "cascade"
    assert supplier_id.deferrable is True
    assert supplier_id.initially_deferred is True
    origin_id = next(column for column in table.columns if column.name == "origin_id")
    assert origin_id.foreign_table is None
    roaster_id = next(column for column in table.columns if column.name == "roaster_id")
    assert roaster_id.foreign_table is None
    assert table.indexes == [
        IndexSnapshot("flavor_name_idx", ["name"]),
        IndexSnapshot(
            "flavor_name_lower_active_idx",
            ["name"],
            where="active = 1",
            expressions=["LOWER(name)"],
        ),
        IndexSnapshot(
            "flavor_lower_active_idx",
            [],
            where="active = 1",
            expressions=["LOWER(name)"],
        ),
    ]
    assert table.check_constraints == [
        TableCheckSnapshot("flavor_name_check", "LENGTH(name) >= 2")
    ]
    assert table.foreign_key_constraints == [
        ForeignKeyConstraintSnapshot(
            "flavor_origin_fk",
            ["origin_id", "origin_code"],
            "origin",
            ["id", "code"],
            on_delete="cascade",
            deferrable=False,
        ),
        ForeignKeyConstraintSnapshot(
            "flavor_roaster_fk",
            ["roaster_id"],
            "roaster",
            ["id"],
            match="full",
        ),
    ]
    assert snapshot.views == [
        ViewSnapshot(
            "active_flavors",
            'SELECT id, name FROM "flavor" WHERE active = 1',
        )
    ]


def test_reflect_server_snapshot_combines_reflected_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class RustExtension:
        pass

    monkeypatch.setattr(
        reflection, "_require_migration_symbol", lambda symbol: RustExtension
    )
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
        "_reflect_server_table_comments",
        lambda rust, url, dialect, schema, table_names: {
            "flavor": "Flavor table",
        },
    )
    monkeypatch.setattr(
        reflection,
        "_reflect_server_column_comments",
        lambda rust, url, dialect, schema, table_names: {
            ("flavor", "code"): "Flavor code",
        },
    )
    monkeypatch.setattr(
        reflection,
        "_reflect_server_table_tablespaces",
        lambda rust, url, dialect, schema, table_names: {
            "flavor": "fastspace",
        },
    )
    monkeypatch.setattr(
        reflection,
        "_reflect_server_oracle_table_compressions",
        lambda rust, url, dialect, schema, table_names: {
            "flavor": 6,
        },
    )
    monkeypatch.setattr(
        reflection,
        "_reflect_server_postgres_inherits",
        lambda rust, url, dialect, schema, table_names: {
            "flavor": ["base_flavor"],
        },
    )
    monkeypatch.setattr(
        reflection,
        "_reflect_server_postgres_with",
        lambda rust, url, dialect, schema, table_names: {
            "flavor": [("fillfactor", "70")],
        },
    )
    monkeypatch.setattr(
        reflection,
        "_reflect_server_postgres_using",
        lambda rust, url, dialect, schema, table_names: {
            "flavor": "custom_heap",
        },
    )
    monkeypatch.setattr(
        reflection,
        "_reflect_server_postgres_unlogged",
        lambda rust, url, dialect, schema, table_names: {"flavor": True},
    )
    monkeypatch.setattr(
        reflection,
        "_reflect_server_postgres_partition_by",
        lambda rust, url, dialect, schema, table_names: {
            "flavor": "RANGE (id)",
        },
    )
    monkeypatch.setattr(
        reflection,
        "_reflect_server_postgres_partitions",
        lambda rust, url, dialect, schema, table_names: {},
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
                    "identity": True,
                    "identity_always": True,
                    "identity_start": 10,
                    "identity_increment": 5,
                    "identity_min_value": 1,
                    "identity_max_value": 1000,
                    "identity_cycle": True,
                    "identity_cache": 20,
                },
                {
                    "name": "code",
                    "kind": "str",
                    "nullable": False,
                    "max_length": 32,
                    "server_default": "'new'",
                    "collation": "C",
                },
                {
                    "name": "supplier_id",
                    "kind": "int",
                    "nullable": True,
                    "max_length": None,
                },
                {
                    "name": "price",
                    "kind": "decimal",
                    "nullable": False,
                    "max_length": None,
                    "numeric_precision": 12,
                    "numeric_scale": 2,
                },
                {
                    "name": "code_lower",
                    "kind": "str",
                    "nullable": True,
                    "max_length": 32,
                    "computed": "lower(code)",
                    "computed_persisted": True,
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
            "flavor": [
                UniqueConstraintSnapshot("flavor_code_unique", ["code"]),
                UniqueConstraintSnapshot(
                    "flavor_code_supplier_unique",
                    ["code", "supplier_id"],
                    deferrable=True,
                    initially_deferred=True,
                ),
                UniqueConstraintSnapshot(
                    "flavor_price_unique",
                    ["price"],
                    nulls_not_distinct=True,
                ),
            ]
        },
    )
    monkeypatch.setattr(
        reflection,
        "_reflect_server_foreign_key_constraints",
        lambda rust, url, dialect, schema, table_names, include_single_column=False: {
            "flavor": [
                ForeignKeyConstraintSnapshot(
                    "flavor_supplier_fk",
                    ["supplier_id"],
                    "supplier",
                    ["id"],
                    on_delete="set_null",
                    on_update="cascade",
                    deferrable=True,
                    initially_deferred=True,
                ),
                ForeignKeyConstraintSnapshot(
                    "flavor_supplier_pair_fk",
                    ["supplier_id", "supplier_code"],
                    "supplier",
                    ["id", "code"],
                    on_delete="set_null",
                ),
            ]
        },
    )
    monkeypatch.setattr(
        reflection,
        "_reflect_server_exclusion_constraints",
        lambda rust, url, dialect, schema, table_names: {
            "flavor": [
                ExclusionConstraintSnapshot(
                    "flavor_active_name_exclusion",
                    columns=[("name", "=")],
                    expressions=[("lower(code)", "=")],
                    using="btree",
                    where="deleted_at IS NULL",
                    deferrable=True,
                )
            ]
        },
    )
    monkeypatch.setattr(
        reflection,
        "_reflect_server_check_constraints",
        lambda rust, url, dialect, schema, table_names: {
            "flavor": [
                TableCheckSnapshot("flavor_code_min_length_check", "LENGTH(code) >= 2"),
                TableCheckSnapshot("flavor_rating_check", "rating BETWEEN 0 AND 100"),
            ]
        },
    )
    monkeypatch.setattr(
        reflection,
        "_reflect_server_indexes",
        lambda rust, url, dialect, schema, table_names: {
            "flavor": [IndexSnapshot("flavor_code_idx", ["code"], unique=True)]
        },
    )
    monkeypatch.setattr(
        reflection,
        "_reflect_server_enum_types",
        lambda rust, url, dialect, schema, table_names=None: [
            EnumTypeSnapshot("flavor_kind", ["mocha", "latte"], schema=schema)
        ],
    )
    monkeypatch.setattr(
        reflection,
        "_reflect_server_namespaces",
        lambda rust, url, dialect, schema, extra_schemas=None: [
            NamespaceSnapshot(str(schema))
        ],
    )
    monkeypatch.setattr(
        reflection,
        "_reflect_server_sequences",
        lambda rust, url, dialect, schema, table_names=None: [
            SequenceSnapshot("flavor_id_seq", schema=schema, start=10, increment=5)
        ],
    )
    monkeypatch.setattr(
        reflection,
        "_reflect_server_views",
        lambda rust, url, dialect, schema, include_tables=None, exclude_tables=None: [
            ViewSnapshot("active_flavors", "SELECT id FROM flavor", schema=schema)
        ],
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
    assert table.comment == "Flavor table"
    assert table.tablespace == "fastspace"
    assert table.oracle_compress == 6
    assert table.postgres_inherits == ["base_flavor"]
    assert table.postgres_with == [("fillfactor", "70")]
    assert table.postgres_using == "custom_heap"
    assert table.postgres_unlogged is True
    assert table.postgres_partition_by == "RANGE (id)"
    assert table.unique_constraints == []
    assert table.named_unique_constraints == [
        UniqueConstraintSnapshot(
            "flavor_code_supplier_unique",
            ["code", "supplier_id"],
            deferrable=True,
            initially_deferred=True,
        ),
        UniqueConstraintSnapshot(
            "flavor_price_unique",
            ["price"],
            nulls_not_distinct=True,
        ),
    ]
    assert table.indexes == [IndexSnapshot("flavor_code_idx", ["code"], unique=True)]
    assert table.columns[0].primary_key
    assert not table.columns[0].nullable
    assert table.columns[0].identity is True
    assert table.columns[0].identity_always is True
    assert table.columns[0].identity_start == 10
    assert table.columns[0].identity_increment == 5
    assert table.columns[1].unique
    assert table.columns[1].comment == "Flavor code"
    assert table.columns[1].server_default == "'new'"
    assert table.columns[1].collation == "C"
    assert table.columns[2].foreign_table == "supplier"
    assert table.columns[2].foreign_column == "id"
    assert table.columns[2].foreign_key_name == "flavor_supplier_fk"
    assert table.columns[2].on_delete == "set_null"
    assert table.columns[2].on_update == "cascade"
    assert table.columns[2].deferrable is True
    assert table.columns[2].initially_deferred is True
    assert table.columns[1].checks == [("length", ">=", "2")]
    assert not table.columns[3].unique
    assert table.columns[3].numeric_precision == 12
    assert table.columns[3].numeric_scale == 2
    assert table.columns[4].computed == "lower(code)"
    assert table.columns[4].computed_persisted is True
    assert table.check_constraints == [
        TableCheckSnapshot("flavor_rating_check", "rating BETWEEN 0 AND 100")
    ]
    assert table.foreign_key_constraints == [
        ForeignKeyConstraintSnapshot(
            "flavor_supplier_pair_fk",
            ["supplier_id", "supplier_code"],
            "supplier",
            ["id", "code"],
            on_delete="set_null",
        )
    ]
    assert table.exclusion_constraints == [
        ExclusionConstraintSnapshot(
            "flavor_active_name_exclusion",
            columns=[("name", "=")],
            expressions=[("lower(code)", "=")],
            using="btree",
            where="deleted_at IS NULL",
            deferrable=True,
        )
    ]
    assert snapshot.sequences == [
        SequenceSnapshot("flavor_id_seq", schema="public", start=10, increment=5)
    ]
    assert snapshot.namespaces == [NamespaceSnapshot("public")]
    assert snapshot.enum_types == [
        EnumTypeSnapshot("flavor_kind", ["mocha", "latte"], schema="public")
    ]
    assert snapshot.views == [
        ViewSnapshot("active_flavors", "SELECT id FROM flavor", schema="public")
    ]


def test_reflect_server_table_comments_preserves_supported_catalogs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_sql: list[str] = []

    def fake_query_rows_url(rust: type[Any], url: str, sql: str) -> list[list[Any]]:
        del rust, url
        captured_sql.append(sql)
        if "obj_description" in sql:
            return [["flavor", "Flavor table"], ["plain", None], ["empty", ""]]
        if "table_comment" in sql:
            return [["flavor", "MySQL table"]]
        if "sys.extended_properties" in sql:
            return [["flavor", "SQL Server table"]]
        if "tab_comments" in sql:
            return [["FLAVOR", "Oracle table"]]
        pytest.fail(f"unexpected table comment reflection SQL: {sql}")

    monkeypatch.setattr(reflection, "_query_rows_url", fake_query_rows_url)

    assert reflection._reflect_server_table_comments(
        object,
        "postgresql://localhost/db",
        "postgresql",
        "public",
        ["flavor", "plain", "empty"],
    ) == {"flavor": "Flavor table"}
    assert reflection._reflect_server_table_comments(
        object,
        "mysql://localhost/db",
        "mysql",
        None,
        ["flavor"],
    ) == {"flavor": "MySQL table"}
    assert reflection._reflect_server_table_comments(
        object,
        "mssql://localhost/db",
        "mssql",
        "dbo",
        ["flavor"],
    ) == {"flavor": "SQL Server table"}
    assert reflection._reflect_server_table_comments(
        object,
        "oracle://localhost/db",
        "oracle",
        "inventory",
        ["FLAVOR"],
    ) == {"FLAVOR": "Oracle table"}

    assert "pg_class" in captured_sql[0]
    assert "obj_description" in captured_sql[0]
    assert "information_schema.tables" in captured_sql[1]
    assert "table_comment" in captured_sql[1]
    assert "sys.extended_properties" in captured_sql[2]
    assert "MS_Description" in captured_sql[2]
    assert "all_tab_comments" in captured_sql[3]


def test_reflect_server_column_comments_preserves_supported_catalogs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_sql: list[str] = []

    def fake_query_rows_url(rust: type[Any], url: str, sql: str) -> list[list[Any]]:
        del rust, url
        captured_sql.append(sql)
        if "pg_description" in sql:
            return [
                ["flavor", "name", "Flavor name"],
                ["flavor", "plain", None],
                ["flavor", "empty", ""],
            ]
        if "column_comment" in sql:
            return [["flavor", "name", "MySQL name"]]
        if "sys.columns" in sql:
            return [["flavor", "name", "SQL Server name"]]
        if "col_comments" in sql:
            return [["FLAVOR", "NAME", "Oracle name"]]
        pytest.fail(f"unexpected column comment reflection SQL: {sql}")

    monkeypatch.setattr(reflection, "_query_rows_url", fake_query_rows_url)

    assert reflection._reflect_server_column_comments(
        object,
        "postgresql://localhost/db",
        "postgresql",
        "public",
        ["flavor", "plain", "empty"],
    ) == {("flavor", "name"): "Flavor name"}
    assert reflection._reflect_server_column_comments(
        object,
        "mysql://localhost/db",
        "mysql",
        None,
        ["flavor"],
    ) == {("flavor", "name"): "MySQL name"}
    assert reflection._reflect_server_column_comments(
        object,
        "mssql://localhost/db",
        "mssql",
        "dbo",
        ["flavor"],
    ) == {("flavor", "name"): "SQL Server name"}
    assert reflection._reflect_server_column_comments(
        object,
        "oracle://localhost/db",
        "oracle",
        "inventory",
        ["FLAVOR"],
    ) == {("FLAVOR", "NAME"): "Oracle name"}

    assert "pg_description" in captured_sql[0]
    assert "pg_attribute" in captured_sql[0]
    assert "information_schema.columns" in captured_sql[1]
    assert "column_comment" in captured_sql[1]
    assert "sys.extended_properties" in captured_sql[2]
    assert "sys.columns" in captured_sql[2]
    assert "all_col_comments" in captured_sql[3]


def test_reflect_server_table_comments_skips_empty_table_list(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_query_rows_url(rust: type[Any], url: str, sql: str) -> list[list[Any]]:
        pytest.fail("empty table filters should not query table comments")

    monkeypatch.setattr(reflection, "_query_rows_url", fail_query_rows_url)

    assert (
        reflection._reflect_server_table_comments(
            object,
            "postgresql://localhost/db",
            "postgresql",
            None,
            [],
        )
        == {}
    )


def test_reflect_server_column_comments_skips_empty_table_list(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_query_rows_url(rust: type[Any], url: str, sql: str) -> list[list[Any]]:
        pytest.fail("empty table filters should not query column comments")

    monkeypatch.setattr(reflection, "_query_rows_url", fail_query_rows_url)

    assert (
        reflection._reflect_server_column_comments(
            object,
            "postgresql://localhost/db",
            "postgresql",
            None,
            [],
        )
        == {}
    )


def test_reflect_server_table_tablespaces_preserves_postgres_explicit_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_sql: list[str] = []

    def fake_query_rows_url(rust: type[Any], url: str, sql: str) -> list[list[Any]]:
        del rust, url
        captured_sql.append(sql)
        return [["flavor", "fastspace"], ["plain", None]]

    monkeypatch.setattr(reflection, "_query_rows_url", fake_query_rows_url)

    assert reflection._reflect_server_table_tablespaces(
        object,
        "postgresql://localhost/db",
        "postgresql",
        "public",
        ["flavor", "plain"],
    ) == {"flavor": "fastspace"}
    assert "pg_tablespace" in captured_sql[0]
    assert "c.reltablespace" in captured_sql[0]


def test_reflect_server_table_tablespaces_preserves_oracle_explicit_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_sql: list[str] = []

    def fake_query_rows_url(rust: type[Any], url: str, sql: str) -> list[list[Any]]:
        del rust, url
        captured_sql.append(sql)
        if "default_tablespace" in sql:
            return [["USERS"]]
        return [["FLAVOR", "FASTSPACE"], ["PLAIN", None]]

    monkeypatch.setattr(reflection, "_query_rows_url", fake_query_rows_url)

    assert reflection._reflect_server_table_tablespaces(
        object,
        "oracle://localhost/db",
        "oracle",
        "inventory",
        ["FLAVOR", "PLAIN"],
    ) == {"FLAVOR": "FASTSPACE"}
    assert "all_users" in captured_sql[0]
    assert "all_tables" in captured_sql[1]
    assert "tablespace_name" in captured_sql[1]
    assert "owner = 'INVENTORY'" in captured_sql[1]


def test_reflect_server_table_tablespaces_omits_oracle_default_tablespace(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_query_rows_url(rust: type[Any], url: str, sql: str) -> list[list[Any]]:
        del rust, url
        if "default_tablespace" in sql:
            return [["SYSTEM"]]
        return [["FLAVOR", "SYSTEM"]]

    monkeypatch.setattr(reflection, "_query_rows_url", fake_query_rows_url)

    assert (
        reflection._reflect_server_table_tablespaces(
            object,
            "oracle://localhost/db",
            "oracle",
            None,
            ["FLAVOR"],
        )
        == {}
    )


def test_reflect_server_oracle_table_compressions_preserves_enabled_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_sql: list[str] = []

    def fake_query_rows_url(rust: type[Any], url: str, sql: str) -> list[list[Any]]:
        del rust, url
        captured_sql.append(sql)
        return [
            ["FLAVOR", "ENABLED", "6"],
            ["BASIC_FLAVOR", "ENABLED", "BASIC"],
            ["PLAIN", "DISABLED", None],
        ]

    monkeypatch.setattr(reflection, "_query_rows_url", fake_query_rows_url)

    assert reflection._reflect_server_oracle_table_compressions(
        object,
        "oracle://localhost/db",
        "oracle",
        "inventory",
        ["FLAVOR", "BASIC_FLAVOR", "PLAIN"],
    ) == {"FLAVOR": 6, "BASIC_FLAVOR": True}
    assert "all_tables" in captured_sql[0]
    assert "t.compression" in captured_sql[0]
    assert "t.compress_for" in captured_sql[0]
    assert "owner = 'INVENTORY'" in captured_sql[0]


def test_reflect_server_oracle_table_compressions_skips_other_dialects(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_query_rows_url(*args: Any, **kwargs: Any) -> list[list[Any]]:
        del args, kwargs
        pytest.fail("unsupported dialects should not query Oracle table compression")

    monkeypatch.setattr(reflection, "_query_rows_url", fail_query_rows_url)

    assert (
        reflection._reflect_server_oracle_table_compressions(
            object,
            "postgresql://localhost/db",
            "postgresql",
            "public",
            ["flavor"],
        )
        == {}
    )


def test_reflect_server_table_tablespaces_preserves_mysql_explicit_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_sql: list[str] = []

    def fake_query_rows_url(rust: type[Any], url: str, sql: str) -> list[list[Any]]:
        del rust, url
        captured_sql.append(sql)
        return [
            ["flavor", "fastspace"],
            ["system_flavor", "innodb_system"],
            ["plain", None],
        ]

    monkeypatch.setattr(reflection, "_query_rows_url", fake_query_rows_url)

    assert reflection._reflect_server_table_tablespaces(
        object,
        "mysql://localhost/db",
        "mysql",
        None,
        ["flavor", "system_flavor", "plain"],
    ) == {"flavor": "fastspace", "system_flavor": "innodb_system"}
    assert "information_schema.innodb_tables" in captured_sql[0]
    assert "information_schema.innodb_tablespaces" in captured_sql[0]
    assert "it.space_type" in captured_sql[0]
    assert "t.table_schema = DATABASE()" in captured_sql[0]
    assert "s.name <> CONCAT(t.table_schema, '/', t.table_name)" in captured_sql[0]


def test_reflect_server_table_tablespaces_preserves_mariadb_explicit_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_sql: list[str] = []

    def fake_query_rows_url(rust: type[Any], url: str, sql: str) -> list[list[Any]]:
        del rust, url
        captured_sql.append(sql)
        return [
            ["flavor", "fastspace"],
            ["system_flavor", "innodb_system"],
            ["plain", None],
        ]

    monkeypatch.setattr(reflection, "_query_rows_url", fake_query_rows_url)

    assert reflection._reflect_server_table_tablespaces(
        object,
        "mariadb://localhost/db",
        "mariadb",
        None,
        ["flavor", "system_flavor", "plain"],
    ) == {"flavor": "fastspace", "system_flavor": "innodb_system"}
    assert "information_schema.innodb_sys_tables" in captured_sql[0]
    assert "information_schema.innodb_sys_tablespaces" in captured_sql[0]
    assert "it.name = CONCAT(t.table_schema, '/', t.table_name)" in captured_sql[0]
    assert "t.table_schema = DATABASE()" in captured_sql[0]
    assert "s.name <> CONCAT(t.table_schema, '/', t.table_name)" in captured_sql[0]


def test_reflect_server_table_tablespaces_preserves_mssql_non_default_filegroups(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_sql: list[str] = []

    def fake_query_rows_url(rust: type[Any], url: str, sql: str) -> list[list[Any]]:
        del rust, url
        captured_sql.append(sql)
        return [["flavor", "fastspace"], ["plain", None]]

    monkeypatch.setattr(reflection, "_query_rows_url", fake_query_rows_url)

    assert reflection._reflect_server_table_tablespaces(
        object,
        "mssql://localhost/db",
        "mssql",
        "dbo",
        ["flavor", "plain"],
    ) == {"flavor": "fastspace"}
    assert "sys.filegroups" in captured_sql[0]
    assert "i.index_id IN (0, 1)" in captured_sql[0]
    assert "fg.is_default = 0" in captured_sql[0]
    assert "s.name = 'dbo'" in captured_sql[0]


def test_reflect_server_table_tablespaces_skips_unsupported_dialects(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_query_rows_url(rust: type[Any], url: str, sql: str) -> list[list[Any]]:
        pytest.fail("unsupported dialects should not query table tablespaces")

    monkeypatch.setattr(reflection, "_query_rows_url", fail_query_rows_url)

    assert (
        reflection._reflect_server_table_tablespaces(
            object,
            "sqlite://localhost/db",
            "sqlite",
            None,
            ["flavor"],
        )
        == {}
    )


def test_reflect_server_postgres_inherits_preserves_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_sql: list[str] = []

    def fake_query_rows_url(rust: type[Any], url: str, sql: str) -> list[list[Any]]:
        del rust, url
        captured_sql.append(sql)
        return [
            ["flavor", "base_flavor"],
            ["flavor", "audited_flavor"],
            ["plain", "base_plain"],
        ]

    monkeypatch.setattr(reflection, "_query_rows_url", fake_query_rows_url)

    assert reflection._reflect_server_postgres_inherits(
        object,
        "postgresql://localhost/db",
        "postgresql",
        "public",
        ["flavor", "plain"],
    ) == {
        "flavor": ["base_flavor", "audited_flavor"],
        "plain": ["base_plain"],
    }
    assert "pg_inherits" in captured_sql[0]
    assert "i.inhseqno" in captured_sql[0]
    assert "child.relname" in captured_sql[0]
    assert "NOT child.relispartition" in captured_sql[0]


def test_reflect_server_postgres_inherits_skips_other_dialects(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_query_rows_url(rust: type[Any], url: str, sql: str) -> list[list[Any]]:
        pytest.fail("non-PostgreSQL dialects should not query table inheritance")

    monkeypatch.setattr(reflection, "_query_rows_url", fail_query_rows_url)

    assert (
        reflection._reflect_server_postgres_inherits(
            object,
            "mysql://localhost/db",
            "mysql",
            None,
            ["flavor"],
        )
        == {}
    )


def test_reflect_server_postgres_with_preserves_explicit_reloptions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_sql: list[str] = []

    def fake_query_rows_url(rust: type[Any], url: str, sql: str) -> list[list[Any]]:
        del rust, url
        captured_sql.append(sql)
        return [
            ["flavor", "fillfactor", "70"],
            ["flavor", "toast.autovacuum_enabled", "false"],
            ["plain", "autovacuum_enabled", "true"],
        ]

    monkeypatch.setattr(reflection, "_query_rows_url", fake_query_rows_url)

    assert reflection._reflect_server_postgres_with(
        object,
        "postgresql://localhost/db",
        "postgresql",
        "public",
        ["flavor", "plain"],
    ) == {
        "flavor": [
            ("fillfactor", "70"),
            ("toast.autovacuum_enabled", "false"),
        ],
        "plain": [("autovacuum_enabled", "true")],
    }
    assert "pg_options_to_table" in captured_sql[0]
    assert "c.reloptions" in captured_sql[0]
    assert "opts.option_name" in captured_sql[0]


def test_reflect_server_postgres_with_skips_other_dialects(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_query_rows_url(rust: type[Any], url: str, sql: str) -> list[list[Any]]:
        pytest.fail("non-PostgreSQL dialects should not query table storage parameters")

    monkeypatch.setattr(reflection, "_query_rows_url", fail_query_rows_url)

    assert (
        reflection._reflect_server_postgres_with(
            object,
            "mysql://localhost/db",
            "mysql",
            None,
            ["flavor"],
        )
        == {}
    )


def test_reflect_server_postgres_using_preserves_non_default_access_methods(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_sql: list[str] = []

    def fake_query_rows_url(rust: type[Any], url: str, sql: str) -> list[list[Any]]:
        del rust, url
        captured_sql.append(sql)
        return [["flavor", "custom_heap"], ["plain", None]]

    monkeypatch.setattr(reflection, "_query_rows_url", fake_query_rows_url)

    assert reflection._reflect_server_postgres_using(
        object,
        "postgresql://localhost/db",
        "postgresql",
        "public",
        ["flavor", "plain"],
    ) == {"flavor": "custom_heap"}
    assert "pg_am" in captured_sql[0]
    assert "c.relam" in captured_sql[0]
    assert "default_table_access_method" in captured_sql[0]


def test_reflect_server_postgres_using_skips_other_dialects(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_query_rows_url(rust: type[Any], url: str, sql: str) -> list[list[Any]]:
        pytest.fail("non-PostgreSQL dialects should not query table access methods")

    monkeypatch.setattr(reflection, "_query_rows_url", fail_query_rows_url)

    assert (
        reflection._reflect_server_postgres_using(
            object,
            "mysql://localhost/db",
            "mysql",
            None,
            ["flavor"],
        )
        == {}
    )


def test_reflect_server_postgres_unlogged_preserves_unlogged_tables(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_sql: list[str] = []

    def fake_query_rows_url(rust: type[Any], url: str, sql: str) -> list[list[Any]]:
        del rust, url
        captured_sql.append(sql)
        return [["flavor", True]]

    monkeypatch.setattr(reflection, "_query_rows_url", fake_query_rows_url)

    assert reflection._reflect_server_postgres_unlogged(
        object,
        "postgresql://localhost/db",
        "postgresql",
        "public",
        ["flavor", "plain"],
    ) == {"flavor": True}
    assert "relpersistence" in captured_sql[0]
    assert "c.relpersistence = 'u'" in captured_sql[0]


def test_reflect_server_postgres_unlogged_skips_other_dialects(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_query_rows_url(rust: type[Any], url: str, sql: str) -> list[list[Any]]:
        pytest.fail("non-PostgreSQL dialects should not query table persistence")

    monkeypatch.setattr(reflection, "_query_rows_url", fail_query_rows_url)

    assert (
        reflection._reflect_server_postgres_unlogged(
            object,
            "mysql://localhost/db",
            "mysql",
            None,
            ["flavor"],
        )
        == {}
    )


def test_reflect_server_postgres_partition_by_preserves_partition_keys(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_sql: list[str] = []

    def fake_query_rows_url(rust: type[Any], url: str, sql: str) -> list[list[Any]]:
        del rust, url
        captured_sql.append(sql)
        return [["flavor", "RANGE (id)"], ["plain", None]]

    monkeypatch.setattr(reflection, "_query_rows_url", fake_query_rows_url)

    assert reflection._reflect_server_postgres_partition_by(
        object,
        "postgresql://localhost/db",
        "postgresql",
        "public",
        ["flavor", "plain"],
    ) == {"flavor": "RANGE (id)"}
    assert "pg_partitioned_table" in captured_sql[0]
    assert "pg_get_partkeydef" in captured_sql[0]
    assert "c.relkind = 'p'" in captured_sql[0]


def test_reflect_server_postgres_partition_by_skips_other_dialects(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_query_rows_url(rust: type[Any], url: str, sql: str) -> list[list[Any]]:
        pytest.fail("non-PostgreSQL dialects should not query table partition keys")

    monkeypatch.setattr(reflection, "_query_rows_url", fail_query_rows_url)

    assert (
        reflection._reflect_server_postgres_partition_by(
            object,
            "mysql://localhost/db",
            "mysql",
            None,
            ["flavor"],
        )
        == {}
    )


def test_reflect_server_postgres_partitions_preserves_parent_and_bound(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_sql: list[str] = []

    def fake_query_rows_url(rust: type[Any], url: str, sql: str) -> list[list[Any]]:
        del rust, url
        captured_sql.append(sql)
        return [
            ["flavor_2026", "flavor", "FOR VALUES FROM (2026) TO (2027)"],
            ["plain", "flavor", None],
        ]

    monkeypatch.setattr(reflection, "_query_rows_url", fake_query_rows_url)

    assert reflection._reflect_server_postgres_partitions(
        object,
        "postgresql://localhost/db",
        "postgresql",
        "public",
        ["flavor_2026", "plain"],
    ) == {
        "flavor_2026": ("flavor", "FOR VALUES FROM (2026) TO (2027)"),
    }
    assert "pg_inherits" in captured_sql[0]
    assert "pg_get_expr(child.relpartbound, child.oid)" in captured_sql[0]
    assert "child.relispartition" in captured_sql[0]


def test_reflect_server_postgres_partitions_skips_other_dialects(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_query_rows_url(rust: type[Any], url: str, sql: str) -> list[list[Any]]:
        pytest.fail("non-PostgreSQL dialects should not query table partitions")

    monkeypatch.setattr(reflection, "_query_rows_url", fail_query_rows_url)

    assert (
        reflection._reflect_server_postgres_partitions(
            object,
            "mysql://localhost/db",
            "mysql",
            None,
            ["flavor"],
        )
        == {}
    )


def test_reflect_server_mysql_table_options_preserves_storage_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_sql: list[str] = []

    def fake_query_rows_url(rust: type[Any], url: str, sql: str) -> list[list[Any]]:
        del rust, url
        captured_sql.append(sql)
        if "SELECT @@" in sql:
            return [
                ["MyISAM", "latin1", "latin1_swedish_ci", "compact"],
            ]
        if "information_schema.partitions" in sql:
            return [
                [
                    "flavor",
                    "HASH",
                    "id",
                    "p0",
                    "KEY",
                    "code",
                    "p0sp0",
                ],
                [
                    "flavor",
                    "HASH",
                    "id",
                    "p0",
                    "KEY",
                    "code",
                    "p0sp1",
                ],
                [
                    "flavor",
                    "HASH",
                    "id",
                    "p1",
                    "KEY",
                    "code",
                    "p1sp0",
                ],
                [
                    "flavor",
                    "HASH",
                    "id",
                    "p1",
                    "KEY",
                    "code",
                    "p1sp1",
                ],
            ]
        return [
            [
                "flavor",
                "InnoDB",
                "utf8mb4",
                "utf8mb4_unicode_ci",
                "Dynamic",
                "row_format=DYNAMIC key_block_size=8 pack_keys=1 "
                "checksum=1 delay_key_write=1 stats_persistent=1 "
                "stats_auto_recalc=0 stats_sample_pages=32 avg_row_length=64 "
                "max_rows=1000 min_rows=10 insert_method=LAST "
                "data directory='/var/lib/mysql/data' "
                "index directory='/var/lib/mysql/idx''dir' "
                "connection='mysql://remote.example/db/flavor' "
                "union=(`flavor_hot`,`flavor_cold`)",
                101,
            ],
            ["plain", None, None, None, None, None, None],
        ]

    monkeypatch.setattr(reflection, "_query_rows_url", fake_query_rows_url)

    assert reflection._reflect_server_mysql_table_options(
        object,
        "mysql://localhost/db",
        "mysql",
        None,
        ["flavor", "plain"],
    ) == {
        "flavor": {
            "engine": "InnoDB",
            "charset": "utf8mb4",
            "collation": "utf8mb4_unicode_ci",
            "row_format": "Dynamic",
            "key_block_size": 8,
            "pack_keys": True,
            "checksum": True,
            "delay_key_write": True,
            "stats_persistent": True,
            "stats_auto_recalc": False,
            "stats_sample_pages": 32,
            "avg_row_length": 64,
            "max_rows": 1000,
            "min_rows": 10,
            "insert_method": "LAST",
            "data_directory": "/var/lib/mysql/data",
            "index_directory": "/var/lib/mysql/idx'dir",
            "connection": "mysql://remote.example/db/flavor",
            "union": ["flavor_hot", "flavor_cold"],
            "partition_by": "HASH (id)",
            "partitions": 2,
            "subpartition_by": "KEY (code)",
            "subpartitions": 2,
            "auto_increment": 101,
        },
        "plain": {
            "engine": None,
            "charset": None,
            "collation": None,
            "row_format": None,
            "key_block_size": None,
            "pack_keys": None,
            "checksum": None,
            "delay_key_write": None,
            "stats_persistent": None,
            "stats_auto_recalc": None,
            "stats_sample_pages": None,
            "avg_row_length": None,
            "max_rows": None,
            "min_rows": None,
            "insert_method": None,
            "data_directory": None,
            "index_directory": None,
            "connection": None,
            "union": [],
            "auto_increment": None,
        },
    }
    assert "SELECT @@" in captured_sql[0]
    assert "information_schema.tables" in captured_sql[1]
    assert "information_schema.partitions" in captured_sql[2]
    assert "row_format" in captured_sql[1]
    assert "create_options" in captured_sql[1]
    assert "collation_character_set_applicability" in captured_sql[1]


def test_reflect_server_mysql_table_options_omits_database_defaults(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_query_rows_url(rust: type[Any], url: str, sql: str) -> list[list[Any]]:
        del rust, url
        if "SELECT @@" in sql:
            return [
                ["InnoDB", "utf8mb4", "utf8mb4_0900_ai_ci", "dynamic"],
            ]
        if "information_schema.partitions" in sql:
            return []
        return [
            [
                "plain",
                "InnoDB",
                "utf8mb4",
                "utf8mb4_0900_ai_ci",
                "Dynamic",
                None,
                None,
            ],
            [
                "explicit_row_format",
                "InnoDB",
                "utf8mb4",
                "utf8mb4_0900_ai_ci",
                "Dynamic",
                "row_format=DYNAMIC",
                None,
            ],
        ]

    monkeypatch.setattr(reflection, "_query_rows_url", fake_query_rows_url)

    reflected = reflection._reflect_server_mysql_table_options(
        object,
        "mysql://localhost/db",
        "mysql",
        None,
        ["plain", "explicit_row_format"],
    )

    assert reflected["plain"]["engine"] is None
    assert reflected["plain"]["charset"] is None
    assert reflected["plain"]["collation"] is None
    assert reflected["plain"]["row_format"] is None
    assert reflected["explicit_row_format"]["row_format"] == "Dynamic"


def test_normalize_mysql_default_column_collations_omits_inherited_values() -> None:
    columns = {
        "plain": [
            {"name": "name", "collation": "utf8mb4_0900_ai_ci"},
            {"name": "code", "collation": "utf8mb4_bin"},
        ],
        "custom": [
            {"name": "name", "collation": "utf8mb4_unicode_ci"},
        ],
    }

    reflection._normalize_mysql_default_column_collations(
        "mysql",
        columns,
        {"custom": {"collation": "utf8mb4_unicode_ci"}},
        {"collation_database": "utf8mb4_0900_ai_ci"},
    )

    assert "collation" not in columns["plain"][0]
    assert columns["plain"][1]["collation"] == "utf8mb4_bin"
    assert "collation" not in columns["custom"][0]


def test_normalize_mssql_default_column_collations_omits_database_default() -> None:
    columns = {
        "flavor": [
            {"name": "name", "collation": "SQL_Latin1_General_CP1_CI_AS"},
            {"name": "code", "collation": "Latin1_General_CS_AS"},
        ]
    }

    reflection._normalize_mssql_default_column_collations(
        "mssql",
        columns,
        "SQL_Latin1_General_CP1_CI_AS",
    )

    assert "collation" not in columns["flavor"][0]
    assert columns["flavor"][1]["collation"] == "Latin1_General_CS_AS"


def test_normalize_oracle_default_column_metadata_omits_default_collation() -> None:
    columns = {
        "FLAVOR": [
            {"name": "NAME", "collation": "USING_NLS_COMP"},
            {"name": "CODE", "collation": "BINARY_CI"},
        ]
    }

    reflection._normalize_oracle_default_column_metadata("oracle", columns)

    assert "collation" not in columns["FLAVOR"][0]
    assert columns["FLAVOR"][1]["collation"] == "BINARY_CI"


def test_oracle_defaulted_tablespace_omits_default_value() -> None:
    assert reflection._oracle_defaulted_tablespace("SYSTEM", "SYSTEM") is None
    assert reflection._oracle_defaulted_tablespace("FASTSPACE", "SYSTEM") == "FASTSPACE"


def test_reflected_server_default_normalizes_mssql_parentheses() -> None:
    assert reflection._reflected_server_default("mssql", "((0))") == "0"
    assert reflection._reflected_server_default("mssql", "('new')") == "'new'"
    assert reflection._reflected_server_default("oracle", "0 ") == "0"
    assert reflection._reflected_server_default("postgresql", "((0))") == "((0))"


def test_reflect_server_mysql_table_options_skips_other_dialects(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_query_rows_url(rust: type[Any], url: str, sql: str) -> list[list[Any]]:
        pytest.fail("non-MySQL dialects should not query MySQL table options")

    monkeypatch.setattr(reflection, "_query_rows_url", fail_query_rows_url)

    assert (
        reflection._reflect_server_mysql_table_options(
            object,
            "postgresql://localhost/db",
            "postgresql",
            None,
            ["flavor"],
        )
        == {}
    )


def test_reflect_server_enum_types_preserves_postgres_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_sql: list[str] = []

    def fake_query_rows_url(rust: type[Any], url: str, sql: str) -> list[list[Any]]:
        del rust, url
        captured_sql.append(sql)
        return [
            ["public", "flavor_kind", "mocha", 1.0, True, "Flavor enum"],
            ["public", "flavor_kind", "latte", 2.0, True, "Flavor enum"],
            ["public", "roast_kind", "light", 1.0, True, None],
        ]

    monkeypatch.setattr(reflection, "_query_rows_url", fake_query_rows_url)

    enum_types = reflection._reflect_server_enum_types(
        object,
        "postgresql://localhost/db",
        "postgresql",
        "public",
    )

    assert "pg_enum" in captured_sql[0]
    assert "e.enumsortorder" in captured_sql[0]
    assert "obj_description" in captured_sql[0]
    assert "n.nspname = 'public'" in captured_sql[0]
    assert enum_types == [
        EnumTypeSnapshot(
            "flavor_kind",
            ["mocha", "latte"],
            schema="public",
            comment="Flavor enum",
        ),
        EnumTypeSnapshot("roast_kind", ["light"], schema="public"),
    ]


def test_reflect_server_enum_types_preserves_current_schema_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_sql: list[str] = []

    def fake_query_rows_url(rust: type[Any], url: str, sql: str) -> list[list[Any]]:
        del rust, url
        captured_sql.append(sql)
        return [["inventory", "flavor_kind", "mocha", 1.0, True, "Flavor enum"]]

    monkeypatch.setattr(reflection, "_query_rows_url", fake_query_rows_url)

    enum_types = reflection._reflect_server_enum_types(
        object,
        "postgresql://localhost/db",
        "postgresql",
        None,
    )

    assert "current_schema()" in captured_sql[0]
    assert enum_types == [
        EnumTypeSnapshot("flavor_kind", ["mocha"], comment="Flavor enum")
    ]


def test_reflect_server_enum_types_includes_cross_schema_table_references(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_sql: list[str] = []

    def fake_query_rows_url(rust: type[Any], url: str, sql: str) -> list[list[Any]]:
        del rust, url
        captured_sql.append(sql)
        return [
            [
                "inventory",
                "coffee_flavor_kind",
                "mocha",
                1.0,
                False,
                "Coffee flavor enum",
            ],
            [
                "inventory",
                "coffee_flavor_kind",
                "latte",
                2.0,
                False,
                "Coffee flavor enum",
            ],
            ["public", "local_kind", "dark", 1.0, True, None],
        ]

    monkeypatch.setattr(reflection, "_query_rows_url", fake_query_rows_url)

    enum_types = reflection._reflect_server_enum_types(
        object,
        "postgresql://localhost/db",
        "postgresql",
        "public",
        ["flavor"],
    )

    assert "pg_attribute" in captured_sql[0]
    assert "rel.relname IN ('flavor')" in captured_sql[0]
    assert enum_types == [
        EnumTypeSnapshot(
            "coffee_flavor_kind",
            ["mocha", "latte"],
            schema="inventory",
            comment="Coffee flavor enum",
        ),
        EnumTypeSnapshot("local_kind", ["dark"], schema="public"),
    ]


def test_reflect_server_enum_types_skips_non_postgres(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_query_rows_url(rust: type[Any], url: str, sql: str) -> list[list[Any]]:
        pytest.fail("non-PostgreSQL dialects should not query enum catalog tables")

    monkeypatch.setattr(reflection, "_query_rows_url", fail_query_rows_url)

    assert (
        reflection._reflect_server_enum_types(
            object,
            "mysql://localhost/db",
            "mysql",
            None,
        )
        == []
    )


def test_reflect_server_namespaces_preserves_supported_catalogs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_sql: list[str] = []

    def fake_query_rows_url(rust: type[Any], url: str, sql: str) -> list[list[Any]]:
        del rust, url
        captured_sql.append(sql)
        if "pg_namespace" in sql:
            return [["inventory", "Warehouse schema"]]
        if "information_schema.SCHEMATA" in sql:
            return [["inventory_db"]]
        if "sys.schemas" in sql:
            return [["warehouse", "Warehouse schema"]]
        return []

    monkeypatch.setattr(reflection, "_query_rows_url", fake_query_rows_url)

    assert reflection._reflect_server_namespaces(
        object,
        "postgresql://localhost/db",
        "postgresql",
        "inventory",
    ) == [NamespaceSnapshot("inventory", comment="Warehouse schema")]
    assert reflection._reflect_server_namespaces(
        object,
        "mysql://localhost/db",
        "mysql",
        "inventory_db",
    ) == [NamespaceSnapshot("inventory_db")]
    assert reflection._reflect_server_namespaces(
        object,
        "mssql://localhost/db",
        "mssql",
        "warehouse",
    ) == [NamespaceSnapshot("warehouse", comment="Warehouse schema")]

    assert "pg_namespace" in captured_sql[0]
    assert "obj_description" in captured_sql[0]
    assert "information_schema.SCHEMATA" in captured_sql[1]
    assert "sys.schemas" in captured_sql[2]
    assert "sys.extended_properties" in captured_sql[2]


def test_reflect_server_namespaces_includes_extra_schemas(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_sql: list[str] = []

    def fake_query_rows_url(rust: type[Any], url: str, sql: str) -> list[list[Any]]:
        del rust, url
        captured_sql.append(sql)
        return [["analytics"], ["inventory"], ["public"]]

    monkeypatch.setattr(reflection, "_query_rows_url", fake_query_rows_url)

    namespaces = reflection._reflect_server_namespaces(
        object,
        "postgresql://localhost/db",
        "postgresql",
        "public",
        extra_schemas=["inventory", "analytics", "inventory", None],
    )

    assert "nspname = 'public'" in captured_sql[0]
    assert "nspname IN ('analytics', 'inventory')" in captured_sql[0]
    assert namespaces == [
        NamespaceSnapshot("analytics"),
        NamespaceSnapshot("inventory"),
        NamespaceSnapshot("public"),
    ]


def test_reflect_server_namespaces_skips_ambient_schema_without_extra_schemas(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_query_rows_url(rust: type[Any], url: str, sql: str) -> list[list[Any]]:
        pytest.fail("ambient/default schema should not be reflected as owned")

    monkeypatch.setattr(reflection, "_query_rows_url", fail_query_rows_url)

    assert (
        reflection._reflect_server_namespaces(
            object,
            "postgresql://localhost/db",
            "postgresql",
            None,
        )
        == []
    )


def test_reflect_server_namespaces_uses_extra_schemas_without_ambient_schema(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_sql: list[str] = []

    def fake_query_rows_url(rust: type[Any], url: str, sql: str) -> list[list[Any]]:
        del rust, url
        captured_sql.append(sql)
        return [["inventory"]]

    monkeypatch.setattr(reflection, "_query_rows_url", fake_query_rows_url)

    namespaces = reflection._reflect_server_namespaces(
        object,
        "postgresql://localhost/db",
        "postgresql",
        None,
        extra_schemas=["inventory"],
    )

    assert "nspname IN ('inventory')" in captured_sql[0]
    assert "current_schema()" not in captured_sql[0]
    assert namespaces == [NamespaceSnapshot("inventory")]


def test_reflect_server_namespaces_skips_unsupported_dialects(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_query_rows_url(rust: type[Any], url: str, sql: str) -> list[list[Any]]:
        pytest.fail("unsupported dialects should not query namespace catalog tables")

    monkeypatch.setattr(reflection, "_query_rows_url", fail_query_rows_url)

    assert (
        reflection._reflect_server_namespaces(
            object,
            "oracle://localhost/db",
            "oracle",
            None,
        )
        == []
    )


def test_reflect_server_sequences_preserves_sequence_options(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_sql: list[str] = []

    def fake_query_rows_url(rust: type[Any], url: str, sql: str) -> list[list[Any]]:
        del rust, url
        captured_sql.append(sql)
        return [
            [
                "public",
                "flavor_id_seq",
                10,
                5,
                1,
                1000,
                True,
                20,
                True,
                "Flavor ids",
                "bigint",
            ]
        ]

    monkeypatch.setattr(reflection, "_query_rows_url", fake_query_rows_url)

    sequences = reflection._reflect_server_sequences(
        object,
        "postgresql://localhost/db",
        "postgresql",
        "public",
    )

    assert "pg_sequences" in captured_sql[0]
    assert "obj_description" in captured_sql[0]
    assert "ps.data_type::text" in captured_sql[0]
    assert sequences == [
        SequenceSnapshot(
            "flavor_id_seq",
            schema="public",
            data_type="bigint",
            start=10,
            increment=5,
            min_value=1,
            max_value=1000,
            cycle=True,
            cache=20,
            comment="Flavor ids",
        )
    ]
    assert (
        reflection._reflect_server_sequences(
            object,
            "sqlite:///:memory:",
            "sqlite",
            None,
        )
        == []
    )


def test_reflect_server_sequences_preserves_current_schema_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_sql: list[str] = []

    def fake_query_rows_url(rust: type[Any], url: str, sql: str) -> list[list[Any]]:
        del rust, url
        captured_sql.append(sql)
        return [
            [
                "inventory",
                "flavor_id_seq",
                10,
                5,
                1,
                1000,
                False,
                20,
                True,
                "Flavor ids",
                "bigint",
            ]
        ]

    monkeypatch.setattr(reflection, "_query_rows_url", fake_query_rows_url)

    sequences = reflection._reflect_server_sequences(
        object,
        "postgresql://localhost/db",
        "postgresql",
        None,
    )

    assert "current_schema()" in captured_sql[0]
    assert sequences == [
        SequenceSnapshot(
            "flavor_id_seq",
            data_type="bigint",
            start=10,
            increment=5,
            min_value=1,
            max_value=1000,
            cache=20,
            comment="Flavor ids",
        )
    ]


def test_reflect_server_sequences_preserves_mariadb_options(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_sql: list[str] = []

    def fake_query_rows_url(rust: type[Any], url: str, sql: str) -> list[list[Any]]:
        del rust, url
        captured_sql.append(sql)
        if "information_schema.COLUMNS" in sql:
            return [["CACHE_SIZE"], ["DATA_TYPE"]]
        return [["flavor_id_seq", 10, 5, 1, 1000, "YES", 20, "bigint unsigned"]]

    monkeypatch.setattr(reflection, "_query_rows_url", fake_query_rows_url)

    sequences = reflection._reflect_server_sequences(
        object,
        "mariadb://localhost/db",
        "mariadb",
        "inventory",
    )

    assert "information_schema.COLUMNS" in captured_sql[0]
    assert "information_schema.SEQUENCES" in captured_sql[1]
    assert "SEQUENCE_SCHEMA = 'inventory'" in captured_sql[1]
    assert "CYCLE_OPTION" in captured_sql[1]
    assert "DATA_TYPE" in captured_sql[1]
    assert sequences == [
        SequenceSnapshot(
            "flavor_id_seq",
            schema="inventory",
            data_type="bigint unsigned",
            start=10,
            increment=5,
            min_value=1,
            max_value=1000,
            cycle=True,
            cache=20,
        )
    ]


def test_reflect_server_sequences_handles_mariadb_catalog_without_cache(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_sql: list[str] = []

    def fake_query_rows_url(rust: type[Any], url: str, sql: str) -> list[list[Any]]:
        del rust, url
        captured_sql.append(sql)
        if "information_schema.COLUMNS" in sql:
            return [["DATA_TYPE"]]
        return [["flavor_id_seq", 10, 5, 1, 1000, "YES", None, "bigint"]]

    monkeypatch.setattr(reflection, "_query_rows_url", fake_query_rows_url)

    sequences = reflection._reflect_server_sequences(
        object,
        "mariadb://localhost/db",
        "mariadb",
        "inventory",
    )

    assert "NULL, DATA_TYPE" in captured_sql[1]
    assert sequences == [
        SequenceSnapshot(
            "flavor_id_seq",
            schema="inventory",
            data_type="bigint",
            start=10,
            increment=5,
            min_value=1,
            max_value=1000,
            cycle=True,
        )
    ]


def test_reflect_server_sequences_preserves_mssql_options(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_sql: list[str] = []

    def fake_query_rows_url(rust: type[Any], url: str, sql: str) -> list[list[Any]]:
        del rust, url
        captured_sql.append(sql)
        return [["flavor_id_seq", 10, 5, 1, 1000, 1, 20, "Flavor ids", "bigint"]]

    monkeypatch.setattr(reflection, "_query_rows_url", fake_query_rows_url)

    sequences = reflection._reflect_server_sequences(
        object,
        "mssql://localhost/db",
        "mssql",
        "inventory",
    )

    assert "sys.sequences" in captured_sql[0]
    assert "schema_ref.name = 'inventory'" in captured_sql[0]
    assert "CONVERT(nvarchar(80), seq.start_value)" in captured_sql[0]
    assert "CONVERT(nvarchar(80), seq.minimum_value)" in captured_sql[0]
    assert "seq.cache_size" in captured_sql[0]
    assert "sys.extended_properties" in captured_sql[0]
    assert "ep.class = 1" in captured_sql[0]
    assert "MS_Description" in captured_sql[0]
    assert "TYPE_NAME(seq.user_type_id)" in captured_sql[0]
    assert sequences == [
        SequenceSnapshot(
            "flavor_id_seq",
            schema="inventory",
            data_type="bigint",
            start=10,
            increment=5,
            min_value=1,
            max_value=1000,
            cycle=True,
            cache=20,
            comment="Flavor ids",
        )
    ]


def test_reflect_server_sequences_preserves_oracle_options(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_sql: list[str] = []

    def fake_query_rows_url(rust: type[Any], url: str, sql: str) -> list[list[Any]]:
        del rust, url
        captured_sql.append(sql)
        return [["FLAVOR_ID_SEQ", 5, 1, 1000, "Y", 20, "Y"]]

    monkeypatch.setattr(reflection, "_query_rows_url", fake_query_rows_url)

    sequences = reflection._reflect_server_sequences(
        object,
        "oracle://localhost/db",
        "oracle",
        "inventory",
    )

    assert "all_sequences" in captured_sql[0]
    assert "owner = 'INVENTORY'" in captured_sql[0]
    assert "TO_CHAR(min_value)" in captured_sql[0]
    assert "TO_CHAR(max_value)" in captured_sql[0]
    assert "cycle_flag" in captured_sql[0]
    assert "order_flag" in captured_sql[0]
    assert sequences == [
        SequenceSnapshot(
            "FLAVOR_ID_SEQ",
            schema="inventory",
            increment=5,
            min_value=1,
            max_value=1000,
            cycle=True,
            cache=20,
            order=True,
        )
    ]


def test_reflect_server_sequences_includes_cross_schema_table_references(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_sql: list[str] = []

    def fake_query_rows_url(rust: type[Any], url: str, sql: str) -> list[list[Any]]:
        del rust, url
        captured_sql.append(sql)
        return [
            [
                "inventory",
                "flavor_id_seq",
                10,
                5,
                1,
                1000,
                True,
                20,
                False,
                "Inventory flavor ids",
                "bigint",
            ],
            [
                "public",
                "local_id_seq",
                1,
                1,
                1,
                None,
                False,
                None,
                True,
                None,
                "integer",
            ],
        ]

    monkeypatch.setattr(reflection, "_query_rows_url", fake_query_rows_url)

    sequences = reflection._reflect_server_sequences(
        object,
        "postgresql://localhost/db",
        "postgresql",
        "public",
        ["flavor"],
    )

    assert "pg_attrdef" in captured_sql[0]
    assert "rel.relname IN ('flavor')" in captured_sql[0]
    assert "WHERE (EXISTS (" in captured_sql[0]
    assert "WHERE (ps.schemaname = 'public' OR EXISTS" not in captured_sql[0]
    assert sequences == [
        SequenceSnapshot(
            "flavor_id_seq",
            schema="inventory",
            data_type="bigint",
            start=10,
            increment=5,
            min_value=1,
            max_value=1000,
            cycle=True,
            cache=20,
            comment="Inventory flavor ids",
        ),
        SequenceSnapshot(
            "local_id_seq",
            schema="public",
            data_type="integer",
            start=1,
            increment=1,
            min_value=1,
        ),
    ]


def test_reflect_server_sequences_returns_no_sequences_for_empty_scoped_tables(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_query_rows_url(rust: type[Any], url: str, sql: str) -> list[list[Any]]:
        pytest.fail("empty scoped table reflection should not query sequences")

    monkeypatch.setattr(reflection, "_query_rows_url", fake_query_rows_url)

    assert (
        reflection._reflect_server_sequences(
            object,
            "postgresql://localhost/db",
            "postgresql",
            "public",
            [],
        )
        == []
    )


def test_reflect_server_sequences_filters_oracle_system_sequences(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_sql: list[str] = []

    def fake_query_rows_url(rust: type[Any], url: str, sql: str) -> list[list[Any]]:
        del rust, url
        captured_sql.append(sql)
        return [
            ["MVIEW$_ADVSEQ_GENERIC", 1, 1, 999999999, "N", 20, "N"],
            ["ISEQ$$_12345", 1, 1, 999999999, "N", 20, "N"],
            ["FLAVOR_ID_SEQ", 5, 1, 1000, "Y", 20, "Y"],
        ]

    monkeypatch.setattr(reflection, "_query_rows_url", fake_query_rows_url)

    assert reflection._reflect_server_sequences(
        object,
        "oracle://localhost/db",
        "oracle",
        None,
        ["FLAVOR"],
    ) == [
        SequenceSnapshot(
            "FLAVOR_ID_SEQ",
            increment=5,
            min_value=1,
            max_value=1000,
            cycle=True,
            cache=20,
            order=True,
        )
    ]
    assert "user_sequences" in captured_sql[0]


def test_reflect_server_views_preserves_view_definitions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_sql: list[str] = []

    def fake_query_rows_url(rust: type[Any], url: str, sql: str) -> list[list[Any]]:
        del rust, url
        captured_sql.append(sql)
        if "pg_matviews" in sql:
            return [
                [
                    "flavor_summary",
                    "SELECT count(*) AS total FROM flavor;",
                    "Flavor summary",
                ]
            ]
        return [["active_flavors", " SELECT id FROM flavor; ", "Active flavors"]]

    monkeypatch.setattr(reflection, "_query_rows_url", fake_query_rows_url)

    views = reflection._reflect_server_views(
        object,
        "postgresql://localhost/db",
        "postgresql",
        "public",
    )

    assert any("information_schema.views" in sql for sql in captured_sql)
    assert any("pg_matviews" in sql for sql in captured_sql)
    assert any("obj_description" in sql for sql in captured_sql)
    assert views == [
        ViewSnapshot(
            "active_flavors",
            "SELECT id FROM flavor",
            schema="public",
            comment="Active flavors",
        ),
        ViewSnapshot(
            "flavor_summary",
            "SELECT count(*) AS total FROM flavor",
            schema="public",
            materialized=True,
            comment="Flavor summary",
        ),
    ]
    assert reflection._reflect_server_views(
        object,
        "postgresql://localhost/db",
        "postgresql",
        "public",
        include_tables=["active_*"],
    ) == [
        ViewSnapshot(
            "active_flavors",
            "SELECT id FROM flavor",
            schema="public",
            comment="Active flavors",
        )
    ]
    assert reflection._reflect_server_views(
        object,
        "postgresql://localhost/db",
        "postgresql",
        "public",
        exclude_tables=["active_*"],
    ) == [
        ViewSnapshot(
            "flavor_summary",
            "SELECT count(*) AS total FROM flavor",
            schema="public",
            materialized=True,
            comment="Flavor summary",
        )
    ]
    assert (
        reflection._reflect_server_views(
            object,
            "sqlite:///:memory:",
            "sqlite",
            None,
        )
        == []
    )


def test_reflect_server_tables_excludes_oracle_materialized_views(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_sql: list[str] = []

    def fake_query_rows_url(rust: type[Any], url: str, sql: str) -> list[list[Any]]:
        del rust, url
        captured_sql.append(sql)
        return [["flavor"]]

    monkeypatch.setattr(reflection, "_query_rows_url", fake_query_rows_url)

    assert reflection._reflect_server_tables(
        object,
        "oracle://localhost/db",
        "oracle",
        "inventory",
    ) == ["flavor"]

    assert "all_tables t" in captured_sql[0]
    assert "NOT EXISTS" in captured_sql[0]
    assert "all_mviews m" in captured_sql[0]
    assert "m.owner = t.owner" in captured_sql[0]


@pytest.mark.parametrize(
    ("dialect", "schema", "expected_catalog", "expected_schema_filter"),
    [
        (
            "mysql",
            "inventory",
            "information_schema.VIEWS",
            "TABLE_SCHEMA = 'inventory'",
        ),
        (
            "mariadb",
            "inventory",
            "information_schema.VIEWS",
            "TABLE_SCHEMA = 'inventory'",
        ),
        ("mssql", "dbo", "INFORMATION_SCHEMA.VIEWS", "TABLE_SCHEMA = 'dbo'"),
    ],
)
def test_reflect_server_views_preserves_backend_catalog_definitions(
    monkeypatch: pytest.MonkeyPatch,
    dialect: str,
    schema: str,
    expected_catalog: str,
    expected_schema_filter: str,
) -> None:
    captured_sql: list[str] = []

    def fake_query_rows_url(rust: type[Any], url: str, sql: str) -> list[list[Any]]:
        del rust, url
        captured_sql.append(sql)
        return [["active_flavors", "\nSELECT id FROM flavor;\n"]]

    monkeypatch.setattr(reflection, "_query_rows_url", fake_query_rows_url)

    views = reflection._reflect_server_views(
        object,
        f"{dialect}://localhost/db",
        dialect,
        schema,
    )

    assert expected_catalog in captured_sql[0]
    assert expected_schema_filter in captured_sql[0]
    assert "ORDER BY" in captured_sql[0]
    assert "TABLE_NAME" in captured_sql[0]
    if dialect == "mssql":
        assert "sys.extended_properties" in captured_sql[0]
    assert views == [
        ViewSnapshot("active_flavors", "SELECT id FROM flavor", schema=schema)
    ]


def test_reflect_server_views_preserves_oracle_catalog_definitions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_sql: list[str] = []

    def fake_query_rows_url(rust: type[Any], url: str, sql: str) -> list[list[Any]]:
        del rust, url
        captured_sql.append(sql)
        if "all_mviews" in sql:
            return [
                [
                    "FLAVOR_SUMMARY",
                    "SELECT count(*) AS total FROM flavor;",
                    "Flavor summary",
                ]
            ]
        if "all_views" in sql:
            return [["ACTIVE_FLAVORS", " SELECT id FROM flavor; ", "Active flavors"]]
        pytest.fail(f"unexpected view reflection SQL: {sql}")

    monkeypatch.setattr(reflection, "_query_rows_url", fake_query_rows_url)

    views = reflection._reflect_server_views(
        object,
        "oracle://localhost/db",
        "oracle",
        "inventory",
    )

    assert any("all_views" in sql for sql in captured_sql)
    assert any("v.text_vc" in sql for sql in captured_sql)
    assert any("all_tab_comments" in sql for sql in captured_sql)
    assert any("all_mviews" in sql for sql in captured_sql)
    assert any(
        "DBMS_METADATA.GET_DDL('MATERIALIZED_VIEW'" in sql for sql in captured_sql
    )
    assert any("DBMS_LOB.SUBSTR" in sql for sql in captured_sql)
    assert any("all_mview_comments" in sql for sql in captured_sql)
    assert all("owner = 'INVENTORY'" in sql for sql in captured_sql)
    assert views == [
        ViewSnapshot(
            "ACTIVE_FLAVORS",
            "SELECT id FROM flavor",
            schema="inventory",
            comment="Active flavors",
        ),
        ViewSnapshot(
            "FLAVOR_SUMMARY",
            "SELECT count(*) AS total FROM flavor",
            schema="inventory",
            materialized=True,
            comment="Flavor summary",
        ),
    ]


def test_reflect_oracle_materialized_view_definition_extracts_query() -> None:
    ddl = (
        'CREATE MATERIALIZED VIEW "SYSTEM"."FLAVOR_SUMMARY" ("id", "name") '
        "BUILD IMMEDIATE REFRESH FORCE ON DEMAND "
        'AS SELECT "id", "name" FROM "flavor"'
    )

    assert reflection._reflected_oracle_materialized_view_definition(ddl) == (
        'SELECT "id", "name" FROM "flavor"'
    )
    assert (
        reflection._reflected_oracle_materialized_view_definition(
            "SELECT id, name FROM flavor"
        )
        == "SELECT id, name FROM flavor"
    )
    assert reflection._optional_view_comment("oracle", True, "") is None
    assert (
        reflection._optional_view_comment(
            "oracle",
            True,
            "snapshot table for snapshot SYSTEM.FLAVOR_SUMMARY",
        )
        is None
    )
    assert (
        reflection._optional_view_comment("oracle", True, "Flavor summary")
        == "Flavor summary"
    )


def test_reflect_server_columns_preserves_postgres_native_enum_kind(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_sql: list[str] = []

    def fake_query_rows_url(rust: type[Any], url: str, sql: str) -> list[list[Any]]:
        del rust, url
        captured_sql.append(sql)
        return [
            [
                "flavor",
                "flavor",
                "USER-DEFINED",
                "NO",
                None,
                None,
                None,
                2,
                None,
                "ddl_flavor",
            ]
        ]

    monkeypatch.setattr(reflection, "_query_rows_url", fake_query_rows_url)

    columns = reflection._reflect_server_columns(
        object,
        "postgresql://localhost/db",
        "postgresql",
        "public",
        ["flavor"],
    )

    assert "pg_enum" in captured_sql[0]
    assert columns == {
        "flavor": [
            {
                "name": "flavor",
                "kind": "enum:ddl_flavor",
                "nullable": False,
                "max_length": None,
                "server_default": None,
            }
        ]
    }


@pytest.mark.parametrize(
    ("schema", "enum_schema", "table_schema", "expected_kind"),
    [
        (None, "inventory", "inventory", "enum:coffee_flavor_kind"),
        ("public", "inventory", "public", "enum:inventory.coffee_flavor_kind"),
    ],
)
def test_reflect_server_columns_preserves_postgres_native_enum_schema(
    monkeypatch: pytest.MonkeyPatch,
    schema: str | None,
    enum_schema: str,
    table_schema: str,
    expected_kind: str,
) -> None:
    captured_sql: list[str] = []

    def fake_query_rows_url(rust: type[Any], url: str, sql: str) -> list[list[Any]]:
        del rust, url
        captured_sql.append(sql)
        return [
            [
                "flavor",
                "flavor",
                "USER-DEFINED",
                "NO",
                None,
                None,
                None,
                2,
                None,
                "coffee_flavor_kind",
                None,
                "NO",
                None,
                None,
                None,
                "NEVER",
                None,
                enum_schema,
                table_schema,
            ]
        ]

    monkeypatch.setattr(reflection, "_query_rows_url", fake_query_rows_url)

    columns = reflection._reflect_server_columns(
        object,
        "postgresql://localhost/db",
        "postgresql",
        schema,
        ["flavor"],
    )

    assert "c.udt_schema" in captured_sql[0]
    assert "c.table_schema" in captured_sql[0]
    assert columns == {
        "flavor": [
            {
                "name": "flavor",
                "kind": expected_kind,
                "nullable": False,
                "max_length": None,
                "server_default": None,
            }
        ]
    }


@pytest.mark.parametrize(
    ("dialect", "schema", "row", "sql_fragment", "expected"),
    [
        (
            "postgresql",
            "public",
            [
                "flavor",
                "name",
                "character varying",
                "NO",
                32,
                None,
                None,
                1,
                None,
                None,
                "C",
            ],
            "c.collation_name",
            "C",
        ),
        (
            "mysql",
            None,
            [
                "flavor",
                "name",
                "varchar",
                "NO",
                32,
                None,
                None,
                1,
                None,
                "utf8mb4_bin",
            ],
            "collation_name",
            "utf8mb4_bin",
        ),
        (
            "mssql",
            "dbo",
            [
                "flavor",
                "name",
                "nvarchar",
                "NO",
                32,
                None,
                None,
                1,
                None,
                "Latin1_General_CS_AS",
            ],
            "COLLATION_NAME",
            "Latin1_General_CS_AS",
        ),
        (
            "oracle",
            None,
            [
                "FLAVOR",
                "NAME",
                "VARCHAR2",
                "N",
                32,
                None,
                None,
                1,
                None,
                "BINARY_CI",
            ],
            "collation",
            "BINARY_CI",
        ),
    ],
)
def test_reflect_server_columns_preserves_collation_metadata(
    monkeypatch: pytest.MonkeyPatch,
    dialect: str,
    schema: str | None,
    row: list[Any],
    sql_fragment: str,
    expected: str,
) -> None:
    captured_sql: list[str] = []

    def fake_query_rows_url(rust: type[Any], url: str, sql: str) -> list[list[Any]]:
        del rust, url
        captured_sql.append(sql)
        return [row]

    monkeypatch.setattr(reflection, "_query_rows_url", fake_query_rows_url)

    columns = reflection._reflect_server_columns(
        object,
        f"{dialect}://localhost/db",
        dialect,
        schema,
        [str(row[0])],
    )

    assert sql_fragment in captured_sql[0]
    assert columns[str(row[0])][0]["collation"] == expected


def test_reflect_server_columns_ignores_integer_numeric_shape(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        reflection,
        "_query_rows_url",
        lambda rust, url, sql: [
            [
                "flavor",
                "rating",
                "integer",
                "NO",
                None,
                32,
                0,
                1,
                None,
            ],
            [
                "flavor",
                "price",
                "numeric",
                "NO",
                None,
                12,
                2,
                2,
                None,
            ],
        ],
    )

    columns = reflection._reflect_server_columns(
        object,
        "postgresql://localhost/db",
        "postgresql",
        "public",
        ["flavor"],
    )

    rating, price = columns["flavor"]
    assert rating["kind"] == "int"
    assert "numeric_precision" not in rating
    assert "numeric_scale" not in rating
    assert price["kind"] == "decimal"
    assert price["numeric_precision"] == 12
    assert price["numeric_scale"] == 2


@pytest.mark.parametrize(
    ("dialect", "schema", "row", "sql_fragment", "expected"),
    [
        (
            "postgresql",
            "public",
            [
                "flavor",
                "id",
                "integer",
                "NO",
                None,
                None,
                None,
                1,
                None,
                None,
                None,
                "YES",
                "ALWAYS",
                "10",
                "5",
                None,
                None,
                None,
                "public",
                "1",
                "1000",
                "YES",
                "20",
            ],
            "c.is_identity",
            {
                "identity": True,
                "identity_always": True,
                "identity_start": 10,
                "identity_increment": 5,
                "identity_min_value": 1,
                "identity_max_value": 1000,
                "identity_cycle": True,
                "identity_cache": 20,
            },
        ),
        (
            "mysql",
            None,
            [
                "flavor",
                "id",
                "int",
                "NO",
                None,
                10,
                0,
                1,
                None,
                None,
                "auto_increment",
            ],
            "extra",
            {"autoincrement": True},
        ),
        (
            "mariadb",
            None,
            [
                "flavor",
                "id",
                "int",
                "NO",
                None,
                10,
                0,
                1,
                None,
                None,
                "auto_increment",
            ],
            "extra",
            {"autoincrement": True},
        ),
        (
            "mssql",
            "dbo",
            [
                "flavor",
                "id",
                "int",
                "NO",
                None,
                10,
                0,
                1,
                None,
                None,
                1,
                10,
                5,
            ],
            "COLUMNPROPERTY",
            {"identity": True, "identity_start": 10, "identity_increment": 5},
        ),
        (
            "oracle",
            None,
            [
                "FLAVOR",
                "ID",
                "NUMBER",
                "N",
                None,
                10,
                0,
                1,
                None,
                None,
                "BY DEFAULT ON NULL",
                "NO",
                "START WITH: 10, INCREMENT BY: 5, MIN_VALUE: 1, "
                "MAX_VALUE: 1000, CYCLE_FLAG: Y, CACHE_SIZE: 20, ORDER_FLAG: Y",
            ],
            "generation_type",
            {
                "identity": True,
                "identity_always": False,
                "identity_on_null": True,
                "identity_start": 10,
                "identity_increment": 5,
                "identity_min_value": 1,
                "identity_max_value": 1000,
                "identity_cycle": True,
                "identity_cache": 20,
                "identity_order": True,
            },
        ),
    ],
)
def test_reflect_server_columns_preserves_identity_metadata(
    monkeypatch: pytest.MonkeyPatch,
    dialect: str,
    schema: str | None,
    row: list[Any],
    sql_fragment: str,
    expected: dict[str, object],
) -> None:
    captured_sql: list[str] = []

    def fake_query_rows_url(rust: type[Any], url: str, sql: str) -> list[list[Any]]:
        del rust, url
        captured_sql.append(sql)
        return [row]

    monkeypatch.setattr(reflection, "_query_rows_url", fake_query_rows_url)

    columns = reflection._reflect_server_columns(
        object,
        f"{dialect}://localhost/db",
        dialect,
        schema,
        [str(row[0])],
    )

    assert sql_fragment in captured_sql[0]
    for key, value in expected.items():
        assert columns[str(row[0])][0][key] == value


def test_oracle_identity_options_skip_disabled_flags() -> None:
    options = reflection._reflected_oracle_identity_sequence_options(
        "START WITH: 1, INCREMENT BY: 1, MIN_VALUE: 1, MAX_VALUE: 1000, "
        "CYCLE_FLAG: N, CACHE_SIZE: 0, ORDER_FLAG: N"
    )

    assert options == {
        "identity_start": 1,
        "identity_increment": 1,
        "identity_min_value": 1,
        "identity_max_value": 1000,
    }


def test_oracle_identity_options_preserve_no_bound_flags() -> None:
    options = reflection._reflected_oracle_identity_sequence_options(
        "START WITH: 10, INCREMENT BY: 5, NOMINVALUE, NOMAXVALUE"
    )

    assert options == {
        "identity_start": 10,
        "identity_increment": 5,
        "identity_no_min_value": True,
        "identity_no_max_value": True,
    }


@pytest.mark.parametrize(
    ("dialect", "schema", "row", "sql_fragment", "expected"),
    [
        (
            "postgresql",
            "public",
            [
                "flavor",
                "name_lower",
                "text",
                "YES",
                None,
                None,
                None,
                2,
                None,
                None,
                None,
                "NO",
                None,
                None,
                None,
                "ALWAYS",
                "lower(name)",
            ],
            "c.generation_expression",
            {"computed": "lower(name)", "computed_persisted": True},
        ),
        (
            "mysql",
            None,
            [
                "flavor",
                "name_lower",
                "varchar",
                "YES",
                255,
                None,
                None,
                2,
                None,
                None,
                "STORED GENERATED",
                "lower(`name`)",
            ],
            "generation_expression",
            {"computed": "lower(`name`)", "computed_persisted": True},
        ),
        (
            "mariadb",
            None,
            [
                "flavor",
                "name_lower",
                "varchar",
                "YES",
                255,
                None,
                None,
                2,
                None,
                None,
                "VIRTUAL GENERATED",
                "lower(`name`)",
            ],
            "generation_expression",
            {"computed": "lower(`name`)", "computed_persisted": False},
        ),
        (
            "mssql",
            "dbo",
            [
                "flavor",
                "name_lower",
                "nvarchar",
                "YES",
                255,
                None,
                None,
                2,
                None,
                None,
                0,
                None,
                None,
                "(lower([name]))",
                1,
            ],
            "sys.computed_columns",
            {"computed": "lower([name])", "computed_persisted": True},
        ),
        (
            "oracle",
            None,
            [
                "FLAVOR",
                "NAME_LOWER",
                "VARCHAR2",
                "Y",
                255,
                None,
                None,
                2,
                'LOWER("NAME")',
                None,
                None,
                "YES",
            ],
            "virtual_column",
            {"computed": 'LOWER("NAME")', "server_default": None},
        ),
    ],
)
def test_reflect_server_columns_preserves_computed_metadata(
    monkeypatch: pytest.MonkeyPatch,
    dialect: str,
    schema: str | None,
    row: list[Any],
    sql_fragment: str,
    expected: dict[str, object],
) -> None:
    captured_sql: list[str] = []

    def fake_query_rows_url(rust: type[Any], url: str, sql: str) -> list[list[Any]]:
        del rust, url
        captured_sql.append(sql)
        return [row]

    monkeypatch.setattr(reflection, "_query_rows_url", fake_query_rows_url)

    columns = reflection._reflect_server_columns(
        object,
        f"{dialect}://localhost/db",
        dialect,
        schema,
        [str(row[0])],
    )

    assert sql_fragment in captured_sql[0]
    for key, value in expected.items():
        assert columns[str(row[0])][0][key] == value


def test_reflect_server_columns_ignores_empty_mysql_generation_expression(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    row = [
        "flavor",
        "name",
        "varchar",
        "NO",
        255,
        None,
        None,
        2,
        None,
        None,
        "",
        "",
    ]

    monkeypatch.setattr(
        reflection,
        "_query_rows_url",
        lambda rust, url, sql: [row],
    )

    columns = reflection._reflect_server_columns(
        object,
        "mysql://localhost/db",
        "mysql",
        None,
        ["flavor"],
    )

    assert "computed" not in columns["flavor"][0]
    assert columns["flavor"][0]["server_default"] is None


def test_reflect_server_columns_uses_oracle_default_vc_projection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_sql: list[str] = []

    def fake_query_rows_url(rust: type[Any], url: str, sql: str) -> list[list[Any]]:
        del rust, url
        captured_sql.append(sql)
        return []

    monkeypatch.setattr(reflection, "_query_rows_url", fake_query_rows_url)

    reflection._reflect_server_columns(
        object,
        "oracle://localhost/db",
        "oracle",
        None,
        ["flavor"],
    )

    assert "c.data_default_vc" in captured_sql[0]
    assert "c.data_default, c.collation" not in captured_sql[0]


def test_reflect_server_indexes_preserves_postgres_advanced_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_sql: list[str] = []

    def fake_query_rows_url(rust: type[Any], url: str, sql: str) -> list[list[Any]]:
        del rust, url
        captured_sql.append(sql)
        return [
            [
                "flavor",
                "flavor_name_idx",
                True,
                "name",
                1,
                "name IS NOT NULL",
                "btree",
                "name",
                True,
                "fillfactor=70",
                "Flavor lookup index",
                "fastspace",
                "text_pattern_ops",
                True,
            ],
            [
                "flavor",
                "flavor_name_idx",
                True,
                None,
                2,
                "name IS NOT NULL",
                "btree",
                "lower(name)",
                True,
                "fillfactor=70",
                "Flavor lookup index",
                "fastspace",
                "pg_catalog.text_pattern_ops",
                True,
            ],
            [
                "flavor",
                "flavor_name_idx",
                True,
                "rating",
                3,
                "name IS NOT NULL",
                "btree",
                "rating",
                False,
                "fillfactor=70",
                "Flavor lookup index",
                "fastspace",
                None,
                True,
            ],
        ]

    monkeypatch.setattr(reflection, "_query_rows_url", fake_query_rows_url)

    indexes = reflection._reflect_server_indexes(
        object, "postgresql://localhost/db", "postgresql", "public", ["flavor"]
    )

    assert "pg_get_expr(ix.indpred" in captured_sql[0]
    assert "pg_options_to_table(i.reloptions)" in captured_sql[0]
    assert "obj_description" in captured_sql[0]
    assert "pg_tablespace" in captured_sql[0]
    assert "pg_opclass" in captured_sql[0]
    assert "indnullsnotdistinct" in captured_sql[0]
    assert indexes == {
        "flavor": [
            IndexSnapshot(
                "flavor_name_idx",
                ["name"],
                unique=True,
                where="name IS NOT NULL",
                include_columns=["rating"],
                expressions=["lower(name)"],
                postgres_with=[("fillfactor", "70")],
                postgres_ops={
                    "name": "text_pattern_ops",
                    "lower(name)": "pg_catalog.text_pattern_ops",
                },
                postgres_nulls_not_distinct=True,
                comment="Flavor lookup index",
                postgres_tablespace="fastspace",
            )
        ]
    }


@pytest.mark.parametrize("dialect", ["mysql", "mariadb"])
def test_reflect_server_indexes_preserves_mysql_index_comments(
    monkeypatch: pytest.MonkeyPatch,
    dialect: str,
) -> None:
    captured_sql: list[str] = []

    def fake_query_rows_url(rust: type[Any], url: str, sql: str) -> list[list[Any]]:
        del rust, url
        captured_sql.append(sql)
        if dialect == "mysql":
            return [
                [
                    "flavor",
                    "flavor_name_idx",
                    False,
                    "name",
                    1,
                    "Flavor lookup index",
                    12,
                    "HASH",
                    "NO",
                ],
                [
                    "flavor",
                    "flavor_code_idx",
                    False,
                    "code",
                    1,
                    "",
                    None,
                    "BTREE",
                    "YES",
                ],
                [
                    "flavor",
                    "flavor_search_idx",
                    False,
                    "name",
                    1,
                    "",
                    None,
                    "FULLTEXT",
                    "YES",
                ],
            ]
        return [
            [
                "flavor",
                "flavor_name_idx",
                False,
                "name",
                1,
                "Flavor lookup index",
                12,
                "HASH",
            ],
            [
                "flavor",
                "flavor_code_idx",
                False,
                "code",
                1,
                "",
                None,
                "BTREE",
            ],
            [
                "flavor",
                "flavor_search_idx",
                False,
                "name",
                1,
                "",
                None,
                "FULLTEXT",
            ],
        ]

    monkeypatch.setattr(reflection, "_query_rows_url", fake_query_rows_url)

    indexes = reflection._reflect_server_indexes(
        object, f"{dialect}://localhost/db", dialect, None, ["flavor"]
    )

    assert "index_comment" in captured_sql[0]
    assert "sub_part" in captured_sql[0]
    assert "index_type" in captured_sql[0]
    assert "information_schema.table_constraints" in captured_sql[0]
    assert "tc.constraint_name = s.index_name" in captured_sql[0]
    if dialect == "mysql":
        assert "is_visible" in captured_sql[0]
    else:
        assert "is_visible" not in captured_sql[0]
    flavor_name_index = IndexSnapshot(
        "flavor_name_idx",
        ["name"],
        comment="Flavor lookup index",
        mysql_length={"name": 12},
        mysql_using="HASH",
        mysql_visible=False if dialect == "mysql" else None,
    )
    assert indexes == {
        "flavor": [
            flavor_name_index,
            IndexSnapshot("flavor_code_idx", ["code"]),
            IndexSnapshot(
                "flavor_search_idx",
                ["name"],
                mysql_prefix="FULLTEXT",
            ),
        ]
    }


def test_reflect_server_indexes_preserves_oracle_tablespaces(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_sql: list[str] = []

    def fake_query_rows_url(rust: type[Any], url: str, sql: str) -> list[list[Any]]:
        del rust, url
        captured_sql.append(sql)
        if "default_tablespace" in sql:
            return [["USERS"]]
        return [
            [
                "FLAVOR",
                "FLAVOR_NAME_IDX",
                0,
                "NAME",
                1,
                "ORACLESPACE",
                "BITMAP",
                "ENABLED",
                2,
            ]
        ]

    monkeypatch.setattr(reflection, "_query_rows_url", fake_query_rows_url)

    indexes = reflection._reflect_server_indexes(
        object, "oracle://localhost/db", "oracle", "inventory", ["FLAVOR"]
    )

    assert "all_users" in captured_sql[0]
    assert "all_indexes" in captured_sql[1]
    assert "i.tablespace_name" in captured_sql[1]
    assert "i.index_type" in captured_sql[1]
    assert "i.compression" in captured_sql[1]
    assert "i.prefix_length" in captured_sql[1]
    assert "i.owner = 'INVENTORY'" in captured_sql[1]
    assert indexes == {
        "FLAVOR": [
            IndexSnapshot(
                "FLAVOR_NAME_IDX",
                ["NAME"],
                oracle_tablespace="ORACLESPACE",
                oracle_bitmap=True,
                oracle_compress=2,
            )
        ]
    }


def test_reflect_server_unique_constraints_preserves_names(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_sql: list[str] = []

    def fake_query_rows_url(rust: type[Any], url: str, sql: str) -> list[list[Any]]:
        del rust, url
        captured_sql.append(sql)
        return [
            [
                "flavor",
                "flavor_name_code_unique",
                "name",
                1,
                "YES",
                "YES",
                True,
                "Flavor identity",
                "rating",
            ],
            [
                "flavor",
                "flavor_name_code_unique",
                "code",
                2,
                "YES",
                "YES",
                True,
                "Flavor identity",
                "rating",
            ],
        ]

    monkeypatch.setattr(reflection, "_query_rows_url", fake_query_rows_url)

    constraints = reflection._reflect_server_unique_constraints(
        object, "postgresql://localhost/db", "postgresql", "public", ["flavor"]
    )

    assert "constraint_name" in captured_sql[0]
    assert "indnullsnotdistinct" in captured_sql[0]
    assert "obj_description" in captured_sql[0]
    assert "indnkeyatts" in captured_sql[0]
    assert "include_columns" in captured_sql[0]
    assert constraints == {
        "flavor": [
            UniqueConstraintSnapshot(
                "flavor_name_code_unique",
                ["name", "code"],
                deferrable=True,
                initially_deferred=True,
                nulls_not_distinct=True,
                comment="Flavor identity",
                postgres_include=["rating"],
            )
        ]
    }


def test_reflect_server_unique_constraints_ignores_default_not_deferrable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        reflection,
        "_query_rows_url",
        lambda rust, url, sql: [
            [
                "flavor",
                "flavor_code_unique",
                "code",
                1,
                "NO",
                "NO",
                None,
                None,
                None,
            ]
        ],
    )

    constraints = reflection._reflect_server_unique_constraints(
        object,
        "postgresql://localhost/db",
        "postgresql",
        "public",
        ["flavor"],
    )

    assert constraints == {
        "flavor": [UniqueConstraintSnapshot("flavor_code_unique", ["code"])]
    }


def test_reflect_server_unique_constraints_preserves_mssql_comments(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_sql: list[str] = []

    def fake_query_rows_url(rust: type[Any], url: str, sql: str) -> list[list[Any]]:
        del rust, url
        captured_sql.append(sql)
        return [
            [
                "flavor",
                "flavor_name_code_unique",
                "name",
                1,
                None,
                None,
                None,
                "Flavor identity",
                None,
                "constraintspace",
                True,
            ],
            [
                "flavor",
                "flavor_name_code_unique",
                "code",
                2,
                None,
                None,
                None,
                "Flavor identity",
                None,
                "constraintspace",
                True,
            ],
        ]

    monkeypatch.setattr(reflection, "_query_rows_url", fake_query_rows_url)

    constraints = reflection._reflect_server_unique_constraints(
        object, "mssql://localhost/db", "mssql", "dbo", ["flavor"]
    )

    assert "sys.key_constraints" in captured_sql[0]
    assert "sys.indexes" in captured_sql[0]
    assert "sys.filegroups" in captured_sql[0]
    assert "unique_index_id" in captured_sql[0]
    assert "mssql_filegroup" in captured_sql[0]
    assert "mssql_clustered" in captured_sql[0]
    assert "sys.extended_properties" in captured_sql[0]
    assert "ep.class = 1" in captured_sql[0]
    assert "MS_Description" in captured_sql[0]
    assert constraints == {
        "flavor": [
            UniqueConstraintSnapshot(
                "flavor_name_code_unique",
                ["name", "code"],
                comment="Flavor identity",
                mssql_filegroup="constraintspace",
                mssql_clustered=True,
            )
        ]
    }


def test_reflect_server_unique_constraints_preserves_oracle_deferrable_timing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_sql: list[str] = []

    def fake_query_rows_url(rust: type[Any], url: str, sql: str) -> list[list[Any]]:
        del rust, url
        captured_sql.append(sql)
        if "default_tablespace" in sql:
            return [["USERS"]]
        return [
            [
                "FLAVOR",
                "FLAVOR_NAME_UNIQUE",
                "NAME",
                1,
                "DEFERRABLE",
                "DEFERRED",
                None,
                None,
                None,
                None,
                None,
                "CONSTRAINTSPACE",
                "ENABLED",
                2,
            ]
        ]

    monkeypatch.setattr(reflection, "_query_rows_url", fake_query_rows_url)

    constraints = reflection._reflect_server_unique_constraints(
        object,
        "oracle://localhost/db",
        "oracle",
        "inventory",
        ["FLAVOR"],
    )

    assert "all_users" in captured_sql[0]
    assert "user_constraints" not in captured_sql[1]
    assert "all_constraints" in captured_sql[1]
    assert "all_indexes" in captured_sql[1]
    assert "c.deferrable" in captured_sql[1]
    assert "c.deferred" in captured_sql[1]
    assert "i.tablespace_name" in captured_sql[1]
    assert "i.compression" in captured_sql[1]
    assert "i.prefix_length" in captured_sql[1]
    assert "CAST(NULL AS NUMBER)" in captured_sql[1]
    assert "CAST(NULL AS VARCHAR2(1))" in captured_sql[1]
    assert "c.owner = 'INVENTORY'" in captured_sql[1]
    assert constraints == {
        "FLAVOR": [
            UniqueConstraintSnapshot(
                "FLAVOR_NAME_UNIQUE",
                ["NAME"],
                deferrable=True,
                initially_deferred=True,
                oracle_tablespace="CONSTRAINTSPACE",
                oracle_compress=2,
            )
        ]
    }


def test_split_reflected_unique_constraints_preserves_table_only_single_column_metadata() -> (
    None
):
    ordinary = UniqueConstraintSnapshot("flavor_code_unique", ["code"])
    nulls_not_distinct = UniqueConstraintSnapshot(
        "flavor_name_unique",
        ["name"],
        nulls_not_distinct=True,
    )
    deferrable = UniqueConstraintSnapshot(
        "flavor_slug_unique",
        ["slug"],
        deferrable=True,
        initially_deferred=True,
    )
    commented = UniqueConstraintSnapshot(
        "flavor_sku_unique",
        ["sku"],
        comment="SKU identity",
    )
    covered = UniqueConstraintSnapshot(
        "flavor_barcode_unique",
        ["barcode"],
        postgres_include=["supplier_id"],
    )
    composite = UniqueConstraintSnapshot(
        "flavor_supplier_code_unique",
        ["supplier_id", "code"],
    )

    unique_columns, table_constraints = reflection._split_reflected_unique_constraints(
        [ordinary, nulls_not_distinct, deferrable, commented, covered, composite]
    )

    assert unique_columns == {"code"}
    assert table_constraints == [
        nulls_not_distinct,
        deferrable,
        commented,
        covered,
        composite,
    ]


def test_reflect_server_foreign_keys_preserves_names_and_actions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_sql: list[str] = []

    def fake_query_rows_url(rust: type[Any], url: str, sql: str) -> list[list[Any]]:
        del rust, url
        captured_sql.append(sql)
        return [
            [
                "flavor",
                "supplier_id",
                "supplier",
                "id",
                "flavor_supplier_fk",
                "SET NULL",
                "CASCADE",
                1,
                "YES",
                "YES",
                True,
                "SIMPLE",
            ]
        ]

    monkeypatch.setattr(reflection, "_query_rows_url", fake_query_rows_url)

    foreign_keys = reflection._reflect_server_foreign_keys(
        object,
        "postgresql://localhost/db",
        "postgresql",
        "public",
        ["flavor"],
    )

    assert "referential_constraints" in captured_sql[0]
    assert "match_option" in captured_sql[0]
    assert foreign_keys == {
        "flavor": {
            "supplier_id": {
                "foreign_table": "supplier",
                "foreign_column": "id",
                "name": "flavor_supplier_fk",
                "on_delete": "set_null",
                "on_update": "cascade",
                "deferrable": True,
                "initially_deferred": True,
            }
        }
    }


def test_reflect_server_foreign_keys_ignores_default_not_deferrable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        reflection,
        "_query_rows_url",
        lambda rust, url, sql: [
            [
                "flavor",
                "supplier_id",
                "supplier",
                "id",
                "flavor_supplier_fk",
                "NO ACTION",
                "NO ACTION",
                1,
                "NO",
                "NO",
                True,
                "SIMPLE",
                "public",
                None,
                "public",
            ]
        ],
    )

    constraints = reflection._reflect_server_foreign_key_constraints(
        object,
        "postgresql://localhost/db",
        "postgresql",
        "public",
        ["flavor"],
        include_single_column=True,
    )

    assert constraints == {
        "flavor": [
            ForeignKeyConstraintSnapshot(
                "flavor_supplier_fk",
                ["supplier_id"],
                "supplier",
                ["id"],
            )
        ]
    }


def test_split_reflected_foreign_keys_preserves_table_only_single_column_metadata() -> (
    None
):
    ordinary = ForeignKeyConstraintSnapshot(
        "flavor_supplier_fk",
        ["supplier_id"],
        "supplier",
        ["id"],
        on_delete="set_null",
        on_update="cascade",
        deferrable=True,
        initially_deferred=True,
    )
    not_valid = ForeignKeyConstraintSnapshot(
        "flavor_roaster_fk",
        ["roaster_id"],
        "roaster",
        ["id"],
        validated=False,
    )
    match_full = ForeignKeyConstraintSnapshot(
        "flavor_origin_fk",
        ["origin_id"],
        "origin",
        ["id"],
        match="full",
    )
    composite = ForeignKeyConstraintSnapshot(
        "flavor_supplier_pair_fk",
        ["supplier_id", "supplier_code"],
        "supplier",
        ["id", "code"],
    )

    column_foreign_keys, table_constraints = (
        reflection._split_reflected_foreign_key_constraints(
            {"flavor": [ordinary, not_valid, match_full, composite]}
        )
    )

    assert column_foreign_keys == {
        "flavor": {
            "supplier_id": {
                "foreign_table": "supplier",
                "foreign_column": "id",
                "name": "flavor_supplier_fk",
                "on_delete": "set_null",
                "on_update": "cascade",
                "deferrable": True,
                "initially_deferred": True,
            }
        }
    }
    assert table_constraints == {
        "flavor": [
            not_valid,
            match_full,
            composite,
        ]
    }


def test_reflected_foreign_table_name_qualifies_only_cross_schema() -> None:
    assert (
        reflection._reflected_foreign_table_name(
            "postgresql",
            "supplier",
            "public",
            "public",
        )
        == "supplier"
    )
    assert (
        reflection._reflected_foreign_table_name(
            "postgresql",
            "supplier",
            "shared",
            "public",
        )
        == "shared.supplier"
    )
    assert (
        reflection._reflected_foreign_table_name(
            "mssql",
            "supplier",
            "Shared",
            "shared",
        )
        == "supplier"
    )
    assert (
        reflection._reflected_foreign_table_name(
            "oracle",
            "SUPPLIER",
            "SHARED",
            "inventory",
        )
        == "SHARED.SUPPLIER"
    )


def test_reflect_server_foreign_key_constraints_preserves_composite_keys(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_sql: list[str] = []

    def fake_query_rows_url(rust: type[Any], url: str, sql: str) -> list[list[Any]]:
        del rust, url
        captured_sql.append(sql)
        return [
            [
                "flavor",
                "supplier_id",
                "supplier",
                "id",
                "flavor_supplier_pair_fk",
                "CASCADE",
                "NO ACTION",
                1,
                "YES",
                "NO",
                False,
                "FULL",
                "shared",
                "Supplier pair lookup",
            ],
            [
                "flavor",
                "supplier_code",
                "supplier",
                "code",
                "flavor_supplier_pair_fk",
                "CASCADE",
                "NO ACTION",
                2,
                "YES",
                "NO",
                False,
                "FULL",
                "shared",
                "Supplier pair lookup",
            ],
            [
                "flavor",
                "roaster_id",
                "roaster",
                "id",
                "flavor_roaster_fk",
                "NO ACTION",
                "NO ACTION",
                1,
                None,
                None,
                None,
                None,
                "public",
                None,
            ],
        ]

    monkeypatch.setattr(reflection, "_query_rows_url", fake_query_rows_url)

    constraints = reflection._reflect_server_foreign_key_constraints(
        object,
        "postgresql://localhost/db",
        "postgresql",
        "public",
        ["flavor"],
    )

    assert "position_in_unique_constraint" in captured_sql[0]
    assert "convalidated" in captured_sql[0]
    assert "match_option" in captured_sql[0]
    assert "rkcu.table_schema" in captured_sql[0]
    assert "obj_description" in captured_sql[0]
    assert constraints == {
        "flavor": [
            ForeignKeyConstraintSnapshot(
                "flavor_supplier_pair_fk",
                ["supplier_id", "supplier_code"],
                "shared.supplier",
                ["id", "code"],
                on_delete="cascade",
                deferrable=True,
                initially_deferred=False,
                validated=False,
                match="full",
                comment="Supplier pair lookup",
            )
        ]
    }


def test_reflect_server_foreign_key_constraints_preserves_mssql_validation_and_comments(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_sql: list[str] = []

    def fake_query_rows_url(rust: type[Any], url: str, sql: str) -> list[list[Any]]:
        del rust, url
        captured_sql.append(sql)
        return [
            [
                "flavor",
                "supplier_id",
                "supplier",
                "id",
                "flavor_supplier_fk",
                "CASCADE",
                "NO ACTION",
                1,
                None,
                None,
                0,
                None,
                "shared",
                "Supplier lookup",
            ]
        ]

    monkeypatch.setattr(reflection, "_query_rows_url", fake_query_rows_url)

    constraints = reflection._reflect_server_foreign_key_constraints(
        object,
        "mssql://localhost/db",
        "mssql",
        "dbo",
        ["flavor"],
        include_single_column=True,
    )

    assert "fk.is_not_trusted" in captured_sql[0]
    assert "sys.extended_properties" in captured_sql[0]
    assert "ep.class = 1" in captured_sql[0]
    assert "MS_Description" in captured_sql[0]
    assert constraints == {
        "flavor": [
            ForeignKeyConstraintSnapshot(
                "flavor_supplier_fk",
                ["supplier_id"],
                "shared.supplier",
                ["id"],
                on_delete="cascade",
                validated=False,
                comment="Supplier lookup",
            )
        ]
    }


@pytest.mark.parametrize(
    ("dialect", "url", "expected_sql"),
    [
        ("mysql", "mysql://localhost/db", "kcu.referenced_table_schema"),
        ("mariadb", "mariadb://localhost/db", "kcu.referenced_table_schema"),
        ("mssql", "mssql://localhost/db", "referenced_schema.name"),
    ],
)
def test_reflect_server_foreign_key_constraints_preserves_cross_schema_references(
    monkeypatch: pytest.MonkeyPatch,
    dialect: str,
    url: str,
    expected_sql: str,
) -> None:
    captured_sql: list[str] = []

    def fake_query_rows_url(rust: type[Any], url: str, sql: str) -> list[list[Any]]:
        del rust, url
        captured_sql.append(sql)
        return [
            [
                "flavor",
                "supplier_id",
                "supplier",
                "id",
                "flavor_supplier_pair_fk",
                "CASCADE",
                "NO ACTION",
                1,
                None,
                None,
                None,
                None,
                "shared",
            ],
            [
                "flavor",
                "supplier_code",
                "supplier",
                "code",
                "flavor_supplier_pair_fk",
                "CASCADE",
                "NO ACTION",
                2,
                None,
                None,
                None,
                None,
                "shared",
            ],
        ]

    monkeypatch.setattr(reflection, "_query_rows_url", fake_query_rows_url)

    constraints = reflection._reflect_server_foreign_key_constraints(
        object,
        url,
        dialect,
        "inventory",
        ["flavor"],
    )

    assert expected_sql in captured_sql[0]
    assert constraints == {
        "flavor": [
            ForeignKeyConstraintSnapshot(
                "flavor_supplier_pair_fk",
                ["supplier_id", "supplier_code"],
                "shared.supplier",
                ["id", "code"],
                on_delete="cascade",
            )
        ]
    }


@pytest.mark.parametrize(
    ("dialect", "url", "local_schema"),
    [
        ("mysql", "mysql://localhost/db", "mysql"),
        ("mariadb", "mariadb://localhost/db", "mariadb"),
        ("mssql", "mssql://localhost/db", "dbo"),
    ],
)
def test_reflect_server_foreign_key_constraints_omits_same_reflected_schema(
    monkeypatch: pytest.MonkeyPatch,
    dialect: str,
    url: str,
    local_schema: str,
) -> None:
    def fake_query_rows_url(rust: type[Any], url: str, sql: str) -> list[list[Any]]:
        del rust, url, sql
        return [
            [
                "flavor",
                "supplier_id",
                "supplier",
                "id",
                "flavor_supplier_fk",
                "NO ACTION",
                "NO ACTION",
                1,
                None,
                None,
                None,
                None,
                local_schema,
                None,
                local_schema,
            ]
        ]

    monkeypatch.setattr(reflection, "_query_rows_url", fake_query_rows_url)

    constraints = reflection._reflect_server_foreign_key_constraints(
        object,
        url,
        dialect,
        None,
        ["flavor"],
        include_single_column=True,
    )

    assert constraints["flavor"][0].foreign_table == "supplier"


def test_reflect_server_foreign_key_constraints_preserves_oracle_validation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_sql: list[str] = []

    def fake_query_rows_url(rust: type[Any], url: str, sql: str) -> list[list[Any]]:
        del rust, url
        captured_sql.append(sql)
        return [
            [
                "FLAVOR",
                "SUPPLIER_ID",
                "SUPPLIER",
                "ID",
                "FLAVOR_SUPPLIER_PAIR_FK",
                "CASCADE",
                None,
                1,
                "DEFERRABLE",
                "IMMEDIATE",
                "NOT VALIDATED",
                None,
                "SUPPLY",
            ],
            [
                "FLAVOR",
                "SUPPLIER_CODE",
                "SUPPLIER",
                "CODE",
                "FLAVOR_SUPPLIER_PAIR_FK",
                "CASCADE",
                None,
                2,
                "DEFERRABLE",
                "IMMEDIATE",
                "NOT VALIDATED",
                None,
                "SUPPLY",
            ],
        ]

    monkeypatch.setattr(reflection, "_query_rows_url", fake_query_rows_url)

    constraints = reflection._reflect_server_foreign_key_constraints(
        object,
        "oracle://localhost/db",
        "oracle",
        "inventory",
        ["FLAVOR"],
    )

    assert "c.validated" in captured_sql[0]
    assert "CAST(NULL AS VARCHAR2(1))" in captured_sql[0]
    assert "rc.owner" in captured_sql[0]
    assert "owner = 'INVENTORY'" in captured_sql[0]
    assert constraints == {
        "FLAVOR": [
            ForeignKeyConstraintSnapshot(
                "FLAVOR_SUPPLIER_PAIR_FK",
                ["SUPPLIER_ID", "SUPPLIER_CODE"],
                "SUPPLY.SUPPLIER",
                ["ID", "CODE"],
                on_delete="cascade",
                deferrable=True,
                initially_deferred=False,
                validated=False,
            )
        ]
    }


def test_reflect_server_exclusion_constraints_preserves_postgres_definition(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_sql: list[str] = []

    def fake_query_rows_url(rust: type[Any], url: str, sql: str) -> list[list[Any]]:
        del rust, url
        captured_sql.append(sql)
        return [
            [
                "booking",
                "booking_room_overlap",
                (
                    'EXCLUDE USING gist ("room_id" gist_int4_ops WITH =, '
                    "during gist_tstzrange_ops WITH &&, "
                    "lower(status) text_ops WITH <>) "
                    "WHERE ((cancelled = false))"
                ),
                True,
                True,
                "No room overlap",
            ]
        ]

    monkeypatch.setattr(reflection, "_query_rows_url", fake_query_rows_url)

    constraints = reflection._reflect_server_exclusion_constraints(
        object,
        "postgresql://localhost/db",
        "postgresql",
        "public",
        ["booking"],
    )

    assert "pg_get_constraintdef" in captured_sql[0]
    assert "c.contype = 'x'" in captured_sql[0]
    assert "obj_description" in captured_sql[0]
    assert constraints == {
        "booking": [
            ExclusionConstraintSnapshot(
                "booking_room_overlap",
                columns=[("room_id", "="), ("during", "&&")],
                expressions=[("lower(status)", "<>")],
                ops={
                    "room_id": "gist_int4_ops",
                    "during": "gist_tstzrange_ops",
                    "lower(status)": "text_ops",
                },
                using="gist",
                where="cancelled = false",
                deferrable=True,
                initially_deferred=True,
                comment="No room overlap",
            )
        ]
    }
    assert (
        reflection._reflect_server_exclusion_constraints(
            object,
            "mysql://localhost/db",
            "mysql",
            None,
            ["booking"],
        )
        == {}
    )


def test_reflect_server_check_constraints_preserves_names_and_expressions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_sql: list[str] = []

    def fake_query_rows_url(rust: type[Any], url: str, sql: str) -> list[list[Any]]:
        del rust, url
        captured_sql.append(sql)
        return [
            [
                "flavor",
                "flavor_rating_check",
                "rating BETWEEN 0 AND 100",
                False,
                True,
                "Rating guard",
            ]
        ]

    monkeypatch.setattr(reflection, "_query_rows_url", fake_query_rows_url)

    checks = reflection._reflect_server_check_constraints(
        object,
        "postgresql://localhost/db",
        "postgresql",
        "public",
        ["flavor"],
    )

    assert "pg_constraint" in captured_sql[0]
    assert "convalidated" in captured_sql[0]
    assert "connoinherit" in captured_sql[0]
    assert "obj_description" in captured_sql[0]
    assert checks == {
        "flavor": [
            TableCheckSnapshot(
                "flavor_rating_check",
                "rating BETWEEN 0 AND 100",
                validated=False,
                no_inherit=True,
                comment="Rating guard",
            )
        ]
    }


def test_reflect_server_check_constraints_preserves_oracle_validation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_sql: list[str] = []

    def fake_query_rows_url(rust: type[Any], url: str, sql: str) -> list[list[Any]]:
        del rust, url
        captured_sql.append(sql)
        return [
            [
                "FLAVOR",
                "FLAVOR_RATING_CHECK",
                "rating BETWEEN 0 AND 100",
                "NOT VALIDATED",
                None,
            ]
        ]

    monkeypatch.setattr(reflection, "_query_rows_url", fake_query_rows_url)

    checks = reflection._reflect_server_check_constraints(
        object,
        "oracle://localhost/db",
        "oracle",
        "inventory",
        ["FLAVOR"],
    )

    assert "validated" in captured_sql[0]
    assert "CAST(NULL AS VARCHAR2(1))" in captured_sql[0]
    assert "owner = 'INVENTORY'" in captured_sql[0]
    assert checks == {
        "FLAVOR": [
            TableCheckSnapshot(
                "FLAVOR_RATING_CHECK",
                "rating BETWEEN 0 AND 100",
                validated=False,
            )
        ]
    }


@pytest.mark.parametrize(
    ("dialect", "schema", "expected_catalog", "expected_schema_filter"),
    [
        (
            "mysql",
            "inventory",
            "information_schema.check_constraints",
            "tc.table_schema = 'inventory'",
        ),
        (
            "mariadb",
            "inventory",
            "information_schema.check_constraints",
            "tc.table_schema = 'inventory'",
        ),
        (
            "mssql",
            "dbo",
            "INFORMATION_SCHEMA.CHECK_CONSTRAINTS",
            "tc.TABLE_SCHEMA = 'dbo'",
        ),
    ],
)
def test_reflect_server_check_constraints_preserves_backend_catalogs(
    monkeypatch: pytest.MonkeyPatch,
    dialect: str,
    schema: str,
    expected_catalog: str,
    expected_schema_filter: str,
) -> None:
    captured_sql: list[str] = []

    def fake_query_rows_url(rust: type[Any], url: str, sql: str) -> list[list[Any]]:
        del rust, url
        captured_sql.append(sql)
        return [
            ["flavor", "flavor_rating_check", "rating BETWEEN 0 AND 100", None, None]
        ]

    monkeypatch.setattr(reflection, "_query_rows_url", fake_query_rows_url)

    checks = reflection._reflect_server_check_constraints(
        object,
        f"{dialect}://localhost/db",
        dialect,
        schema,
        ["flavor"],
    )

    assert expected_catalog in captured_sql[0]
    assert expected_schema_filter in captured_sql[0]
    assert "constraint_type = 'check'" in captured_sql[0].lower()
    assert checks == {
        "flavor": [
            TableCheckSnapshot(
                "flavor_rating_check",
                "rating BETWEEN 0 AND 100",
            )
        ]
    }


def test_reflect_server_check_constraints_preserves_mysql_enforcement(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_sql: list[str] = []

    def fake_query_rows_url(rust: type[Any], url: str, sql: str) -> list[list[Any]]:
        del rust, url
        captured_sql.append(sql)
        return [
            [
                "flavor",
                "flavor_rating_check",
                "rating BETWEEN 0 AND 100",
                "NO",
                None,
                None,
            ]
        ]

    monkeypatch.setattr(reflection, "_query_rows_url", fake_query_rows_url)

    checks = reflection._reflect_server_check_constraints(
        object,
        "mysql://localhost/db",
        "mysql",
        "inventory",
        ["flavor"],
    )

    assert "tc.enforced" in captured_sql[0]
    assert checks == {
        "flavor": [
            TableCheckSnapshot(
                "flavor_rating_check",
                "rating BETWEEN 0 AND 100",
                validated=False,
            )
        ]
    }


def test_reflect_server_check_constraints_preserves_mssql_validation_and_comments(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_sql: list[str] = []

    def fake_query_rows_url(rust: type[Any], url: str, sql: str) -> list[list[Any]]:
        del rust, url
        captured_sql.append(sql)
        return [
            [
                "flavor",
                "flavor_rating_check",
                "rating BETWEEN 0 AND 100",
                0,
                None,
                "Rating guard",
            ]
        ]

    monkeypatch.setattr(reflection, "_query_rows_url", fake_query_rows_url)

    checks = reflection._reflect_server_check_constraints(
        object,
        "mssql://localhost/db",
        "mssql",
        "dbo",
        ["flavor"],
    )

    assert "sys.check_constraints" in captured_sql[0]
    assert "check_ref.is_not_trusted" in captured_sql[0]
    assert "sys.extended_properties" in captured_sql[0]
    assert "ep.class = 1" in captured_sql[0]
    assert "MS_Description" in captured_sql[0]
    assert checks == {
        "flavor": [
            TableCheckSnapshot(
                "flavor_rating_check",
                "rating BETWEEN 0 AND 100",
                validated=False,
                comment="Rating guard",
            )
        ]
    }


def test_reflect_server_indexes_preserves_mssql_include_and_filter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_sql: list[str] = []

    def fake_query_rows_url(rust: type[Any], url: str, sql: str) -> list[list[Any]]:
        del rust, url
        captured_sql.append(sql)
        assert "filter_definition" in sql
        assert "i.type_desc" in sql
        return [
            [
                "flavor",
                "flavor_name_idx",
                1,
                "name",
                1,
                0,
                "[name] IS NOT NULL",
                "Flavor lookup index",
                "indexspace",
                1,
            ],
            [
                "flavor",
                "flavor_name_idx",
                1,
                "rating",
                2,
                1,
                "[name] IS NOT NULL",
                "Flavor lookup index",
                "indexspace",
                1,
            ],
        ]

    monkeypatch.setattr(reflection, "_query_rows_url", fake_query_rows_url)

    indexes = reflection._reflect_server_indexes(
        object, "mssql://localhost/db", "mssql", "dbo", ["flavor"]
    )

    assert "sys.extended_properties" in captured_sql[0]
    assert "ep.class = 7" in captured_sql[0]
    assert "MS_Description" in captured_sql[0]
    assert "sys.filegroups" in captured_sql[0]
    assert "i.type_desc" in captured_sql[0]
    assert indexes == {
        "flavor": [
            IndexSnapshot(
                "flavor_name_idx",
                ["name"],
                unique=True,
                where="[name] IS NOT NULL",
                include_columns=["rating"],
                comment="Flavor lookup index",
                mssql_filegroup="indexspace",
                mssql_clustered=True,
            )
        ]
    }


def test_sqlite_named_constraint_parser_handles_quoted_names() -> None:
    sql = (
        'CREATE TABLE "flavor" ('
        '"id" TEXT PRIMARY KEY, '
        'CONSTRAINT "flavor ""name"" unique" UNIQUE ("name", "supplier_id") '
        "ON CONFLICT IGNORE, "
        'CONSTRAINT "flavor ""rating"" check" '
        "CHECK ((rating >= 0) AND note <> 'CHECK (skip)'), "
        'CONSTRAINT "flavor ""supplier"" fk" '
        'FOREIGN KEY ("supplier_id") REFERENCES "supplier" ("id") '
        "ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED)"
    )

    assert reflection._sqlite_named_check_constraints(sql) == [
        TableCheckSnapshot(
            'flavor "rating" check',
            "(rating >= 0) AND note <> 'CHECK (skip)'",
        )
    ]
    assert reflection._sqlite_named_unique_constraints(sql) == [
        UniqueConstraintSnapshot(
            'flavor "name" unique',
            ["name", "supplier_id"],
            sqlite_on_conflict="IGNORE",
        )
    ]
    assert reflection._sqlite_named_foreign_key_names(sql) == {
        "supplier_id": 'flavor "supplier" fk'
    }
    assert reflection._sqlite_named_foreign_key_constraints(sql) == [
        ForeignKeyConstraintSnapshot(
            'flavor "supplier" fk',
            ["supplier_id"],
            "supplier",
            ["id"],
            on_delete="cascade",
            deferrable=True,
            initially_deferred=True,
        )
    ]


def test_sqlite_column_collations_parse_column_definitions_only() -> None:
    sql = (
        'CREATE TABLE "flavor" ('
        '"id" INTEGER PRIMARY KEY AUTOINCREMENT, '
        "\"name\" TEXT COLLATE NOCASE CHECK (name <> 'x,y'), "
        "\"checked\" TEXT CHECK (checked COLLATE NOCASE <> ''), "
        '"name_lower" TEXT GENERATED ALWAYS AS (LOWER(name)) STORED, '
        "\"name_key\" TEXT GENERATED ALWAYS AS (LOWER(name) || ',x'), "
        "\"plain\" TEXT DEFAULT 'GENERATED ALWAYS AS (ignored)', "
        '"sort key" TEXT COLLATE "custom collation", '
        'CONSTRAINT "flavor_name_check" CHECK (LENGTH(name) >= 2))'
    )

    assert reflection._sqlite_column_collations(sql) == {
        "name": "NOCASE",
        "sort key": "custom collation",
    }
    assert reflection._sqlite_column_generated(sql) == {
        "name_lower": ("LOWER(name)", True),
        "name_key": ("LOWER(name) || ',x'", False),
    }
    assert reflection._sqlite_autoincrement_columns(sql) == {"id"}


def test_sqlite_column_conflicts_parse_column_definitions_only() -> None:
    sql = (
        'CREATE TABLE "flavor" ('
        '"id" INTEGER PRIMARY KEY DESC ON CONFLICT REPLACE AUTOINCREMENT, '
        "\"name\" TEXT DEFAULT 'ON CONFLICT IGNORE' "
        "NOT NULL ON CONFLICT FAIL UNIQUE ON CONFLICT IGNORE, "
        "\"note\" TEXT CHECK (note <> 'UNIQUE ON CONFLICT ABORT'), "
        'CONSTRAINT "flavor_note_unique" UNIQUE ("note") ON CONFLICT ABORT)'
    )

    assert reflection._sqlite_column_conflicts(sql) == {
        "id": {"sqlite_on_conflict_primary_key": "REPLACE"},
        "name": {
            "sqlite_on_conflict_not_null": "FAIL",
            "sqlite_on_conflict_unique": "IGNORE",
        },
    }


def test_sqlite_index_metadata_parses_partial_and_expression_indexes() -> None:
    assert reflection._sqlite_index_metadata(
        (
            'CREATE INDEX "flavor_mixed_idx" ON "main"."flavor" '
            '("name", LOWER(name), "rating" COLLATE NOCASE DESC) '
            "WHERE name IS NOT NULL AND note <> 'WHERE ignored'"
        ),
        ["name", "rating"],
    ) == (
        ["name"],
        ["LOWER(name)", '"rating" COLLATE NOCASE DESC'],
        "name IS NOT NULL AND note <> 'WHERE ignored'",
    )
    assert reflection._sqlite_index_metadata("", ["name"]) == (["name"], [], None)


def test_sqlite_table_options_parser_handles_strict_without_rowid() -> None:
    assert reflection._sqlite_table_options(
        'CREATE TABLE "flavor" ('
        '"id" INTEGER PRIMARY KEY, '
        "\"note\" TEXT CHECK (note <> 'STRICT WITHOUT ROWID')) "
        "STRICT, WITHOUT ROWID"
    ) == (True, True)
    assert reflection._sqlite_table_options(
        'CREATE TABLE "flavor" ("id" INTEGER PRIMARY KEY) WITHOUT ROWID'
    ) == (False, True)
    assert reflection._sqlite_table_options(
        'CREATE TABLE "flavor" ("id" INTEGER PRIMARY KEY) STRICT'
    ) == (True, False)


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
    assert reflection._oracle_identity_columns_view("app") == "all_tab_identity_cols"
    assert reflection._oracle_identity_columns_view(None) == "user_tab_identity_cols"
    assert reflection._oracle_constraints_view(None) == "user_constraints"
    assert reflection._oracle_cons_columns_view("app") == "all_cons_columns"
    assert reflection._oracle_indexes_view(None) == "user_indexes"
    assert reflection._oracle_ind_columns_view("app") == "all_ind_columns"
    assert reflection._oracle_sequences_view("app") == "all_sequences"
    assert reflection._oracle_sequences_view(None) == "user_sequences"
    assert reflection._oracle_views_view("app") == "all_views"
    assert reflection._oracle_views_view(None) == "user_views"
    assert reflection._oracle_materialized_views_view("app") == "all_mviews"
    assert reflection._oracle_materialized_views_view(None) == "user_mviews"
    assert (
        reflection._oracle_materialized_view_comments_view("app")
        == "all_mview_comments"
    )
    assert (
        reflection._oracle_materialized_view_comments_view(None)
        == "user_mview_comments"
    )
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
    assert reflection._reflected_max_length(0, "NUMBER") is None
    assert reflection._reflected_max_length("32") == 32
    assert reflection._reflected_max_length("255", "varchar") == 255
    assert reflection._reflected_max_length("65535", "text") is None
    assert reflection._reflected_max_length("4000", "clob") is None


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
    assert reflection._normalize_reflected_type(dialect, value, scale=scale) == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, "str"),
        ("INTEGER", "int"),
        ("VARCHAR(32)", "str"),
        ("BLOB", "bytes"),
        ("DOUBLE", "float"),
        ("BOOLEAN", "bool"),
        ("NUMERIC", "decimal"),
        ("DECIMAL_TEXT(12, 2)", "decimal"),
    ],
)
def test_normalize_sqlite_type(value: Any, expected: str) -> None:
    assert reflection._normalize_sqlite_type(value) == expected


def test_sqlite_numeric_precision_scale_parser() -> None:
    assert reflection._sqlite_numeric_precision_scale("DECIMAL_TEXT(12, 2)") == (
        12,
        2,
    )
    assert reflection._sqlite_numeric_precision_scale("TEXT") == (None, None)
