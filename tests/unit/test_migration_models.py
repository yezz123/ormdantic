from __future__ import annotations

from dataclasses import replace

import pytest

from ormdantic import migrations
from ormdantic._migrations import documents
from ormdantic._migrations.models import (
    ColumnSnapshot,
    EnumTypeSnapshot,
    ExclusionConstraintSnapshot,
    ForeignKeyConstraintSnapshot,
    IndexSnapshot,
    MigrationOperation,
    MigrationPlan,
    NamespaceSnapshot,
    SchemaSnapshot,
    SequenceSnapshot,
    TableCheckSnapshot,
    TableSnapshot,
    UniqueConstraintSnapshot,
    ViewSnapshot,
)


def test_public_migration_facade_re_exports_model_objects() -> None:
    assert migrations.SchemaSnapshot is SchemaSnapshot
    assert migrations.MigrationPlan is MigrationPlan
    assert migrations.NamespaceSnapshot is NamespaceSnapshot
    assert migrations.EnumTypeSnapshot is EnumTypeSnapshot
    assert migrations.ExclusionConstraintSnapshot is ExclusionConstraintSnapshot
    assert migrations.ForeignKeyConstraintSnapshot is ForeignKeyConstraintSnapshot
    assert migrations.SequenceSnapshot is SequenceSnapshot
    assert migrations.ViewSnapshot is ViewSnapshot
    assert migrations.TableCheckSnapshot is TableCheckSnapshot
    assert migrations.UniqueConstraintSnapshot is UniqueConstraintSnapshot


def test_schema_snapshot_roundtrips_json_toml_and_runtime(tmp_path) -> None:
    snapshot = SchemaSnapshot(
        enum_types=[
            EnumTypeSnapshot(
                "flavor_kind",
                ["mocha", "latte"],
                schema="public",
            )
        ],
        namespaces=[NamespaceSnapshot("inventory")],
        sequences=[
            SequenceSnapshot(
                "flavor_id_seq",
                schema="public",
                start=10,
                increment=5,
                min_value=1,
                max_value=1000,
                cycle=True,
                cache=20,
                comment="Flavor ids",
                data_type="bigint",
                order=True,
            )
        ],
        views=[
            ViewSnapshot(
                "active_flavors",
                "SELECT id, name FROM flavor WHERE deleted_at IS NULL",
                schema="public",
            )
        ],
        tables=[
            TableSnapshot(
                model_key="Flavor",
                name="flavor",
                primary_key="id",
                schema="inventory",
                columns=[
                    ColumnSnapshot(
                        "id",
                        "str",
                        nullable=False,
                        primary_key=True,
                        max_length=64,
                    )
                ],
                named_unique_constraints=[
                    UniqueConstraintSnapshot(
                        "flavor_name_unique",
                        ["name"],
                        nulls_not_distinct=True,
                        sqlite_on_conflict="IGNORE",
                    )
                ],
                check_constraints=[
                    TableCheckSnapshot(
                        "flavor_name_not_empty_check",
                        "LENGTH(name) > 0",
                    )
                ],
                foreign_key_constraints=[
                    ForeignKeyConstraintSnapshot(
                        "flavor_supplier_pair_fk",
                        ["supplier_id", "supplier_code"],
                        "supplier",
                        ["id", "code"],
                        on_delete="cascade",
                        deferrable=True,
                    )
                ],
                exclusion_constraints=[
                    ExclusionConstraintSnapshot(
                        "flavor_active_name_exclusion",
                        columns=[("name", "=")],
                        expressions=[("lower(code)", "=")],
                        where="deleted_at IS NULL",
                        deferrable=True,
                    )
                ],
                comment="Flavor table",
                tablespace="fastspace",
                mysql_engine="InnoDB",
                mysql_charset="utf8mb4",
                mysql_collation="utf8mb4_unicode_ci",
                mysql_row_format="DYNAMIC",
                mysql_key_block_size=8,
                mysql_pack_keys=True,
                mysql_checksum=True,
                mysql_delay_key_write=True,
                mysql_stats_persistent=True,
                mysql_stats_auto_recalc=False,
                mysql_stats_sample_pages=32,
                mysql_avg_row_length=64,
                mysql_max_rows=1000,
                mysql_min_rows=10,
                mysql_insert_method="LAST",
                mysql_data_directory="/var/lib/mysql/data",
                mysql_index_directory="/var/lib/mysql/index",
                mysql_connection="mysql://remote.example/db/flavor",
                mysql_union=["flavor_hot", "flavor_cold"],
                mysql_partition_by="HASH (id)",
                mysql_partitions=4,
                mysql_subpartition_by="KEY (id)",
                mysql_subpartitions=2,
                mysql_auto_increment=101,
                postgres_inherits=["base_flavor"],
                postgres_with=[("fillfactor", "70")],
                postgres_using="heap",
                postgres_partition_by="RANGE (id)",
                postgres_partition_of="base_partitioned_flavor",
                postgres_partition_for="FOR VALUES FROM (0) TO (100)",
                postgres_unlogged=True,
                sqlite_strict=True,
                sqlite_without_rowid=True,
                oracle_compress=6,
            )
        ],
    )

    assert SchemaSnapshot.from_json(snapshot.to_json()).to_dict() == snapshot.to_dict()
    assert SchemaSnapshot.from_toml(snapshot.to_toml()).to_dict() == snapshot.to_dict()
    assert snapshot.tables[0].to_runtime()[10][-23] == "inventory"
    assert snapshot.tables[0].to_runtime()[10][-22] is False
    assert snapshot.tables[0].to_runtime()[10][-21] == "6"
    assert snapshot.tables[0].to_runtime()[10][-20] == 8
    assert snapshot.tables[0].to_runtime()[10][-19] is True
    assert snapshot.tables[0].to_runtime()[10][-18] is True
    assert snapshot.tables[0].to_runtime()[10][-17] is True
    assert snapshot.tables[0].to_runtime()[10][-16] is True
    assert snapshot.tables[0].to_runtime()[10][-15] is False
    assert snapshot.tables[0].to_runtime()[10][-14] == 32
    assert snapshot.tables[0].to_runtime()[10][-13] == 64
    assert snapshot.tables[0].to_runtime()[10][-12] == 1000
    assert snapshot.tables[0].to_runtime()[10][-11] == 10
    assert snapshot.tables[0].to_runtime()[10][-10] == "LAST"
    assert snapshot.tables[0].to_runtime()[10][-9] == "/var/lib/mysql/data"
    assert snapshot.tables[0].to_runtime()[10][-8] == "/var/lib/mysql/index"
    assert snapshot.tables[0].to_runtime()[10][-7] == "mysql://remote.example/db/flavor"
    assert snapshot.tables[0].to_runtime()[10][-6] == [
        "flavor_hot",
        "flavor_cold",
    ]
    assert snapshot.tables[0].to_runtime()[10][-5] == "HASH (id)"
    assert snapshot.tables[0].to_runtime()[10][-4] == 4
    assert snapshot.tables[0].to_runtime()[10][-3] == "KEY (id)"
    assert snapshot.tables[0].to_runtime()[10][-2] == 2
    assert snapshot.tables[0].to_runtime()[10][-1] == 101

    json_path = tmp_path / "snapshot.json"
    toml_path = tmp_path / "snapshot.toml"
    snapshot.write(json_path)
    snapshot.write(toml_path)

    assert SchemaSnapshot.read(json_path).to_runtime() == snapshot.to_runtime()
    assert SchemaSnapshot.read(toml_path).to_runtime() == snapshot.to_runtime()
    assert SchemaSnapshot.read(json_path).namespaces == snapshot.namespaces
    assert SchemaSnapshot.read(json_path).enum_types == snapshot.enum_types
    assert (
        SchemaSnapshot.from_runtime(snapshot.to_runtime()).tables[0].mysql_engine
        == "InnoDB"
    )
    assert (
        SchemaSnapshot.from_runtime(snapshot.to_runtime()).tables[0].mysql_row_format
        == "DYNAMIC"
    )
    assert (
        SchemaSnapshot.from_runtime(snapshot.to_runtime())
        .tables[0]
        .mysql_key_block_size
        == 8
    )
    assert (
        SchemaSnapshot.from_runtime(snapshot.to_runtime()).tables[0].mysql_pack_keys
        is True
    )
    assert (
        SchemaSnapshot.from_runtime(snapshot.to_runtime()).tables[0].mysql_checksum
        is True
    )
    assert (
        SchemaSnapshot.from_runtime(snapshot.to_runtime())
        .tables[0]
        .mysql_delay_key_write
        is True
    )
    assert (
        SchemaSnapshot.from_runtime(snapshot.to_runtime())
        .tables[0]
        .mysql_stats_persistent
        is True
    )
    assert (
        SchemaSnapshot.from_runtime(snapshot.to_runtime())
        .tables[0]
        .mysql_stats_auto_recalc
        is False
    )
    assert (
        SchemaSnapshot.from_runtime(snapshot.to_runtime())
        .tables[0]
        .mysql_stats_sample_pages
        == 32
    )
    assert (
        SchemaSnapshot.from_runtime(snapshot.to_runtime())
        .tables[0]
        .mysql_avg_row_length
        == 64
    )
    assert (
        SchemaSnapshot.from_runtime(snapshot.to_runtime()).tables[0].mysql_max_rows
        == 1000
    )
    assert (
        SchemaSnapshot.from_runtime(snapshot.to_runtime()).tables[0].mysql_min_rows
        == 10
    )
    assert (
        SchemaSnapshot.from_runtime(snapshot.to_runtime()).tables[0].mysql_insert_method
        == "LAST"
    )
    assert (
        SchemaSnapshot.from_runtime(snapshot.to_runtime())
        .tables[0]
        .mysql_data_directory
        == "/var/lib/mysql/data"
    )
    assert (
        SchemaSnapshot.from_runtime(snapshot.to_runtime())
        .tables[0]
        .mysql_index_directory
        == "/var/lib/mysql/index"
    )
    assert (
        SchemaSnapshot.from_runtime(snapshot.to_runtime()).tables[0].mysql_connection
        == "mysql://remote.example/db/flavor"
    )
    assert SchemaSnapshot.from_runtime(snapshot.to_runtime()).tables[0].mysql_union == [
        "flavor_hot",
        "flavor_cold",
    ]
    assert (
        SchemaSnapshot.from_runtime(snapshot.to_runtime()).tables[0].mysql_partition_by
        == "HASH (id)"
    )
    assert (
        SchemaSnapshot.from_runtime(snapshot.to_runtime()).tables[0].mysql_partitions
        == 4
    )
    assert (
        SchemaSnapshot.from_runtime(snapshot.to_runtime())
        .tables[0]
        .mysql_subpartition_by
        == "KEY (id)"
    )
    assert (
        SchemaSnapshot.from_runtime(snapshot.to_runtime()).tables[0].mysql_subpartitions
        == 2
    )
    assert (
        SchemaSnapshot.from_runtime(snapshot.to_runtime())
        .tables[0]
        .mysql_auto_increment
        == 101
    )
    assert SchemaSnapshot.from_runtime(snapshot.to_runtime()).tables[
        0
    ].postgres_inherits == ["base_flavor"]
    assert SchemaSnapshot.from_runtime(snapshot.to_runtime()).tables[
        0
    ].postgres_with == [("fillfactor", "70")]
    assert (
        SchemaSnapshot.from_runtime(snapshot.to_runtime()).tables[0].postgres_using
        == "heap"
    )
    assert (
        SchemaSnapshot.from_runtime(snapshot.to_runtime())
        .tables[0]
        .postgres_partition_by
        == "RANGE (id)"
    )
    assert (
        SchemaSnapshot.from_runtime(snapshot.to_runtime())
        .tables[0]
        .postgres_partition_of
        == "base_partitioned_flavor"
    )
    assert (
        SchemaSnapshot.from_runtime(snapshot.to_runtime())
        .tables[0]
        .postgres_partition_for
        == "FOR VALUES FROM (0) TO (100)"
    )
    assert (
        SchemaSnapshot.from_runtime(snapshot.to_runtime()).tables[0].postgres_unlogged
    )
    assert SchemaSnapshot.from_runtime(snapshot.to_runtime()).tables[0].sqlite_strict
    assert (
        SchemaSnapshot.from_runtime(snapshot.to_runtime())
        .tables[0]
        .sqlite_without_rowid
    )
    assert (
        SchemaSnapshot.from_runtime(snapshot.to_runtime()).tables[0].oracle_compress
        == 6
    )
    assert (
        SchemaSnapshot.from_runtime(snapshot.to_runtime())
        .tables[0]
        .named_unique_constraints[0]
        .sqlite_on_conflict
        == "IGNORE"
    )
    assert SchemaSnapshot.read(json_path).sequences == snapshot.sequences
    assert SchemaSnapshot.read(json_path).views == snapshot.views


def test_column_snapshot_coerces_runtime_values() -> None:
    column = ColumnSnapshot.from_runtime(
        (
            "name",
            "str",
            False,
            False,
            None,
            None,
            32,
            True,
            [("length", ">=", 2)],
            (
                "'vanilla'",
                "LOWER(name)",
                True,
                False,
                "NOCASE",
                None,
                None,
                (True, 10, 5, 1, 1000, True, 20, True, False, False, False),
                "flavor_supplier_fk",
                "set_null",
                "cascade",
                (
                    (True, False),
                    "Flavor display name",
                    ("REPLACE", "FAIL", "IGNORE"),
                ),
            ),
        )
    )

    assert column.foreign_table is None
    assert column.max_length == 32
    assert column.checks == [("length", ">=", "2")]
    assert column.server_default == "'vanilla'"
    assert column.computed == "LOWER(name)"
    assert column.computed_persisted is True
    assert column.identity is True
    assert column.identity_always is True
    assert column.identity_start == 10
    assert column.identity_increment == 5
    assert column.identity_min_value == 1
    assert column.identity_max_value == 1000
    assert column.identity_no_min_value is False
    assert column.identity_no_max_value is False
    assert column.identity_cycle is True
    assert column.identity_cache == 20
    assert column.identity_order is True
    assert column.identity_on_null is False
    assert column.collation == "NOCASE"
    assert column.foreign_key_name == "flavor_supplier_fk"
    assert column.on_delete == "set_null"
    assert column.on_update == "cascade"
    assert column.deferrable is True
    assert column.initially_deferred is False
    assert column.comment == "Flavor display name"
    assert column.sqlite_on_conflict_primary_key == "REPLACE"
    assert column.sqlite_on_conflict_not_null == "FAIL"
    assert column.sqlite_on_conflict_unique == "IGNORE"
    assert ColumnSnapshot.from_runtime(
        ("legacy", "str", False, False, None, None, None, False, [])
    ) == ColumnSnapshot("legacy", "str", False, False)
    no_bound_identity = ColumnSnapshot.from_runtime(
        (
            "id",
            "int",
            False,
            True,
            None,
            None,
            None,
            False,
            [],
            (
                None,
                None,
                False,
                False,
                None,
                None,
                None,
                (False, None, None, None, None, False, None, False, False, True, True),
                None,
                None,
                None,
                None,
            ),
        )
    )
    assert no_bound_identity.identity is True
    assert no_bound_identity.identity_no_min_value is True
    assert no_bound_identity.identity_no_max_value is True
    legacy_timing = ColumnSnapshot.from_runtime(
        (
            "supplier_id",
            "int",
            False,
            False,
            "supplier",
            "id",
            None,
            False,
            [],
            (
                None,
                None,
                False,
                False,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                (None, True),
            ),
        )
    )
    assert legacy_timing.comment is None
    assert legacy_timing.deferrable is None
    assert legacy_timing.initially_deferred is True


def test_index_snapshot_roundtrips_advanced_metadata() -> None:
    index = IndexSnapshot(
        "flavor_lower_name_idx",
        ["name"],
        unique=True,
        where="name IS NOT NULL",
        include_columns=["rating"],
        method="btree",
        expressions=["LOWER(name)"],
        postgres_with=[("fillfactor", "70")],
    )

    assert IndexSnapshot.from_runtime(index.to_runtime()) == index
    assert IndexSnapshot.from_runtime(("flavor_name_idx", ["name"], False)) == (
        IndexSnapshot("flavor_name_idx", ["name"])
    )
    assert IndexSnapshot.from_dict(index.to_dict()) == index
    assert index.to_runtime()[7] == [("fillfactor", "70")]
    assert "where" not in IndexSnapshot("flavor_name_idx", ["name"]).to_dict()
    spaced = IndexSnapshot(
        "flavor_name_idx",
        ["name"],
        comment="Flavor lookup index",
        postgres_tablespace="fastspace",
        mssql_filegroup="indexspace",
        mssql_clustered=True,
        oracle_tablespace="oraclespace",
        oracle_bitmap=True,
        oracle_compress=2,
        mysql_length={"name": 12},
        mysql_using="HASH",
    )
    assert (
        IndexSnapshot.from_runtime(
            (
                "flavor_name_idx",
                ["name"],
                False,
                None,
                [],
                None,
                [],
                [],
                "Flavor lookup index",
                "fastspace",
                "indexspace",
                True,
                "oraclespace",
                None,
                {"name": 12},
                "HASH",
                {},
                None,
                True,
                2,
            )
        )
        == spaced
    )
    assert IndexSnapshot.from_runtime(
        (
            "flavor_name_idx",
            ["name"],
            False,
            None,
            [],
            None,
            [],
            [],
            "Flavor lookup index",
            "fastspace",
            "indexspace",
            True,
            "oraclespace",
            {"name": 12},
            "HASH",
        )
    ) == replace(spaced, oracle_bitmap=False, oracle_compress=None)
    assert IndexSnapshot.from_dict(spaced.to_dict()) == spaced
    assert spaced.to_dict()["oracle_bitmap"] is True
    assert spaced.to_dict()["oracle_compress"] == 2
    postgres_nulls = IndexSnapshot(
        "flavor_name_unique_idx",
        ["name"],
        unique=True,
        postgres_nulls_not_distinct=True,
    )
    assert (
        IndexSnapshot.from_runtime(
            (
                "flavor_name_unique_idx",
                ["name"],
                True,
                None,
                [],
                None,
                [],
                [],
                None,
                None,
                None,
                False,
                None,
                None,
                {},
                None,
                {},
                None,
                False,
                None,
                True,
            )
        )
        == postgres_nulls
    )
    assert IndexSnapshot.from_dict(postgres_nulls.to_dict()) == postgres_nulls
    assert postgres_nulls.to_dict()["postgres_nulls_not_distinct"] is True
    prefixed = IndexSnapshot(
        "flavor_search_idx",
        ["name"],
        mysql_prefix="FULLTEXT",
    )
    assert (
        IndexSnapshot.from_runtime(
            (
                "flavor_search_idx",
                ["name"],
                False,
                None,
                [],
                None,
                [],
                [],
                None,
                None,
                None,
                False,
                None,
                "FULLTEXT",
            )
        )
        == prefixed
    )
    assert IndexSnapshot.from_dict(prefixed.to_dict()) == prefixed
    postgres_ops = IndexSnapshot(
        "flavor_search_idx",
        ["name"],
        expressions=["LOWER(name)"],
        postgres_ops={
            "name": "text_pattern_ops",
            "LOWER(name)": "pg_catalog.text_pattern_ops",
        },
    )
    assert (
        IndexSnapshot.from_runtime(
            (
                "flavor_search_idx",
                ["name"],
                False,
                None,
                [],
                None,
                ["LOWER(name)"],
                [],
                None,
                None,
                None,
                False,
                None,
                None,
                {},
                None,
                {
                    "name": "text_pattern_ops",
                    "LOWER(name)": "pg_catalog.text_pattern_ops",
                },
            )
        )
        == postgres_ops
    )
    assert IndexSnapshot.from_dict(postgres_ops.to_dict()) == postgres_ops
    invisible = IndexSnapshot(
        "flavor_hidden_idx",
        ["name"],
        mysql_visible=False,
    )
    assert (
        IndexSnapshot.from_runtime(
            (
                "flavor_hidden_idx",
                ["name"],
                False,
                None,
                [],
                None,
                [],
                [],
                None,
                None,
                None,
                False,
                None,
                None,
                {},
                None,
                {},
                False,
            )
        )
        == invisible
    )
    assert IndexSnapshot.from_dict(invisible.to_dict()) == invisible


def test_table_check_snapshot_roundtrips_runtime_and_dict() -> None:
    check = TableCheckSnapshot(
        "flavor_rating_range_check",
        "rating BETWEEN 0 AND 100",
        validated=False,
        no_inherit=True,
        comment="Rating guard",
    )

    assert TableCheckSnapshot.from_runtime(check.to_runtime()) == replace(
        check, comment=None
    )
    assert TableCheckSnapshot.from_dict(check.to_dict()) == check
    assert check.to_dict()["validated"] is False
    assert check.to_dict()["no_inherit"] is True
    assert check.to_dict()["comment"] == "Rating guard"
    assert (
        TableCheckSnapshot.from_runtime(
            (
                "flavor_rating_range_check",
                "rating BETWEEN 0 AND 100",
                False,
                True,
                "Rating guard",
            )
        )
        == check
    )
    assert TableCheckSnapshot.from_runtime(("legacy_check", "rating >= 0")).validated
    assert not TableCheckSnapshot.from_runtime(
        ("legacy_check", "rating >= 0")
    ).no_inherit


def test_unique_constraint_snapshot_roundtrips_runtime_and_dict() -> None:
    constraint = UniqueConstraintSnapshot(
        "flavor_name_code_unique",
        ["name", "code"],
        postgres_include=["rating"],
        deferrable=True,
        initially_deferred=True,
        nulls_not_distinct=True,
        mssql_filegroup="constraintspace",
        mssql_clustered=True,
        oracle_tablespace="oraclespace",
        oracle_compress=2,
        comment="Flavor code identity",
    )

    assert UniqueConstraintSnapshot.from_runtime(constraint.to_runtime()) == replace(
        constraint, comment=None, postgres_include=[]
    )
    assert UniqueConstraintSnapshot.from_dict(constraint.to_dict()) == constraint
    assert constraint.to_dict()["nulls_not_distinct"] is True
    assert constraint.to_dict()["mssql_filegroup"] == "constraintspace"
    assert constraint.to_dict()["mssql_clustered"] is True
    assert constraint.to_dict()["oracle_tablespace"] == "oraclespace"
    assert constraint.to_dict()["oracle_compress"] == 2
    assert constraint.to_dict()["comment"] == "Flavor code identity"
    assert constraint.to_dict()["postgres_include"] == ["rating"]
    assert (
        UniqueConstraintSnapshot.from_runtime(
            (
                "flavor_name_code_unique",
                ["name", "code"],
                True,
                True,
                True,
                None,
                "constraintspace",
                True,
                "Flavor code identity",
                ["rating"],
                "oraclespace",
                2,
            )
        )
        == constraint
    )
    legacy = UniqueConstraintSnapshot.from_runtime(
        (
            "flavor_name_code_unique",
            ["name", "code"],
            True,
            True,
            True,
            None,
            "Flavor code identity",
            ["rating"],
        )
    )
    assert legacy == replace(
        constraint,
        mssql_filegroup=None,
        mssql_clustered=None,
        oracle_tablespace=None,
        oracle_compress=None,
    )


def test_foreign_key_constraint_snapshot_roundtrips_runtime_and_dict() -> None:
    constraint = ForeignKeyConstraintSnapshot(
        "flavor_supplier_pair_fk",
        ["supplier_id", "supplier_code"],
        "supplier",
        ["id", "code"],
        on_delete="cascade",
        on_update="restrict",
        deferrable=True,
        initially_deferred=True,
        validated=False,
        match="full",
        comment="Supplier pair lookup",
    )

    assert ForeignKeyConstraintSnapshot.from_runtime(
        constraint.to_runtime()
    ) == replace(constraint, comment=None)
    assert ForeignKeyConstraintSnapshot.from_dict(constraint.to_dict()) == constraint
    assert constraint.to_dict()["validated"] is False
    assert constraint.to_dict()["match"] == "full"
    assert constraint.to_dict()["comment"] == "Supplier pair lookup"
    legacy = ForeignKeyConstraintSnapshot.from_runtime(
        (
            "legacy_fk",
            ["supplier_id", "supplier_code"],
            "supplier",
            ["id", "code"],
            None,
            None,
            None,
            False,
        )
    )
    assert legacy.validated
    assert legacy.match is None


def test_exclusion_constraint_snapshot_roundtrips_runtime_and_dict() -> None:
    constraint = ExclusionConstraintSnapshot(
        "booking_room_overlap",
        columns=[("room_id", "="), ("during", "&&")],
        expressions=[("lower(status)", "<>")],
        ops={"during": "gist_tstzrange_ops", "lower(status)": "text_ops"},
        using="gist",
        where="cancelled = false",
        deferrable=True,
        initially_deferred=True,
        comment="No room overlap",
    )

    assert ExclusionConstraintSnapshot.from_runtime(constraint.to_runtime()) == replace(
        constraint, comment=None
    )
    assert ExclusionConstraintSnapshot.from_dict(constraint.to_dict()) == constraint
    legacy = ExclusionConstraintSnapshot.from_runtime(
        (
            "booking_room_overlap",
            [("room_id", "=")],
            [],
            "gist",
            None,
            None,
            False,
            "No room overlap",
        )
    )
    assert legacy.ops == {}
    assert legacy.comment == "No room overlap"


def test_enum_type_snapshot_roundtrips_runtime_and_dict() -> None:
    enum_type = EnumTypeSnapshot(
        "flavor_kind",
        ["mocha", "latte"],
        schema="public",
        comment="Flavor enum",
    )

    assert EnumTypeSnapshot.from_runtime(enum_type.to_runtime()) == enum_type
    assert EnumTypeSnapshot.from_runtime(("flavor_kind", ["mocha"])) == (
        EnumTypeSnapshot("flavor_kind", ["mocha"])
    )
    assert EnumTypeSnapshot.from_dict(enum_type.to_dict()) == enum_type


def test_sequence_snapshot_roundtrips_runtime_and_dict() -> None:
    sequence = SequenceSnapshot(
        "flavor_id_seq",
        schema="public",
        start=10,
        increment=5,
        min_value=1,
        max_value=1000,
        cycle=True,
        cache=20,
        comment="Flavor ids",
        data_type="bigint",
        order=True,
    )

    assert SequenceSnapshot.from_runtime(sequence.to_runtime()) == sequence
    assert SequenceSnapshot.from_dict(sequence.to_dict()) == sequence
    assert sequence.to_runtime()[-5] == "Flavor ids"
    assert sequence.to_runtime()[-4] == "bigint"
    assert sequence.to_runtime()[-3] is True
    assert sequence.to_runtime()[-2] is False
    assert sequence.to_runtime()[-1] is False
    no_bound_sequence = SequenceSnapshot(
        "flavor_id_seq",
        no_min_value=True,
        no_max_value=True,
    )
    assert (
        SequenceSnapshot.from_runtime(no_bound_sequence.to_runtime())
        == no_bound_sequence
    )
    assert SequenceSnapshot.from_dict(no_bound_sequence.to_dict()) == no_bound_sequence
    assert no_bound_sequence.to_runtime()[-2:] == (True, True)
    assert SequenceSnapshot.from_runtime(
        ("flavor_id_seq", "public", 10, 5, 1, 1000, True, 20)
    ) == SequenceSnapshot(
        "flavor_id_seq",
        schema="public",
        start=10,
        increment=5,
        min_value=1,
        max_value=1000,
        cycle=True,
        cache=20,
    )


def test_namespace_snapshot_roundtrips_runtime_and_dict() -> None:
    namespace = NamespaceSnapshot("inventory", comment="Warehouse schema")

    assert NamespaceSnapshot.from_runtime(namespace.to_runtime()) == namespace
    assert NamespaceSnapshot.from_dict(namespace.to_dict()) == namespace
    assert NamespaceSnapshot.from_runtime(("legacy",)) == NamespaceSnapshot("legacy")


def test_view_snapshot_roundtrips_runtime_and_dict() -> None:
    view = ViewSnapshot(
        "active_flavors",
        "SELECT id, name FROM flavor WHERE deleted_at IS NULL",
        schema="public",
        materialized=True,
        comment="Active flavor summary",
    )

    assert ViewSnapshot.from_runtime(view.to_runtime()) == view
    assert ViewSnapshot.from_dict(view.to_dict()) == view
    assert ViewSnapshot.from_dict(
        {
            "name": "active_flavors",
            "definition": " SELECT id FROM flavor; ",
        }
    ) == ViewSnapshot("active_flavors", "SELECT id FROM flavor")
    assert ViewSnapshot.from_runtime(
        ("active_flavors", "public", " SELECT id FROM flavor; ", False)
    ) == ViewSnapshot("active_flavors", "SELECT id FROM flavor", schema="public")


def test_migration_plan_destructive_detection_lives_with_model() -> None:
    plan = MigrationPlan([MigrationOperation("DROP TABLE flavor")])

    assert plan.has_destructive_operations
    assert plan.dry_run() == ["DROP TABLE flavor"]


def test_document_toml_helpers_reject_null_values() -> None:
    assert documents.toml_loads(documents.toml_dumps({"name": "flavor"})) == {
        "name": "flavor"
    }
    with pytest.raises(ValueError, match="TOML does not support null values"):
        documents.toml_value(None)
