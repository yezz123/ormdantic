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
    MigrationChange,
    MigrationOperation,
    MigrationPlan,
    NamespaceSnapshot,
    RelationshipSnapshot,
    SchemaDiff,
    SchemaSnapshot,
    SequenceSnapshot,
    TableCheckSnapshot,
    TableSnapshot,
    UniqueConstraintSnapshot,
    ViewSnapshot,
    exclusion_elements,
    mysql_index_lengths,
    oracle_index_compress,
    oracle_index_compress_runtime,
    oracle_table_compress,
    oracle_table_compress_runtime,
    postgres_index_ops,
    postgres_storage_parameters,
    runtime_column_options,
    runtime_column_tail,
    runtime_sqlite_column_conflict,
    string_list,
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


def test_autogenerate_scope_filters_owned_schema_objects() -> None:
    snapshot = SchemaSnapshot(
        namespaces=[
            NamespaceSnapshot("inventory"),
            NamespaceSnapshot("enum_schema"),
            NamespaceSnapshot("seq_schema"),
            NamespaceSnapshot("view_schema"),
            NamespaceSnapshot("unused"),
        ],
        enum_types=[
            EnumTypeSnapshot("flavor_kind", ["mocha"], schema="enum_schema"),
            EnumTypeSnapshot("unused_kind", ["old"], schema="unused"),
        ],
        sequences=[
            SequenceSnapshot("flavor_seq", schema="seq_schema"),
            SequenceSnapshot("unused_seq", schema="unused"),
        ],
        views=[
            ViewSnapshot("flavor_view", "SELECT 1", schema="view_schema"),
            ViewSnapshot("legacy_view", "SELECT 1", schema="unused"),
        ],
        tables=[
            TableSnapshot(
                model_key="Flavor",
                name="flavor",
                primary_key="id",
                columns=[
                    ColumnSnapshot("id", "int", nullable=False, primary_key=True),
                    ColumnSnapshot(
                        "kind",
                        "enum:enum_schema.flavor_kind",
                        nullable=True,
                        primary_key=False,
                        server_default=(
                            'nextval(\'"seq_schema"."flavor_seq"\'::regclass)'
                        ),
                    ),
                ],
            ),
            TableSnapshot("Legacy", "legacy", "id"),
        ],
    )

    filtered = migrations._filter_snapshot_for_autogenerate_scope(
        snapshot,
        include_tables=["flavor*"],
        exclude_tables=None,
        schema="inventory",
    )

    assert [table.name for table in filtered.tables] == ["flavor"]
    assert [enum_type.name for enum_type in filtered.enum_types] == ["flavor_kind"]
    assert [sequence.name for sequence in filtered.sequences] == ["flavor_seq"]
    assert [view.name for view in filtered.views] == ["flavor_view"]
    assert {namespace.name for namespace in filtered.namespaces} == {
        "inventory",
        "enum_schema",
        "seq_schema",
        "view_schema",
    }


def test_migration_snapshot_key_helpers_cover_quoted_edges() -> None:
    assert migrations._enum_column_kind_key("enum:flavor_kind") == (
        None,
        "flavor_kind",
    )
    assert migrations._enum_column_kind_key("enum:inventory.flavor_kind") == (
        "inventory",
        "flavor_kind",
    )
    assert migrations._sequence_key_from_default("CURRENT_TIMESTAMP") is None
    assert migrations._sequence_key_from_default("nextval('flavor_seq'::regclass)") == (
        None,
        "flavor_seq",
    )
    assert migrations._sequence_key_from_default(
        'nextval(\'"seq.schema"."flavor""seq"\'::regclass)'
    ) == ("seq.schema", 'flavor"seq')


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


def test_runtime_normalization_helpers_cover_scalar_and_legacy_edges() -> None:
    assert string_list(None) == []
    assert string_list("single") == ["single"]
    assert postgres_storage_parameters({"b": 2, "a": 1}) == [
        ("a", "1"),
        ("b", "2"),
    ]
    assert mysql_index_lengths({"name": 12}) == {"name": 12}
    assert mysql_index_lengths([("code", "8")]) == {"code": 8}
    assert postgres_index_ops({"name": "text_ops"}) == {"name": "text_ops"}
    assert postgres_index_ops([("code", "varchar_ops")]) == {"code": "varchar_ops"}

    with pytest.raises(ValueError, match="Oracle index compression"):
        oracle_index_compress(object())

    assert (
        runtime_column_options(
            ("id", "int", False, True, None, None, None, False, [], "legacy")
        )
        == {}
    )
    assert runtime_column_tail("legacy") == {}
    assert runtime_sqlite_column_conflict("legacy") == {}
    assert runtime_column_tail(((True, False), "Name", ("REPLACE", "FAIL"))) == {
        "deferrable": True,
        "initially_deferred": False,
        "comment": "Name",
        "sqlite_on_conflict_primary_key": "REPLACE",
        "sqlite_on_conflict_not_null": "FAIL",
        "sqlite_on_conflict_unique": None,
    }
    assert exclusion_elements("legacy") == []
    assert exclusion_elements([("period", "&&")]) == [("period", "&&")]

    legacy = UniqueConstraintSnapshot.from_runtime(
        ("uq", ["id"], None, False, False, None, None, "Legacy comment")
    )
    assert legacy.comment == "Legacy comment"


def test_column_snapshot_roundtrips_optional_dict_fields() -> None:
    column = ColumnSnapshot(
        "id",
        "int",
        nullable=False,
        primary_key=True,
        comment="Synthetic identifier",
        max_length=32,
        unique=True,
        checks=[("multiple_of", "=", "5")],
        server_default="nextval('flavor_id_seq')",
        computed="id + 1",
        computed_persisted=True,
        autoincrement=True,
        identity=True,
        identity_always=True,
        identity_start=10,
        identity_increment=5,
        identity_min_value=1,
        identity_max_value=100,
        identity_no_min_value=True,
        identity_no_max_value=True,
        identity_cycle=True,
        identity_cache=20,
        identity_order=True,
        identity_on_null=True,
        collation="NOCASE",
        numeric_precision=12,
        numeric_scale=2,
        foreign_key_name="flavor_supplier_fk",
        on_delete="cascade",
        on_update="restrict",
        deferrable=False,
        initially_deferred=True,
        sqlite_on_conflict_primary_key="REPLACE",
        sqlite_on_conflict_not_null="FAIL",
        sqlite_on_conflict_unique="IGNORE",
    )

    payload = column.to_dict()

    assert payload["comment"] == "Synthetic identifier"
    assert payload["server_default"] == "nextval('flavor_id_seq')"
    assert payload["computed"] == "id + 1"
    assert payload["computed_persisted"] is True
    assert payload["autoincrement"] is True
    assert payload["identity"] is True
    assert payload["identity_always"] is True
    assert payload["identity_start"] == 10
    assert payload["identity_increment"] == 5
    assert payload["identity_min_value"] == 1
    assert payload["identity_max_value"] == 100
    assert payload["identity_no_min_value"] is True
    assert payload["identity_no_max_value"] is True
    assert payload["identity_cycle"] is True
    assert payload["identity_cache"] == 20
    assert payload["identity_order"] is True
    assert payload["identity_on_null"] is True
    assert payload["collation"] == "NOCASE"
    assert payload["numeric_precision"] == 12
    assert payload["numeric_scale"] == 2
    assert payload["foreign_key_name"] == "flavor_supplier_fk"
    assert payload["on_delete"] == "cascade"
    assert payload["on_update"] == "restrict"
    assert payload["deferrable"] is False
    assert payload["initially_deferred"] is True
    assert payload["sqlite_on_conflict_primary_key"] == "REPLACE"
    assert payload["sqlite_on_conflict_not_null"] == "FAIL"
    assert payload["sqlite_on_conflict_unique"] == "IGNORE"
    assert ColumnSnapshot.from_dict(payload) == column


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


@pytest.mark.parametrize(
    ("runtime_constraint", "expected"),
    [
        (
            (
                "flavor_name_unique",
                ["name"],
                None,
                False,
                False,
                None,
                "constraintspace",
                False,
                "Legacy unique comment",
                ["rating"],
            ),
            UniqueConstraintSnapshot(
                "flavor_name_unique",
                ["name"],
                mssql_filegroup="constraintspace",
                mssql_clustered=False,
                comment="Legacy unique comment",
                postgres_include=["rating"],
            ),
        ),
        (
            (
                "flavor_name_unique",
                ["name"],
                None,
                False,
                False,
                None,
                "constraintspace",
                False,
                "oraclespace",
                "COMPRESS",
            ),
            UniqueConstraintSnapshot(
                "flavor_name_unique",
                ["name"],
                mssql_filegroup="constraintspace",
                mssql_clustered=False,
                oracle_tablespace="oraclespace",
                oracle_compress=True,
            ),
        ),
        (
            (
                "flavor_name_unique",
                ["name"],
                None,
                False,
                False,
                None,
                "constraintspace",
                True,
                "oraclespace",
            ),
            UniqueConstraintSnapshot(
                "flavor_name_unique",
                ["name"],
                mssql_filegroup="constraintspace",
                mssql_clustered=True,
                oracle_tablespace="oraclespace",
            ),
        ),
        (
            (
                "flavor_name_unique",
                ["name"],
                None,
                False,
                False,
                None,
                False,
            ),
            UniqueConstraintSnapshot(
                "flavor_name_unique",
                ["name"],
                mssql_clustered=False,
            ),
        ),
        (
            (
                "flavor_name_unique",
                ["name"],
                None,
                False,
                False,
                None,
                "Legacy unique comment",
            ),
            UniqueConstraintSnapshot(
                "flavor_name_unique",
                ["name"],
                comment="Legacy unique comment",
            ),
        ),
        (
            (
                "flavor_name_unique",
                ["name"],
                None,
                False,
                False,
                None,
                "constraintspace",
                True,
                "Legacy unique comment",
            ),
            UniqueConstraintSnapshot(
                "flavor_name_unique",
                ["name"],
                mssql_filegroup="constraintspace",
                mssql_clustered=True,
                oracle_tablespace="Legacy unique comment",
            ),
        ),
        (
            (
                "flavor_name_unique",
                ["name"],
                None,
                False,
                False,
                None,
                "constraintspace",
                True,
            ),
            UniqueConstraintSnapshot(
                "flavor_name_unique",
                ["name"],
                mssql_filegroup="constraintspace",
                mssql_clustered=True,
            ),
        ),
        (
            (
                "flavor_name_unique",
                ["name"],
                None,
                False,
                False,
                None,
                None,
                "Legacy unique comment",
            ),
            UniqueConstraintSnapshot(
                "flavor_name_unique",
                ["name"],
                comment="Legacy unique comment",
            ),
        ),
    ],
)
def test_unique_constraint_snapshot_decodes_legacy_runtime_shapes(
    runtime_constraint: tuple[object, ...], expected: UniqueConstraintSnapshot
) -> None:
    assert UniqueConstraintSnapshot.from_runtime(runtime_constraint) == expected


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


def test_relationship_snapshot_roundtrips_runtime_and_dict() -> None:
    relationship = RelationshipSnapshot(
        "supplier",
        "supplier",
        "id",
        back_reference="flavors",
    )

    assert RelationshipSnapshot.from_runtime(relationship.to_runtime()) == relationship
    assert RelationshipSnapshot.from_dict(relationship.to_dict()) == relationship
    assert relationship.to_dict() == {
        "field": "supplier",
        "foreign_table": "supplier",
        "foreign_column": "id",
        "back_reference": "flavors",
    }


@pytest.mark.parametrize(
    ("runtime_table", "expected"),
    [
        (
            (
                "Flavor",
                "flavor",
                "id",
                [],
                [],
                [],
                [("supplier", "supplier", "id", "flavor")],
            ),
            TableSnapshot(
                "Flavor",
                "flavor",
                "id",
                relationships=[
                    RelationshipSnapshot("supplier", "supplier", "id", "flavor")
                ],
            ),
        ),
        (
            (
                "Flavor",
                "flavor",
                "id",
                [],
                [],
                [],
                [("flavor_name_check", "length(name) > 0")],
                [("supplier", "supplier", "id", None)],
            ),
            TableSnapshot(
                "Flavor",
                "flavor",
                "id",
                check_constraints=[
                    TableCheckSnapshot("flavor_name_check", "length(name) > 0")
                ],
                relationships=[RelationshipSnapshot("supplier", "supplier", "id")],
            ),
        ),
        (
            (
                "Flavor",
                "flavor",
                "id",
                [],
                [],
                [["name"]],
                [],
                [("flavor_name_check", "length(name) > 0", False, True)],
                [("supplier", "supplier", "id", None)],
            ),
            TableSnapshot(
                "Flavor",
                "flavor",
                "id",
                unique_constraints=[["name"]],
                check_constraints=[
                    TableCheckSnapshot(
                        "flavor_name_check",
                        "length(name) > 0",
                        validated=False,
                        no_inherit=True,
                    )
                ],
                relationships=[RelationshipSnapshot("supplier", "supplier", "id")],
            ),
        ),
        (
            (
                "Flavor",
                "flavor",
                "id",
                [],
                [],
                [],
                [("flavor_name_unique", ["name"], None)],
                [("flavor_name_check", "length(name) > 0")],
                [("flavor_supplier_fk", ["supplier_id"], "supplier", ["id"])],
                [("supplier", "supplier", "id", None)],
            ),
            TableSnapshot(
                "Flavor",
                "flavor",
                "id",
                named_unique_constraints=[
                    UniqueConstraintSnapshot("flavor_name_unique", ["name"])
                ],
                check_constraints=[
                    TableCheckSnapshot("flavor_name_check", "length(name) > 0")
                ],
                foreign_key_constraints=[
                    ForeignKeyConstraintSnapshot(
                        "flavor_supplier_fk", ["supplier_id"], "supplier", ["id"]
                    )
                ],
                relationships=[RelationshipSnapshot("supplier", "supplier", "id")],
            ),
        ),
        (
            (
                "Flavor",
                "flavor",
                "id",
                [],
                [],
                [],
                [("flavor_name_unique", ["name"], None)],
                [("flavor_name_check", "length(name) > 0")],
                [("flavor_supplier_fk", ["supplier_id"], "supplier", ["id"])],
                [("flavor_name_excl", [("name", "=")], [], "gist")],
                [("supplier", "supplier", "id", None)],
            ),
            TableSnapshot(
                "Flavor",
                "flavor",
                "id",
                named_unique_constraints=[
                    UniqueConstraintSnapshot("flavor_name_unique", ["name"])
                ],
                check_constraints=[
                    TableCheckSnapshot("flavor_name_check", "length(name) > 0")
                ],
                foreign_key_constraints=[
                    ForeignKeyConstraintSnapshot(
                        "flavor_supplier_fk", ["supplier_id"], "supplier", ["id"]
                    )
                ],
                exclusion_constraints=[
                    ExclusionConstraintSnapshot(
                        "flavor_name_excl", columns=[("name", "=")]
                    )
                ],
                relationships=[RelationshipSnapshot("supplier", "supplier", "id")],
            ),
        ),
    ],
)
def test_table_snapshot_decodes_legacy_runtime_shapes(
    runtime_table: tuple[object, ...], expected: TableSnapshot
) -> None:
    assert TableSnapshot.from_runtime(runtime_table) == expected


def test_table_snapshot_decodes_legacy_scalar_comment_runtime_shape() -> None:
    table = TableSnapshot.from_runtime(
        (
            "Flavor",
            "flavor",
            "id",
            [],
            [],
            [],
            [],
            [],
            [],
            [],
            "Legacy table comment",
            [("supplier", "supplier", "id", None)],
        )
    )

    assert table.comment == "Legacy table comment"
    assert table.relationships == [RelationshipSnapshot("supplier", "supplier", "id")]
    assert table.schema is None
    assert table.postgres_inherits == []
    assert table.mysql_union == []
    assert table.oracle_compress is None


def test_migration_plan_destructive_detection_lives_with_model() -> None:
    destructive = MigrationChange(
        "drop",
        "table",
        "flavor",
        "flavor",
        "Removed table flavor",
        destructive=True,
    )
    unsafe = MigrationChange(
        "alter",
        "column",
        "flavor",
        "name",
        "Changed column flavor.name: kind",
        unsafe=True,
    )
    diff = SchemaDiff(changes=[destructive, unsafe])
    plan = MigrationPlan(
        [MigrationOperation("DROP TABLE flavor")],
        rollback_operations=[MigrationOperation("CREATE TABLE flavor (id TEXT)")],
        diff=diff,
    )

    assert plan.has_destructive_operations
    assert plan.has_unsafe_operations
    assert plan.rollback_available
    assert plan.dry_run() == ["DROP TABLE flavor"]
    assert plan.rollback_sql() == ["CREATE TABLE flavor (id TEXT)"]
    assert not diff.is_empty()
    assert diff.destructive_changes == [destructive]
    assert diff.unsafe_changes == [unsafe]
    assert diff.has_destructive_operations
    assert diff.has_unsafe_operations
    assert diff.summary() == [
        "Removed table flavor",
        "Changed column flavor.name: kind",
    ]


def test_oracle_compression_helpers_normalize_runtime_values() -> None:
    assert oracle_index_compress(None) is None
    assert oracle_index_compress(False) is None
    assert oracle_index_compress("disabled") is None
    assert oracle_index_compress(True) is True
    assert oracle_index_compress("COMPRESS") is True
    assert oracle_index_compress("2") == 2
    assert oracle_index_compress_runtime(True) == "true"
    assert oracle_index_compress_runtime(3) == "3"
    assert oracle_table_compress_runtime(True) == "true"
    assert oracle_table_compress(4) == 4

    with pytest.raises(ValueError, match="Oracle index compression"):
        oracle_index_compress("bad")
    with pytest.raises(ValueError, match="Oracle index compression"):
        oracle_index_compress(0)
    with pytest.raises(ValueError, match="Oracle table compression"):
        oracle_table_compress("bad")


def test_document_toml_helpers_reject_null_values() -> None:
    assert documents.toml_loads(documents.toml_dumps({"name": "flavor"})) == {
        "name": "flavor"
    }
    with pytest.raises(ValueError, match="TOML does not support null values"):
        documents.toml_value(None)
