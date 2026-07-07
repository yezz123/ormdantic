from __future__ import annotations

import pytest

from ormdantic.models.models import (
    DatabaseNamespace,
    DatabaseSequence,
    DatabaseView,
    TableCheck,
    TableColumn,
    TableExclusion,
    TableForeignKey,
    TableIndex,
    TableUnique,
    _balanced_parentheses,
    normalized_bool,
    normalized_mysql_index_lengths,
    normalized_mysql_partition_by,
    normalized_oracle_index_compress,
    normalized_oracle_table_compress,
    normalized_positive_int,
    normalized_postgres_exclusion_ops,
    normalized_postgres_index_ops,
    normalized_postgres_opclass,
    normalized_postgres_partition_by,
    normalized_postgres_partition_for,
    normalized_postgres_storage_parameters,
    normalized_sequence_data_type,
    normalized_sqlite_conflict,
    normalized_storage_identifier_list,
    normalized_storage_path,
    normalized_storage_string,
)


def assert_invalid(message: str, factory) -> None:
    with pytest.raises(ValueError, match=message):
        factory()


def test_table_index_metadata_validation_edges() -> None:
    with pytest.raises(ValueError):
        TableIndex.model_validate("not a mapping")
    assert_invalid(
        "at least one column",
        lambda: TableIndex(name="empty", columns=[]),
    )
    assert_invalid(
        "index comment cannot be empty",
        lambda: TableIndex(name="idx", columns=["name"], comment=" "),
    )
    assert_invalid(
        "reference columns not present",
        lambda: TableIndex(
            name="idx",
            columns=["name"],
            mysql_length={"missing": 12},
        ),
    )
    assert_invalid(
        "unique indexes",
        lambda: TableIndex(
            name="idx",
            columns=["name"],
            unique=True,
            mysql_prefix="FULLTEXT",
        ),
    )
    assert_invalid(
        "expression indexes",
        lambda: TableIndex(
            name="idx",
            columns=[],
            expressions=["lower(name)"],
            mysql_prefix="FULLTEXT",
        ),
    )
    assert_invalid(
        "USING methods",
        lambda: TableIndex(
            name="idx",
            columns=["name"],
            mysql_prefix="FULLTEXT",
            mysql_using="BTREE",
        ),
    )
    assert_invalid(
        "bitmap indexes cannot be unique",
        lambda: TableIndex(
            name="idx",
            columns=["name"],
            unique=True,
            oracle_bitmap=True,
        ),
    )

    index = TableIndex(
        name="idx",
        columns=["name"],
        postgres_with={"fillfactor": 80},
        postgres_ops={"name": "text_pattern_ops"},
        method=" BTREE ",
        postgres_tablespace=" fastspace ",
        mssql_filegroup=" idxspace ",
        oracle_tablespace=" oradata ",
        mysql_length={"name": 8},
        oracle_compress=True,
    )
    assert index.postgres_with == [("fillfactor", "80")]
    assert index.postgres_ops == {"name": "text_pattern_ops"}
    assert index.method == "BTREE"
    assert index.mysql_length == {"name": 8}
    assert index.oracle_compress is True


def test_table_column_metadata_validation_edges() -> None:
    assert_invalid("column comment cannot be empty", lambda: TableColumn(comment=" "))
    assert_invalid(
        "provided together",
        lambda: TableColumn(numeric_precision=10),
    )
    assert_invalid(
        "autoincrement and identity",
        lambda: TableColumn(autoincrement=True, identity=True),
    )
    assert_invalid(
        "identity_increment cannot be zero",
        lambda: TableColumn(identity_increment=0),
    )
    assert_invalid(
        "requires BY DEFAULT",
        lambda: TableColumn(identity_always=True, identity_on_null=True),
    )
    assert_invalid(
        "identity_no_min_value",
        lambda: TableColumn(identity_no_min_value=True, identity_min_value=1),
    )
    assert_invalid(
        "identity_no_max_value",
        lambda: TableColumn(identity_no_max_value=True, identity_max_value=100),
    )
    assert_invalid(
        "identity_cache must be positive",
        lambda: TableColumn(identity_cache=0),
    )
    assert_invalid(
        "cannot exceed",
        lambda: TableColumn(identity_min_value=10, identity_max_value=1),
    )

    column = TableColumn(
        identity_start=10,
        identity_increment=2,
        on_delete="set null",
        on_update="cascade",
        deferrable=None,
        initially_deferred=True,
    )
    assert column.identity is True
    assert column.has_identity
    assert column.has_foreign_key_options
    assert column.on_delete == "set_null"
    assert column.on_update == "cascade"


def test_public_metadata_constraint_validators() -> None:
    assert_invalid(
        "table check constraint comment",
        lambda: TableCheck(name="c", expression="x", comment=" "),
    )
    assert_invalid("at least one column", lambda: TableUnique(name="u", columns=[]))
    assert_invalid(
        "unique constraint comment",
        lambda: TableUnique(name="u", columns=["name"], comment=" "),
    )
    unique = TableUnique(
        name="u",
        columns=["name"],
        postgres_include=[" id "],
        deferrable=None,
        initially_deferred=True,
        sqlite_on_conflict="rollback",
        mssql_filegroup=" fg ",
        oracle_tablespace=" ts ",
        oracle_compress=2,
    )
    assert unique.postgres_include == ["id"]
    assert unique.deferrable is True
    assert unique.sqlite_on_conflict == "ROLLBACK"
    assert unique.oracle_compress == 2

    assert_invalid(
        "must reference at least one column",
        lambda: TableForeignKey(
            name="fk",
            columns=[],
            foreign_table="supplier",
            foreign_columns=["id"],
        ),
    )
    assert_invalid(
        "same length",
        lambda: TableForeignKey(
            name="fk",
            columns=["supplier_id"],
            foreign_table="supplier",
            foreign_columns=["id", "code"],
        ),
    )
    assert_invalid(
        "foreign key constraint comment",
        lambda: TableForeignKey(
            name="fk",
            columns=["supplier_id"],
            foreign_table="supplier",
            foreign_columns=["id"],
            comment=" ",
        ),
    )
    fk = TableForeignKey(
        name="fk",
        columns=["supplier_id"],
        foreign_table="supplier",
        foreign_columns=["id"],
        on_delete="restrict",
        on_update="no action",
        match="simple",
        initially_deferred=True,
    )
    assert fk.on_delete == "restrict"
    assert fk.on_update == "no_action"
    assert fk.match == "simple"
    assert fk.deferrable is True

    assert_invalid("at least one column", lambda: TableExclusion(name="ex"))
    assert_invalid(
        "not present",
        lambda: TableExclusion(
            name="ex",
            columns=[("period", "&&")],
            ops={"missing": "gist_text_ops"},
        ),
    )
    assert_invalid(
        "using cannot be empty",
        lambda: TableExclusion(name="ex", columns=[("period", "&&")], using=" "),
    )
    assert_invalid(
        "where cannot be empty",
        lambda: TableExclusion(name="ex", columns=[("period", "&&")], where=" "),
    )
    assert_invalid(
        "constraint comment",
        lambda: TableExclusion(name="ex", columns=[("period", "&&")], comment=" "),
    )
    assert_invalid(
        "column cannot be empty",
        lambda: TableExclusion(name="ex", columns=[(" ", "&&")]),
    )
    assert_invalid(
        "operator cannot be empty",
        lambda: TableExclusion(name="ex", columns=[("period", " ")]),
    )


def test_storage_and_partition_normalizers_cover_error_edges() -> None:
    assert normalized_storage_string(None, option_name="path") is None
    assert normalized_storage_path(" /tmp/data ", option_name="path") == "/tmp/data"
    assert_invalid(
        "must be a string", lambda: normalized_storage_string(1, option_name="path")
    )  # type: ignore[arg-type]
    assert_invalid(
        "cannot be empty", lambda: normalized_storage_string(" ", option_name="path")
    )
    assert_invalid(
        "NUL bytes", lambda: normalized_storage_string("a\x00b", option_name="path")
    )

    assert normalized_storage_identifier_list(None, option_name="tables") == []
    assert_invalid(
        "must be a list",
        lambda: normalized_storage_identifier_list("table", option_name="tables"),  # type: ignore[arg-type]
    )
    assert_invalid(
        "item 0 must be a string",
        lambda: normalized_storage_identifier_list([1], option_name="tables"),  # type: ignore[list-item]
    )
    assert_invalid(
        "duplicate",
        lambda: normalized_storage_identifier_list(["a", "a"], option_name="tables"),
    )

    assert normalized_sequence_data_type(None, sequence_name="seq") is None
    assert (
        normalized_sequence_data_type(" BIGINT unsigned ", sequence_name="seq")
        == "bigint unsigned"
    )
    assert_invalid(
        "must be a string",
        lambda: normalized_sequence_data_type(1, sequence_name="seq"),
    )  # type: ignore[arg-type]
    assert_invalid(
        "cannot be empty",
        lambda: normalized_sequence_data_type(" ", sequence_name="seq"),
    )
    assert_invalid(
        "safe SQL type",
        lambda: normalized_sequence_data_type("int;drop", sequence_name="seq"),
    )

    assert normalized_postgres_index_ops(None, index_name="idx") == {}
    assert normalized_postgres_index_ops(
        {" name ": " public.text_ops "}, index_name="idx"
    ) == {"name": "public.text_ops"}
    assert_invalid(
        "must be a mapping", lambda: normalized_postgres_index_ops([], index_name="idx")
    )  # type: ignore[arg-type]
    assert_invalid(
        "cannot be empty",
        lambda: normalized_postgres_index_ops({" ": "ops"}, index_name="idx"),
    )
    assert_invalid(
        "operator class", lambda: normalized_postgres_opclass(1, option_name="ops")
    )
    assert_invalid(
        "cannot be empty", lambda: normalized_postgres_opclass(" ", option_name="ops")
    )
    assert_invalid(
        "at most one", lambda: normalized_postgres_opclass("a.b.c", option_name="ops")
    )

    assert normalized_postgres_exclusion_ops(None, constraint_name="ex") == {}
    assert normalized_postgres_exclusion_ops(
        {"period": "gist_range_ops"}, constraint_name="ex"
    ) == {"period": "gist_range_ops"}
    assert_invalid(
        "must be a mapping",
        lambda: normalized_postgres_exclusion_ops([], constraint_name="ex"),  # type: ignore[arg-type]
    )
    assert_invalid(
        "cannot be empty",
        lambda: normalized_postgres_exclusion_ops({" ": "ops"}, constraint_name="ex"),
    )

    assert normalized_positive_int(None, option_name="size") is None
    assert normalized_positive_int(3, option_name="size") == 3
    assert_invalid(
        "positive integer", lambda: normalized_positive_int(True, option_name="size")
    )  # type: ignore[arg-type]
    assert_invalid(
        "must be positive", lambda: normalized_positive_int(0, option_name="size")
    )
    assert normalized_bool(None, option_name="flag") is None
    assert normalized_bool(False, option_name="flag") is False
    assert_invalid("true or false", lambda: normalized_bool("yes", option_name="flag"))  # type: ignore[arg-type]

    assert normalized_oracle_index_compress(False, index_name="idx") is None
    assert normalized_oracle_index_compress(True, index_name="idx") is True
    assert normalized_oracle_index_compress(3, index_name="idx") == 3
    assert_invalid(
        "must be true",
        lambda: normalized_oracle_index_compress("yes", index_name="idx"),
    )  # type: ignore[arg-type]
    assert_invalid(
        "must be positive",
        lambda: normalized_oracle_index_compress(0, index_name="idx"),
    )
    assert normalized_oracle_table_compress(False, table_name="flavor") is None
    assert normalized_oracle_table_compress(True, table_name="flavor") is True
    assert normalized_oracle_table_compress(6, table_name="flavor") == 6
    assert_invalid(
        "must be true",
        lambda: normalized_oracle_table_compress("yes", table_name="flavor"),
    )  # type: ignore[arg-type]
    assert_invalid(
        "must be positive",
        lambda: normalized_oracle_table_compress(0, table_name="flavor"),
    )

    assert normalized_mysql_index_lengths(None, index_name="idx") == {}
    assert normalized_mysql_index_lengths({" name ": 4}, index_name="idx") == {
        "name": 4
    }
    assert_invalid(
        "must be a mapping",
        lambda: normalized_mysql_index_lengths([], index_name="idx"),
    )  # type: ignore[arg-type]
    assert_invalid(
        "cannot be empty",
        lambda: normalized_mysql_index_lengths({" ": 4}, index_name="idx"),
    )
    assert_invalid(
        "positive integer",
        lambda: normalized_mysql_index_lengths({"name": True}, index_name="idx"),
    )  # type: ignore[arg-type]
    assert_invalid(
        "must be positive",
        lambda: normalized_mysql_index_lengths({"name": 0}, index_name="idx"),
    )

    assert normalized_postgres_storage_parameters(None, table_name="flavor") == []
    assert normalized_postgres_storage_parameters(
        {"fillfactor": False}, table_name="flavor"
    ) == [("fillfactor", "false")]
    assert_invalid(
        "must be a string",
        lambda: normalized_postgres_storage_parameters({1: 2}, table_name="flavor"),
    )
    assert_invalid(
        "cannot be empty",
        lambda: normalized_postgres_storage_parameters({" ": 2}, table_name="flavor"),
    )
    assert_invalid(
        "identifier",
        lambda: normalized_postgres_storage_parameters(
            {"bad-name": 2}, table_name="flavor"
        ),
    )
    assert_invalid(
        "value.*cannot be empty",
        lambda: normalized_postgres_storage_parameters(
            {"fillfactor": " "}, table_name="flavor"
        ),
    )
    assert_invalid(
        "must contain only",
        lambda: normalized_postgres_storage_parameters(
            {"fillfactor": "70;"}, table_name="flavor"
        ),
    )

    assert normalized_postgres_partition_by(None, table_name="flavor") is None
    assert (
        normalized_postgres_partition_by("range (id)", table_name="flavor")
        == "RANGE (id)"
    )
    assert (
        normalized_mysql_partition_by("linear key (id)", table_name="flavor")
        == "LINEAR KEY (id)"
    )
    assert_invalid(
        "cannot be empty",
        lambda: normalized_postgres_partition_by(" ", table_name="flavor"),
    )
    assert_invalid(
        "cannot be empty",
        lambda: normalized_mysql_partition_by(" ", table_name="flavor"),
    )
    assert_invalid(
        "statement separators",
        lambda: normalized_postgres_partition_by("range (id);", table_name="flavor"),
    )
    assert_invalid(
        "RANGE",
        lambda: normalized_postgres_partition_by("bad (id)", table_name="flavor"),
    )
    assert_invalid(
        "balanced",
        lambda: normalized_postgres_partition_by("range (id))", table_name="flavor"),
    )
    assert_invalid(
        "KEY", lambda: normalized_mysql_partition_by("bad (id)", table_name="flavor")
    )
    assert_invalid(
        "balanced",
        lambda: normalized_mysql_partition_by("hash (id))", table_name="flavor"),
    )

    assert normalized_postgres_partition_for(None, table_name="flavor") is None
    assert (
        normalized_postgres_partition_for("default", table_name="flavor") == "DEFAULT"
    )
    assert (
        normalized_postgres_partition_for("for values in (1, 2)", table_name="flavor")
        == "FOR VALUES IN (1, 2)"
    )
    assert (
        normalized_postgres_partition_for("from (1) to (10)", table_name="flavor")
        == "FOR VALUES FROM (1) TO (10)"
    )
    assert_invalid(
        "cannot be empty",
        lambda: normalized_postgres_partition_for(" ", table_name="flavor"),
    )
    assert_invalid(
        "statement separators",
        lambda: normalized_postgres_partition_for("default;", table_name="flavor"),
    )
    assert_invalid(
        "must use",
        lambda: normalized_postgres_partition_for(
            "between 1 and 2", table_name="flavor"
        ),
    )
    assert_invalid(
        "balanced",
        lambda: normalized_postgres_partition_for("in (1))", table_name="flavor"),
    )
    assert _balanced_parentheses("('it''s ok')") is True
    assert _balanced_parentheses('("ok")') is True

    assert normalized_sqlite_conflict(None, option_name="conflict") is None
    assert normalized_sqlite_conflict("fail", option_name="conflict") == "FAIL"
    assert_invalid(
        "must be one of",
        lambda: normalized_sqlite_conflict("explode", option_name="conflict"),
    )


def test_namespace_sequence_and_view_runtime_metadata() -> None:
    assert_invalid(
        "namespace comment", lambda: DatabaseNamespace(name="inventory", comment=" ")
    )
    namespace = DatabaseNamespace(name=" inventory ", comment=" Warehouse ")
    assert namespace.to_runtime() == ("inventory", "Warehouse")

    assert_invalid(
        "sequence increment", lambda: DatabaseSequence(name="seq", increment=0)
    )
    assert_invalid(
        "no_min_value",
        lambda: DatabaseSequence(name="seq", no_min_value=True, min_value=1),
    )
    assert_invalid(
        "no_max_value",
        lambda: DatabaseSequence(name="seq", no_max_value=True, max_value=1),
    )
    assert_invalid("sequence cache", lambda: DatabaseSequence(name="seq", cache=0))
    assert_invalid(
        "min_value", lambda: DatabaseSequence(name="seq", min_value=10, max_value=1)
    )
    assert_invalid(
        "sequence comment", lambda: DatabaseSequence(name="seq", comment=" ")
    )
    sequence = DatabaseSequence(
        name=" seq ",
        schema=" public ",
        data_type="BIGINT",
        start=10,
        increment=2,
        min_value=1,
        max_value=100,
        cycle=True,
        cache=20,
        comment=" ids ",
        order=True,
        no_min_value=False,
        no_max_value=False,
    )
    assert sequence.to_runtime() == (
        "seq",
        "public",
        10,
        2,
        1,
        100,
        True,
        20,
        "ids",
        "bigint",
        True,
        False,
        False,
    )

    assert_invalid("view definition", lambda: DatabaseView(name="v", definition=" ; "))
    assert_invalid(
        "view comment",
        lambda: DatabaseView(name="v", definition="SELECT 1", comment=" "),
    )
    view = DatabaseView(
        name=" active ", definition=" SELECT 1; ", schema=" public ", comment=" ok "
    )
    assert view.name == "active"
    assert view.schema_name == "public"
    assert view.definition == "SELECT 1"
    assert view.comment == "ok"


def test_table_exclusion_before_validator_non_mapping_edge() -> None:
    with pytest.raises(ValueError):
        TableExclusion.model_validate("not a mapping")
