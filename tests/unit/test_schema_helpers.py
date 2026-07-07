from __future__ import annotations

from enum import Enum
from types import SimpleNamespace
from typing import Callable

import pytest
from pydantic import BaseModel, Field

import ormdantic.schema as schema
from ormdantic.models import (
    Map,
    OrmTable,
    Relationship,
    TableCheck,
    TableColumn,
    TableExclusion,
    TableForeignKey,
    TableIndex,
    TableUnique,
)


class SchemaHelperModel(BaseModel):
    id: int


class FlavorKind(str, Enum):
    mocha = "mocha"
    latte = "latte"


class NumericKind(Enum):
    one = 1


class AlternateFlavorKind(str, Enum):
    caramel = "caramel"


class EnumModel(BaseModel):
    primary: FlavorKind
    fallback: FlavorKind
    numeric: NumericKind


class EnumConflictModel(BaseModel):
    primary: FlavorKind
    alternate: AlternateFlavorKind


class FieldKindModel(BaseModel):
    callback: Callable[[], None]
    maybe_dict: dict[str, int] | None = None
    none_first_dict: None | dict[str, int] = None
    maybe_list: list[int] | None = None
    child: SchemaHelperModel | None = None
    bounded: int = Field(ge=1, gt=0, le=10, lt=11, multiple_of=2)
    named: str = Field(min_length=2, max_length=8, pattern=r"^[a-z]+$")


def helper_table(**overrides):
    data = {
        "model": SchemaHelperModel,
        "tablename": "flavor",
        "pk": "id",
        "schema_name": None,
        "tablespace": None,
        "indexes": [],
        "named_unique_constraints": [],
        "check_constraints": [],
        "foreign_key_constraints": [],
        "exclusion_constraints": [],
        "unique_constraints": [],
        "unique": [],
        "relationships": {},
        "column_options": {},
    }
    data.update(overrides)
    return SimpleNamespace(**data)


def test_validate_table_map_returns_none_without_native_validator(monkeypatch) -> None:
    monkeypatch.setattr(schema, "_ormdantic", None)
    assert schema.validate_table_map(Map()) is None


def test_validate_table_map_sends_relationship_descriptors(monkeypatch) -> None:
    class SupplierModel(BaseModel):
        id: int

    class ProductModel(BaseModel):
        id: int

    supplier_table = OrmTable[SupplierModel](
        model=SupplierModel,
        tablename="supplier",
        pk="id",
        indexed=[],
        unique=[],
        unique_constraints=[],
        columns=["id"],
        relationships={},
        back_references={},
    )
    product_table = OrmTable[ProductModel](
        model=ProductModel,
        tablename="product",
        pk="id",
        indexed=[],
        unique=[],
        unique_constraints=[],
        columns=["id"],
        relationships={"supplier": Relationship(foreign_table="supplier")},
        back_references={},
    )
    captured: dict[str, object] = {}

    class FakeRuntime:
        @staticmethod
        def validate_schema_tables(tables: list[object]) -> int:
            captured["tables"] = tables
            return len(tables)

    monkeypatch.setattr(schema, "_ormdantic", FakeRuntime)

    assert (
        schema.validate_table_map(
            Map(name_to_data={"supplier": supplier_table, "product": product_table})
        )
        == 2
    )
    product_payload = captured["tables"][1]
    assert product_payload[11] == [
        ("supplier", "supplier", "id", None),
    ]


def test_compile_helper_post_processors_cover_edge_branches() -> None:
    table = helper_table(tablespace="fastspace")
    assert schema._compile_mssql_filegroup_create_fallback(
        "mssql",
        table,
        ValueError("table tablespace is unsupported"),
        lambda tablespace: (
            ["CREATE TABLE [flavor] ([id] int)"] if tablespace is None else []
        ),
    ) == ["CREATE TABLE [flavor] ([id] int) ON [fastspace]"]
    with pytest.raises(ValueError, match="other"):
        schema._compile_mssql_filegroup_create_fallback(
            "postgresql",
            table,
            ValueError("other"),
            lambda _tablespace: [],
        )
    assert (
        schema._compile_mssql_filegroup_create_fallback(
            "mssql",
            table,
            ValueError("table tablespace is unsupported"),
            lambda _tablespace: [],
        )
        == []
    )

    assert schema._compile_inline_index_comment_sql(
        "postgresql",
        helper_table(indexes=[TableIndex(name="idx", columns=["id"])]),
        ["CREATE INDEX"],
    ) == ["CREATE INDEX"]
    assert schema._compile_oracle_index_tablespace_sql(
        "oracle",
        helper_table(indexes=[TableIndex(name="idx", columns=["id"])]),
        ["CREATE INDEX"],
    ) == ["CREATE INDEX"]
    assert (
        schema._compile_constraint_comment_sql(
            "postgresql",
            helper_table(
                named_unique_constraints=[
                    TableUnique(name="uq", columns=["id"], comment=None)
                ],
                check_constraints=[
                    TableCheck(name="ck", expression="id > 0", comment=None)
                ],
            ),
        )
        == []
    )
    assert schema._compile_constraint_comment_sql(
        "postgresql",
        helper_table(
            named_unique_constraints=[
                TableUnique(name="uq", columns=["id"], comment="Unique id")
            ],
            check_constraints=[TableCheck(name="ck", expression="id > 0")],
        ),
    ) == ['COMMENT ON CONSTRAINT "uq" ON "flavor" IS \'Unique id\'']
    assert schema._compile_index_tablespace_sql(
        "postgresql",
        helper_table(
            indexes=[
                TableIndex(name="plain", columns=["id"]),
                TableIndex(
                    name="spaced",
                    columns=["id"],
                    postgres_tablespace="fastspace",
                ),
            ]
        ),
    ) == ['ALTER INDEX "spaced" SET TABLESPACE "fastspace"']
    assert (
        schema._compile_index_comment_sql(
            "postgresql",
            helper_table(
                indexes=[TableIndex(name="idx", columns=["id"], comment=None)]
            ),
        )
        == []
    )
    assert (
        schema._compile_index_comment_sql(
            "mysql",
            helper_table(
                indexes=[TableIndex(name="idx", columns=["id"], comment="note")]
            ),
        )
        == []
    )
    assert schema._compile_index_comment_sql(
        "postgresql",
        helper_table(
            indexes=[
                TableIndex(name="plain", columns=["id"]),
                TableIndex(name="idx", columns=["id"], comment="Index note"),
            ]
        ),
    ) == ["COMMENT ON INDEX \"idx\" IS 'Index note'"]


def test_schema_enum_descriptor_merge_and_display_edges() -> None:
    table = OrmTable[EnumModel](
        model=EnumModel,
        tablename="enum_model",
        pk="primary",
        indexed=[],
        unique=[],
        unique_constraints=[],
        columns=["primary", "fallback", "numeric"],
        relationships={},
        back_references={},
        column_options={
            "primary": TableColumn(enum_type_comment="Flavor enum"),
            "fallback": TableColumn(enum_type_comment="Flavor enum"),
        },
    )
    descriptors = schema.enum_type_descriptors(
        Map(name_to_data={"enum_model": table}),
        schema="inventory",
    )
    assert descriptors == [
        (
            "flavor_kind",
            ["mocha", "latte"],
            "inventory",
            "Flavor enum",
        )
    ]
    assert (
        schema.enum_type_qualified_name(("flavor_kind", ["mocha"], None, None))
        == "flavor_kind"
    )
    assert (
        schema.enum_type_qualified_name(("flavor_kind", ["mocha"], "inventory", None))
        == "inventory.flavor_kind"
    )

    conflict_table = table.model_copy(
        update={
            "column_options": {
                "primary": TableColumn(enum_type_comment="A"),
                "fallback": TableColumn(enum_type_comment="B"),
            }
        }
    )
    with pytest.raises(ValueError, match="different comments"):
        schema.enum_type_descriptors(Map(name_to_data={"enum_model": conflict_table}))

    skipped_back_reference = table.model_copy(
        update={
            "back_references": {"fallback": "primary"},
            "column_options": {
                "primary": TableColumn(enum_type_comment="Flavor enum"),
            },
        }
    )
    assert schema.enum_type_descriptors(
        Map(name_to_data={"enum_model": skipped_back_reference})
    ) == [
        (
            "flavor_kind",
            ["mocha", "latte"],
            None,
            "Flavor enum",
        )
    ]

    value_conflict = OrmTable[EnumConflictModel](
        model=EnumConflictModel,
        tablename="enum_conflict",
        pk="primary",
        indexed=[],
        unique=[],
        unique_constraints=[],
        columns=["primary", "alternate"],
        relationships={},
        back_references={},
        column_options={
            "primary": TableColumn(enum_type_name="flavor_kind"),
            "alternate": TableColumn(enum_type_name="flavor_kind"),
        },
    )
    with pytest.raises(ValueError, match="different values"):
        schema.enum_type_descriptors(
            Map(name_to_data={"enum_conflict": value_conflict})
        )


def test_constraint_descriptor_duplicate_guards_and_runtime_views() -> None:
    table = helper_table(
        relationships={"supplier": Relationship(foreign_table="supplier")},
        foreign_key_constraints=[
            TableForeignKey(
                name="flavor_supplier_foreign_key",
                columns=["supplier_id"],
                foreign_table="supplier",
                foreign_columns=["id"],
            )
        ],
    )
    with pytest.raises(ValueError, match="duplicate foreign key"):
        schema.foreign_key_constraint_descriptors(table)

    exclusion_table = helper_table(
        named_unique_constraints=[TableUnique(name="overlap", columns=["id"])],
        exclusion_constraints=[
            TableExclusion(name="overlap", columns=[("period", "&&")])
        ],
    )
    with pytest.raises(ValueError, match="duplicate exclusion"):
        schema.exclusion_constraint_descriptors(exclusion_table)

    unique_table = helper_table(
        unique_constraints=[["id"]],
        named_unique_constraints=[TableUnique(name="flavor_unique_0", columns=["id"])],
    )
    with pytest.raises(ValueError, match="duplicate unique"):
        schema.unique_constraint_descriptors(unique_table)

    descriptor_table = helper_table(
        foreign_key_constraints=[
            TableForeignKey(
                name="fk",
                columns=["supplier_id"],
                foreign_table="supplier",
                foreign_columns=["id"],
                comment="Supplier",
            )
        ],
        exclusion_constraints=[
            TableExclusion(
                name="ex",
                columns=[("period", "&&")],
                comment="No overlap",
            )
        ],
        named_unique_constraints=[
            TableUnique(name="uq", columns=["id"], oracle_compress=True)
        ],
    )
    assert schema.rust_foreign_key_constraint_descriptors(descriptor_table)[0] == (
        "fk",
        ["supplier_id"],
        "supplier",
        ["id"],
        None,
        None,
        None,
        False,
        True,
        None,
    )
    assert schema.rust_exclusion_constraint_descriptors(descriptor_table)[0] == (
        "ex",
        [("period", "&&")],
        [],
        "gist",
        None,
        None,
        False,
        {},
    )
    assert schema.rust_unique_constraint_descriptors(descriptor_table)[0][-1] == "true"


def test_field_kind_checks_and_sql_literal_edges() -> None:
    fields = schema.model_fields(FieldKindModel)
    with pytest.raises(schema.TypeConversionError):
        schema.field_kind(fields["callback"])
    assert schema.field_kind(fields["maybe_dict"]) == "dict"
    assert schema.field_kind(fields["none_first_dict"]) == "dict"
    assert schema.field_kind(fields["maybe_list"]) == "list"
    assert schema.field_kind(fields["child"]) == "uuid"
    assert (
        schema.field_kind(
            schema.model_fields(EnumModel)["primary"],
            native_enum_types=True,
            options=TableColumn(enum_type_name="kind", enum_schema="inventory"),
        )
        == "enum:inventory.kind"
    )
    assert (
        schema.field_kind(
            schema.model_fields(EnumModel)["numeric"],
            native_enum_types=True,
        )
        == "enum"
    )

    checks = schema.check_constraints("bounded", fields["bounded"])
    assert ("comparison", ">=", "1") in checks
    assert ("comparison", ">", "0") in checks
    assert ("comparison", "<=", "10") in checks
    assert ("comparison", "<", "11") in checks
    assert ("multiple_of", "=", "2") in checks
    assert ("length", ">=", "2") in schema.check_constraints("named", fields["named"])
    assert ("length", "<=", "8") in schema.check_constraints("named", fields["named"])
    assert (
        "pattern",
        "matches",
        "'^[a-z]+$'",
    ) in schema.check_constraints("named", fields["named"])
    assert schema.sql_literal(None) == "NULL"
    assert schema.sql_literal(True) == "1"
    assert schema.sql_literal(False) == "0"
    assert schema.sql_literal(1.5) == "1.5"
    assert schema.sql_literal("chef's") == "'chef''s'"


def test_require_schema_symbol_reports_missing_symbol(monkeypatch) -> None:
    monkeypatch.setattr(schema, "_ormdantic", object())

    with pytest.raises(schema.NativeExtensionError):
        schema._require_schema_symbol("compile_drop_table_sql")

    rust = type(
        "Rust",
        (),
        {
            "compile_drop_table_sql": staticmethod(
                lambda dialect, tablename: f"{dialect}:{tablename}"
            )
        },
    )
    monkeypatch.setattr(schema, "_ormdantic", rust)
    assert schema.compile_drop_table_sql("flavor", "sqlite") == "sqlite:flavor"
