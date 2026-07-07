from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterator, Literal
from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel, Field

from ormdantic.hydration import hydrate_flat_payload
from ormdantic.loaders import LoaderOption
from ormdantic.models import Map, OrmTable, Relationship
from ormdantic.serializer import OrmSerializer, ResultSchema


class HydratedFlavor(BaseModel):
    id: UUID = Field(default_factory=uuid4)
    name: str
    strength: int


class HydratedNote(BaseModel):
    id: int
    label: str
    meta: dict[str, int] = Field(default_factory=dict)
    tags: list[str] = Field(default_factory=list)
    maybe_count: None | int = None
    flexible: Any = None
    child: "HydratedNote | None" = None
    children: list["HydratedNote"] = Field(default_factory=list)


class LiteralHydratedNote(BaseModel):
    id: int
    kind: Literal["keep"]


@dataclass
class FakeCursor:
    description: list[tuple[str]]


class FakeResult:
    def __init__(self, columns: list[str], rows: list[tuple[Any, ...]]) -> None:
        self.cursor = FakeCursor([(column,) for column in columns])
        self._rows = rows

    def __iter__(self) -> Iterator[tuple[Any, ...]]:
        return iter(self._rows)


def flavor_table() -> OrmTable[HydratedFlavor]:
    return OrmTable[HydratedFlavor](
        model=HydratedFlavor,
        tablename="hydrated_flavors",
        pk="id",
        indexed=[],
        unique=[],
        unique_constraints=[],
        columns=["id", "name", "strength"],
        relationships={},
        back_references={},
    )


def note_table(
    *, relationships: dict[str, Relationship] | None = None
) -> OrmTable[HydratedNote]:
    return OrmTable[HydratedNote](
        model=HydratedNote,
        tablename="hydrated_notes",
        pk="id",
        indexed=[],
        unique=[],
        unique_constraints=[],
        columns=["id", "label", "meta", "tags", "maybe_count", "flexible"],
        relationships=relationships or {},
        back_references={},
    )


def serializer_for_notes(
    table: OrmTable[HydratedNote] | None = None,
    *,
    load_paths: tuple[str, ...] | None = None,
    load_options: tuple[LoaderOption, ...] = (),
    depth: int = 0,
) -> OrmSerializer[Any]:
    table = table or note_table()
    return OrmSerializer[Any](
        table_data=table,
        table_map=Map(name_to_data={table.tablename: table}, model_to_data={}),
        result_set=FakeResult(["hydrated_notes\\id"], []),
        is_array=False,
        depth=depth,
        load_paths=load_paths,
        load_options=load_options,
    )


def test_hydrate_flat_payload_deduplicates_array_rows_by_pk() -> None:
    flavor_id = str(uuid4())

    payload = hydrate_flat_payload(
        tablename="hydrated_flavors",
        pk="id",
        columns=[
            "hydrated_flavors\\id",
            "hydrated_flavors\\name",
            "hydrated_flavors\\strength",
        ],
        rows=[
            (flavor_id, "mocha", 1),
            (flavor_id, "duplicate", 2),
        ],
        is_array=True,
    )

    assert payload == [{"id": flavor_id, "name": "mocha", "strength": 1}]


def test_hydrate_flat_payload_returns_single_record() -> None:
    flavor_id = str(uuid4())

    payload = hydrate_flat_payload(
        tablename="hydrated_flavors",
        pk="id",
        columns=[
            "hydrated_flavors\\id",
            "hydrated_flavors\\name",
            "hydrated_flavors\\strength",
        ],
        rows=[(flavor_id, "mocha", 1)],
        is_array=False,
    )

    assert payload == {"id": flavor_id, "name": "mocha", "strength": 1}


def test_serializer_uses_flat_hydration_path_for_array_results() -> None:
    table = flavor_table()
    rows = [
        (str(uuid4()), "mocha", 1),
        (str(uuid4()), "vanilla", 2),
    ]
    result = FakeResult(
        [
            "hydrated_flavors\\id",
            "hydrated_flavors\\name",
            "hydrated_flavors\\strength",
        ],
        rows,
    )

    hydrated = OrmSerializer[list[HydratedFlavor]](
        table_data=table,
        table_map=Map(name_to_data={table.tablename: table}, model_to_data={}),
        result_set=result,
        is_array=True,
        depth=0,
    ).deserialize()

    assert hydrated == [
        HydratedFlavor(id=rows[0][0], name="mocha", strength=1),
        HydratedFlavor(id=rows[1][0], name="vanilla", strength=2),
    ]


def test_serializer_schema_path_and_identity_edge_branches() -> None:
    table = note_table(
        relationships={
            "child": Relationship(foreign_table="hydrated_notes"),
            "children": Relationship(
                foreign_table="hydrated_notes",
                back_references="parent",
            ),
        }
    )
    serializer = serializer_for_notes(
        table,
        load_paths=("missing.branch", "children//child"),
        depth=1,
    )

    assert serializer._path_tree(("children//child", ".child")) == {
        "children": {"child": {}},
        "child": {},
    }
    assert serializer._identity_for({"label": "missing id"}, table) is None
    assert (
        serializer._path_pks(
            ResultSchema(
                is_array=False,
                references={"missing": ResultSchema(is_array=False)},
            )
        )
        == []
    )
    assert serializer._prep_result({"raw": "value"}, ResultSchema(is_array=False)) == {
        "raw": "value"
    }

    with pytest.raises(ValueError, match="missing table metadata"):
        serializer._build_model({"id": 1}, ResultSchema(is_array=False))

    fallback = serializer_for_notes(depth=-1)
    assert fallback._result_schema.references["hydrated_notes"].table_data is table or (
        fallback._result_schema.references["hydrated_notes"].table_data is not None
    )


def test_serializer_loader_options_filter_order_and_nested_none() -> None:
    serializer = serializer_for_notes(
        load_options=(
            LoaderOption(path="child", strategy="joined"),
            LoaderOption(
                path="children",
                strategy="joined",
                filter_by={"label": "keep"},
                order_by=("-id",),
            ),
            LoaderOption(
                path="children.child",
                strategy="joined",
                filter_by={"label": "keep"},
                order_by=("-id",),
            ),
        )
    )
    keep = HydratedNote(id=2, label="keep", child=HydratedNote(id=3, label="keep"))
    drop = HydratedNote(id=1, label="drop", child=HydratedNote(id=4, label="drop"))
    root = HydratedNote(
        id=10,
        label="root",
        child=HydratedNote(id=5, label="keep"),
        children=[drop, keep],
    )

    assert serializer._apply_loader_options(root) is root
    assert [note.id for note in root.children] == [2]
    assert (
        serializer._filter_and_order_relationship(None, serializer._load_options[1])
        is None
    )
    assert (
        serializer._filter_and_order_relationship(
            HydratedNote(id=1, label="drop"),
            serializer._load_options[1],
        )
        is None
    )
    serializer._apply_loader_option(root, (), serializer._load_options[1])
    serializer._apply_loader_option(None, ("children",), serializer._load_options[1])
    serializer._apply_loader_option(
        root,
        ("child", "child"),
        serializer._load_options[2],
    )


def test_serializer_sql_type_conversion_edges() -> None:
    assert OrmSerializer._sql_type_to_py(HydratedNote, "meta", None) == {}
    assert OrmSerializer._sql_type_to_py(HydratedNote, "meta", '{"a": 1}') == {"a": 1}
    assert OrmSerializer._sql_type_to_py(HydratedNote, "tags", None) == []
    assert OrmSerializer._sql_type_to_py(HydratedNote, "tags", '["x"]') == ["x"]
    assert OrmSerializer._sql_type_to_py(HydratedNote, "maybe_count", "5") == 5
    marker = object()
    assert OrmSerializer._sql_type_to_py(HydratedNote, "flexible", marker) is marker
    assert OrmSerializer._sql_type_to_py(LiteralHydratedNote, "kind", "drop") == "drop"
