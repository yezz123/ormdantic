from __future__ import annotations

import logging
from typing import Any

import pytest
from pydantic import BaseModel

import ormdantic.table as table_module
from ormdantic.errors import (
    HydrationError,
    QueryCompilationError,
    RelationshipLoadingError,
)
from ormdantic.events import EventRegistry
from ormdantic.expressions import column, select_query, update_query
from ormdantic.loaders import joinedload, noload, selectinload
from ormdantic.models import Map, OrmTable, Relationship, TableColumn
from ormdantic.table import DEFAULT_SELECTIN_BATCH_SIZE, Order, Table, _ResolvedLoadPlan


class BatchModel(BaseModel):
    id: int
    kind: str = "keep"


class PayloadModel(BaseModel):
    id: int | None = None
    kind: str
    created_at: str | None = None
    computed_label: str | None = None
    identity_value: int | None = None
    optional_identity: int | None = None


class BindLimitHandle:
    def __init__(self, limit: int | None) -> None:
        self.limit = limit

    def max_bind_parameters(self) -> int | None:
        return self.limit


class ExpressionHandle(BindLimitHandle):
    def __init__(self) -> None:
        super().__init__(None)
        self.calls: list[tuple[str, Any]] = []

    def select_expression(self, payload: dict[str, Any]) -> dict[str, Any]:
        self.calls.append(("select_expression", payload))
        return {"columns": ["id"], "rows": [(2,), (1,)]}

    def find_many_with_paths(
        self,
        filters: Any,
        values: dict[str, Any],
        order_by: list[str],
        order: str,
        limit: int | None,
        offset: int | None,
        load_paths: list[str],
        joined_filters: Any,
        joined_order_by: Any,
    ) -> dict[str, Any]:
        self.calls.append(
            (
                "find_many_with_paths",
                {
                    "filters": filters,
                    "values": dict(values),
                    "order_by": order_by,
                    "order": order,
                    "limit": limit,
                    "offset": offset,
                    "load_paths": load_paths,
                    "joined_filters": joined_filters,
                    "joined_order_by": joined_order_by,
                },
            )
        )
        return {"columns": ["id", "kind"], "rows": [(1, "keep"), (2, "keep")]}

    def find_many(
        self,
        filters: Any,
        values: dict[str, Any],
        order_by: list[str],
        order: str,
        limit: int | None,
        offset: int | None,
        depth: int,
    ) -> dict[str, Any]:
        self.calls.append(
            (
                "find_many",
                {
                    "filters": filters,
                    "values": dict(values),
                    "order_by": order_by,
                    "order": order,
                    "limit": limit,
                    "offset": offset,
                    "depth": depth,
                },
            )
        )
        return {"columns": ["id", "kind"], "rows": [(1, "keep"), (2, "keep")]}


def table_for_bind_limit(
    limit: int | None,
    *,
    table_map: Map | None = None,
    table_data: OrmTable[BatchModel] | None = None,
) -> Table[BatchModel]:
    table_data = table_data or OrmTable[BatchModel](
        model=BatchModel,
        tablename="batch_model",
        pk="id",
        indexed=[],
        unique=[],
        unique_constraints=[],
        columns=["id", "kind"],
        relationships={},
        back_references={},
    )
    return Table(
        table_data=table_data,
        table_map=table_map or Map(),
        rust_handle=BindLimitHandle(limit),
        events=EventRegistry(),
    )


def expression_table() -> tuple[Table[BatchModel], ExpressionHandle]:
    child_table = OrmTable[BatchModel](
        model=BatchModel,
        tablename="children",
        pk="id",
        indexed=[],
        unique=[],
        unique_constraints=[],
        columns=["id", "kind"],
        relationships={},
        back_references={},
    )
    table_data = OrmTable[BatchModel](
        model=BatchModel,
        tablename="batch_model",
        pk="id",
        indexed=[],
        unique=[],
        unique_constraints=[],
        columns=["id", "kind"],
        relationships={"children": Relationship(foreign_table="children")},
        back_references={},
    )
    table_map = Map(name_to_data={"batch_model": table_data, "children": child_table})
    handle = ExpressionHandle()
    return Table(
        table_data=table_data,
        table_map=table_map,
        rust_handle=handle,
        events=EventRegistry(),
        connection="sqlite:///memory",
    ), handle


def test_selectin_batches_cap_requested_size_to_backend_bind_limit() -> None:
    table = table_for_bind_limit(3)
    option = selectinload("children").filter(kind="keep").batched(10)

    assert table._selectin_batches([1, 2, 3, 4, 5], option) == [
        [1, 2],
        [3, 4],
        [5],
    ]


def test_selectin_batches_keep_requested_size_without_backend_limit() -> None:
    table = table_for_bind_limit(None)
    option = selectinload("children").batched(DEFAULT_SELECTIN_BATCH_SIZE + 1)

    assert table._selectin_batches(list(range(3)), option) == [[0, 1, 2]]


def test_selectin_and_relationship_helpers_normalize_values() -> None:
    table = table_for_bind_limit(10)
    option = selectinload("children").filter(kind="keep").sorted_by("-id")
    related_table = table._table_data
    first = BatchModel(id=1, kind="keep")
    duplicate = BatchModel(id=1, kind="drop")
    second = BatchModel(id=2, kind="keep")

    assert table._path_tree(("children.leaves", "children/tags")) == {
        "children": {"leaves": {}, "tags": {}}
    }
    assert table._selectin_where("parent", [1, 2], option) == {
        "parent__in": [1, 2],
        "kind": "keep",
    }
    assert table._foreign_key_value(None) is None
    assert table._foreign_key_value(first) is None
    assert table._foreign_key_value("fk-1") == "fk-1"
    assert table._unique_values([1, "1", 2, None, 1]) == [1, 2]
    assert table._loaded_relationship_values(
        [
            BatchModel(id=10, kind="keep").model_copy(
                update={"children": [first, duplicate, second]}
            )
        ],
        "children",
        related_table,
    ) == [first, second]
    assert table._filter_and_order_relationship([first, duplicate, second], option) == [
        second,
        first,
    ]
    assert table._filter_and_order_relationship(first, option) == first
    assert table._filter_and_order_relationship(duplicate, option) is None
    assert table._filter_and_order_relationship(None, option) is None
    assert table._matches_loader_filter(first, option)
    assert not table._matches_loader_filter(duplicate, option)


def test_joined_loader_query_parts_keep_loader_parameters_ordered() -> None:
    table = table_for_bind_limit(None)
    option = joinedload("children").filter(kind="keep").sorted_by("-id")
    plan = _ResolvedLoadPlan(
        depth=0,
        paths=("children",),
        options=(option, selectinload("children.tags").filter(kind="tag")),
    )

    filters, order_by, values = table._joined_loader_query_parts(plan)

    assert filters == [("batch_model/children", [("kind", "eq", ["loader_0__kind"])])]
    assert order_by == [("batch_model/children", "id", "desc")]
    assert values == {"loader_0__kind": "keep"}


def test_table_helper_methods_cover_debug_backend_and_ordering() -> None:
    table = table_for_bind_limit(None)
    table._debug = True
    table._connection = "postgres://localhost/db"
    items = [
        BatchModel(id=2, kind="keep"),
        BatchModel(id=1, kind="keep"),
        BatchModel(id=3, kind="keep"),
    ]

    assert table._requires_expression_select(column("id").eq(column("kind")), None)
    assert table._requires_expression_select(None, [column("kind").desc()])
    assert not table._requires_expression_select({"kind": "keep"}, ["id"])
    assert table._legacy_order_columns(["id", column("kind").desc(), "-kind"]) == [
        "id",
        "-kind",
    ]
    assert [
        order.direction for order in table._expression_order_by(["-id"], Order.asc)
    ] == ["desc"]
    assert [
        order.direction for order in table._expression_order_by(["id"], Order.desc)
    ] == ["desc"]
    assert [
        item.id for item in table._order_by_primary_key_sequence(items, [3, 1, 2])
    ] == [3, 1, 2]
    assert table._backend() == "postgresql"
    table._connection = None
    assert table._backend() == "unknown"
    assert table._row_count({"rows": [(1,), (2,)]}) == 2
    assert table._row_count({"rows": "not rows"}) is None
    assert table._debug_payload(
        "select",
        parameters={"name": "Mocha"},
        compile_query=lambda: {"sql": "SELECT ?", "params": ["name"]},
        context=table._context("select"),
    ) == {
        "debug": True,
        "sql": "SELECT ?",
        "bind_names": ["name"],
        "parameters": {"name": "Mocha"},
    }


async def test_find_many_expression_uses_primary_key_loader_for_eager_paths() -> None:
    table, _handle = expression_table()
    plan = _ResolvedLoadPlan(depth=1, paths=("children",))
    calls: list[dict[str, Any]] = []

    async def fake_primary_key_loader(**kwargs: Any) -> list[BatchModel]:
        calls.append(kwargs)
        return [BatchModel(id=1)]

    table._find_many_expression_by_primary_keys = fake_primary_key_loader  # type: ignore[method-assign]

    result = await table._find_many_expression(
        where=column("kind").eq("keep"),
        order_by=["id"],
        order=Order.desc,
        limit=5,
        offset=2,
        load_plan=plan,
    )

    assert result.data == [BatchModel(id=1)]
    assert result.limit == 5
    assert result.offset == 2
    assert calls[0]["load_plan"] is plan


async def test_table_loader_helpers_cover_empty_scalar_and_runtime_edges() -> None:
    table = table_for_bind_limit(None)
    plan = _ResolvedLoadPlan(
        depth=0,
        paths=None,
        selectin_paths=("children",),
        options=(),
        use_selectin=True,
    )

    assert await table._load_selectin_graph([], plan) is None
    assert (
        await table._load_selectin_tree(
            [],
            table._table_data,
            {"children": {}},
            (),
            {},
            {},
            set(),
        )
        is None
    )

    child = BatchModel(id=2)
    parent = BatchModel(id=1)
    object.__setattr__(parent, "child_id", 2)

    class RelatedHandle:
        async def find_many(self, *, where: dict[str, Any]) -> Any:
            assert where == {"id__in": [2]}
            return type("Result", (), {"data": [child]})()

    table._related_table = lambda _: RelatedHandle()  # type: ignore[method-assign]
    assigned = await table._selectin_load_relationship(
        [parent],
        table._table_data,
        "child_id",
        None,
        table._table_data,
        None,
        {},
    )

    assert assigned == [child]
    assert parent.child_id is child

    scalar_parent = BatchModel(id=3)
    object.__setattr__(scalar_parent, "child", child)
    assert table._loaded_relationship_values(
        [scalar_parent],
        "child",
        table._table_data,
    ) == [child]

    no_limit_table = Table(
        table_data=table._table_data,
        table_map=Map(),
        rust_handle=object(),
        events=EventRegistry(),
    )
    assert no_limit_table._max_bind_parameters() is None
    with pytest.raises(RuntimeError, match="select-in relationship loading"):
        no_limit_table._related_table(table._table_data)

    no_limit_table._debug = True
    assert no_limit_table._debug_payload(
        "select",
        parameters={"id": 1},
        compile_query=lambda: {"sql": "SELECT 1", "params": []},
        context=no_limit_table._context("select"),
    )["bind_names"] == ["id"]


async def test_expression_loader_with_paths_selects_primary_keys_then_reorders() -> (
    None
):
    table, handle = expression_table()
    plan = _ResolvedLoadPlan(
        depth=0,
        paths=("children",),
        options=(joinedload("children").filter(kind="keep").sorted_by("-id"),),
    )

    async def fake_deserialize(*_: Any, **__: Any) -> list[BatchModel]:
        return [BatchModel(id=1), BatchModel(id=2)]

    table._deserialize = fake_deserialize  # type: ignore[method-assign]

    result = await table._find_many_expression_by_primary_keys(
        where=column("kind").eq("keep"),
        order_by=["-id"],
        order=Order.asc,
        limit=10,
        offset=5,
        load_plan=plan,
    )

    assert [model.id for model in result] == [2, 1]
    assert handle.calls[0][0] == "select_expression"
    assert handle.calls[1][0] == "find_many_with_paths"
    path_call = handle.calls[1][1]
    assert path_call["filters"]["connector"] == "leaf"
    assert path_call["filters"]["filters"][0][:2] == ("id", "in")
    id_params = path_call["filters"]["filters"][0][2]
    assert path_call["values"] == {
        id_params[0]: 2,
        id_params[1]: 1,
        "loader_0__kind": "keep",
    }
    assert path_call["load_paths"] == ["children"]
    assert path_call["joined_filters"] == [
        ("batch_model/children", [("kind", "eq", ["loader_0__kind"])])
    ]
    assert path_call["joined_order_by"] == [("batch_model/children", "id", "desc")]


async def test_expression_loader_without_paths_uses_depth_loader_and_selectin() -> None:
    table, handle = expression_table()
    loaded_graphs: list[list[int]] = []
    plan = _ResolvedLoadPlan(
        depth=1,
        paths=None,
        selectin_paths=("children",),
        use_selectin=True,
    )

    async def fake_deserialize(*_: Any, **__: Any) -> list[BatchModel]:
        return [BatchModel(id=1), BatchModel(id=2)]

    async def fake_load_selectin_graph(
        roots: list[BatchModel], load_plan: _ResolvedLoadPlan
    ) -> None:
        loaded_graphs.append([root.id for root in roots])
        assert load_plan is plan

    table._deserialize = fake_deserialize  # type: ignore[method-assign]
    table._load_selectin_graph = fake_load_selectin_graph  # type: ignore[method-assign]

    result = await table._find_many_expression_by_primary_keys(
        where=None,
        order_by=[],
        order=Order.desc,
        limit=0,
        offset=0,
        load_plan=plan,
    )

    assert [model.id for model in result] == [2, 1]
    assert loaded_graphs == [[1, 2]]
    assert handle.calls[1][0] == "find_many"
    depth_call = handle.calls[1][1]
    assert depth_call["filters"]["connector"] == "leaf"
    assert depth_call["filters"]["filters"][0][:2] == ("id", "in")
    id_params = depth_call["filters"]["filters"][0][2]
    assert depth_call["values"] == {id_params[0]: 2, id_params[1]: 1}
    assert depth_call["depth"] == 1
    assert depth_call["order"] == "asc"


async def test_expression_loader_returns_empty_without_primary_keys() -> None:
    table, handle = expression_table()

    def empty_select_expression(payload: dict[str, Any]) -> dict[str, Any]:
        handle.calls.append(("select_expression", payload))
        return {"columns": ["id"], "rows": []}

    handle.select_expression = empty_select_expression  # type: ignore[method-assign]

    result = await table._find_many_expression_by_primary_keys(
        where=None,
        order_by=[],
        order=Order.asc,
        limit=0,
        offset=0,
        load_plan=_ResolvedLoadPlan(depth=0, paths=("children",)),
    )

    assert result == []
    assert [name for name, _ in handle.calls] == ["select_expression"]


async def test_selectin_relationship_assigns_empty_collections_and_scalars() -> None:
    table = table_for_bind_limit(None)
    related_table = table._table_data
    collection_parent = BatchModel(id=1)
    scalar_parent = BatchModel(id=2)
    object.__setattr__(collection_parent, "id", None)
    object.__setattr__(collection_parent, "children", [])
    object.__setattr__(scalar_parent, "child_id", None)
    table._related_table = lambda _: table  # type: ignore[method-assign]

    collection = await table._selectin_load_relationship(
        [collection_parent],
        table._table_data,
        "children",
        "parent_id",
        related_table,
        None,
        {},
    )
    scalar = await table._selectin_load_relationship(
        [scalar_parent],
        table._table_data,
        "child_id",
        None,
        related_table,
        None,
        {},
    )

    assert collection == []
    assert collection_parent.children == []
    assert scalar == []
    assert scalar_parent.child_id is None


async def test_load_selectin_tree_wraps_relationship_errors() -> None:
    leaf_table = OrmTable[BatchModel](
        model=BatchModel,
        tablename="leaves",
        pk="id",
        indexed=[],
        unique=[],
        unique_constraints=[],
        columns=["id", "kind"],
        relationships={},
        back_references={},
    )
    child_table = OrmTable[BatchModel](
        model=BatchModel,
        tablename="children",
        pk="id",
        indexed=[],
        unique=[],
        unique_constraints=[],
        columns=["id", "kind"],
        relationships={"leaf": Relationship(foreign_table="leaves")},
        back_references={},
    )
    root_table = OrmTable[BatchModel](
        model=BatchModel,
        tablename="batch_model",
        pk="id",
        indexed=[],
        unique=[],
        unique_constraints=[],
        columns=["id", "kind"],
        relationships={"children": Relationship(foreign_table="children")},
        back_references={},
    )
    table = table_for_bind_limit(
        None,
        table_data=root_table,
        table_map=Map(name_to_data={"children": child_table, "leaves": leaf_table}),
    )

    async def explode(*_: Any, **__: Any) -> list[Any]:
        raise RuntimeError("loader broke")

    table._selectin_load_relationship = explode  # type: ignore[method-assign]

    with pytest.raises(RelationshipLoadingError, match="BatchModel.children") as exc:
        await table._load_selectin_tree(
            [BatchModel(id=1)],
            root_table,
            {"children": {}},
            (),
            {},
            {},
            set(),
        )

    assert exc.value.__cause__ is not None
    assert exc.value.context["relationship"] == "children"


def test_payload_skips_generated_defaults_and_identity_columns() -> None:
    table_data = OrmTable[PayloadModel](
        model=PayloadModel,
        tablename="payloads",
        pk="id",
        indexed=[],
        unique=[],
        unique_constraints=[],
        columns=[
            "id",
            "kind",
            "created_at",
            "computed_label",
            "identity_value",
            "optional_identity",
        ],
        column_options={
            "id": TableColumn(identity=True),
            "created_at": TableColumn(server_default="CURRENT_TIMESTAMP"),
            "computed_label": TableColumn(computed="kind || id"),
            "identity_value": TableColumn(identity_always=True),
            "optional_identity": TableColumn(identity=True),
        },
        relationships={},
        back_references={},
    )
    table = Table(
        table_data=table_data,
        table_map=Map(),
        rust_handle=BindLimitHandle(None),
        events=EventRegistry(),
    )
    model = PayloadModel(
        id=None,
        kind="keep",
        created_at=None,
        computed_label="ignored",
        identity_value=9,
        optional_identity=7,
    )

    assert table._payload(model, mode="insert") == {
        "kind": "keep",
        "optional_identity": 7,
    }
    assert table._payload(model.model_copy(update={"id": 1}), mode="update") == {
        "id": 1,
        "kind": "keep",
        "created_at": None,
    }
    assert table._payload(model, mode="upsert") == {
        "kind": "keep",
        "optional_identity": 7,
    }


def test_compile_helpers_delegate_to_native_with_qualified_names(monkeypatch) -> None:
    class FakeCompiler:
        def compile_select_pk(self, *args: Any) -> dict[str, Any]:
            return {"method": "select_pk", "args": args}

        def compile_find_many(self, *args: Any) -> dict[str, Any]:
            return {"method": "find_many", "args": args}

        def compile_count(self, *args: Any) -> dict[str, Any]:
            return {"method": "count", "args": args}

        def compile_insert(self, *args: Any) -> dict[str, Any]:
            return {"method": "insert", "args": args}

        def compile_update(self, *args: Any) -> dict[str, Any]:
            return {"method": "update", "args": args}

        def compile_upsert(self, *args: Any) -> dict[str, Any]:
            return {"method": "upsert", "args": args}

        def compile_delete_pk(self, *args: Any) -> dict[str, Any]:
            return {"method": "delete", "args": args}

        def compile_typed_expression_query(self, *args: Any) -> dict[str, Any]:
            return {"method": "typed_select", "args": args}

        def compile_typed_update_query(self, *args: Any) -> dict[str, Any]:
            return {"method": "typed_update", "args": args}

    monkeypatch.setattr(table_module, "_ormdantic", FakeCompiler())
    table = table_for_bind_limit(None)
    table._connection = "sqlite:///db.sqlite3"
    table._table_data.schema_name = "tenant"

    assert table._qualified_table_name() == "tenant.batch_model"
    assert table._compile_select_pk_query() == {
        "method": "select_pk",
        "args": (
            "sqlite:///db.sqlite3",
            "tenant.batch_model",
            "id",
            ["id", "kind"],
            ["batch_model\\id", "batch_model\\kind"],
        ),
    }
    assert (
        table._compile_find_many_query(
            [("kind", "eq", ["param_0"])], ["kind"], "desc", 5, 10
        )["method"]
        == "find_many"
    )
    assert table._compile_count_query([("kind", "eq", ["param_0"])]) == {
        "method": "count",
        "args": (
            "sqlite:///db.sqlite3",
            "tenant.batch_model",
            [("kind", "eq", ["param_0"])],
        ),
    }
    assert table._compile_insert_query({"id": 1, "kind": "keep"})["method"] == "insert"
    assert table._compile_update_query({"id": 1, "kind": "keep"})["method"] == "update"
    assert table._compile_update_query({"id": 1}) is None
    assert table._compile_upsert_query({"id": 1})["method"] == "upsert"
    assert table._compile_delete_query()["method"] == "delete"
    assert table._compile_typed_select_query({"table": "batch_model"}) == {
        "method": "typed_select",
        "args": ("sqlite:///db.sqlite3", {"table": "batch_model"}),
    }
    assert table._compile_typed_update_query({"table": "batch_model"}) == {
        "method": "typed_update",
        "args": ("sqlite:///db.sqlite3", {"table": "batch_model"}),
    }

    table._connection = None
    assert table._compile_select_pk_query() is None
    assert table._compile_find_many_query([], [], "asc", None, None) is None
    assert table._compile_count_query([]) is None
    assert table._compile_insert_query({"id": 1}) is None
    assert table._compile_update_query({"id": 1}) is None
    assert table._compile_upsert_query({"id": 1}) is None
    assert table._compile_delete_query() is None
    assert table._compile_typed_select_query({"table": "batch_model"}) is None
    assert table._compile_typed_update_query({"table": "batch_model"}) is None


async def test_typed_query_validation_rejects_conflicting_and_wrong_table_payloads() -> (
    None
):
    table = table_for_bind_limit(None)
    wrong_select = select_query("other_table", column("id"))
    wrong_update = update_query("other_table", column("kind").set("drop"))

    with pytest.raises(ValueError, match="projection arguments"):
        await table.select(
            column("id"), query=select_query("batch_model", column("id"))
        )
    with pytest.raises(ValueError, match="targets table 'other_table'"):
        await table.select(query=wrong_select)
    with pytest.raises(ValueError, match="assignment arguments"):
        await table.update_where(
            column("kind").set("keep"),
            query=update_query("batch_model", column("kind").set("drop")),
        )
    with pytest.raises(ValueError, match="typed update targets table 'other_table'"):
        await table.update_where(query=wrong_update)


def test_debug_payload_wraps_compile_errors_and_log_query(caplog) -> None:
    table = table_for_bind_limit(None)
    table._debug = True
    table._log_queries = True

    with pytest.raises(QueryCompilationError, match="select compilation failed"):
        table._debug_payload(
            "select",
            parameters={"kind": "keep"},
            compile_query=lambda: (_ for _ in ()).throw(ValueError("bad sql")),
            context=table._context("select"),
        )

    with caplog.at_level(logging.INFO, logger="ormdantic.query"):
        table._log_query(
            {
                "operation": "select",
                "table_name": "batch_model",
                "debug": True,
                "parameters": {"kind": "keep"},
            },
            duration_ms=1.25,
            row_count=2,
            error=None,
        )
        table._log_query(
            {
                "operation": "select",
                "table_name": "batch_model",
                "debug": True,
                "parameters": {"kind": "keep"},
            },
            duration_ms=1.5,
            row_count=None,
            error=RuntimeError("boom"),
        )

    assert "ormdantic query ok operation=select table=batch_model" in caplog.text
    assert "ormdantic query error operation=select table=batch_model" in caplog.text


async def test_deserialize_wraps_serializer_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FailingSerializer:
        def __init__(self, **_kwargs: Any) -> None:
            pass

        def deserialize(self) -> object:
            raise ValueError("bad row")

    table = table_for_bind_limit(None)
    monkeypatch.setattr(table_module, "OrmSerializer", FailingSerializer)

    with pytest.raises(HydrationError, match="hydration failed"):
        await table._deserialize(
            {"columns": ["id", "kind"], "rows": [(1, "keep")]},
            is_array=True,
            depth=0,
        )


async def test_load_selectin_graph_requires_runtime_for_selectin_paths() -> None:
    table = expression_table()[0]
    plan = _ResolvedLoadPlan(
        depth=0,
        paths=("children",),
        selectin_paths=("children",),
        options=(selectinload("children"),),
        use_selectin=True,
    )

    with pytest.raises(RuntimeError, match="select-in relationship loading"):
        await table._load_selectin_graph([BatchModel(id=1, kind="keep")], plan)


def test_loader_plan_validation_covers_disabled_paths_and_bad_columns() -> None:
    leaf_table = OrmTable[BatchModel](
        model=BatchModel,
        tablename="leaves",
        pk="id",
        indexed=[],
        unique=[],
        unique_constraints=[],
        columns=["id", "kind"],
        relationships={},
        back_references={},
    )
    child_table = OrmTable[BatchModel](
        model=BatchModel,
        tablename="children",
        pk="id",
        indexed=[],
        unique=[],
        unique_constraints=[],
        columns=["id", "kind"],
        relationships={"leaf": Relationship(foreign_table="leaves")},
        back_references={},
    )
    root_table = OrmTable[BatchModel](
        model=BatchModel,
        tablename="batch_model",
        pk="id",
        indexed=[],
        unique=[],
        unique_constraints=[],
        columns=["id", "kind"],
        relationships={"children": Relationship(foreign_table="children")},
        back_references={},
    )
    table = table_for_bind_limit(
        None,
        table_data=root_table,
        table_map=Map(name_to_data={"children": child_table, "leaves": leaf_table}),
    )

    with pytest.raises(ValueError, match="relationship depth"):
        table._resolve_load_plan(-1, None)
    with pytest.raises(ValueError, match="conflicting strategies"):
        table._resolve_load_plan(0, [joinedload("children"), selectinload("children")])
    with pytest.raises(ValueError, match="cannot be eager loaded"):
        table._resolve_load_plan(0, [noload("children"), joinedload("children.leaf")])
    with pytest.raises(ValueError, match="cannot define filtering or ordering"):
        table._resolve_load_plan(0, [noload("children").filter(kind="keep")])
    with pytest.raises(ValueError, match="'missing' is not a column"):
        table._resolve_load_plan(0, [selectinload("children").filter(missing=True)])
    with pytest.raises(ValueError, match="'missing' is not a column"):
        table._resolve_load_plan(0, [selectinload("children").sorted_by("-missing")])

    plan = table._resolve_load_plan(2, [noload("children")])
    assert plan == _ResolvedLoadPlan(depth=0, paths=None)
    assert table._expand_depth_paths(2) == {"children", "children/leaf"}

    cyclic_table = OrmTable[BatchModel](
        model=BatchModel,
        tablename="batch_model",
        pk="id",
        indexed=[],
        unique=[],
        unique_constraints=[],
        columns=["id", "kind"],
        relationships={"parent": Relationship(foreign_table="batch_model")},
        back_references={},
    )
    cyclic = table_for_bind_limit(
        None,
        table_data=cyclic_table,
        table_map=Map(name_to_data={"batch_model": cyclic_table}),
    )
    assert cyclic._expand_depth_paths(3) == {"parent"}
