from __future__ import annotations

import pytest
from pydantic import BaseModel, Field

from ormdantic.models import Map, OrmTable, Relationship
from ormdantic.session import Session, _SessionSavepoint


class Flavor(BaseModel):
    id: str
    name: str


class Supplier(BaseModel):
    id: str | None
    name: str
    products: list[object] = Field(default_factory=list)


class Product(BaseModel):
    id: str | None
    name: str
    supplier: object | None = None


class NodeA(BaseModel):
    id: str
    peer: object | None = None


class NodeB(BaseModel):
    id: str
    peer: object | None = None


class EventRecorder:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, object]]] = []

    async def dispatch(self, event: str, **payload: object) -> None:
        self.calls.append((event, payload))


class FakeTable:
    def __init__(self, stored: dict[str, Flavor]) -> None:
        self.stored = stored
        self.updated: list[Flavor] = []
        self.deleted: list[str] = []

    async def find_one(self, pk: str, *, depth: int = 0) -> Flavor | None:
        return self.stored.get(pk)

    async def insert(self, model: Flavor) -> Flavor:
        self.stored[model.id] = model
        return model

    async def update(self, model: Flavor) -> Flavor:
        self.updated.append(model)
        self.stored[model.id] = model
        return model

    async def delete(self, pk: str) -> None:
        self.deleted.append(pk)
        self.stored.pop(pk, None)


class FakeDatabase:
    def __init__(self, *, fail_commit: bool = False) -> None:
        table = OrmTable[Flavor](
            model=Flavor,
            tablename="flavors",
            pk="id",
            columns=["id", "name"],
            indexed=[],
            unique=[],
            unique_constraints=[],
            relationships={},
            back_references={},
        )
        self._table_map = Map(name_to_data={"flavors": table}, model_to_data={})
        self._table_map.model_to_data = {Flavor: table}
        self._events = EventRecorder()
        self.table = FakeTable(
            {
                "1": Flavor(id="1", name="mocha"),
                "2": Flavor(id="2", name="latte"),
            }
        )
        self.fail_commit = fail_commit
        self.calls: list[str] = []

    def __getitem__(self, model_type: type[BaseModel]) -> FakeTable:
        assert model_type is Flavor
        return self.table

    async def _begin(self, transaction_options: object | None = None) -> None:
        self.calls.append("begin")

    async def _commit(self) -> None:
        self.calls.append("commit")
        if self.fail_commit:
            raise RuntimeError("commit failed")

    async def _rollback(self) -> None:
        self.calls.append("rollback")

    async def _savepoint(self, name: str) -> None:
        self.calls.append(f"savepoint:{name}")

    async def _rollback_to_savepoint(self, name: str) -> None:
        self.calls.append(f"rollback_to:{name}")

    async def _release_savepoint(self, name: str) -> None:
        self.calls.append(f"release:{name}")


class MultiFakeTable:
    def __init__(self) -> None:
        self.stored: dict[object, BaseModel] = {}
        self.inserted: list[BaseModel] = []
        self.insert_many_calls: list[list[BaseModel]] = []
        self.updated: list[BaseModel] = []
        self.deleted: list[object] = []

    async def find_one(self, pk: object, *, depth: int = 0) -> BaseModel | None:
        return self.stored.get(pk)

    async def insert(self, model: BaseModel) -> BaseModel:
        self.inserted.append(model)
        self.stored[model.id] = model
        return model

    async def insert_many(self, models: list[BaseModel]) -> list[BaseModel]:
        materialized = list(models)
        self.insert_many_calls.append(materialized)
        for model in materialized:
            self.inserted.append(model)
            self.stored[model.id] = model
        return materialized

    async def update(self, model: BaseModel) -> BaseModel:
        self.updated.append(model)
        self.stored[model.id] = model
        return model

    async def delete(self, pk: object) -> None:
        self.deleted.append(pk)
        self.stored.pop(pk, None)


class RelationshipDatabase:
    def __init__(self) -> None:
        self._events = EventRecorder()
        self.calls: list[str] = []
        supplier_table = OrmTable[Supplier](
            model=Supplier,
            tablename="suppliers",
            pk="id",
            columns=["id", "name"],
            indexed=[],
            unique=[],
            unique_constraints=[],
            relationships={
                "products": Relationship(
                    foreign_table="products",
                    back_references="supplier",
                )
            },
            back_references={},
        )
        product_table = OrmTable[Product](
            model=Product,
            tablename="products",
            pk="id",
            columns=["id", "name", "supplier"],
            indexed=[],
            unique=[],
            unique_constraints=[],
            relationships={"supplier": Relationship(foreign_table="suppliers")},
            back_references={},
        )
        node_a_table = OrmTable[NodeA](
            model=NodeA,
            tablename="node_a",
            pk="id",
            columns=["id", "peer"],
            indexed=[],
            unique=[],
            unique_constraints=[],
            relationships={"peer": Relationship(foreign_table="node_b")},
            back_references={},
        )
        node_b_table = OrmTable[NodeB](
            model=NodeB,
            tablename="node_b",
            pk="id",
            columns=["id", "peer"],
            indexed=[],
            unique=[],
            unique_constraints=[],
            relationships={"peer": Relationship(foreign_table="node_a")},
            back_references={},
        )
        self._table_map = Map(
            name_to_data={
                "suppliers": supplier_table,
                "products": product_table,
                "node_a": node_a_table,
                "node_b": node_b_table,
            },
            model_to_data={},
        )
        self._table_map.model_to_data = {
            Supplier: supplier_table,
            Product: product_table,
            NodeA: node_a_table,
            NodeB: node_b_table,
        }
        self.tables = {
            Supplier: MultiFakeTable(),
            Product: MultiFakeTable(),
            NodeA: MultiFakeTable(),
            NodeB: MultiFakeTable(),
        }

    def __getitem__(self, model_type: type[BaseModel]) -> MultiFakeTable:
        return self.tables[model_type]

    async def _begin(self, transaction_options: object | None = None) -> None:
        self.calls.append("begin")

    async def _commit(self) -> None:
        self.calls.append("commit")

    async def _rollback(self) -> None:
        self.calls.append("rollback")

    async def _savepoint(self, name: str) -> None:
        self.calls.append(f"savepoint:{name}")

    async def _rollback_to_savepoint(self, name: str) -> None:
        self.calls.append(f"rollback_to:{name}")

    async def _release_savepoint(self, name: str) -> None:
        self.calls.append(f"release:{name}")


async def test_session_context_rolls_back_when_commit_fails() -> None:
    database = FakeDatabase(fail_commit=True)
    session = Session(database)

    with pytest.raises(RuntimeError, match="commit failed"):
        async with session:
            pass

    assert database.calls == ["begin", "commit", "rollback"]
    await session.commit()
    await session.rollback()
    assert database.calls == ["begin", "commit", "rollback"]


async def test_session_merge_refresh_and_generated_savepoint_edges() -> None:
    database = FakeDatabase()
    session = Session(database)

    staged_dirty = Flavor(id="1", name="mocha")
    session.mark_dirty(staged_dirty)
    merged_dirty = session.merge(Flavor(id="1", name="vanilla"))
    assert merged_dirty is staged_dirty
    assert staged_dirty.name == "vanilla"

    detached = Flavor(id="2", name="hazelnut")
    merged_detached = session.merge(detached)
    assert merged_detached is detached
    assert detached in session._dirty

    missing = await session.refresh(Flavor(id="missing", name="none"))
    assert missing is None

    loaded = await session.get(Flavor, "1")
    assert loaded is database.table.stored["1"]
    assert session.get_cached(Flavor, "1") is loaded
    assert await session.get(Flavor, "1") is loaded

    async with session.savepoint():
        session.add(Flavor(id="3", name="mint"))

    assert "savepoint:session_sp_1" in database.calls
    assert "release:session_sp_1" in database.calls


async def test_session_relationship_edges_stage_order_and_delete_new_models() -> None:
    database = RelationshipDatabase()
    session = Session(database)
    supplier = Supplier(id="supplier-1", name="Acme")
    product = Product(id="product-1", name="Beans")
    supplier.products.append(product)

    session.add(supplier)
    assert product.supplier is supplier
    assert session._collection_related_models(supplier) == [(product, "supplier")]
    assert session._snapshot_value(product.supplier) == "supplier-1"

    await session.flush()
    assert database.tables[Supplier].inserted == [supplier]
    assert database.tables[Product].inserted == [product]

    pending = Product(id="pending", name="Transient")
    session.add(pending)
    session.delete(pending)
    assert pending not in session._new

    session._cascade_delete(supplier, {id(supplier)})
    assert session._collection_related_models(Product(id="x", name="No list")) == []
    supplier_without_loaded_collection = Supplier(id="supplier-2", name="No list")
    object.__setattr__(supplier_without_loaded_collection, "products", object())
    assert session._collection_related_models(supplier_without_loaded_collection) == []

    savepoint = _SessionSavepoint(session, "manual")
    await savepoint.__aexit__(None, None, None)


async def test_session_flush_uses_one_bulk_insert_per_dependency_group() -> None:
    database = RelationshipDatabase()
    session = Session(database)
    first = Product(id="p1", name="One")
    second = Product(id="p2", name="Two")
    session.add(first)
    session.add(second)

    await session.flush()

    assert database.tables[Product].insert_many_calls == [[first, second]]
    assert database.tables[Product].inserted == [first, second]


async def test_session_detects_relationship_changes_identity_conflicts_and_cycles() -> (
    None
):
    database = RelationshipDatabase()
    session = Session(database)
    product = Product(id="product-2", name="Cocoa")
    session._remember(product)
    supplier = Supplier(id="supplier-2", name="New supplier")
    product.supplier = supplier

    session._cascade_relationship_changes(product, set())
    assert supplier in session._new

    with pytest.raises(ValueError, match="present in this session"):
        session.add(Product(id="product-2", name="Detached"))

    pending = Product(id="product-3", name="Pending")
    session.add(pending)
    with pytest.raises(ValueError, match="staged in this session"):
        session.add(Product(id="product-3", name="Duplicate"))

    nullable = Product(id=None, name="No identity")
    assert session._staged_model_for_key((Product, None)) is None
    session._raise_for_identity_conflict(nullable)

    node_a = NodeA(id="a")
    node_b = NodeB(id="b")
    node_a.peer = node_b
    node_b.peer = node_a
    assert session._dependency_ordered([node_a, node_b]) == [node_b, node_a]
