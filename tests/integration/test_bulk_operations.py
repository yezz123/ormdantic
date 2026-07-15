from __future__ import annotations

import pytest
from pydantic import BaseModel

from ormdantic import Ormdantic, TableColumn, column


async def test_insert_many_persists_models_in_input_order(tmp_path) -> None:
    db = Ormdantic(f"sqlite:///{tmp_path / 'bulk_insert.sqlite3'}")

    @db.table("bulk_items", pk="id")
    class Item(BaseModel):
        id: str
        category: str
        score: int

    await db.init()
    models = [Item(id=str(index), category="keep", score=index) for index in range(20)]

    inserted = await db[Item].insert_many(models, batch_size=7)

    assert inserted == models
    assert await db[Item].count() == 20


async def test_upsert_many_updates_and_inserts_in_one_call(tmp_path) -> None:
    db = Ormdantic(f"sqlite:///{tmp_path / 'bulk_upsert.sqlite3'}")

    @db.table("bulk_items", pk="id")
    class Item(BaseModel):
        id: str
        category: str
        score: int

    await db.init()
    await db[Item].insert(Item(id="1", category="old", score=1))

    upserted = await db[Item].upsert_many(
        [
            Item(id="1", category="changed", score=99),
            Item(id="2", category="new", score=2),
        ]
    )

    assert [item.id for item in upserted] == ["1", "2"]
    assert (await db[Item].find_one("1")).score == 99  # type: ignore[union-attr]
    assert await db[Item].count() == 2


async def test_delete_where_removes_matching_rows_with_one_statement(tmp_path) -> None:
    db = Ormdantic(f"sqlite:///{tmp_path / 'bulk_delete.sqlite3'}")

    @db.table("bulk_items", pk="id")
    class Item(BaseModel):
        id: str
        category: str
        score: int

    await db.init()
    await db[Item].insert_many(
        [
            Item(id="1", category="keep", score=1),
            Item(id="2", category="delete", score=2),
            Item(id="3", category="delete", score=3),
        ]
    )

    deleted = await db[Item].delete_where(column("category") == "delete")

    assert deleted == 2
    assert await db[Item].count() == 1


async def test_delete_where_accepts_dictionary_filters(tmp_path) -> None:
    db = Ormdantic(f"sqlite:///{tmp_path / 'bulk_delete_dict.sqlite3'}")

    @db.table("bulk_items", pk="id")
    class Item(BaseModel):
        id: str
        category: str
        score: int

    await db.init()
    await db[Item].insert_many(
        [
            Item(id="1", category="keep", score=1),
            Item(id="2", category="delete", score=2),
        ]
    )

    deleted = await db[Item].delete_where({"category": "delete"})

    assert deleted == 1
    assert await db[Item].count() == 1


async def test_bulk_writes_validate_input_before_executing(tmp_path) -> None:
    db = Ormdantic(f"sqlite:///{tmp_path / 'bulk_validation.sqlite3'}")

    @db.table("bulk_items", pk="id")
    class Item(BaseModel):
        id: str

    class Other(BaseModel):
        id: str

    await db.init()

    assert await db[Item].insert_many([]) == []
    with pytest.raises(TypeError, match="Item"):
        await db[Item].insert_many([Item(id="1"), Other(id="2")])  # type: ignore[list-item]
    with pytest.raises(ValueError, match="batch_size"):
        await db[Item].insert_many([Item(id="1")], batch_size=0)
    assert await db[Item].count() == 0


async def test_insert_many_groups_generated_column_shapes(tmp_path) -> None:
    db = Ormdantic(f"sqlite:///{tmp_path / 'bulk_shapes.sqlite3'}")

    @db.table(
        "bulk_items",
        pk="id",
        column_options={
            "id": TableColumn(autoincrement=True),
            "status": TableColumn(server_default="'fresh'"),
        },
    )
    class Item(BaseModel):
        id: int | None = None
        status: str | None = None
        name: str

    await db.init()

    await db[Item].insert_many(
        [
            Item(name="generated"),
            Item(id=100, status="explicit", name="provided"),
        ]
    )

    assert await db[Item].count() == 2
    assert (await db[Item].find_one(100)).status == "explicit"  # type: ignore[union-attr]


async def test_bulk_events_preserve_model_order(tmp_path) -> None:
    db = Ormdantic(f"sqlite:///{tmp_path / 'bulk_events.sqlite3'}")

    @db.table("bulk_items", pk="id")
    class Item(BaseModel):
        id: str

    events: list[tuple[str, str]] = []
    db.on(
        "before_insert",
        lambda model, **_: events.append(("before", model.id)),
    )
    db.on(
        "after_insert",
        lambda model, **_: events.append(("after", model.id)),
    )
    await db.init()

    await db[Item].insert_many([Item(id="1"), Item(id="2"), Item(id="3")], batch_size=2)

    assert events == [
        ("before", "1"),
        ("before", "2"),
        ("after", "1"),
        ("after", "2"),
        ("before", "3"),
        ("after", "3"),
    ]


async def test_delete_where_requires_explicit_full_table_permission(tmp_path) -> None:
    db = Ormdantic(f"sqlite:///{tmp_path / 'bulk_delete_all.sqlite3'}")

    @db.table("bulk_items", pk="id")
    class Item(BaseModel):
        id: str

    await db.init()
    await db[Item].insert_many([Item(id="1"), Item(id="2")])

    with pytest.raises(ValueError, match="allow_all"):
        await db[Item].delete_where()
    assert await db[Item].delete_where(allow_all=True) == 2
    assert await db[Item].count() == 0
