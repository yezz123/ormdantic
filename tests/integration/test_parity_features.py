from __future__ import annotations

import pytest
from pydantic import BaseModel

from ormdantic import Ormdantic, association_proxy, column, hybrid_property
from ormdantic.migrations import MigrationOperation, MigrationPlan


@pytest.mark.asyncio
async def test_expression_facade_reflection_migrations_and_session_delete(tmp_path) -> None:
    db = Ormdantic(f"sqlite:///{tmp_path / 'parity.sqlite3'}")

    @db.table(pk="id")
    class Flavor(BaseModel):
        id: str
        name: str
        strength: int

    await db.init()

    mocha = Flavor(id="1", name="mocha", strength=5)
    await db[Flavor].insert(mocha)

    results = await db[Flavor].find_many(
        where=column("strength").ge(5) & column("name").like("mo%")
    )
    assert results.data == [mocha]

    inspector = db.inspect()
    assert "flavor" in await inspector.table_names()
    columns = await inspector.columns("flavor")
    assert {column["name"] for column in columns} >= {"id", "name", "strength"}

    revisions = await db.migrations.applied_revisions()
    assert revisions == []
    await db.migrations.apply(
        "001",
        MigrationPlan([MigrationOperation("CREATE TABLE parity_extra (id TEXT)")]),
    )
    assert await db.migrations.applied_revisions() == ["001"]
    await db.migrations.rollback(
        "001",
        MigrationPlan([MigrationOperation("DROP TABLE parity_extra")]),
    )
    assert await db.migrations.applied_revisions() == []

    async with db.session() as session:
        session.delete(mocha)
    assert await db[Flavor].count() == 0


def test_event_removal_and_association_hybrid_descriptors() -> None:
    db = Ormdantic("sqlite:///:memory:")
    called = []

    def handler(**kwargs) -> None:
        called.append(kwargs)

    db.on("custom", handler)
    db._events.off("custom", handler)

    class Child:
        name = "mocha"

    class Parent:
        child = Child()
        child_name = association_proxy("child", "name")

        @hybrid_property
        def label(self) -> str:
            return f"flavor:{self.child_name}"

    parent = Parent()
    assert parent.child_name == "mocha"
    assert parent.label == "flavor:mocha"
