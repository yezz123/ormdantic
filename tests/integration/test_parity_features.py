from __future__ import annotations

import pytest
from pydantic import BaseModel, Field

from ormdantic import (
    Ormdantic,
    association_proxy,
    column,
    count,
    hybrid_property,
    selectinload,
    sum,
)
from ormdantic.migrations import MigrationOperation, MigrationPlan


@pytest.mark.asyncio
async def test_expression_facade_reflection_migrations_and_session_delete(
    tmp_path,
) -> None:
    db = Ormdantic(f"sqlite:///{tmp_path / 'parity.sqlite3'}")

    @db.table(pk="id")
    class Flavor(BaseModel):
        id: str
        name: str
        strength: int

    await db.init()

    mocha = Flavor(id="1", name="mocha", strength=5)
    latte = Flavor(id="2", name="latte", strength=3)
    await db[Flavor].insert(mocha)
    await db[Flavor].insert(latte)

    results = await db[Flavor].find_many(
        where=(column("strength") >= 5) & (column("name").like("mo%"))
    )
    assert results.data == [mocha]
    or_results = await db[Flavor].find_many(
        where=(column("name") == "mocha") | (column("name") == "latte")
    )
    assert {flavor.name for flavor in or_results.data} == {"mocha", "latte"}
    not_in_results = await db[Flavor].find_many(where=column("id").not_in(["2"]))
    assert not_in_results.data == [mocha]

    inspector = db.inspect()
    assert "flavor" in await inspector.table_names()
    columns = await inspector.columns("flavor")
    assert {column["name"] for column in columns} >= {"id", "name", "strength"}
    assert await inspector.indexes("flavor") == []
    assert await inspector.foreign_keys("flavor") == []

    revisions = await db.migrations.applied_revisions()
    assert revisions == []
    await db.migrations.apply(
        "001",
        MigrationPlan(
            [
                MigrationOperation("CREATE TABLE parity_extra (id TEXT)"),
                MigrationOperation(
                    "INSERT INTO parity_extra (id) VALUES (?)",
                    ("extra",),
                ),
            ]
        ),
    )
    assert await db.migrations.applied_revisions() == ["001"]
    await db.migrations.rollback(
        "001",
        MigrationPlan(
            operations=[MigrationOperation("CREATE TABLE parity_extra (id TEXT)")],
            rollback_operations=[MigrationOperation("DROP TABLE parity_extra")],
        ),
        allow_destructive=True,
    )
    assert await db.migrations.applied_revisions() == []

    async with db.session() as session:
        session.delete(mocha)
        assert await session.get(Flavor, "1") == mocha
    assert await db[Flavor].count() == 1
    assert (await db[Flavor].find_one("1")) is None

    async with db.transaction():
        await db[Flavor].insert(Flavor(id="3", name="saved", strength=1))
        try:
            async with db.savepoint("before_extra"):
                await db[Flavor].insert(Flavor(id="4", name="rolled", strength=1))
                raise RuntimeError("rollback savepoint")
        except RuntimeError:
            pass
    names = {flavor.name for flavor in (await db[Flavor].find_many()).data}
    assert "saved" in names
    assert "rolled" not in names


@pytest.mark.asyncio
async def test_expression_facade_preserves_boolean_grouping_and_count(tmp_path) -> None:
    db = Ormdantic(f"sqlite:///{tmp_path / 'expression_groups.sqlite3'}")

    @db.table(pk="id")
    class Flavor(BaseModel):
        id: str
        name: str
        strength: int

    await db.init()
    await db[Flavor].insert(Flavor(id="1", name="mocha", strength=5))
    await db[Flavor].insert(Flavor(id="2", name="latte", strength=3))
    await db[Flavor].insert(Flavor(id="3", name="vanilla", strength=1))

    mixed = (column("strength") >= 5) & column("name").like("mo%")
    mixed_results = await db[Flavor].find_many(
        where=mixed | (column("name") == "latte")
    )
    assert {flavor.name for flavor in mixed_results.data} == {"mocha", "latte"}

    grouped_results = await db[Flavor].find_many(
        where=(column("name") == "vanilla") | mixed
    )
    assert {flavor.name for flavor in grouped_results.data} == {"mocha", "vanilla"}

    repeated_column_count = await db[Flavor].count(
        where=(column("name") == "mocha") | (column("name") == "latte")
    )
    assert repeated_column_count == 2


@pytest.mark.asyncio
async def test_table_select_executes_grouped_aggregate_expression_query(
    tmp_path,
) -> None:
    db = Ormdantic(f"sqlite:///{tmp_path / 'expression_select.sqlite3'}")

    @db.table(pk="id")
    class Order(BaseModel):
        id: str
        customer_id: str
        status: str
        total: int
        deleted_at: str | None = None

    await db.init()
    await db[Order].insert(Order(id="1", customer_id="alice", status="paid", total=25))
    await db[Order].insert(Order(id="2", customer_id="alice", status="paid", total=40))
    await db[Order].insert(Order(id="3", customer_id="bob", status="paid", total=10))
    await db[Order].insert(Order(id="4", customer_id="bob", status="draft", total=100))

    total = sum(column("total"))
    result = await db[Order].select(
        column("customer_id"),
        total.as_("total_sum"),
        count().as_("row_count"),
        where=column("status").in_(["paid"]) & column("deleted_at").is_null(),
        group_by=[column("customer_id")],
        having=total >= 20,
        order_by=[total.desc(nulls="last"), column("customer_id").asc()],
    )

    assert [column[0] for column in result.cursor.description] == [
        "customer_id",
        "total_sum",
        "row_count",
    ]
    assert list(result) == [("alice", 65, 2)]

    await db[Order].update_where(
        column("total").set(column("total") + 5),
        where=column("customer_id") == "alice",
    )
    updated = await db[Order].find_many(
        where=column("customer_id") == "alice", order_by=["id"]
    )
    assert [order.total for order in updated.data] == [30, 45]


@pytest.mark.asyncio
async def test_relation_predicates_and_relation_count_ordering(tmp_path) -> None:
    db = Ormdantic(f"sqlite:///{tmp_path / 'relation_predicates.sqlite3'}")

    @db.table(pk="id", back_references={"posts": "author"})
    class BlogAuthor(BaseModel):
        id: str
        name: str
        posts: list[BlogPost] = Field(default_factory=list)

    @db.table(pk="id")
    class BlogPost(BaseModel):
        id: str
        title: str
        status: str
        author: BlogAuthor | str

    BlogAuthor.model_rebuild()
    BlogPost.model_rebuild()

    await db.init()
    await db.drop_all()
    await db.create_all()

    alice = await db[BlogAuthor].insert(BlogAuthor(id="a", name="alice"))
    bob = await db[BlogAuthor].insert(BlogAuthor(id="b", name="bob"))
    await db[BlogAuthor].insert(BlogAuthor(id="c", name="cara"))
    await db[BlogPost].insert(
        BlogPost(id="1", title="first", status="published", author=alice)
    )
    await db[BlogPost].insert(
        BlogPost(id="2", title="draft", status="draft", author=alice)
    )
    await db[BlogPost].insert(
        BlogPost(id="3", title="second", status="published", author=bob)
    )
    await db[BlogPost].insert(
        BlogPost(id="4", title="third", status="published", author=bob)
    )

    posts = db.relation(BlogAuthor, "posts")
    published = posts.column("status") == "published"

    with_published = await db[BlogAuthor].find_many(
        where=posts.any(published),
        order_by=[posts.count(published).desc(), "name"],
        load=[selectinload("posts")],
    )
    assert [author.name for author in with_published.data] == ["bob", "alice"]
    assert [
        [post.title for post in author.posts] for author in with_published.data
    ] == [
        ["second", "third"],
        ["first", "draft"],
    ]

    without_drafts = await db[BlogAuthor].find_many(
        where=posts.none(posts.column("status") == "draft"),
        order_by=["name"],
    )
    assert [author.name for author in without_drafts.data] == ["bob", "cara"]

    every_post_published = await db[BlogAuthor].find_many(
        where=posts.every(published),
        order_by=["name"],
    )
    assert [author.name for author in every_post_published.data] == ["bob", "cara"]
    assert await db[BlogAuthor].count(where=posts.any(published)) == 2

    author = db.relation(BlogPost, "author")
    authored_by_alice = await db[BlogPost].find_many(
        where=author.has(author.column("name") == "alice"),
        order_by=["title"],
    )
    assert [post.title for post in authored_by_alice.data] == ["draft", "first"]

    with pytest.raises(ValueError, match="not a scalar relationship"):
        posts.has()
    with pytest.raises(ValueError, match="not a collection relationship"):
        author.any()


def test_event_removal_and_association_hybrid_descriptors() -> None:
    db = Ormdantic("sqlite:///:memory:")
    called = []

    def handler(**kwargs) -> None:
        called.append(kwargs)

    db.on("custom", handler)
    db.off("custom", handler)
    db.clear_events("custom")

    class Child:
        name = "mocha"

    class Parent:
        child = Child()
        child_name = association_proxy("child", "name")

        @child_name.expression
        def child_name(cls):
            return column("child_name")

        @hybrid_property
        def label(self) -> str:
            return f"flavor:{self.child_name}"

        @label.expression
        def label(cls):
            return column("name")

    parent = Parent()
    assert parent.child_name == "mocha"
    parent.child_name = "latte"
    assert parent.child.name == "latte"
    assert parent.label == "flavor:latte"
    assert Parent.label.like("la%").to_where() == {"name__like": "la%"}
    assert Parent.child_name.like("la%").to_where() == {"child_name__like": "la%"}
