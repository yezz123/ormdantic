from __future__ import annotations

import sqlite3
from uuid import uuid4

import pytest
from pydantic import BaseModel, Field

from ormdantic import (
    Ormdantic,
    joined,
    joinedload,
    lazy,
    load,
    noload,
    selectin,
    selectinload,
)
from tests.integration.relationship_stress import run_relationship_loader_stress


async def test_joined_and_selectin_loader_options(tmp_path) -> None:
    db = Ormdantic(f"sqlite:///{tmp_path / 'loaders.sqlite3'}")

    @db.table(pk="id")
    class Flavor(BaseModel):
        id: str = Field(default_factory=lambda: str(uuid4()))
        name: str

    @db.table(pk="id")
    class Coffee(BaseModel):
        id: str = Field(default_factory=lambda: str(uuid4()))
        flavor: Flavor | str

    await db.init()
    await db.drop_all()
    await db.create_all()
    flavor = await db[Flavor].insert(Flavor(name="mocha"))
    coffee = await db[Coffee].insert(Coffee(flavor=flavor))

    joined_loaded = await db[Coffee].find_one(coffee.id, load=[joined("flavor")])
    selectin_loaded = await db[Coffee].find_one(coffee.id, load=[selectin("flavor")])

    assert joined_loaded is not None
    assert selectin_loaded is not None
    assert joined_loaded.flavor == flavor
    assert selectin_loaded.flavor == flavor


async def test_explicit_lazy_loader(tmp_path) -> None:
    db = Ormdantic(f"sqlite:///{tmp_path / 'lazy.sqlite3'}")

    @db.table(pk="id")
    class Flavor(BaseModel):
        id: str = Field(default_factory=lambda: str(uuid4()))
        name: str

    @db.table(pk="id")
    class Coffee(BaseModel):
        id: str = Field(default_factory=lambda: str(uuid4()))
        flavor: Flavor | str

    await db.init()
    await db.drop_all()
    await db.create_all()
    flavor = await db[Flavor].insert(Flavor(name="mocha"))
    coffee = await db[Coffee].insert(Coffee(flavor=flavor))

    shallow = await db[Coffee].find_one(coffee.id, load=[lazy("flavor")])
    loaded_flavor = await db.load(shallow, "flavor")  # type: ignore[arg-type]

    assert shallow is not None
    assert shallow.flavor == flavor.id
    assert loaded_flavor == flavor


async def test_nested_loader_paths_are_branch_limited(tmp_path) -> None:
    db = Ormdantic(f"sqlite:///{tmp_path / 'nested_loaders.sqlite3'}")

    @db.table(pk="id")
    class Country(BaseModel):
        id: str = Field(default_factory=lambda: str(uuid4()))
        name: str

    @db.table(pk="id")
    class Person(BaseModel):
        id: str = Field(default_factory=lambda: str(uuid4()))
        name: str
        country: Country | str
        manager: Person | str | None = None

    @db.table(pk="id")
    class Article(BaseModel):
        id: str = Field(default_factory=lambda: str(uuid4()))
        title: str
        author: Person | str | None = None

    Person.model_rebuild()

    await db.init()
    await db.drop_all()
    await db.create_all()
    country = await db[Country].insert(Country(name="Morocco"))
    manager = await db[Person].insert(Person(name="lead", country=country))
    author = await db[Person].insert(
        Person(name="writer", country=country, manager=manager)
    )
    article = await db[Article].insert(Article(title="nested", author=author))

    loaded = await db[Article].find_one(article.id, load=[joinedload("author.country")])
    descriptor_loaded = await db[Article].find_one(
        article.id, load=[load(Article.author.country)]
    )
    selectin_loaded = await db[Article].find_one(
        article.id, load=[selectinload("author.country")]
    )

    assert loaded is not None
    assert isinstance(loaded.author, Person)
    assert loaded.author.country == country
    assert loaded.author.manager == manager.id
    assert descriptor_loaded is not None
    assert descriptor_loaded.author == loaded.author
    assert selectin_loaded is not None
    assert selectin_loaded.author == loaded.author


async def test_noload_keeps_foreign_key_scalar(tmp_path) -> None:
    db = Ormdantic(f"sqlite:///{tmp_path / 'noload.sqlite3'}")

    @db.table(pk="id")
    class Flavor(BaseModel):
        id: str = Field(default_factory=lambda: str(uuid4()))
        name: str

    @db.table(pk="id")
    class Coffee(BaseModel):
        id: str = Field(default_factory=lambda: str(uuid4()))
        flavor: Flavor | str

    await db.init()
    await db.drop_all()
    await db.create_all()
    flavor = await db[Flavor].insert(Flavor(name="mocha"))
    coffee = await db[Coffee].insert(Coffee(flavor=flavor))

    loaded = await db[Coffee].find_one(coffee.id, load=[noload("flavor")])

    assert loaded is not None
    assert loaded.flavor == flavor.id


async def test_invalid_loader_path_errors_are_actionable(tmp_path) -> None:
    db = Ormdantic(f"sqlite:///{tmp_path / 'invalid_loader.sqlite3'}")

    @db.table(pk="id")
    class Flavor(BaseModel):
        id: str = Field(default_factory=lambda: str(uuid4()))
        name: str

    @db.table(pk="id")
    class Coffee(BaseModel):
        id: str = Field(default_factory=lambda: str(uuid4()))
        flavor: Flavor | str

    await db.init()
    await db.drop_all()
    await db.create_all()

    with pytest.raises(
        ValueError,
        match=(
            r"invalid loader path 'flavor.beans': "
            r"'Flavor.beans' is not a relationship; available relationships: none"
        ),
    ):
        await db[Coffee].find_one("missing", load=[joinedload("flavor.beans")])


async def test_joinedload_one_to_many_nested_many_to_one(tmp_path) -> None:
    db = Ormdantic(f"sqlite:///{tmp_path / 'one_to_many_loader.sqlite3'}")

    @db.table(pk="id", back_references={"posts": "author"})
    class BlogAuthor(BaseModel):
        id: str = Field(default_factory=lambda: str(uuid4()))
        name: str
        posts: list[BlogPost] = Field(default_factory=list)

    @db.table(pk="id")
    class BlogPost(BaseModel):
        id: str = Field(default_factory=lambda: str(uuid4()))
        title: str
        author: BlogAuthor | str

    BlogAuthor.model_rebuild()
    BlogPost.model_rebuild()

    await db.init()
    await db.drop_all()
    await db.create_all()
    author = await db[BlogAuthor].insert(BlogAuthor(name="writer"))
    await db[BlogPost].insert(BlogPost(title="first", author=author))
    await db[BlogPost].insert(BlogPost(title="second", author=author))

    loaded = await db[BlogAuthor].find_one(author.id, load=[joinedload("posts.author")])
    selected = await db[BlogAuthor].find_one(
        author.id, load=[selectinload("posts.author")]
    )
    mixed = await db[BlogAuthor].find_one(
        author.id, load=[joinedload("posts"), selectinload("posts.author")]
    )

    assert loaded is not None
    assert len(loaded.posts) == 2
    assert {post.title for post in loaded.posts} == {"first", "second"}
    assert all(isinstance(post.author, BlogAuthor) for post in loaded.posts)
    assert {post.author.id for post in loaded.posts} == {author.id}  # type: ignore[union-attr]
    assert loaded.posts[0].author is loaded.posts[1].author
    assert selected is not None
    assert len(selected.posts) == 2
    assert selected.posts[0].author is selected.posts[1].author
    assert mixed is not None
    assert len(mixed.posts) == 2
    assert mixed.posts[0].author is mixed.posts[1].author


async def test_relationship_loader_filtering_and_ordering(tmp_path) -> None:
    db = Ormdantic(f"sqlite:///{tmp_path / 'relationship_filter_order.sqlite3'}")

    @db.table(pk="id", back_references={"children": "parent"})
    class FilterParent(BaseModel):
        id: str = Field(default_factory=lambda: str(uuid4()))
        name: str
        children: list[FilterChild] = Field(default_factory=list)

    @db.table(pk="id")
    class FilterChild(BaseModel):
        id: str = Field(default_factory=lambda: str(uuid4()))
        title: str
        kind: str
        parent: FilterParent | str

    FilterParent.model_rebuild()
    FilterChild.model_rebuild()

    await db.init()
    await db.drop_all()
    await db.create_all()
    parent = await db[FilterParent].insert(FilterParent(name="parent"))
    empty_parent = await db[FilterParent].insert(FilterParent(name="empty"))
    await db[FilterChild].insert(FilterChild(title="zeta", kind="keep", parent=parent))
    await db[FilterChild].insert(FilterChild(title="alpha", kind="skip", parent=parent))
    await db[FilterChild].insert(FilterChild(title="beta", kind="keep", parent=parent))
    await db[FilterChild].insert(
        FilterChild(title="orphan", kind="skip", parent=empty_parent)
    )

    loaded = await db[FilterParent].find_one(
        parent.id,
        load=[joinedload("children").filter(kind="keep").sorted_by("title")],
    )

    assert loaded is not None
    assert [child.title for child in loaded.children] == ["beta", "zeta"]
    selected = await db[FilterParent].find_one(
        parent.id,
        load=[selectinload("children").filter(kind="keep").sorted_by("-title")],
    )

    assert selected is not None
    assert [child.title for child in selected.children] == ["zeta", "beta"]
    parents = await db[FilterParent].find_many(
        load=[joinedload("children").filter(kind="keep").sorted_by("title")]
    )
    by_name = {item.name: item for item in parents.data}
    assert set(by_name) == {"parent", "empty"}
    assert [child.title for child in by_name["parent"].children] == ["beta", "zeta"]
    assert by_name["empty"].children == []

    with pytest.raises(
        ValueError,
        match=(
            r"invalid loader option for path 'children': "
            r"'missing' is not a column on FilterChild; "
            r"available columns: id, kind, parent, title"
        ),
    ):
        await db[FilterParent].find_one(
            parent.id, load=[joinedload("children").filter(missing="value")]
        )


async def test_selectinload_batches_collection_relationships(tmp_path) -> None:
    db = Ormdantic(f"sqlite:///{tmp_path / 'selectin_collection_batches.sqlite3'}")

    @db.table(pk="id", back_references={"children": "parent"})
    class BatchParent(BaseModel):
        id: str = Field(default_factory=lambda: str(uuid4()))
        name: str
        children: list[BatchChild] = Field(default_factory=list)

    @db.table(pk="id")
    class BatchChild(BaseModel):
        id: str = Field(default_factory=lambda: str(uuid4()))
        name: str
        parent: BatchParent | str

    BatchParent.model_rebuild()
    BatchChild.model_rebuild()

    await db.init()
    await db.drop_all()
    await db.create_all()
    first = await db[BatchParent].insert(BatchParent(name="first"))
    second = await db[BatchParent].insert(BatchParent(name="second"))
    await db[BatchChild].insert(BatchChild(name="first-child", parent=first))
    await db[BatchChild].insert(BatchChild(name="second-child", parent=second))

    loaded = await db[BatchParent].find_many(
        order_by=["name"], load=[selectinload("children").batched(1)]
    )

    assert [parent.name for parent in loaded.data] == ["first", "second"]
    assert [[child.name for child in parent.children] for parent in loaded.data] == [
        ["first-child"],
        ["second-child"],
    ]


async def test_selectinload_batches_scalar_relationships(tmp_path) -> None:
    db = Ormdantic(f"sqlite:///{tmp_path / 'selectin_scalar_batches.sqlite3'}")

    @db.table(pk="id")
    class BatchFlavor(BaseModel):
        id: str = Field(default_factory=lambda: str(uuid4()))
        name: str

    @db.table(pk="id")
    class BatchCoffee(BaseModel):
        id: str = Field(default_factory=lambda: str(uuid4()))
        name: str
        flavor: BatchFlavor | str

    await db.init()
    await db.drop_all()
    await db.create_all()
    mocha = await db[BatchFlavor].insert(BatchFlavor(name="mocha"))
    latte = await db[BatchFlavor].insert(BatchFlavor(name="latte"))
    await db[BatchCoffee].insert(BatchCoffee(name="first", flavor=mocha))
    await db[BatchCoffee].insert(BatchCoffee(name="second", flavor=latte))

    loaded = await db[BatchCoffee].find_many(
        order_by=["name"], load=[selectinload("flavor").batched(1)]
    )

    assert [coffee.name for coffee in loaded.data] == ["first", "second"]
    assert [coffee.flavor.name for coffee in loaded.data] == ["mocha", "latte"]  # type: ignore[union-attr]


async def test_relationship_loader_large_selectin_and_mixed_graph_stress(
    tmp_path,
) -> None:
    await run_relationship_loader_stress(
        f"sqlite:///{tmp_path / 'relationship_loader_stress.sqlite3'}",
        suffix="sqlite",
    )


def test_selectinload_rejects_invalid_batch_size() -> None:
    with pytest.raises(ValueError, match="loader batch size must be greater than zero"):
        selectinload("children").batched(0)


async def test_self_referential_nested_loader_path(tmp_path) -> None:
    db = Ormdantic(f"sqlite:///{tmp_path / 'self_reference_loader.sqlite3'}")

    @db.table(pk="id")
    class Employee(BaseModel):
        id: str = Field(default_factory=lambda: str(uuid4()))
        name: str
        manager: Employee | str | None = None

    Employee.model_rebuild()

    await db.init()
    await db.drop_all()
    await db.create_all()
    ceo = await db[Employee].insert(Employee(name="ceo"))
    lead = await db[Employee].insert(Employee(name="lead", manager=ceo))
    engineer = await db[Employee].insert(Employee(name="engineer", manager=lead))

    loaded = await db[Employee].find_one(
        engineer.id, load=[joinedload("manager.manager")]
    )

    assert loaded is not None
    assert isinstance(loaded.manager, Employee)
    assert isinstance(loaded.manager.manager, Employee)
    assert loaded.manager.manager == ceo


async def test_missing_related_row_loads_as_none(tmp_path) -> None:
    db_path = tmp_path / "missing_related.sqlite3"
    db = Ormdantic(f"sqlite:///{db_path}")

    @db.table(pk="id")
    class Flavor(BaseModel):
        id: str = Field(default_factory=lambda: str(uuid4()))
        name: str

    @db.table(pk="id")
    class Coffee(BaseModel):
        id: str = Field(default_factory=lambda: str(uuid4()))
        flavor: Flavor | str | None = None

    await db.init()
    await db.drop_all()
    await db.create_all()
    coffee_id = str(uuid4())
    with sqlite3.connect(db_path) as connection:
        connection.execute("PRAGMA foreign_keys = OFF")
        connection.execute(
            'INSERT INTO "coffee" ("id", "flavor") VALUES (?, ?)',
            (coffee_id, "missing-flavor"),
        )

    loaded = await db[Coffee].find_one(coffee_id, load=[joinedload("flavor")])

    assert loaded is not None
    assert loaded.flavor is None
