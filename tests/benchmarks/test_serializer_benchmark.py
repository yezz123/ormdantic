from __future__ import annotations

import asyncio
import tempfile
from dataclasses import dataclass
from typing import Any, Iterator
from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel, Field

from ormdantic import Ormdantic, column, joinedload, selectinload
from ormdantic.migrations import (
    ColumnSnapshot,
    MigrationOperation,
    MigrationPlan,
    SchemaSnapshot,
    TableSnapshot,
    diff_snapshots,
)
from ormdantic.models import Map, OrmTable, Relationship
from ormdantic.serializer import OrmSerializer

pytest.importorskip("pytest_benchmark")


class _BenchFlavor(BaseModel):
    id: UUID = Field(default_factory=uuid4)
    name: str
    strength: int


class _BenchCoffee(BaseModel):
    id: UUID = Field(default_factory=uuid4)
    name: str
    flavor: _BenchFlavor | UUID


class _BenchOne(BaseModel):
    id: UUID = Field(default_factory=uuid4)
    name: str
    many: list[_BenchMany] = Field(default_factory=list)


class _BenchMany(BaseModel):
    id: UUID = Field(default_factory=uuid4)
    one: _BenchOne | UUID


class _BenchAuthor(BaseModel):
    id: UUID = Field(default_factory=uuid4)
    name: str
    books: list[_BenchBook] = Field(default_factory=list)


class _BenchBook(BaseModel):
    id: UUID = Field(default_factory=uuid4)
    title: str
    author: _BenchAuthor | UUID
    pages: list[_BenchPage] = Field(default_factory=list)


class _BenchPage(BaseModel):
    id: UUID = Field(default_factory=uuid4)
    body: str
    position: int
    book: _BenchBook | UUID


_BenchOne.model_rebuild()
_BenchMany.model_rebuild()
_BenchAuthor.model_rebuild()
_BenchBook.model_rebuild()
_BenchPage.model_rebuild()


@dataclass
class _FakeCursor:
    description: list[tuple[str]]


class _FakeResult:
    def __init__(self, columns: list[str], rows: list[tuple[Any, ...]]) -> None:
        self.cursor = _FakeCursor([(column,) for column in columns])
        self._rows = rows

    def __iter__(self) -> Iterator[tuple[Any, ...]]:
        return iter(self._rows)


def _flat_table() -> OrmTable[_BenchFlavor]:
    return OrmTable[_BenchFlavor](
        model=_BenchFlavor,
        tablename="bench_flavors",
        pk="id",
        indexed=[],
        unique=[],
        unique_constraints=[],
        columns=["id", "name", "strength"],
        relationships={},
        back_references={},
    )


def _coffee_table() -> OrmTable[_BenchCoffee]:
    return OrmTable[_BenchCoffee](
        model=_BenchCoffee,
        tablename="bench_coffees",
        pk="id",
        indexed=[],
        unique=[],
        unique_constraints=[],
        columns=["id", "name", "flavor"],
        relationships={"flavor": Relationship(foreign_table="bench_flavors")},
        back_references={},
    )


def _one_table() -> OrmTable[_BenchOne]:
    return OrmTable[_BenchOne](
        model=_BenchOne,
        tablename="bench_ones",
        pk="id",
        indexed=[],
        unique=[],
        unique_constraints=[],
        columns=["id", "name"],
        relationships={
            "many": Relationship(foreign_table="bench_many", back_references="one")
        },
        back_references={"many": "one"},
    )


def _many_table() -> OrmTable[_BenchMany]:
    return OrmTable[_BenchMany](
        model=_BenchMany,
        tablename="bench_many",
        pk="id",
        indexed=[],
        unique=[],
        unique_constraints=[],
        columns=["id", "one"],
        relationships={"one": Relationship(foreign_table="bench_ones")},
        back_references={},
    )


def _author_table() -> OrmTable[_BenchAuthor]:
    return OrmTable[_BenchAuthor](
        model=_BenchAuthor,
        tablename="bench_authors",
        pk="id",
        indexed=[],
        unique=[],
        unique_constraints=[],
        columns=["id", "name"],
        relationships={
            "books": Relationship(foreign_table="bench_books", back_references="author")
        },
        back_references={"books": "author"},
    )


def _book_table() -> OrmTable[_BenchBook]:
    return OrmTable[_BenchBook](
        model=_BenchBook,
        tablename="bench_books",
        pk="id",
        indexed=[],
        unique=[],
        unique_constraints=[],
        columns=["id", "title", "author"],
        relationships={
            "author": Relationship(foreign_table="bench_authors"),
            "pages": Relationship(foreign_table="bench_pages", back_references="book"),
        },
        back_references={"pages": "book"},
    )


def _page_table() -> OrmTable[_BenchPage]:
    return OrmTable[_BenchPage](
        model=_BenchPage,
        tablename="bench_pages",
        pk="id",
        indexed=[],
        unique=[],
        unique_constraints=[],
        columns=["id", "body", "position", "book"],
        relationships={"book": Relationship(foreign_table="bench_books")},
        back_references={},
    )


def _flat_rows(row_count: int) -> list[tuple[Any, ...]]:
    return [(str(uuid4()), f"flavor-{index}", index) for index in range(row_count)]


def _joined_rows(row_count: int) -> list[tuple[Any, ...]]:
    return [
        (
            str(uuid4()),
            f"coffee-{index}",
            str(uuid4()),
            f"flavor-{index}",
            index,
        )
        for index in range(row_count)
    ]


def _one_to_many_rows(
    parent_count: int, children_per_parent: int
) -> list[tuple[Any, ...]]:
    rows = []
    for parent_index in range(parent_count):
        parent_id = str(uuid4())
        for _ in range(children_per_parent):
            rows.append(
                (
                    parent_id,
                    f"one-{parent_index}",
                    str(uuid4()),
                    parent_id,
                )
            )
    return rows


def _nested_joined_rows(
    author_count: int, books_per_author: int, pages_per_book: int
) -> list[tuple[Any, ...]]:
    rows = []
    for author_index in range(author_count):
        author_id = str(uuid4())
        for book_index in range(books_per_author):
            book_id = str(uuid4())
            for page_index in range(pages_per_book):
                rows.append(
                    (
                        author_id,
                        f"author-{author_index}",
                        book_id,
                        f"book-{author_index}-{book_index}",
                        author_id,
                        str(uuid4()),
                        f"page-{page_index}",
                        page_index,
                        book_id,
                    )
                )
    return rows


def _deserialize_flat(rows: list[tuple[Any, ...]]) -> list[_BenchFlavor]:
    table = _flat_table()
    result = _FakeResult(
        ["bench_flavors\\id", "bench_flavors\\name", "bench_flavors\\strength"],
        rows,
    )
    return OrmSerializer[list[_BenchFlavor]](
        table_data=table,
        table_map=Map(name_to_data={table.tablename: table}, model_to_data={}),
        result_set=result,
        is_array=True,
        depth=0,
    ).deserialize()


def _deserialize_joined(rows: list[tuple[Any, ...]]) -> list[_BenchCoffee]:
    flavor = _flat_table()
    coffee = _coffee_table()
    result = _FakeResult(
        [
            "bench_coffees\\id",
            "bench_coffees\\name",
            "bench_coffees/flavor\\id",
            "bench_coffees/flavor\\name",
            "bench_coffees/flavor\\strength",
        ],
        rows,
    )
    return OrmSerializer[list[_BenchCoffee]](
        table_data=coffee,
        table_map=Map(
            name_to_data={coffee.tablename: coffee, flavor.tablename: flavor},
            model_to_data={},
        ),
        result_set=result,
        is_array=True,
        depth=1,
    ).deserialize()


def _deserialize_one_to_many(rows: list[tuple[Any, ...]]) -> list[_BenchOne]:
    one = _one_table()
    many = _many_table()
    result = _FakeResult(
        [
            "bench_ones\\id",
            "bench_ones\\name",
            "bench_ones/many\\id",
            "bench_ones/many\\one",
        ],
        rows,
    )
    return OrmSerializer[list[_BenchOne]](
        table_data=one,
        table_map=Map(
            name_to_data={one.tablename: one, many.tablename: many},
            model_to_data={},
        ),
        result_set=result,
        is_array=True,
        depth=1,
    ).deserialize()


def _deserialize_nested_joined(
    rows: list[tuple[Any, ...]], load_options: tuple[Any, ...] = ()
) -> list[_BenchAuthor]:
    author = _author_table()
    book = _book_table()
    page = _page_table()
    result = _FakeResult(
        [
            "bench_authors\\id",
            "bench_authors\\name",
            "bench_authors/books\\id",
            "bench_authors/books\\title",
            "bench_authors/books\\author",
            "bench_authors/books/pages\\id",
            "bench_authors/books/pages\\body",
            "bench_authors/books/pages\\position",
            "bench_authors/books/pages\\book",
        ],
        rows,
    )
    return OrmSerializer[list[_BenchAuthor]](
        table_data=author,
        table_map=Map(
            name_to_data={
                author.tablename: author,
                book.tablename: book,
                page.tablename: page,
            },
            model_to_data={},
        ),
        result_set=result,
        is_array=True,
        depth=0,
        load_paths=("books.pages",),
        load_options=load_options,
    ).deserialize()


@pytest.mark.parametrize("row_count", [1, 1_000, 10_000])
def test_flat_serializer_benchmark(benchmark: Any, row_count: int) -> None:
    rows = _flat_rows(row_count)

    result = benchmark(_deserialize_flat, rows)

    assert len(result) == row_count


def test_joined_serializer_benchmark(benchmark: Any) -> None:
    rows = _joined_rows(1_000)

    result = benchmark(_deserialize_joined, rows)

    assert len(result) == 1_000
    assert isinstance(result[0].flavor, _BenchFlavor)


def test_one_to_many_serializer_benchmark(benchmark: Any) -> None:
    rows = _one_to_many_rows(parent_count=100, children_per_parent=10)

    result = benchmark(_deserialize_one_to_many, rows)

    assert len(result) == 100
    assert len(result[0].many) == 10


@pytest.mark.parametrize(
    ("author_count", "books_per_author", "pages_per_book"),
    [(100, 3, 5), (250, 4, 8)],
    ids=["medium", "large"],
)
def test_nested_serializer_path_benchmark(
    benchmark: Any,
    author_count: int,
    books_per_author: int,
    pages_per_book: int,
) -> None:
    rows = _nested_joined_rows(author_count, books_per_author, pages_per_book)

    result = benchmark(_deserialize_nested_joined, rows)

    assert len(result) == author_count
    assert (
        sum(len(author.books) for author in result) == author_count * books_per_author
    )
    assert (
        sum(len(book.pages) for author in result for book in author.books)
        == author_count * books_per_author * pages_per_book
    )


def test_nested_serializer_loader_option_benchmark(benchmark: Any) -> None:
    rows = _nested_joined_rows(author_count=100, books_per_author=4, pages_per_book=8)
    load_options = (
        joinedload("books").sorted_by("-title"),
        joinedload("books.pages").filter(position=3),
    )

    result = benchmark(_deserialize_nested_joined, rows, load_options)

    assert len(result) == 100
    assert result[0].books[0].title.endswith("-3")
    assert all(
        len(book.pages) == 1 and book.pages[0].position == 3
        for author in result
        for book in author.books
    )


async def _prepare_runtime_crud(
    database_url: str,
) -> tuple[Any, type[BaseModel]]:
    db = Ormdantic(database_url)

    @db.table(pk="id")
    class BenchRuntimeFlavor(BaseModel):
        id: str
        name: str
        strength: int

    await db.init()
    await db.drop_all()
    await db.create_all()
    return db[BenchRuntimeFlavor], BenchRuntimeFlavor


async def _runtime_crud_once(table: Any, model: type[BaseModel]) -> int:
    await table.delete_where(allow_all=True)
    await table.insert_many(
        [
            model(id=str(index), name=f"flavor-{index}", strength=index)
            for index in range(25)
        ]
    )
    results = await table.find_many(
        where=column("strength").ge(10) & column("name").like("flavor-%")
    )
    return len(results.data)


def test_runtime_crud_expression_benchmark(benchmark: Any) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        database_url = f"sqlite:///{tmp}/runtime.sqlite3"
        loop = asyncio.new_event_loop()
        try:
            table, model = loop.run_until_complete(_prepare_runtime_crud(database_url))
            result = benchmark(
                lambda: loop.run_until_complete(_runtime_crud_once(table, model))
            )
        finally:
            loop.run_until_complete(loop.shutdown_default_executor())
            loop.close()

    assert result == 15


async def _prepare_relationship_load(
    database_url: str,
    parent_count: int,
    children_per_parent: int,
) -> Any:
    db = Ormdantic(database_url)

    @db.table(pk="id", back_references={"children": "parent"})
    class BenchParent(BaseModel):
        id: str
        name: str
        children: list[BenchChild] = Field(default_factory=list)

    @db.table(pk="id")
    class BenchChild(BaseModel):
        id: str
        name: str
        parent: BenchParent | str

    BenchParent.model_rebuild(_types_namespace={"BenchChild": BenchChild})
    BenchChild.model_rebuild(_types_namespace={"BenchParent": BenchParent})

    await db.init()
    await db.drop_all()
    await db.create_all()
    parents = [
        BenchParent(id=f"parent-{index}", name=f"parent-{index}")
        for index in range(parent_count)
    ]
    children = [
        BenchChild(
            id=f"{parent.id}-child-{child_index}",
            name=f"child-{child_index}",
            parent=parent,
        )
        for parent in parents
        for child_index in range(children_per_parent)
    ]
    await db[BenchParent].insert_many(parents)
    await db[BenchChild].insert_many(children)
    return db[BenchParent]


async def _relationship_load_once(table: Any, strategy: Any) -> int:
    result = await table.find_many(load=[strategy("children")])
    return sum(len(parent.children) for parent in result.data)


@pytest.mark.parametrize(
    ("parent_count", "children_per_parent"),
    [(100, 5), (500, 10)],
    ids=["medium", "large"],
)
@pytest.mark.parametrize(
    "strategy",
    [joinedload, selectinload],
    ids=["joined", "selectin"],
)
def test_relationship_loader_strategy_benchmark(
    benchmark: Any,
    strategy: Any,
    parent_count: int,
    children_per_parent: int,
) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        database_url = f"sqlite:///{tmp}/relationship-loaders.sqlite3"
        loop = asyncio.new_event_loop()
        try:
            table = loop.run_until_complete(
                _prepare_relationship_load(
                    database_url,
                    parent_count,
                    children_per_parent,
                )
            )
            result = benchmark(
                lambda: loop.run_until_complete(
                    _relationship_load_once(table, strategy)
                )
            )
        finally:
            loop.run_until_complete(loop.shutdown_default_executor())
            loop.close()

    assert result == parent_count * children_per_parent


async def _prepare_nested_relationship_load(
    database_url: str,
    parent_count: int,
    children_per_parent: int,
    leaves_per_child: int,
) -> Any:
    db = Ormdantic(database_url)

    @db.table(pk="id", back_references={"children": "parent"})
    class BenchNestedParent(BaseModel):
        id: str
        name: str
        children: list[BenchNestedChild] = Field(default_factory=list)

    @db.table(pk="id", back_references={"leaves": "child"})
    class BenchNestedChild(BaseModel):
        id: str
        name: str
        parent: BenchNestedParent | str
        leaves: list[BenchNestedLeaf] = Field(default_factory=list)

    @db.table(pk="id")
    class BenchNestedLeaf(BaseModel):
        id: str
        label: str
        rank: int
        child: BenchNestedChild | str

    types_namespace = {
        "BenchNestedParent": BenchNestedParent,
        "BenchNestedChild": BenchNestedChild,
        "BenchNestedLeaf": BenchNestedLeaf,
    }
    BenchNestedParent.model_rebuild(_types_namespace=types_namespace)
    BenchNestedChild.model_rebuild(_types_namespace=types_namespace)
    BenchNestedLeaf.model_rebuild(_types_namespace=types_namespace)

    await db.init()
    await db.drop_all()
    await db.create_all()
    parents = [
        BenchNestedParent(id=f"parent-{index}", name=f"parent-{index}")
        for index in range(parent_count)
    ]
    children = [
        BenchNestedChild(
            id=f"{parent.id}-child-{child_index}",
            name=f"child-{child_index}",
            parent=parent,
        )
        for parent in parents
        for child_index in range(children_per_parent)
    ]
    leaves = [
        BenchNestedLeaf(
            id=f"{child.id}-leaf-{leaf_index}",
            label=f"leaf-{leaf_index}",
            rank=leaf_index,
            child=child,
        )
        for child in children
        for leaf_index in range(leaves_per_child)
    ]
    await db[BenchNestedParent].insert_many(parents)
    await db[BenchNestedChild].insert_many(children)
    await db[BenchNestedLeaf].insert_many(leaves)
    return db[BenchNestedParent]


async def _nested_relationship_load_once(
    table: Any, root_strategy: Any, nested_strategy: Any
) -> int:
    result = await table.find_many(
        load=[
            root_strategy("children"),
            nested_strategy("children.leaves").sorted_by("-rank"),
        ]
    )
    return sum(len(child.leaves) for parent in result.data for child in parent.children)


@pytest.mark.parametrize(
    ("root_strategy", "nested_strategy"),
    [
        (joinedload, joinedload),
        (joinedload, selectinload),
        (selectinload, selectinload),
    ],
    ids=["joined-joined", "joined-selectin", "selectin-selectin"],
)
def test_nested_relationship_loader_strategy_benchmark(
    benchmark: Any,
    root_strategy: Any,
    nested_strategy: Any,
) -> None:
    parent_count = 40
    children_per_parent = 4
    leaves_per_child = 4
    with tempfile.TemporaryDirectory() as tmp:
        database_url = f"sqlite:///{tmp}/nested-relationship-loaders.sqlite3"
        loop = asyncio.new_event_loop()
        try:
            table = loop.run_until_complete(
                _prepare_nested_relationship_load(
                    database_url,
                    parent_count,
                    children_per_parent,
                    leaves_per_child,
                )
            )
            result = benchmark(
                lambda: loop.run_until_complete(
                    _nested_relationship_load_once(
                        table, root_strategy, nested_strategy
                    )
                )
            )
        finally:
            loop.run_until_complete(loop.shutdown_default_executor())
            loop.close()

    assert result == parent_count * children_per_parent * leaves_per_child


async def _reflection_migration_once(database_url: str) -> int:
    db = Ormdantic(database_url)

    @db.table(pk="id")
    class BenchMigrationFlavor(BaseModel):
        id: str
        name: str

    await db.init()
    await db.migrations.apply(
        f"bench-{uuid4()}",
        MigrationPlan(
            [MigrationOperation("CREATE TABLE IF NOT EXISTS bench_extra (id TEXT)")]
        ),
    )
    return len(await db.inspect().table_names())


def test_reflection_migration_benchmark(benchmark: Any) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        database_url = f"sqlite:///{tmp}/migration.sqlite3"

        result = benchmark(
            lambda: asyncio.run(_reflection_migration_once(database_url))
        )

    assert result >= 2


def _migration_snapshot(table_count: int, expanded: bool) -> SchemaSnapshot:
    tables = []
    for index in range(table_count):
        columns = [
            ColumnSnapshot("id", "str", nullable=False, primary_key=True),
            ColumnSnapshot("name", "str", nullable=False, primary_key=False),
        ]
        if expanded:
            columns.append(
                ColumnSnapshot("score", "int", nullable=True, primary_key=False)
            )
        tables.append(
            TableSnapshot(
                model_key=f"BenchMigrationModel{index}",
                name=f"bench_migration_table_{index}",
                primary_key="id",
                columns=columns,
            )
        )
    return SchemaSnapshot(tables=tables)


def _diff_migration_snapshots(
    before: SchemaSnapshot, after: SchemaSnapshot
) -> tuple[int, bool]:
    diff = diff_snapshots(before, after)
    return len(diff.changes), diff.has_unsafe_operations


def test_migration_diff_planning_benchmark(benchmark: Any) -> None:
    before = _migration_snapshot(table_count=150, expanded=False)
    after = _migration_snapshot(table_count=150, expanded=True)

    change_count, has_unsafe_operations = benchmark(
        _diff_migration_snapshots, before, after
    )

    assert change_count == 150
    assert has_unsafe_operations is False
