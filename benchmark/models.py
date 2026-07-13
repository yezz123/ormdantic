from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from pydantic import BaseModel, Field

BENCH_ITEM_TABLE = "ormdantic_bench_items"
BENCH_PARENT_TABLE = "ormdantic_bench_parents"
BENCH_CHILD_TABLE = "ormdantic_bench_children"
BENCH_LEAF_TABLE = "ormdantic_bench_leaves"


try:
    from sqlalchemy import ForeignKey, Integer, String
    from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
except ModuleNotFoundError as exc:  # pragma: no cover - exercised by CLI setup errors
    SQLALCHEMY_IMPORT_ERROR: ModuleNotFoundError | None = exc
else:
    SQLALCHEMY_IMPORT_ERROR = None

    class SqlAlchemyBase(DeclarativeBase):
        """SQLAlchemy declarative base for benchmark tables."""

    class SqlAlchemyBenchItem(SqlAlchemyBase):
        """SQLAlchemy model matching the shared item workload."""

        __tablename__ = BENCH_ITEM_TABLE

        id: Mapped[str] = mapped_column(String(32), primary_key=True)
        category: Mapped[str] = mapped_column(String(24), index=True)
        name: Mapped[str] = mapped_column(String(96))
        score: Mapped[int] = mapped_column(Integer, index=True)
        payload: Mapped[str] = mapped_column(String(160))

    class SqlAlchemyBenchParent(SqlAlchemyBase):
        """Parent row for relationship benchmarks."""

        __tablename__ = BENCH_PARENT_TABLE

        id: Mapped[str] = mapped_column(String(32), primary_key=True)
        name: Mapped[str] = mapped_column(String(96), index=True)
        children: Mapped[list[SqlAlchemyBenchChild]] = relationship(
            back_populates="parent",
            cascade="all, delete-orphan",
        )

    class SqlAlchemyBenchChild(SqlAlchemyBase):
        """Child row for relationship benchmarks."""

        __tablename__ = BENCH_CHILD_TABLE

        id: Mapped[str] = mapped_column(String(32), primary_key=True)
        name: Mapped[str] = mapped_column(String(96), index=True)
        parent_id: Mapped[str] = mapped_column(
            String(32), ForeignKey(f"{BENCH_PARENT_TABLE}.id"), index=True
        )
        parent: Mapped[SqlAlchemyBenchParent] = relationship(back_populates="children")
        leaves: Mapped[list[SqlAlchemyBenchLeaf]] = relationship(
            back_populates="child",
            cascade="all, delete-orphan",
        )

    class SqlAlchemyBenchLeaf(SqlAlchemyBase):
        """Leaf row for nested relationship benchmarks."""

        __tablename__ = BENCH_LEAF_TABLE

        id: Mapped[str] = mapped_column(String(32), primary_key=True)
        name: Mapped[str] = mapped_column(String(96), index=True)
        child_id: Mapped[str] = mapped_column(
            String(32), ForeignKey(f"{BENCH_CHILD_TABLE}.id"), index=True
        )
        child: Mapped[SqlAlchemyBenchChild] = relationship(back_populates="leaves")


try:
    from sqlmodel import Field as SQLModelField
    from sqlmodel import Relationship, SQLModel
except ModuleNotFoundError as exc:  # pragma: no cover - exercised by CLI setup errors
    SQLMODEL_IMPORT_ERROR: ModuleNotFoundError | None = exc
else:
    SQLMODEL_IMPORT_ERROR = None

    class SQLModelBenchItem(SQLModel, table=True):
        """SQLModel model matching the shared item workload."""

        __tablename__ = BENCH_ITEM_TABLE

        id: str = SQLModelField(primary_key=True, max_length=32)
        category: str = SQLModelField(index=True, max_length=24)
        name: str = SQLModelField(max_length=96)
        score: int = SQLModelField(index=True)
        payload: str = SQLModelField(max_length=160)

    class SQLModelBenchParent(SQLModel, table=True):
        """Parent row for SQLModel relationship benchmarks."""

        __tablename__ = BENCH_PARENT_TABLE

        id: str = SQLModelField(primary_key=True, max_length=32)
        name: str = SQLModelField(index=True, max_length=96)
        children: list["SQLModelBenchChild"] = Relationship(
            sa_relationship=relationship(
                "SQLModelBenchChild",
                back_populates="parent",
                cascade="all, delete-orphan",
            )
        )

    class SQLModelBenchChild(SQLModel, table=True):
        """Child row for SQLModel relationship benchmarks."""

        __tablename__ = BENCH_CHILD_TABLE

        id: str = SQLModelField(primary_key=True, max_length=32)
        name: str = SQLModelField(index=True, max_length=96)
        parent_id: str = SQLModelField(
            foreign_key=f"{BENCH_PARENT_TABLE}.id",
            index=True,
            max_length=32,
        )
        parent: "SQLModelBenchParent | None" = Relationship(
            sa_relationship=relationship(
                "SQLModelBenchParent",
                back_populates="children",
            )
        )
        leaves: list["SQLModelBenchLeaf"] = Relationship(
            sa_relationship=relationship(
                "SQLModelBenchLeaf",
                back_populates="child",
                cascade="all, delete-orphan",
            )
        )

    class SQLModelBenchLeaf(SQLModel, table=True):
        """Leaf row for SQLModel nested relationship benchmarks."""

        __tablename__ = BENCH_LEAF_TABLE

        id: str = SQLModelField(primary_key=True, max_length=32)
        name: str = SQLModelField(index=True, max_length=96)
        child_id: str = SQLModelField(
            foreign_key=f"{BENCH_CHILD_TABLE}.id",
            index=True,
            max_length=32,
        )
        child: "SQLModelBenchChild | None" = Relationship(
            sa_relationship=relationship(
                "SQLModelBenchChild",
                back_populates="leaves",
            )
        )


def sqlmodel_benchmark_tables() -> list[Any]:
    """Return only SQLModel tables owned by the benchmark suite."""
    if SQLMODEL_IMPORT_ERROR is not None:
        return []
    return [
        SQLModelBenchItem.__table__,
        SQLModelBenchParent.__table__,
        SQLModelBenchChild.__table__,
        SQLModelBenchLeaf.__table__,
    ]


class SimplePayload(BaseModel):
    """Pydantic payload used for serialization-only benchmark cases."""

    id: str
    category: str
    name: str
    score: int
    payload: str


class NestedLeafPayload(BaseModel):
    """Nested leaf payload used for serialization-only benchmark cases."""

    id: str
    name: str


class NestedChildPayload(BaseModel):
    """Nested child payload used for serialization-only benchmark cases."""

    id: str
    name: str
    leaves: list[NestedLeafPayload] = Field(default_factory=list)


class NestedParentPayload(BaseModel):
    """Nested parent payload used for serialization-only benchmark cases."""

    id: str
    name: str
    children: list[NestedChildPayload] = Field(default_factory=list)


@dataclass(frozen=True)
class OrmdanticModels:
    """Ormdantic model classes registered to one Ormdantic instance."""

    item: type[BaseModel]
    parent: type[BaseModel]
    child: type[BaseModel]
    leaf: type[BaseModel]


def register_ormdantic_models(db: Any) -> OrmdanticModels:
    """Register all benchmark models against an Ormdantic database."""

    @db.table(BENCH_ITEM_TABLE, pk="id")
    class OrmdanticBenchItem(BaseModel):
        id: str = Field(max_length=32)
        category: str = Field(max_length=24)
        name: str = Field(max_length=96)
        score: int
        payload: str = Field(max_length=160)

    @db.table(BENCH_PARENT_TABLE, pk="id", back_references={"children": "parent"})
    class OrmdanticBenchParent(BaseModel):
        id: str = Field(max_length=32)
        name: str = Field(max_length=96)
        children: list["OrmdanticBenchChild"] = Field(default_factory=list)

    @db.table(BENCH_CHILD_TABLE, pk="id", back_references={"leaves": "child"})
    class OrmdanticBenchChild(BaseModel):
        id: str = Field(max_length=32)
        name: str = Field(max_length=96)
        parent: OrmdanticBenchParent | str
        leaves: list["OrmdanticBenchLeaf"] = Field(default_factory=list)

    @db.table(BENCH_LEAF_TABLE, pk="id")
    class OrmdanticBenchLeaf(BaseModel):
        id: str = Field(max_length=32)
        name: str = Field(max_length=96)
        child: OrmdanticBenchChild | str

    namespace = {
        "OrmdanticBenchParent": OrmdanticBenchParent,
        "OrmdanticBenchChild": OrmdanticBenchChild,
        "OrmdanticBenchLeaf": OrmdanticBenchLeaf,
    }
    OrmdanticBenchParent.model_rebuild(_types_namespace=namespace)
    OrmdanticBenchChild.model_rebuild(_types_namespace=namespace)
    OrmdanticBenchLeaf.model_rebuild(_types_namespace=namespace)
    return OrmdanticModels(
        item=OrmdanticBenchItem,
        parent=OrmdanticBenchParent,
        child=OrmdanticBenchChild,
        leaf=OrmdanticBenchLeaf,
    )
