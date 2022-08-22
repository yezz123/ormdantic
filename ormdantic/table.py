"""Module providing classes to store table metadata."""
from enum import Enum, auto
from typing import Generic, Type

from pydantic import BaseModel
from pydantic.generics import GenericModel

from ormdantic.types import ModelType


class RelationType(Enum):
    """Table relationship types."""

    ONE_TO_MANY = auto()
    MANY_TO_MANY = auto()


class M2M(BaseModel):
    """Stores information about M2M relationships."""

    name: str | None = None
    table_a: str | None = None
    table_b: str | None = None
    table_a_column: str | None = None
    table_b_column: str | None = None


class Relation(BaseModel):
    # https://stackoverflow.com/a/59920780/12927850
    """Describes a relationship from one table to another."""

    foreign_table: str
    back_references: str | None = None
    relation_type: RelationType
    m2m_data: M2M | None = None


class PydanticTableMeta(GenericModel, Generic[ModelType]):
    """Class to store table information, including relationships and back references for many-to-many relationships."""

    name: str
    model: Type[ModelType]
    pk: str
    indexed: list[str]
    unique: list[str]
    unique_constraints: list[list[str]]
    columns: list[str]
    relationships: dict[str, Relation]
    back_references: dict[str, str]


class Result(GenericModel, Generic[ModelType]):
    """Search result object."""

    offset: int
    limit: int
    data: list[ModelType]
