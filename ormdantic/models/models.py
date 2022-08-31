from enum import Enum
from typing import Generic, Type

from pydantic import BaseModel, Field
from pydantic.generics import GenericModel

from ormdantic.types import ModelType


class Result(GenericModel, Generic[ModelType]):
    """Search result object."""

    offset: int
    limit: int
    data: list[ModelType]


class RelationType(Enum):
    """Table relationship types."""

    ONE_TO_MANY = 1
    MANY_TO_MANY = 2


class M2M(BaseModel):
    """Stores information about MTM relationships."""

    tablename: str | None = None
    table_a: str | None = None
    table_b: str | None = None
    table_a_column: str | None = None
    table_b_column: str | None = None


class Relationship(BaseModel):
    """Describes a relationship from one table to another."""

    foreign_table: str
    relationship_type: RelationType
    back_references: str | None = None
    mtm_data: M2M | None = None


class OrmTable(BaseModel):
    """Class to store table information, including relationships and back references for many-to-many relationships."""

    model: Type[ModelType]
    tablename: str
    pk: str
    indexed: list[str]
    unique: list[str]
    unique_constraints: list[list[str]]
    relationships: dict[str, Relationship]
    back_references: dict[str, str]


class Map(BaseModel):
    """Map tablename to table data and model to table data."""

    name_to_data: dict[str, OrmTable] = Field(default_factory=lambda: {})
    model_to_data: dict[ModelType, OrmTable] = Field(default_factory=lambda: {})
