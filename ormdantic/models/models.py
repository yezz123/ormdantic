from enum import Enum

from pydantic import BaseModel

from ormdantic.types import ModelType


class RelationType(Enum):
    """Table relationship types."""

    ONE_TO_MANY = 1
    MANY_TO_MANY = 2


class M2MData(BaseModel):
    """Stores information about MTM relationships."""

    tablename: str | None = None
    table_a: str | None = None
    table_b: str | None = None
    table_a_column: str | None = None
    table_b_column: str | None = None


class Relationship(BaseModel):
    """Relationship data."""

    foreign_table: str
    relationship_type: RelationType
    mtm_data: M2MData | None = None


class OrmTable(BaseModel):
    """Table metadata."""

    tablename: str
    pk: str
    indexed: list[str]
    unique: list[str]
    unique_constraints: list[list[str]]
    columns: list[str]
    model: ModelType
    relationships: dict[str, Relationship]
    back_references: dict[str, str]


class Map(BaseModel):
    """Map tablename to table data and model to table data."""

    name_to_data: dict[str, OrmTable]
    model_to_data: dict[ModelType, OrmTable]