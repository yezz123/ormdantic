from typing import Generic, Type

from pydantic import BaseModel, Field
from pydantic.generics import GenericModel

from ormdantic.types import ModelType


class Result(GenericModel, Generic[ModelType]):
    """Search result object."""

    offset: int
    limit: int
    data: list[ModelType]


class Relationship(BaseModel):
    """Describes a relationship from one table to another."""

    foreign_table: str
    back_references: str | None = None


class OrmTable(GenericModel, Generic[ModelType]):
    """Class to store table information, including relationships and back references for many-to-many relationships."""

    model: Type[ModelType]
    tablename: str
    pk: str
    indexed: list[str]
    unique: list[str]
    unique_constraints: list[list[str]]
    columns: list[str]
    relationships: dict[str, Relationship]
    back_references: dict[str, str]


class Map(BaseModel):
    """Map tablename to table data and model to table data."""

    name_to_data: dict[str, OrmTable] = Field(  # type: ignore
        default_factory=lambda: {}
    )
    model_to_data: dict[ModelType, OrmTable] = Field(  # type: ignore
        default_factory=lambda: {}
    )
