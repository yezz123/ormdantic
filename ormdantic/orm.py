"""Module providing a way to create ORM models and schemas"""

from types import UnionType
from typing import Callable, ForwardRef, Type, Union, get_args, get_origin

from sqlalchemy import MetaData
from sqlalchemy.ext.asyncio import create_async_engine

from ormdantic._introspect import (
    FieldMetadata,
    contains_list_annotation,
    first_model_arg,
    is_list_annotation,
    model_field,
    model_fields,
)
from ormdantic.generator import CRUD, Table
from ormdantic.engine import NativeEngine
from ormdantic.generator._rust_schema import compile_drop_table_sql
from ormdantic.handler import (
    MismatchingBackReferenceError,
    MustUnionForeignKeyError,
    UndefinedBackReferenceError,
    snake_case,
)
from ormdantic.models import Map, OrmTable, Relationship
from ormdantic.types import ModelType


class Ormdantic:
    """
    Ormdantic provides a way to create ORM models and schemas.
    """

    def __init__(self, connection: str) -> None:
        """Register models as ORM models and create schemas"""
        self._metadata: MetaData | None = None
        self._crud_generators: dict[Type, CRUD] = {}  # type: ignore
        self._engine = create_async_engine(connection)
        self._native_engine = NativeEngine(connection)
        self._table_map: Map = Map()

    def __getitem__(self, item: Type[ModelType]) -> CRUD[ModelType]:
        """Get a `Table` for the given pydantic model."""
        return self._crud_generators[item]

    def table(
        self,
        tablename: str | None = None,
        *,
        pk: str,
        indexed: list[str] | None = None,
        unique: list[str] | None = None,
        unique_constraints: list[list[str]] | None = None,
        back_references: dict[str, str] | None = None,
    ) -> Callable[[Type[ModelType]], Type[ModelType]]:
        """Register a model as a database table."""

        def _wrapper(cls: Type[ModelType]) -> Type[ModelType]:
            """Decorator function."""
            tablename_ = tablename or snake_case(cls.__name__)
            cls_back_references = back_references or {}
            table_metadata = OrmTable[ModelType](
                model=cls,
                tablename=tablename_,
                pk=pk,
                indexed=indexed or [],
                unique=unique or [],
                unique_constraints=unique_constraints or [],
                columns=[
                    field
                    for field in model_fields(cls)
                    if field not in cls_back_references
                ],
                relationships={},
                back_references=cls_back_references,
            )
            self._table_map.model_to_data[cls] = table_metadata
            self._table_map.name_to_data[tablename_] = table_metadata
            return cls

        return _wrapper

    async def init(self) -> None:
        """Initialize ORM models."""
        # Populate relation information.
        for table_data in self._table_map.name_to_data.values():
            rels = self.get(table_data)
            table_data.relationships = rels
        # Now that relation information is populated generate tables.
        self._metadata = MetaData()
        for table_data in self._table_map.name_to_data.values():
            self._crud_generators[table_data.model] = CRUD(
                table_data,
                self._table_map,
                self._engine,
            )
        await Table(self._engine, self._metadata, self._table_map).init()

    async def create_all(self) -> None:
        """Create all registered tables."""
        await Table(self._engine, self._metadata or MetaData(), self._table_map).init()

    async def drop_all(self) -> None:
        """Drop all registered tables."""
        for tablename in reversed(list(self._table_map.name_to_data)):
            sql = compile_drop_table_sql(tablename, self._engine.name)
            await self._native_engine.execute(sql, ())

    def get(self, table_data: OrmTable[ModelType]) -> dict[str, Relationship]:
        """Get relationships for a given table."""
        relationships = {}
        for field_name, field in model_fields(table_data.model).items():
            related_table = self._get_related_table(field)
            if related_table is None:
                continue
            if back_reference := table_data.back_references.get(field_name):
                relationships[field_name] = self._get_many_relationship(
                    field_name, back_reference, table_data, related_table
                )

                continue
            if contains_list_annotation(field.annotation) or field.annotation == ForwardRef(
                f"{related_table.model.__name__}"
            ):
                raise UndefinedBackReferenceError(
                    table_data.tablename, related_table.tablename, field_name
                )

            args = get_args(field.annotation)
            correct_type = (
                model_field(related_table.model, related_table.pk).annotation in args
            )
            origin = get_origin(field.annotation)
            if not args or origin not in {UnionType, Union} or not correct_type:
                raise MustUnionForeignKeyError(
                    table_data.tablename,
                    related_table.tablename,
                    field_name,
                    related_table.model,
                    model_field(
                        related_table.model, related_table.pk
                    ).annotation.__name__,
                )

            relationships[field_name] = Relationship(
                foreign_table=related_table.tablename
            )

        return relationships

    def _get_related_table(self, field: FieldMetadata) -> OrmTable | None:  # type: ignore
        """Get related table for a given field."""
        model = first_model_arg(
            field.annotation, set(self._table_map.model_to_data.keys())
        )
        return self._table_map.model_to_data.get(model) if model else None

    @staticmethod
    def _get_many_relationship(
        field_name: str,
        back_reference: str,
        table_data: OrmTable,  # type: ignore
        related_table: OrmTable,  # type: ignore
    ) -> Relationship:
        """Get many-to-many relationship."""
        back_referenced_field = model_fields(related_table.model).get(back_reference)
        if back_referenced_field is None:  # pragma: no cover
            raise MismatchingBackReferenceError(
                table_data.tablename,
                related_table.tablename,
                field_name,
                back_reference,
            )
        # TODO: Check if back-reference is present but mismatched in type.
        if (
            table_data.model not in get_args(back_referenced_field.annotation)
            and table_data.model != back_referenced_field.annotation
        ):
            raise MismatchingBackReferenceError(
                table_data.tablename,
                related_table.tablename,
                field_name,
                back_reference,
            )
        # Is the back referenced field also a list?
        return Relationship(
            foreign_table=related_table.tablename, back_references=back_reference
        )
