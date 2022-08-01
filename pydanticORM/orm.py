"""Module providing a way to create ORM models and schemas"""
from types import UnionType
from typing import Callable, ForwardRef, Type, get_args, get_origin

from pydantic import BaseModel
from sqlalchemy import MetaData
from sqlalchemy.ext.asyncio import AsyncEngine

from pydanticORM.generator import CRUD, Table
from pydanticORM.handler import (
    Get_M2M_TableName,
    MismatchingBackReferenceError,
    MustUnionForeignKeyError,
    UndefinedBackReferenceError,
    snake_case,
)
from pydanticORM.table import PydanticTableMeta, Relation, RelationType
from pydanticORM.types import ModelType


class PydanticORM:
    """Class to use pydantic models as ORM models."""

    def __init__(self, engine: AsyncEngine) -> None:
        """Register models as ORM models and create schemas"""
        self.metadata: MetaData | None = None
        self._crud_generators: dict[Type, CRUD] = {}  # type: ignore
        self._schema: dict[str, PydanticTableMeta] = {}  # type: ignore
        self._model_to_metadata: dict[Type[BaseModel], PydanticTableMeta] = {}  # type: ignore
        self._engine = engine

    def __getitem__(self, item: Type[ModelType]) -> CRUD[ModelType]:
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
        def _wrapper(cls: Type[ModelType]) -> Type[ModelType]:
            tablename_ = tablename or snake_case(cls.__name__)
            metadata: PydanticTableMeta = PydanticTableMeta(  # type: ignore
                name=tablename_,
                model=cls,
                pk=pk,
                indexed=indexed or [],
                unique=unique or [],
                unique_constraints=unique_constraints or [],
                columns=[],
                relationships={},
                back_references=back_references or {},
            )
            self._schema[tablename_] = metadata
            self._model_to_metadata[cls] = metadata
            return cls

        return _wrapper

    async def init(self) -> None:
        # Populate relation information.
        for tablename, table_data in self._schema.items():
            cols, rels = self.get(tablename, table_data)
            table_data.columns = cols
            table_data.relationships = rels
        # Now that relation information is populated generate tables.
        self.metadata = MetaData()
        for tablename, table_data in self._schema.items():
            # noinspection PyTypeChecker
            self._crud_generators[table_data.model] = CRUD(
                tablename,
                self._engine,
                self._schema,
            )
        await Table(self._engine, self.metadata, self._schema).init()

    def get(
        self, tablename: str, table_data: PydanticTableMeta  # type: ignore
    ) -> tuple[list[str], dict[str, Relation]]:
        columns = []
        relationships = {}
        for field_name, field in table_data.model.__fields__.items():
            related_table = self._get_related_table(field)
            if related_table is None:
                columns.append(field_name)
                continue
            # Check if back-reference is present but mismatched in type.
            back_reference = table_data.back_references.get(field_name)
            back_referenced_field = related_table.model.__fields__.get(back_reference)
            if (
                back_reference
                and table_data.model not in get_args(back_referenced_field.type_)
                and table_data.model != back_referenced_field.type_
            ):
                raise MismatchingBackReferenceError(
                    tablename, related_table.name, field_name, back_reference
                )
            # If this is not a list of another table, add foreign key.
            if get_origin(field.outer_type_) != list and field.type_ != ForwardRef(
                f"list[{table_data.model.__name__}]"
            ):
                args = get_args(field.type_)
                correct_type = (
                    related_table.model.__fields__[related_table.pk].type_ in args
                )
                origin = get_origin(field.type_)
                if not args or origin != UnionType or not correct_type:
                    raise MustUnionForeignKeyError(
                        tablename,
                        related_table.name,
                        field_name,
                        related_table.model,
                        related_table.model.__fields__[related_table.pk].type_.__name__,
                    )
                columns.append(field_name)
                relationships[field_name] = Relation(
                    foreign_table=related_table.name,
                    relation_type=RelationType.ONE_TO_MANY,
                )
                continue
            # MTM Must have a back-reference.
            if not back_reference:
                raise UndefinedBackReferenceError(
                    tablename, related_table.name, field_name
                )
            # Is the back referenced field also a list?
            is_mtm = get_origin(back_referenced_field.outer_type_) == list
            relation_type = RelationType.ONE_TO_MANY
            mtm_tablename = None
            if is_mtm:
                relation_type = RelationType.MANY_TO_MANY
                # Get mtm tablename or make one.
                if rel := related_table.relationships.get(back_reference):
                    mtm_tablename = rel.m2m_table
                else:
                    mtm_tablename = Get_M2M_TableName(
                        table_data.name, field_name, related_table.name, back_reference
                    )
            relationships[field_name] = Relation(
                foreign_table=related_table.name,
                relation_type=relation_type,
                back_references=back_reference,
                mtm_table=mtm_tablename,
            )
        return columns, relationships

    def _get_related_table(self, field) -> PydanticTableMeta:  # type: ignore
        related_table: PydanticTableMeta | None = None  # type: ignore
        # Try to get foreign model from union.
        if args := get_args(field.type_):
            for arg in args:
                try:
                    related_table = self._model_to_metadata.get(arg)
                except TypeError:
                    break
                if related_table is not None:
                    break
        # Try to get foreign table from type.
        related_table = related_table or self._model_to_metadata.get(field.type_)
        return related_table  # type: ignore
