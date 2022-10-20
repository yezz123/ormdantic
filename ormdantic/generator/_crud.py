"""Handle table interactions for a model."""


from typing import Any, Generic

from pypika import Order
from pypika.queries import QueryBuilder
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession
from sqlalchemy.orm import sessionmaker

from ormdantic.generator._field import OrmField
from ormdantic.generator._query import OrmQuery
from ormdantic.generator._serializer import OrmSerializer
from ormdantic.models import Map, OrmTable, Result
from ormdantic.types import ModelType


class PydanticSQLCRUDGenerator(Generic[ModelType]):
    """Provides DB CRUD methods and table information for a model."""

    def __init__(
        self,
        table_data: OrmTable,  # type: ignore
        table_map: Map,
        engine: AsyncEngine,
    ) -> None:

        self._engine = engine
        self._table_map = table_map
        self._table_data = table_data
        self.tablename = table_data.tablename
        self.columns = table_data.columns

    async def find_one(self, pk: Any, depth: int = 0) -> ModelType | None:
        """Find a model instance by primary key.

        :param pk: Primary key of the record to get.
        :param depth: ORM fetch depth.
        :return: A model representing the record if it exists else None.
        """
        result = await self._execute_query(
            OrmField(self._table_data, self._table_map).get_find_one_query(pk, depth)
        )
        return OrmSerializer[ModelType | None](
            table_data=self._table_data,
            table_map=self._table_map,
            result_set=result,
            is_array=False,
            depth=depth,
        ).deserialize()

    async def find_many(
        self,
        where: dict[str, Any] | None = None,
        order_by: list[str] | None = None,
        order: Order = Order.asc,
        limit: int = 0,
        offset: int = 0,
        depth: int = 0,
    ) -> Result[ModelType]:
        """Find many model instances."""
        result = await self._execute_query(
            OrmField(self._table_data, self._table_map).get_find_many_query(
                where, order_by, order, limit, offset, depth
            )
        )
        deserialized_data = OrmSerializer[ModelType | None](
            table_data=self._table_data,
            table_map=self._table_map,
            result_set=result,
            is_array=True,
            depth=depth,
        ).deserialize()
        return Result(
            offset=offset,
            limit=limit,
            data=deserialized_data or [],
        )

    async def insert(self, model_instance: ModelType) -> ModelType:
        """Insert a model instance."""
        await self._execute_query(
            OrmQuery(model_instance, self._table_map).get_insert_query()
        )
        return model_instance

    async def update(self, model_instance: ModelType) -> ModelType:
        """Update a record.

        :param `model_instance``: Model representing record to update.
        :return: The updated model.
        """
        await self._execute_query(
            OrmQuery(model_instance, self._table_map).get_update_queries()
        )
        return model_instance

    async def upsert(self, model_instance: ModelType) -> ModelType:
        """Insert a record if it does not exist, else update it.

        :param `model_instance``: Model representing record to insert or update.
        :return: The inserted or updated model.
        """

        await self._execute_query(
            OrmQuery(model_instance, self._table_map).get_upsert_query()
        )
        return model_instance

    async def delete(self, pk: Any) -> bool:
        """Delete a model instance by primary key.

        :param pk: Primary key of the record to delete.
        """
        await self._execute_query(
            OrmField(self._table_data, self._table_map).get_delete_query(pk)
        )
        return True

    async def count(self, where: dict[str, Any] | None = None, depth: int = 0) -> int:
        """Count records.

        :param where: Where clause to filter records.
        :param depth: ORM fetch depth.
        :return: Number of records.
        """
        result = await self._execute_query(
            OrmField(self._table_data, self._table_map).get_count_query(where, depth)
        )
        return result.scalar()

    async def _execute_query(self, query: QueryBuilder) -> Any:
        async_session = sessionmaker(
            self._engine, expire_on_commit=False, class_=AsyncSession
        )
        async with async_session() as session:
            async with session.begin():
                result = await session.execute(text(str(query)))
            await session.commit()
        await self._engine.dispose()
        return result
