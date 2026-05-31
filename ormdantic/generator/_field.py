"""Module for building queries from field data."""

from typing import Any

from pypika import Field, Order
from pypika.functions import Count
from pypika.queries import Query, QueryBuilder, Table

from ormdantic.generator._rust_query import (
    RustQuery,
    bind_compiled_query,
    compile_count,
    compile_delete_pk,
    compile_find_many,
    compile_select_pk,
)
from ormdantic.handler import py_type_to_sql
from ormdantic.models import Map, OrmTable


class OrmField:
    """Build SQL queries from field information."""

    def __init__(
        self, table_data: OrmTable[Any], table_map: Map, dialect: str
    ) -> None:
        """Build CRUD queries from tablename and field info.

        :param table_data: Meta data of target table for SQL script.
        :param table_map: Map of tablenames and models.
        """
        self._table_data = table_data
        self._table_map = table_map
        self._dialect = dialect
        self._table = Table(table_data.tablename)
        self._query = Query.from_(self._table)

    def get_find_one_query(self, pk: Any, depth: int = 1) -> QueryBuilder | RustQuery:
        """Get query to find one model."""
        if depth <= 0:
            rust_query = bind_compiled_query(
                compile_select_pk(
                    dialect=self._dialect,
                    table=self._table_data.tablename,
                    primary_key=self._table_data.pk,
                    columns=self._flat_column_names(depth),
                    aliases=self._flat_column_aliases(depth),
                ),
                {self._table_data.pk: py_type_to_sql(self._table_map, pk)},
            )
            if rust_query is not None:
                return rust_query

        query, columns = self._build_joins(
            Query.from_(self._table),
            self._table_data,
            depth,
            self._columns(depth),
        )
        query = query.where(
            self._table.field(self._table_data.pk)
            == py_type_to_sql(self._table_map, pk)
        ).select(*columns)
        return query

    def get_find_many_query(
        self,
        where: dict[str, Any] | None,
        order_by: list[str] | None,
        order: Order,
        limit: int,
        offset: int,
        depth: int,
    ) -> QueryBuilder | RustQuery:
        """Get find query for many records.

        :param where: Dictionary of column name to desired value.
        :param order_by: Columns to order by.
        :param order: Order results by ascending or descending.
        :param limit: Number of records to return.
        :param offset: Number of records to offset by.
        :param depth: Depth of relations to populate.
        :return: A list of models representing table records.
        """
        where = where or {}
        order_by = order_by or []
        if depth <= 0:
            filter_values = {
                field: py_type_to_sql(self._table_map, value)
                for field, value in where.items()
            }
            rust_query = bind_compiled_query(
                compile_find_many(
                    dialect=self._dialect,
                    table=self._table_data.tablename,
                    columns=self._flat_column_names(depth),
                    filter_columns=list(where),
                    order_columns=order_by,
                    order_direction="desc" if order == Order.desc else "asc",
                    limit=limit or None,
                    offset=offset or None,
                    aliases=self._flat_column_aliases(depth),
                ),
                filter_values,
            )
            if rust_query is not None:
                return rust_query

        query, columns = self._build_joins(
            Query.from_(self._table),
            self._table_data,
            depth,
            self._columns(depth),
        )
        for field, value in where.items():
            query = query.where(self._table.field(field) == value)
        query = query.orderby(*order_by, order=order).select(*columns)
        if limit:
            query = query.limit(limit)
        if offset:
            query = query.offset(offset)
        return query

    def get_delete_query(self, pk: Any) -> QueryBuilder | RustQuery:
        """Get a `delete` query.

        :param pk: Primary key of the record to delete.
        :return: Query to delete a record.
        """
        rust_query = bind_compiled_query(
            compile_delete_pk(
                dialect=self._dialect,
                table=self._table_data.tablename,
                primary_key=self._table_data.pk,
            ),
            {self._table_data.pk: py_type_to_sql(self._table_map, pk)},
        )
        if rust_query is not None:
            return rust_query
        return self._query.where(self._table.field(self._table_data.pk) == pk).delete()

    def get_count_query(
        self,
        where: dict[str, Any] | None,
        depth: int,
    ) -> QueryBuilder | RustQuery:
        """Get a `count` query.

        :param where: Dictionary of column name to desired value.
        :param depth: Depth of relations to populate.
        :return: Query to count records.
        """
        where = where or {}
        if depth <= 0:
            filter_values = {
                field: py_type_to_sql(self._table_map, value)
                for field, value in where.items()
            }
            rust_query = bind_compiled_query(
                compile_count(
                    dialect=self._dialect,
                    table=self._table_data.tablename,
                    filter_columns=list(where),
                ),
                filter_values,
            )
            if rust_query is not None:
                return rust_query

        query, columns = self._build_joins(
            Query.from_(self._table),
            self._table_data,
            depth,
            self._columns(depth),
        )
        for field, value in where.items():
            query = query.where(self._table.field(field) == value)
        return query.select(Count("*"))

    def _build_joins(
        self,
        query: QueryBuilder,
        table_data: OrmTable,  # type: ignore
        depth: int,
        columns: list[Field],
        table_tree: str | None = None,
    ) -> tuple[QueryBuilder, list[Field]]:
        if depth <= 0:
            return query, columns
        if not (
            relationships := self._table_map.name_to_data[
                table_data.tablename
            ].relationships
        ):
            return query, columns
        depth -= 1
        table_tree = table_tree or table_data.tablename
        pypika_table: Table = Table(table_data.tablename)
        if table_data.tablename != table_tree:
            pypika_table = pypika_table.as_(table_tree)
        # For each related table, add join to query.
        for field_name, relation in relationships.items():
            relation_name = f"{table_tree}/{field_name}"
            rel_table = Table(relation.foreign_table).as_(relation_name)
            if relation.back_references is not None:
                query = query.left_join(rel_table).on(
                    pypika_table.field(table_data.pk)
                    == rel_table.field(relation.back_references)
                )
            else:
                query = query.left_join(rel_table).on(
                    pypika_table.field(field_name)
                    == rel_table.field(
                        self._table_map.name_to_data[relation.foreign_table].pk
                    )
                )
            # Add columns of rel table to this query.
            rel_table_data = self._table_map.name_to_data[relation.foreign_table]
            columns.extend(
                [
                    rel_table.field(c).as_(f"{relation_name}\\{c}")
                    for c in self._table_map.name_to_data[
                        relation.foreign_table
                    ].columns
                    if depth <= 0 or c not in rel_table_data.relationships
                ]
            )

            # Add joins of relations of this table to query.
            query, new_cols = self._build_joins(
                query,
                self._table_map.name_to_data[relation.foreign_table],
                depth,
                columns,
                relation_name,
            )
            columns.extend([c for c in new_cols if c not in columns])
        return query, columns

    def _columns(self, depth: int) -> list[Field]:
        table = Table(self._table_data.tablename)
        return [
            table.field(c).as_(f"{self._table_data.tablename}\\{c}")
            for c in self._table_data.columns
            if depth <= 0 or c not in self._table_data.relationships
        ]

    def _flat_column_names(self, depth: int) -> list[str]:
        return [
            column
            for column in self._table_data.columns
            if depth <= 0 or column not in self._table_data.relationships
        ]

    def _flat_column_aliases(self, depth: int) -> list[str]:
        return [
            f"{self._table_data.tablename}\\{column}"
            for column in self._flat_column_names(depth)
        ]
