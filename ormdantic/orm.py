"""Module providing a way to create ORM models and schemas"""

from time import perf_counter
from types import TracebackType, UnionType
from typing import Any, Callable, ForwardRef, Literal, Type, Union, get_args, get_origin

from typing_extensions import Self

from ormdantic._introspect import (
    FieldMetadata,
    contains_list_annotation,
    first_model_arg,
    model_field,
    model_fields,
)
from ormdantic._native import import_native_extension
from ormdantic.errors import (
    MismatchingBackReferenceError,
    MustUnionForeignKeyError,
    SchemaError,
    TransactionError,
    UndefinedBackReferenceError,
    classify_native_error,
)
from ormdantic.events import EventHandler, EventRegistry
from ormdantic.expressions import RelationExpression
from ormdantic.expressions import relation as relation_expression
from ormdantic.loaders import (
    LoaderPathLike,
    install_relationship_path_descriptor,
    joinedload,
    path_parts,
)
from ormdantic.migrations import MigrationManager
from ormdantic.models import (
    DatabaseNamespace,
    DatabaseSequence,
    DatabaseView,
    Map,
    OrmTable,
    Relationship,
    TableCheck,
    TableColumn,
    TableExclusion,
    TableForeignKey,
    TableIndex,
    TableUnique,
    normalized_bool,
    normalized_mysql_partition_by,
    normalized_oracle_table_compress,
    normalized_positive_int,
    normalized_postgres_partition_by,
    normalized_postgres_partition_for,
    normalized_postgres_storage_parameters,
    normalized_storage_identifier,
    normalized_storage_identifier_list,
    normalized_storage_path,
    normalized_storage_string,
    normalized_storage_token,
)
from ormdantic.naming import snake_case
from ormdantic.reflection import Inspector
from ormdantic.schema import (
    column_descriptor,
    enum_type_descriptors,
    exclusion_constraint_descriptors,
    foreign_key_constraint_descriptors,
    index_descriptors,
    oracle_index_compress_runtime,
    rust_exclusion_constraint_descriptors,
    rust_foreign_key_constraint_descriptors,
    rust_index_descriptors,
    rust_table_check_descriptors,
    rust_unique_constraint_descriptors,
    table_check_descriptors,
    unique_constraint_descriptors,
)
from ormdantic.session import Session
from ormdantic.table import Table
from ormdantic.types import ModelType

_ormdantic: Any = import_native_extension(
    context="database runtime initialization",
    required_symbols=(
        "PyDatabase",
        "PyTransactionOptions",
        "compile_drop_table_sql",
        "execute_native",
        "runtime_capabilities",
    ),
)

TransactionIsolationLevel = Literal[
    "read_uncommitted",
    "read_committed",
    "repeatable_read",
    "serializable",
    "snapshot",
]


def split_unique_constraints(
    constraints: list[list[str] | TableUnique],
) -> tuple[list[list[str]], list[TableUnique]]:
    """Split legacy anonymous unique constraints from named constraints."""
    anonymous: list[list[str]] = []
    named: list[TableUnique] = []
    for constraint in constraints:
        if isinstance(constraint, TableUnique):
            named.append(constraint)
        else:
            anonymous.append(list(constraint))
    return anonymous, named


def normalized_table_comment(tablename: str, comment: str | None) -> str | None:
    """Return a stripped table comment or reject empty comment metadata."""
    if comment is None:
        return None
    normalized = comment.strip()
    if not normalized:
        raise ValueError(f"comment for table '{tablename}' cannot be empty")
    return normalized


def _uses_native_enum_types(connection: str) -> bool:
    normalized = connection.lower().split("://", 1)[0].split("+", 1)[0]
    return normalized in {"postgres", "postgresql"}


def _drop_runtime_enum_type_sql(enum_type: tuple[Any, ...]) -> str:
    name = str(enum_type[0])
    schema = enum_type[2] if len(enum_type) > 2 else None
    if schema is None:
        return f"DROP TYPE IF EXISTS {_quote_postgres_ident(name)}"
    return (
        "DROP TYPE IF EXISTS "
        f"{_quote_postgres_ident(str(schema))}.{_quote_postgres_ident(name)}"
    )


def _quote_postgres_ident(value: str) -> str:
    escaped = value.replace('"', '""')
    return f'"{escaped}"'


class Ormdantic:
    """
    Ormdantic provides a way to create ORM models and schemas.
    """

    def __init__(
        self,
        connection: str,
        *,
        debug: bool = False,
        log_queries: bool = False,
        query_logger: EventHandler | None = None,
    ) -> None:
        """Register models as ORM models and create schemas"""
        self._tables: dict[Type, Table] = {}  # type: ignore
        self._connection = connection
        self._events = EventRegistry()
        self._table_map: Map = Map()
        self._namespaces: list[DatabaseNamespace] = []
        self._sequences: list[DatabaseSequence] = []
        self._views: list[DatabaseView] = []
        self._runtime: Any | None = None
        self._debug = debug
        self._log_queries = log_queries
        if query_logger is not None:
            self._events.on("after_execute", query_logger)

    def __getitem__(self, item: Type[ModelType]) -> Table[ModelType]:
        """Get a `Table` for the given pydantic model."""
        return self._tables[item]

    def table(
        self,
        tablename: str | None = None,
        *,
        pk: str,
        schema: str | None = None,
        indexed: list[str] | None = None,
        unique: list[str] | None = None,
        indexes: list[TableIndex] | None = None,
        column_options: dict[str, TableColumn] | None = None,
        unique_constraints: list[list[str] | TableUnique] | None = None,
        check_constraints: list[TableCheck] | None = None,
        foreign_key_constraints: list[TableForeignKey] | None = None,
        exclusion_constraints: list[TableExclusion] | None = None,
        comment: str | None = None,
        tablespace: str | None = None,
        mysql_engine: str | None = None,
        mysql_charset: str | None = None,
        mysql_collation: str | None = None,
        mysql_row_format: str | None = None,
        mysql_key_block_size: int | None = None,
        mysql_pack_keys: bool | None = None,
        mysql_checksum: bool | None = None,
        mysql_delay_key_write: bool | None = None,
        mysql_stats_persistent: bool | None = None,
        mysql_stats_auto_recalc: bool | None = None,
        mysql_stats_sample_pages: int | None = None,
        mysql_avg_row_length: int | None = None,
        mysql_max_rows: int | None = None,
        mysql_min_rows: int | None = None,
        mysql_insert_method: str | None = None,
        mysql_data_directory: str | None = None,
        mysql_index_directory: str | None = None,
        mysql_connection: str | None = None,
        mysql_union: list[str] | None = None,
        mysql_partition_by: str | None = None,
        mysql_partitions: int | None = None,
        mysql_subpartition_by: str | None = None,
        mysql_subpartitions: int | None = None,
        mysql_auto_increment: int | None = None,
        oracle_compress: int | bool | None = None,
        postgres_inherits: list[str] | None = None,
        postgres_with: dict[str, str | int | bool] | None = None,
        postgres_using: str | None = None,
        postgres_unlogged: bool = False,
        postgres_partition_by: str | None = None,
        postgres_partition_of: str | None = None,
        postgres_partition_for: str | None = None,
        sqlite_strict: bool = False,
        sqlite_without_rowid: bool = False,
        back_references: dict[str, str] | None = None,
    ) -> Callable[[Type[ModelType]], Type[ModelType]]:
        """Register a model as a database table."""

        def _wrapper(cls: Type[ModelType]) -> Type[ModelType]:
            """Decorator function."""
            tablename_ = tablename or snake_case(cls.__name__)
            schema_ = normalized_storage_identifier(
                schema,
                option_name=f"schema for table '{tablename_}'",
            )
            comment_ = normalized_table_comment(tablename_, comment)
            tablespace_ = normalized_storage_identifier(
                tablespace,
                option_name=f"tablespace for table '{tablename_}'",
            )
            mysql_engine_ = normalized_storage_token(
                mysql_engine,
                option_name=f"MySQL engine for table '{tablename_}'",
            )
            mysql_charset_ = normalized_storage_token(
                mysql_charset,
                option_name=f"MySQL charset for table '{tablename_}'",
            )
            mysql_collation_ = normalized_storage_token(
                mysql_collation,
                option_name=f"MySQL collation for table '{tablename_}'",
            )
            mysql_row_format_ = normalized_storage_token(
                mysql_row_format,
                option_name=f"MySQL row format for table '{tablename_}'",
            )
            mysql_key_block_size_ = normalized_positive_int(
                mysql_key_block_size,
                option_name=(f"MySQL/MariaDB KEY_BLOCK_SIZE for table '{tablename_}'"),
            )
            mysql_pack_keys_ = normalized_bool(
                mysql_pack_keys,
                option_name=f"MySQL/MariaDB PACK_KEYS for table '{tablename_}'",
            )
            mysql_checksum_ = normalized_bool(
                mysql_checksum,
                option_name=f"MySQL/MariaDB CHECKSUM for table '{tablename_}'",
            )
            mysql_delay_key_write_ = normalized_bool(
                mysql_delay_key_write,
                option_name=(f"MySQL/MariaDB DELAY_KEY_WRITE for table '{tablename_}'"),
            )
            mysql_stats_persistent_ = normalized_bool(
                mysql_stats_persistent,
                option_name=(
                    f"MySQL/MariaDB STATS_PERSISTENT for table '{tablename_}'"
                ),
            )
            mysql_stats_auto_recalc_ = normalized_bool(
                mysql_stats_auto_recalc,
                option_name=(
                    f"MySQL/MariaDB STATS_AUTO_RECALC for table '{tablename_}'"
                ),
            )
            mysql_stats_sample_pages_ = normalized_positive_int(
                mysql_stats_sample_pages,
                option_name=(
                    f"MySQL/MariaDB STATS_SAMPLE_PAGES for table '{tablename_}'"
                ),
            )
            mysql_avg_row_length_ = normalized_positive_int(
                mysql_avg_row_length,
                option_name=f"MySQL/MariaDB AVG_ROW_LENGTH for table '{tablename_}'",
            )
            mysql_max_rows_ = normalized_positive_int(
                mysql_max_rows,
                option_name=f"MySQL/MariaDB MAX_ROWS for table '{tablename_}'",
            )
            mysql_min_rows_ = normalized_positive_int(
                mysql_min_rows,
                option_name=f"MySQL/MariaDB MIN_ROWS for table '{tablename_}'",
            )
            mysql_insert_method_ = normalized_storage_token(
                mysql_insert_method,
                option_name=f"MySQL/MariaDB INSERT_METHOD for table '{tablename_}'",
            )
            mysql_data_directory_ = normalized_storage_path(
                mysql_data_directory,
                option_name=f"MySQL/MariaDB DATA DIRECTORY for table '{tablename_}'",
            )
            mysql_index_directory_ = normalized_storage_path(
                mysql_index_directory,
                option_name=f"MySQL/MariaDB INDEX DIRECTORY for table '{tablename_}'",
            )
            mysql_connection_ = normalized_storage_string(
                mysql_connection,
                option_name=f"MySQL/MariaDB CONNECTION for table '{tablename_}'",
            )
            mysql_union_ = normalized_storage_identifier_list(
                mysql_union,
                option_name=f"MySQL/MariaDB UNION for table '{tablename_}'",
            )
            mysql_partition_by_ = normalized_mysql_partition_by(
                mysql_partition_by,
                table_name=tablename_,
            )
            mysql_partitions_ = normalized_positive_int(
                mysql_partitions,
                option_name=f"MySQL/MariaDB PARTITIONS for table '{tablename_}'",
            )
            mysql_subpartition_by_ = normalized_mysql_partition_by(
                mysql_subpartition_by,
                table_name=tablename_,
                option="SUBPARTITION BY",
            )
            mysql_subpartitions_ = normalized_positive_int(
                mysql_subpartitions,
                option_name=f"MySQL/MariaDB SUBPARTITIONS for table '{tablename_}'",
            )
            mysql_auto_increment_ = normalized_positive_int(
                mysql_auto_increment,
                option_name=f"MySQL/MariaDB AUTO_INCREMENT for table '{tablename_}'",
            )
            oracle_compress_ = normalized_oracle_table_compress(
                oracle_compress,
                table_name=tablename_,
            )
            postgres_inherits_ = [
                normalized_storage_identifier(
                    parent,
                    option_name=f"PostgreSQL inherited table for table '{tablename_}'",
                )
                or ""
                for parent in postgres_inherits or []
            ]
            postgres_with_ = normalized_postgres_storage_parameters(
                postgres_with,
                table_name=tablename_,
            )
            postgres_using_ = normalized_storage_token(
                postgres_using,
                option_name=f"PostgreSQL table access method for table '{tablename_}'",
            )
            postgres_partition_by_ = normalized_postgres_partition_by(
                postgres_partition_by,
                table_name=tablename_,
            )
            postgres_partition_of_ = normalized_storage_identifier(
                postgres_partition_of,
                option_name=f"PostgreSQL partition parent for table '{tablename_}'",
            )
            postgres_partition_for_ = normalized_postgres_partition_for(
                postgres_partition_for,
                table_name=tablename_,
            )
            if (postgres_partition_of_ is None) != (postgres_partition_for_ is None):
                raise ValueError(
                    f"PostgreSQL partition table '{tablename_}' requires both "
                    "postgres_partition_of and postgres_partition_for"
                )
            if postgres_partition_of_ is not None and postgres_inherits_:
                raise ValueError(
                    f"PostgreSQL partition table '{tablename_}' cannot also use "
                    "postgres_inherits"
                )
            indexes_ = indexes or []
            anonymous_unique_constraints, named_unique_constraints = (
                split_unique_constraints(unique_constraints or [])
            )
            clustered_names = [
                f"index {index.name}" for index in indexes_ if index.mssql_clustered
            ]
            clustered_names.extend(
                f"unique constraint {constraint.name}"
                for constraint in named_unique_constraints
                if constraint.mssql_clustered is True
            )
            if len(clustered_names) > 1:
                names = ", ".join(clustered_names)
                raise ValueError(
                    f"table '{tablename_}' has multiple SQL Server clustered "
                    f"indexes or unique constraints: {names}"
                )
            cls_back_references = back_references or {}
            fields = model_fields(cls)
            column_options_ = column_options or {}
            unknown_options = set(column_options_) - set(fields)
            if unknown_options:
                unknown = ", ".join(sorted(unknown_options))
                raise ValueError(
                    f"column_options for table '{tablename_}' reference unknown fields: {unknown}"
                )
            foreign_key_constraints_ = foreign_key_constraints or []
            unknown_foreign_key_columns = sorted(
                {
                    column
                    for constraint in foreign_key_constraints_
                    for column in constraint.columns
                    if column not in fields
                }
            )
            if unknown_foreign_key_columns:
                unknown = ", ".join(unknown_foreign_key_columns)
                raise ValueError(
                    f"foreign_key_constraints for table '{tablename_}' "
                    f"reference unknown fields: {unknown}"
                )
            exclusion_constraints_ = exclusion_constraints or []
            unknown_exclusion_columns = sorted(
                {
                    column
                    for constraint in exclusion_constraints_
                    for column, _operator in constraint.columns
                    if column not in fields
                }
            )
            if unknown_exclusion_columns:
                unknown = ", ".join(unknown_exclusion_columns)
                raise ValueError(
                    f"exclusion_constraints for table '{tablename_}' "
                    f"reference unknown fields: {unknown}"
                )
            table_metadata = OrmTable[ModelType](
                model=cls,
                tablename=tablename_,
                pk=pk,
                schema_name=schema_,
                comment=comment_,
                tablespace=tablespace_,
                mysql_engine=mysql_engine_,
                mysql_charset=mysql_charset_,
                mysql_collation=mysql_collation_,
                mysql_row_format=mysql_row_format_,
                mysql_key_block_size=mysql_key_block_size_,
                mysql_pack_keys=mysql_pack_keys_,
                mysql_checksum=mysql_checksum_,
                mysql_delay_key_write=mysql_delay_key_write_,
                mysql_stats_persistent=mysql_stats_persistent_,
                mysql_stats_auto_recalc=mysql_stats_auto_recalc_,
                mysql_stats_sample_pages=mysql_stats_sample_pages_,
                mysql_avg_row_length=mysql_avg_row_length_,
                mysql_max_rows=mysql_max_rows_,
                mysql_min_rows=mysql_min_rows_,
                mysql_insert_method=mysql_insert_method_,
                mysql_data_directory=mysql_data_directory_,
                mysql_index_directory=mysql_index_directory_,
                mysql_connection=mysql_connection_,
                mysql_union=mysql_union_,
                mysql_partition_by=mysql_partition_by_,
                mysql_partitions=mysql_partitions_,
                mysql_subpartition_by=mysql_subpartition_by_,
                mysql_subpartitions=mysql_subpartitions_,
                mysql_auto_increment=mysql_auto_increment_,
                oracle_compress=oracle_compress_,
                postgres_inherits=postgres_inherits_,
                postgres_with=postgres_with_,
                postgres_using=postgres_using_,
                postgres_unlogged=postgres_unlogged,
                postgres_partition_by=postgres_partition_by_,
                postgres_partition_of=postgres_partition_of_,
                postgres_partition_for=postgres_partition_for_,
                sqlite_strict=sqlite_strict,
                sqlite_without_rowid=sqlite_without_rowid,
                indexed=indexed or [],
                unique=unique or [],
                indexes=indexes_,
                column_options=column_options_,
                unique_constraints=anonymous_unique_constraints,
                named_unique_constraints=named_unique_constraints,
                check_constraints=check_constraints or [],
                foreign_key_constraints=foreign_key_constraints_,
                exclusion_constraints=exclusion_constraints_,
                columns=[field for field in fields if field not in cls_back_references],
                relationships={},
                back_references=cls_back_references,
            )
            self._table_map.model_to_data[cls] = table_metadata
            self._table_map.name_to_data[tablename_] = table_metadata
            return cls

        return _wrapper

    def namespace(self, name: str, *, comment: str | None = None) -> DatabaseNamespace:
        """Register a database namespace/schema for snapshots and migrations."""
        namespace = DatabaseNamespace(name=name, comment=comment)
        if any(existing.name == namespace.name for existing in self._namespaces):
            raise ValueError(f"duplicate namespace '{namespace.name}'")
        self._namespaces.append(namespace)
        return namespace

    def sequence(
        self,
        name: str,
        *,
        schema: str | None = None,
        data_type: str | None = None,
        start: int | None = None,
        increment: int | None = None,
        min_value: int | None = None,
        max_value: int | None = None,
        no_min_value: bool = False,
        no_max_value: bool = False,
        cycle: bool = False,
        cache: int | None = None,
        comment: str | None = None,
        order: bool = False,
    ) -> DatabaseSequence:
        """Register a database sequence for schema snapshots and migrations."""
        sequence = DatabaseSequence(
            name=name,
            schema=schema,
            data_type=data_type,
            start=start,
            increment=increment,
            min_value=min_value,
            max_value=max_value,
            no_min_value=no_min_value,
            no_max_value=no_max_value,
            cycle=cycle,
            cache=cache,
            comment=comment,
            order=order,
        )
        key = (sequence.schema_name, sequence.name)
        if any(
            (existing.schema_name, existing.name) == key for existing in self._sequences
        ):
            qualified = (
                sequence.name
                if sequence.schema_name is None
                else f"{sequence.schema_name}.{sequence.name}"
            )
            raise ValueError(f"duplicate sequence '{qualified}'")
        self._sequences.append(sequence)
        return sequence

    def view(
        self,
        name: str,
        definition: str,
        *,
        schema: str | None = None,
        materialized: bool = False,
        comment: str | None = None,
    ) -> DatabaseView:
        """Register a database view for schema snapshots and migrations."""
        view = DatabaseView(
            name=name,
            schema=schema,
            definition=definition,
            materialized=materialized,
            comment=comment,
        )
        key = (view.schema_name, view.name)
        if any(
            (existing.schema_name, existing.name) == key for existing in self._views
        ):
            qualified = (
                view.name
                if view.schema_name is None
                else f"{view.schema_name}.{view.name}"
            )
            raise ValueError(f"duplicate view '{qualified}'")
        self._views.append(view)
        return view

    async def init(self) -> None:
        """Initialize ORM models."""
        for table_data in self._table_map.name_to_data.values():
            rels = self.get(table_data)
            table_data.relationships = rels
        for table_data in self._table_map.name_to_data.values():
            for field_name in table_data.relationships:
                install_relationship_path_descriptor(table_data.model, field_name)
        self._runtime = self._build_runtime_database()
        for table_data in self._table_map.name_to_data.values():
            self._tables[table_data.model] = Table(
                table_data=table_data,
                table_map=self._table_map,
                rust_handle=self._runtime.table(table_data.model.__name__),
                events=self._events,
                runtime=self._runtime,
                connection=self._connection,
                debug=self._debug,
                log_queries=self._log_queries,
            )
        await self.create_all()

    async def create_all(self) -> None:
        """Create all registered tables."""
        try:
            if self._runtime is None:
                self._runtime = self._build_runtime_database()
            self._create_registered_namespaces()
            self._create_registered_sequences()
            self._runtime.create_all()
            self._create_registered_postgres_unique_options()
            self._create_registered_constraint_comments()
            self._create_registered_postgres_index_options()
            self._create_registered_mssql_index_options()
            self._create_registered_oracle_index_options()
            self._create_registered_mysql_index_options()
            self._create_registered_index_tablespaces()
            self._create_registered_index_comments()
            self._create_registered_enum_type_comments()
            self._create_registered_views()
        except Exception as exc:
            error = classify_native_error(
                exc,
                default=SchemaError,
                message="schema creation failed",
                context=self._context("create_all"),
            )
            raise error from exc

    async def drop_all(self) -> None:
        """Drop all registered tables."""
        try:
            if self._runtime is not None:
                self._drop_registered_views()
                self._runtime.drop_all()
                self._drop_registered_sequences()
                self._drop_registered_namespaces()
                return
            self._drop_registered_views()
            for table_data in reversed(list(self._table_map.name_to_data.values())):
                sql = _ormdantic.compile_drop_table_sql(
                    self._connection, table_data.tablename
                )
                _ormdantic.execute_native(self._connection, sql, [])
            for enum_type in reversed(self._runtime_enum_type_specs()):
                _ormdantic.execute_native(
                    self._connection,
                    _drop_runtime_enum_type_sql(enum_type),
                    [],
                )
            self._drop_registered_sequences()
            self._drop_registered_namespaces()
        except Exception as exc:
            error = classify_native_error(
                exc,
                default=SchemaError,
                message="schema drop failed",
                context=self._context("drop_all"),
            )
            raise error from exc

    def transaction(
        self,
        *,
        isolation_level: TransactionIsolationLevel | str | None = None,
        read_only: bool = False,
        deferrable: bool | None = None,
    ) -> Any:
        """Open a native transaction context."""
        return _OrmdanticTransaction(
            self,
            self._transaction_options(
                isolation_level=isolation_level,
                read_only=read_only,
                deferrable=deferrable,
            ),
        )

    def session(
        self,
        *,
        isolation_level: TransactionIsolationLevel | str | None = None,
        read_only: bool = False,
        deferrable: bool | None = None,
    ) -> Session:
        """Open an async unit-of-work session."""
        return Session(
            self,
            transaction_options=self._transaction_options(
                isolation_level=isolation_level,
                read_only=read_only,
                deferrable=deferrable,
            ),
        )

    def inspect(self) -> Inspector:
        """Return a database inspector."""
        return Inspector(self)

    def relation(
        self,
        model: Type[ModelType],
        relationship: str,
        *,
        outer_alias: str | None = None,
        target_alias: str | None = None,
    ) -> RelationExpression:
        """Return a typed helper for relationship predicates and aggregates."""
        table = self._table_map.model_to_data.get(model)
        if table is not None and not table.relationships:
            table.relationships = self.get(table)
        return relation_expression(
            self._table_map,
            model,
            relationship,
            outer_alias=outer_alias,
            target_alias=target_alias,
        )

    @property
    def migrations(self) -> MigrationManager:
        """Return the migration manager."""
        return MigrationManager(self)

    def savepoint(self, name: str) -> Any:
        """Open a savepoint context."""
        return _OrmdanticSavepoint(self, name)

    def on(self, event: str, handler: EventHandler) -> EventHandler:
        """Register an event handler."""
        return self._events.on(event, handler)

    def on_query(self, handler: EventHandler) -> EventHandler:
        """Register a handler for completed query diagnostics."""
        return self._events.on("after_execute", handler)

    def off(self, event: str, handler: EventHandler) -> None:
        """Remove a registered event handler."""
        self._events.off(event, handler)

    def clear_events(self, event: str | None = None) -> None:
        """Clear event handlers for one event or all events."""
        self._events.clear(event)

    def runtime_diagnostics(self) -> dict[str, Any]:
        """Return non-secret runtime metadata for this database instance."""
        return {
            "backend": self._backend(),
            "debug": self._debug,
            "log_queries": self._log_queries,
            "runtime_initialized": self._runtime is not None,
            "registered_tables": sorted(self._table_map.name_to_data),
            "capabilities": _ormdantic.runtime_capabilities(),
        }

    async def load(self, model: ModelType, path: LoaderPathLike) -> Any:
        """Explicitly load a relationship path for a model instance."""
        table = self._table_map.model_to_data[type(model)]
        normalized_path = ".".join(path_parts(path))
        loaded = await self[type(model)].find_one(
            getattr(model, table.pk), load=[joinedload(normalized_path)]
        )
        if loaded is None:
            return None
        value: Any = loaded
        for part in path_parts(normalized_path):
            value = getattr(value, part)
        return value

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
            if contains_list_annotation(
                field.annotation
            ) or field.annotation == ForwardRef(f"{related_table.model.__name__}"):
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

    def _runtime_table_specs(
        self,
        *,
        native_enum_types: bool = False,
        enum_schema: str | None = None,
        for_rust: bool = False,
    ) -> list[tuple[Any, ...]]:
        """Build compact table descriptors for snapshots or the Rust runtime."""
        specs = []
        for table in self._table_map.name_to_data.values():
            columns = [
                column_descriptor(
                    table_map=self._table_map,
                    table=table,
                    field_name=field_name,
                    field=field,
                    native_enum_types=native_enum_types,
                    enum_schema=enum_schema,
                )
                for field_name, field in model_fields(table.model).items()
                if field_name not in table.back_references
            ]
            relationships = []
            for field_name, relationship in table.relationships.items():
                related = self._table_map.name_to_data[relationship.foreign_table]
                relationships.append(
                    (
                        field_name,
                        relationship.foreign_table,
                        related.pk,
                        relationship.back_references,
                    )
                )
            specs.append(
                (
                    table.model.__name__,
                    table.tablename,
                    table.pk,
                    columns,
                    (
                        rust_index_descriptors(table)
                        if for_rust
                        else index_descriptors(table)
                    ),
                    table.unique_constraints,
                    (
                        rust_unique_constraint_descriptors(table)
                        if for_rust
                        else unique_constraint_descriptors(table)
                    ),
                    (
                        rust_table_check_descriptors(table)
                        if for_rust
                        else table_check_descriptors(table)
                    ),
                    (
                        rust_foreign_key_constraint_descriptors(table)
                        if for_rust
                        else foreign_key_constraint_descriptors(table)
                    ),
                    (
                        rust_exclusion_constraint_descriptors(table)
                        if for_rust
                        else exclusion_constraint_descriptors(table)
                    ),
                    (
                        table.comment,
                        table.tablespace,
                        table.mysql_engine,
                        table.mysql_charset,
                        table.mysql_collation,
                        table.mysql_row_format,
                        table.postgres_inherits,
                        table.postgres_with,
                        table.postgres_using,
                        table.postgres_partition_by,
                        table.postgres_partition_of,
                        table.postgres_partition_for,
                        table.postgres_unlogged,
                        table.sqlite_strict,
                        table.sqlite_without_rowid,
                        table.schema_name,
                        any(index.mssql_clustered for index in table.indexes),
                        oracle_index_compress_runtime(table.oracle_compress),
                        table.mysql_key_block_size,
                        table.mysql_pack_keys,
                        table.mysql_checksum,
                        table.mysql_delay_key_write,
                        table.mysql_stats_persistent,
                        table.mysql_stats_auto_recalc,
                        table.mysql_stats_sample_pages,
                        table.mysql_avg_row_length,
                        table.mysql_max_rows,
                        table.mysql_min_rows,
                        table.mysql_insert_method,
                        table.mysql_data_directory,
                        table.mysql_index_directory,
                        table.mysql_connection,
                        list(table.mysql_union),
                        table.mysql_partition_by,
                        table.mysql_partitions,
                        table.mysql_subpartition_by,
                        table.mysql_subpartitions,
                        table.mysql_auto_increment,
                    ),
                    relationships,
                )
            )
        return specs

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
        return Relationship(
            foreign_table=related_table.tablename, back_references=back_reference
        )

    def _ensure_runtime(self) -> Any:
        if self._runtime is None:
            self._runtime = self._build_runtime_database()
        return self._runtime

    def _backend(self) -> str:
        scheme = self._connection.split("://", 1)[0].split("+", 1)[0].lower()
        if scheme == "postgres":
            return "postgresql"
        return scheme

    def _context(self, operation: str, **extra: Any) -> dict[str, Any]:
        return {
            "operation": operation,
            "backend": self._backend(),
            **{key: value for key, value in extra.items() if value is not None},
        }

    def _build_runtime_database(self) -> Any:
        try:
            enum_types = self._runtime_enum_type_specs()
            native_enum_types = bool(enum_types)
            tables = self._runtime_table_specs(
                native_enum_types=native_enum_types,
                for_rust=True,
            )
            if enum_types:
                runtime_enum_types = [
                    (name, values, schema)
                    for name, values, schema, _comment in enum_types
                ]
                return _ormdantic.PyDatabase(
                    self._connection, tables, runtime_enum_types
                )
            return _ormdantic.PyDatabase(self._connection, tables)
        except Exception as exc:
            error = classify_native_error(
                exc,
                default=SchemaError,
                message="database runtime initialization failed",
                context=self._context("runtime_init"),
            )
            raise error from exc

    def _runtime_enum_type_specs(
        self,
    ) -> list[tuple[str, list[str], str | None, str | None]]:
        if not _uses_native_enum_types(self._connection):
            return []
        return enum_type_descriptors(self._table_map)

    def _runtime_namespace_specs(self) -> list[tuple[str, str | None]]:
        return [namespace.to_runtime() for namespace in self._namespaces]

    def _runtime_sequence_specs(
        self,
    ) -> list[
        tuple[
            str,
            str | None,
            int | None,
            int | None,
            int | None,
            int | None,
            bool,
            int | None,
            str | None,
            str | None,
            bool,
            bool,
            bool,
        ]
    ]:
        return [sequence.to_runtime() for sequence in self._sequences]

    def _runtime_view_specs(
        self,
    ) -> list[tuple[str, str | None, str, bool, str | None]]:
        return [view.to_runtime() for view in self._views]

    def _create_registered_namespaces(self) -> None:
        if not self._namespaces:
            return
        from ormdantic._migrations.models import NamespaceSnapshot
        from ormdantic._migrations.planning import (
            _create_namespace_sql,
            _set_namespace_comment_sql,
        )

        for namespace in self._namespaces:
            snapshot = NamespaceSnapshot.from_runtime(namespace.to_runtime())
            sql = _create_namespace_sql(
                self._connection,
                snapshot,
            )
            _ormdantic.execute_native(self._connection, sql, [])
            comment_sql = _set_namespace_comment_sql(
                self._connection,
                snapshot.name,
                snapshot.comment,
                for_create=True,
            )
            if comment_sql is not None:
                _ormdantic.execute_native(self._connection, comment_sql, [])

    def _create_registered_enum_type_comments(self) -> None:
        enum_types = self._runtime_enum_type_specs()
        if not enum_types:
            return
        from ormdantic._migrations.models import EnumTypeSnapshot
        from ormdantic._migrations.planning import _set_enum_type_comment_sql

        for enum_type in enum_types:
            snapshot = EnumTypeSnapshot.from_runtime(enum_type)
            comment_sql = _set_enum_type_comment_sql(
                self._connection,
                snapshot,
                for_create=True,
            )
            if comment_sql is not None:
                _ormdantic.execute_native(self._connection, comment_sql, [])

    def _create_registered_index_comments(self) -> None:
        if not any(table.indexes for table in self._table_map.name_to_data.values()):
            return
        from ormdantic._migrations.sql import dialect_name

        dialect = dialect_name(self._connection)
        if dialect in {"mysql", "mariadb"}:
            self._recreate_registered_inline_index_comments(dialect)
            return
        from ormdantic._migrations.models import IndexSnapshot, TableSnapshot
        from ormdantic._migrations.planning import _set_index_comment_sql

        for table in self._table_map.name_to_data.values():
            if not any(index.comment is not None for index in table.indexes):
                continue
            snapshot_table = TableSnapshot(
                model_key=table.model.__name__,
                name=table.tablename,
                primary_key=table.pk,
                schema=table.schema_name,
            )
            for index in table.indexes:
                if index.comment is None:
                    continue
                if index.postgres_ops:
                    continue
                comment_sql = _set_index_comment_sql(
                    self._connection,
                    snapshot_table,
                    IndexSnapshot(
                        index.name, list(index.columns), comment=index.comment
                    ),
                    for_create=True,
                )
                if comment_sql is not None:
                    _ormdantic.execute_native(self._connection, comment_sql, [])

    def _recreate_registered_inline_index_comments(self, dialect: str) -> None:
        from dataclasses import replace

        from ormdantic._migrations.models import SchemaSnapshot
        from ormdantic._migrations.planning import _compile_index_recreate_sql

        snapshot = SchemaSnapshot.from_database(self)
        for table in snapshot.tables:
            for index in table.indexes:
                if (
                    index.comment is None
                    or index.mysql_prefix is not None
                    or index.mysql_length
                    or index.mysql_using is not None
                    or index.mysql_visible is not None
                ):
                    continue
                plain_index = replace(index, comment=None)
                for operation in _compile_index_recreate_sql(
                    dialect,
                    replace(table, indexes=[plain_index]),
                    replace(table, indexes=[index]),
                    plain_index,
                    index,
                ):
                    _ormdantic.execute_native(
                        self._connection, str(operation["sql"]), []
                    )

    def _create_registered_mysql_index_options(self) -> None:
        if not any(table.indexes for table in self._table_map.name_to_data.values()):
            return
        from dataclasses import replace

        from ormdantic._migrations.models import SchemaSnapshot
        from ormdantic._migrations.planning import _compile_index_recreate_sql
        from ormdantic._migrations.sql import dialect_name

        dialect = dialect_name(self._connection)
        has_mysql_visibility = any(
            index.mysql_visible is not None
            for table in self._table_map.name_to_data.values()
            for index in table.indexes
        )
        if dialect != "mysql" and has_mysql_visibility:
            raise ValueError("MySQL index visibility only supports MySQL")
        if dialect not in {"mysql", "mariadb"}:
            return
        snapshot = SchemaSnapshot.from_database(self)
        for table in snapshot.tables:
            for index in table.indexes:
                if (
                    index.mysql_prefix is None
                    and not index.mysql_length
                    and index.mysql_using is None
                    and index.mysql_visible is None
                ):
                    continue
                plain_index = replace(
                    index,
                    comment=None,
                    mysql_prefix=None,
                    mysql_length={},
                    mysql_using=None,
                    mysql_visible=None,
                )
                for operation in _compile_index_recreate_sql(
                    dialect,
                    replace(table, indexes=[plain_index]),
                    replace(table, indexes=[index]),
                    plain_index,
                    index,
                ):
                    _ormdantic.execute_native(
                        self._connection, str(operation["sql"]), []
                    )

    def _create_registered_constraint_comments(self) -> None:
        if not any(
            table.named_unique_constraints
            or table.check_constraints
            or table.foreign_key_constraints
            or table.exclusion_constraints
            for table in self._table_map.name_to_data.values()
        ):
            return
        from ormdantic._migrations.models import TableSnapshot
        from ormdantic._migrations.planning import _set_constraint_comment_sql

        for table in self._table_map.name_to_data.values():
            constraints = (
                list(table.named_unique_constraints)
                + list(table.check_constraints)
                + list(table.foreign_key_constraints)
                + list(table.exclusion_constraints)
            )
            if not any(constraint.comment is not None for constraint in constraints):
                continue
            snapshot_table = TableSnapshot(
                model_key=table.model.__name__,
                name=table.tablename,
                primary_key=table.pk,
                schema=table.schema_name,
            )
            for constraint in constraints:
                if constraint.comment is None:
                    continue
                comment_sql = _set_constraint_comment_sql(
                    self._connection,
                    snapshot_table,
                    constraint.name,
                    constraint.comment,
                    for_create=True,
                )
                if comment_sql is not None:
                    _ormdantic.execute_native(self._connection, comment_sql, [])

    def _create_registered_postgres_unique_options(self) -> None:
        if not any(
            table.named_unique_constraints
            for table in self._table_map.name_to_data.values()
        ):
            return
        from dataclasses import replace

        from ormdantic._migrations.models import SchemaSnapshot
        from ormdantic._migrations.planning import (
            _compile_unique_constraint_recreate_sql,
        )
        from ormdantic._migrations.sql import dialect_name

        has_postgres_include = any(
            constraint.postgres_include
            for table in self._table_map.name_to_data.values()
            for constraint in table.named_unique_constraints
        )
        if not has_postgres_include:
            return
        if dialect_name(self._connection) != "postgresql":
            raise ValueError(
                "PostgreSQL unique constraint INCLUDE columns only support PostgreSQL"
            )
        snapshot = SchemaSnapshot.from_database(self)
        for table in snapshot.tables:
            for constraint in table.named_unique_constraints:
                if not constraint.postgres_include:
                    continue
                plain_constraint = replace(
                    constraint,
                    comment=None,
                    postgres_include=[],
                )
                for operation in _compile_unique_constraint_recreate_sql(
                    "postgresql",
                    replace(table, named_unique_constraints=[plain_constraint]),
                    replace(table, named_unique_constraints=[constraint]),
                    plain_constraint,
                    constraint,
                ):
                    _ormdantic.execute_native(
                        self._connection, str(operation["sql"]), []
                    )

    def _create_registered_index_tablespaces(self) -> None:
        if not any(table.indexes for table in self._table_map.name_to_data.values()):
            return
        from ormdantic._migrations.models import IndexSnapshot, TableSnapshot
        from ormdantic._migrations.planning import _set_index_tablespace_sql

        for table in self._table_map.name_to_data.values():
            if not any(
                index.postgres_tablespace is not None for index in table.indexes
            ):
                continue
            snapshot_table = TableSnapshot(
                model_key=table.model.__name__,
                name=table.tablename,
                primary_key=table.pk,
                schema=table.schema_name,
            )
            for index in table.indexes:
                if index.postgres_tablespace is None:
                    continue
                if index.postgres_ops or index.postgres_nulls_not_distinct:
                    continue
                tablespace_sql = _set_index_tablespace_sql(
                    self._connection,
                    snapshot_table,
                    IndexSnapshot(
                        index.name,
                        list(index.columns),
                        postgres_tablespace=index.postgres_tablespace,
                    ),
                    for_create=True,
                )
                if tablespace_sql is not None:
                    _ormdantic.execute_native(self._connection, tablespace_sql, [])

    def _create_registered_postgres_index_options(self) -> None:
        if not any(table.indexes for table in self._table_map.name_to_data.values()):
            return
        from dataclasses import replace

        from ormdantic._migrations.models import SchemaSnapshot
        from ormdantic._migrations.planning import _compile_index_recreate_sql
        from ormdantic._migrations.sql import dialect_name

        has_postgres_options = any(
            index.postgres_ops or index.postgres_nulls_not_distinct
            for table in self._table_map.name_to_data.values()
            for index in table.indexes
        )
        if not has_postgres_options:
            return
        if dialect_name(self._connection) != "postgresql":
            raise ValueError(
                "PostgreSQL index operator classes and NULLS NOT DISTINCT "
                "only support PostgreSQL"
            )
        snapshot = SchemaSnapshot.from_database(self)
        for table in snapshot.tables:
            for index in table.indexes:
                if not index.postgres_ops and not index.postgres_nulls_not_distinct:
                    continue
                plain_index = replace(
                    index,
                    comment=None,
                    postgres_tablespace=None,
                    postgres_ops={},
                    postgres_nulls_not_distinct=False,
                )
                for operation in _compile_index_recreate_sql(
                    "postgresql",
                    replace(table, indexes=[plain_index]),
                    replace(table, indexes=[index]),
                    plain_index,
                    index,
                ):
                    _ormdantic.execute_native(
                        self._connection, str(operation["sql"]), []
                    )

    def _create_registered_mssql_index_options(self) -> None:
        if not any(table.indexes for table in self._table_map.name_to_data.values()):
            return
        from dataclasses import replace

        from ormdantic._migrations.models import IndexSnapshot, TableSnapshot
        from ormdantic._migrations.planning import _compile_index_recreate_sql
        from ormdantic._migrations.sql import dialect_name

        if dialect_name(self._connection) != "mssql":
            return
        for table in self._table_map.name_to_data.values():
            if not any(
                index.mssql_filegroup is not None or index.mssql_clustered
                for index in table.indexes
            ):
                continue
            snapshot_table = TableSnapshot(
                model_key=table.model.__name__,
                name=table.tablename,
                primary_key=table.pk,
                schema=table.schema_name,
            )
            for index in table.indexes:
                if index.mssql_filegroup is None and not index.mssql_clustered:
                    continue
                placed_index = IndexSnapshot(
                    index.name,
                    list(index.columns),
                    unique=index.unique,
                    where=index.where,
                    include_columns=list(index.include_columns),
                    method=index.method,
                    expressions=list(index.expressions),
                    postgres_with=list(index.postgres_with),
                    mssql_filegroup=index.mssql_filegroup,
                    mssql_clustered=index.mssql_clustered,
                )
                plain_index = replace(
                    placed_index,
                    mssql_filegroup=None,
                    mssql_clustered=False,
                )
                for operation in _compile_index_recreate_sql(
                    self._connection,
                    replace(snapshot_table, indexes=[plain_index]),
                    replace(snapshot_table, indexes=[placed_index]),
                    plain_index,
                    placed_index,
                ):
                    _ormdantic.execute_native(
                        self._connection, str(operation["sql"]), []
                    )

    def _create_registered_oracle_index_options(self) -> None:
        if not any(table.indexes for table in self._table_map.name_to_data.values()):
            return
        from dataclasses import replace

        from ormdantic._migrations.models import IndexSnapshot, TableSnapshot
        from ormdantic._migrations.planning import _compile_index_recreate_sql
        from ormdantic._migrations.sql import dialect_name

        if dialect_name(self._connection) != "oracle":
            return
        for table in self._table_map.name_to_data.values():
            if not any(
                index.oracle_tablespace is not None
                or index.oracle_bitmap
                or index.oracle_compress is not None
                for index in table.indexes
            ):
                continue
            snapshot_table = TableSnapshot(
                model_key=table.model.__name__,
                name=table.tablename,
                primary_key=table.pk,
                schema=table.schema_name,
            )
            for index in table.indexes:
                if (
                    index.oracle_tablespace is None
                    and not index.oracle_bitmap
                    and index.oracle_compress is None
                ):
                    continue
                placed_index = IndexSnapshot(
                    index.name,
                    list(index.columns),
                    unique=index.unique,
                    where=index.where,
                    include_columns=list(index.include_columns),
                    method=index.method,
                    expressions=list(index.expressions),
                    postgres_with=list(index.postgres_with),
                    oracle_tablespace=index.oracle_tablespace,
                    oracle_bitmap=index.oracle_bitmap,
                    oracle_compress=index.oracle_compress,
                )
                plain_index = replace(
                    placed_index,
                    oracle_tablespace=None,
                    oracle_bitmap=False,
                    oracle_compress=None,
                )
                for operation in _compile_index_recreate_sql(
                    self._connection,
                    replace(snapshot_table, indexes=[plain_index]),
                    replace(snapshot_table, indexes=[placed_index]),
                    plain_index,
                    placed_index,
                ):
                    _ormdantic.execute_native(
                        self._connection, str(operation["sql"]), []
                    )

    def _create_registered_sequences(self) -> None:
        if not self._sequences:
            return
        from ormdantic._migrations.models import SequenceSnapshot
        from ormdantic._migrations.planning import (
            _create_sequence_sql,
            _set_sequence_comment_sql,
        )

        for sequence in self._sequences:
            snapshot = SequenceSnapshot.from_runtime(sequence.to_runtime())
            sql = _create_sequence_sql(
                self._connection,
                snapshot,
            )
            _ormdantic.execute_native(self._connection, sql, [])
            comment_sql = _set_sequence_comment_sql(
                self._connection,
                snapshot,
                for_create=True,
            )
            if comment_sql is not None:
                _ormdantic.execute_native(self._connection, comment_sql, [])

    def _create_registered_views(self) -> None:
        if not self._views:
            return
        from ormdantic._migrations.models import ViewSnapshot
        from ormdantic._migrations.planning import (
            _create_view_sql,
            _set_view_comment_sql,
        )

        for view in self._views:
            snapshot = ViewSnapshot.from_runtime(view.to_runtime())
            sql = _create_view_sql(
                self._connection,
                snapshot,
            )
            _ormdantic.execute_native(self._connection, sql, [])
            comment_sql = _set_view_comment_sql(
                self._connection,
                snapshot,
                for_create=True,
            )
            if comment_sql is not None:
                _ormdantic.execute_native(self._connection, comment_sql, [])

    def _drop_registered_views(self) -> None:
        if not self._views:
            return
        from ormdantic._migrations.models import ViewSnapshot
        from ormdantic._migrations.planning import _drop_view_sql

        for view in reversed(self._views):
            sql = _drop_view_sql(
                self._connection,
                ViewSnapshot.from_runtime(view.to_runtime()),
            )
            _ormdantic.execute_native(self._connection, sql, [])

    def _drop_registered_sequences(self) -> None:
        if not self._sequences:
            return
        from ormdantic._migrations.models import SequenceSnapshot
        from ormdantic._migrations.planning import _drop_sequence_sql

        for sequence in reversed(self._sequences):
            sql = _drop_sequence_sql(
                self._connection,
                SequenceSnapshot.from_runtime(sequence.to_runtime()),
            )
            _ormdantic.execute_native(self._connection, sql, [])

    def _drop_registered_namespaces(self) -> None:
        if not self._namespaces:
            return
        from ormdantic._migrations.models import NamespaceSnapshot
        from ormdantic._migrations.planning import _drop_namespace_sql

        for namespace in reversed(self._namespaces):
            sql = _drop_namespace_sql(
                self._connection,
                NamespaceSnapshot.from_runtime(namespace.to_runtime()),
            )
            _ormdantic.execute_native(self._connection, sql, [])

    def _transaction_options(
        self,
        *,
        isolation_level: TransactionIsolationLevel | str | None,
        read_only: bool,
        deferrable: bool | None,
    ) -> Any | None:
        if isolation_level is None and not read_only and deferrable is None:
            return None
        return _ormdantic.PyTransactionOptions(
            isolation_level=isolation_level,
            read_only=read_only,
            deferrable=deferrable,
        )

    async def _begin(self, options: Any | None = None) -> None:
        payload = {"database": self, **self._context("begin")}
        await self._events.dispatch("before_begin", **payload)
        started = perf_counter()
        try:
            self._ensure_runtime().begin(options)
        except Exception as exc:
            error = classify_native_error(
                exc,
                default=TransactionError,
                message="transaction begin failed",
                context=self._context("begin"),
            )
            await self._events.dispatch(
                "after_begin",
                **payload,
                duration_ms=(perf_counter() - started) * 1000,
                error=error,
            )
            raise error from exc
        await self._events.dispatch(
            "after_begin",
            **payload,
            duration_ms=(perf_counter() - started) * 1000,
            error=None,
        )

    async def _commit(self) -> None:
        payload = {"database": self, **self._context("commit")}
        await self._events.dispatch("before_commit", **payload)
        started = perf_counter()
        try:
            self._ensure_runtime().commit()
        except Exception as exc:
            error = classify_native_error(
                exc,
                default=TransactionError,
                message="transaction commit failed",
                context=self._context("commit"),
            )
            await self._events.dispatch(
                "after_commit",
                **payload,
                duration_ms=(perf_counter() - started) * 1000,
                error=error,
            )
            raise error from exc
        await self._events.dispatch(
            "after_commit",
            **payload,
            duration_ms=(perf_counter() - started) * 1000,
            error=None,
        )

    async def _rollback(self) -> None:
        payload = {"database": self, **self._context("rollback")}
        await self._events.dispatch("before_rollback", **payload)
        started = perf_counter()
        try:
            self._ensure_runtime().rollback()
        except Exception as exc:
            error = classify_native_error(
                exc,
                default=TransactionError,
                message="transaction rollback failed",
                context=self._context("rollback"),
            )
            await self._events.dispatch(
                "after_rollback",
                **payload,
                duration_ms=(perf_counter() - started) * 1000,
                error=error,
            )
            raise error from exc
        await self._events.dispatch(
            "after_rollback",
            **payload,
            duration_ms=(perf_counter() - started) * 1000,
            error=None,
        )

    async def _savepoint(self, name: str) -> None:
        payload = {"database": self, **self._context("savepoint", savepoint=name)}
        await self._events.dispatch("before_savepoint", **payload)
        started = perf_counter()
        try:
            self._ensure_runtime().savepoint(name)
        except Exception as exc:
            error = classify_native_error(
                exc,
                default=TransactionError,
                message=f"savepoint '{name}' failed",
                context=self._context("savepoint", savepoint=name),
            )
            await self._events.dispatch(
                "after_savepoint",
                **payload,
                duration_ms=(perf_counter() - started) * 1000,
                error=error,
            )
            raise error from exc
        await self._events.dispatch(
            "after_savepoint",
            **payload,
            duration_ms=(perf_counter() - started) * 1000,
            error=None,
        )

    async def _rollback_to_savepoint(self, name: str) -> None:
        payload = {
            "database": self,
            **self._context("rollback_to_savepoint", savepoint=name),
        }
        await self._events.dispatch("before_rollback_to_savepoint", **payload)
        started = perf_counter()
        try:
            self._ensure_runtime().rollback_to_savepoint(name)
        except Exception as exc:
            error = classify_native_error(
                exc,
                default=TransactionError,
                message=f"rollback to savepoint '{name}' failed",
                context=self._context("rollback_to_savepoint", savepoint=name),
            )
            await self._events.dispatch(
                "after_rollback_to_savepoint",
                **payload,
                duration_ms=(perf_counter() - started) * 1000,
                error=error,
            )
            raise error from exc
        await self._events.dispatch(
            "after_rollback_to_savepoint",
            **payload,
            duration_ms=(perf_counter() - started) * 1000,
            error=None,
        )

    async def _release_savepoint(self, name: str) -> None:
        payload = {
            "database": self,
            **self._context("release_savepoint", savepoint=name),
        }
        await self._events.dispatch("before_release_savepoint", **payload)
        started = perf_counter()
        try:
            self._ensure_runtime().release_savepoint(name)
        except Exception as exc:
            error = classify_native_error(
                exc,
                default=TransactionError,
                message=f"release savepoint '{name}' failed",
                context=self._context("release_savepoint", savepoint=name),
            )
            await self._events.dispatch(
                "after_release_savepoint",
                **payload,
                duration_ms=(perf_counter() - started) * 1000,
                error=error,
            )
            raise error from exc
        await self._events.dispatch(
            "after_release_savepoint",
            **payload,
            duration_ms=(perf_counter() - started) * 1000,
            error=None,
        )


class _OrmdanticTransaction:
    def __init__(self, database: Ormdantic, options: Any | None) -> None:
        self._database = database
        self._options = options

    async def __aenter__(self) -> Self:
        await self._database._begin(self._options)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        if exc_type is None:
            await self._database._commit()
        else:
            await self._database._rollback()


class _OrmdanticSavepoint:
    def __init__(self, database: Ormdantic, name: str) -> None:
        self._database = database
        self._name = name

    async def __aenter__(self) -> Self:
        await self._database._savepoint(self._name)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        if exc_type is not None:
            await self._database._rollback_to_savepoint(self._name)
        else:
            await self._database._release_savepoint(self._name)
