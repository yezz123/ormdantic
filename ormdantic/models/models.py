"""Internal Pydantic models for ORM metadata and result wrappers."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from typing import Any, Generic, Type

from pydantic import BaseModel, Field, model_validator

from ormdantic.types import ModelType

FOREIGN_KEY_ACTIONS = {
    "cascade",
    "restrict",
    "set_null",
    "set_default",
    "no_action",
}
FOREIGN_KEY_MATCH_TYPES = {"simple", "full"}
SQLITE_CONFLICT_ALGORITHMS = {"rollback", "abort", "fail", "ignore", "replace"}


class Result(BaseModel, Generic[ModelType]):
    """Search result object."""

    offset: int
    limit: int
    data: list[ModelType]


class Relationship(BaseModel):
    """Describes a relationship from one table to another."""

    foreign_table: str
    back_references: str | None = None


class TableIndex(BaseModel):
    """Public metadata for a database index attached to a registered table."""

    name: str
    columns: list[str] = Field(default_factory=list)
    unique: bool = False
    where: str | None = None
    include_columns: list[str] = Field(default_factory=list)
    method: str | None = None
    expressions: list[str] = Field(default_factory=list)
    postgres_with: list[tuple[str, str]] = Field(default_factory=list)
    postgres_ops: dict[str, str] = Field(default_factory=dict)
    postgres_nulls_not_distinct: bool = False
    comment: str | None = None
    postgres_tablespace: str | None = None
    mssql_filegroup: str | None = None
    mssql_clustered: bool = False
    oracle_tablespace: str | None = None
    oracle_bitmap: bool = False
    oracle_compress: int | bool | None = None
    mysql_prefix: str | None = None
    mysql_length: dict[str, int] = Field(default_factory=dict)
    mysql_using: str | None = None
    mysql_visible: bool | None = None

    @model_validator(mode="before")
    @classmethod
    def normalize_backend_options(cls, data: Any) -> Any:
        if not isinstance(data, Mapping):
            return data
        values = dict(data)
        index_name = values.get("name") or "<unknown>"
        values["postgres_with"] = normalized_postgres_storage_parameters(
            values.get("postgres_with"),
            object_name=f"index '{index_name}'",
        )
        values["postgres_ops"] = normalized_postgres_index_ops(
            values.get("postgres_ops"),
            index_name=str(index_name),
        )
        values["method"] = normalized_storage_token(
            values.get("method"),
            option_name=f"index '{index_name}' method",
        )
        values["postgres_tablespace"] = normalized_storage_identifier(
            values.get("postgres_tablespace"),
            option_name=f"PostgreSQL tablespace for index '{index_name}'",
        )
        values["mssql_filegroup"] = normalized_storage_identifier(
            values.get("mssql_filegroup"),
            option_name=f"SQL Server filegroup for index '{index_name}'",
        )
        values["oracle_tablespace"] = normalized_storage_identifier(
            values.get("oracle_tablespace"),
            option_name=f"Oracle tablespace for index '{index_name}'",
        )
        values["mysql_prefix"] = normalized_mysql_index_prefix(
            values.get("mysql_prefix"),
            index_name=str(index_name),
        )
        values["mysql_length"] = normalized_mysql_index_lengths(
            values.get("mysql_length"),
            index_name=str(index_name),
        )
        values["mysql_using"] = normalized_storage_token(
            values.get("mysql_using"),
            option_name=f"MySQL/MariaDB index USING method for index '{index_name}'",
        )
        return values

    @model_validator(mode="after")
    def validate_metadata(self) -> TableIndex:
        if not self.columns and not self.expressions:
            raise ValueError(
                "table index must reference at least one column or SQL expression"
            )
        if self.comment is not None:
            self.comment = self.comment.strip()
            if not self.comment:
                raise ValueError("index comment cannot be empty")
        if self.mysql_length:
            unknown_columns = sorted(set(self.mysql_length) - set(self.columns))
            if unknown_columns:
                unknown = ", ".join(unknown_columns)
                raise ValueError(
                    f"MySQL/MariaDB index prefix lengths for index '{self.name}' "
                    f"reference columns not present in the index: {unknown}"
                )
        if self.postgres_ops:
            unknown_items = sorted(
                set(self.postgres_ops) - set(self.columns) - set(self.expressions)
            )
            if unknown_items:
                unknown = ", ".join(unknown_items)
                raise ValueError(
                    f"PostgreSQL index operator classes for index '{self.name}' "
                    f"reference columns or expressions not present in the index: {unknown}"
                )
        if self.postgres_nulls_not_distinct and not self.unique:
            raise ValueError(
                "PostgreSQL index NULLS NOT DISTINCT requires a unique index "
                f"for index '{self.name}'"
            )
        if self.mysql_prefix is not None:
            if self.unique:
                raise ValueError(
                    "MySQL/MariaDB index prefixes cannot be combined with "
                    f"unique indexes for index '{self.name}'"
                )
            if self.expressions:
                raise ValueError(
                    "MySQL/MariaDB index prefixes cannot be combined with "
                    f"expression indexes for index '{self.name}'"
                )
            if self.mysql_using is not None:
                raise ValueError(
                    "MySQL/MariaDB index prefixes cannot be combined with "
                    f"USING methods for index '{self.name}'"
                )
        self.oracle_compress = normalized_oracle_index_compress(
            self.oracle_compress,
            index_name=self.name,
        )
        if self.oracle_bitmap and self.unique:
            raise ValueError(
                f"Oracle bitmap indexes cannot be unique for index '{self.name}'"
            )
        return self


class TableColumn(BaseModel):
    """Public database-native options for a registered model field."""

    comment: str | None = None
    server_default: str | None = None
    computed: str | None = None
    computed_persisted: bool = False
    autoincrement: bool = False
    identity: bool = False
    identity_always: bool = False
    identity_start: int | None = None
    identity_increment: int | None = None
    identity_min_value: int | None = None
    identity_max_value: int | None = None
    identity_no_min_value: bool = False
    identity_no_max_value: bool = False
    identity_cycle: bool = False
    identity_cache: int | None = None
    identity_order: bool = False
    identity_on_null: bool = False
    collation: str | None = None
    numeric_precision: int | None = None
    numeric_scale: int | None = None
    foreign_key_name: str | None = None
    on_delete: str | None = None
    on_update: str | None = None
    deferrable: bool | None = None
    initially_deferred: bool = False
    enum_type_name: str | None = None
    enum_schema: str | None = None
    enum_type_comment: str | None = None
    sqlite_on_conflict_primary_key: str | None = None
    sqlite_on_conflict_not_null: str | None = None
    sqlite_on_conflict_unique: str | None = None

    @property
    def has_identity(self) -> bool:
        return (
            self.identity
            or self.identity_always
            or self.identity_start is not None
            or self.identity_increment is not None
            or self.identity_min_value is not None
            or self.identity_max_value is not None
            or self.identity_no_min_value
            or self.identity_no_max_value
            or self.identity_cycle
            or self.identity_cache is not None
            or self.identity_order
            or self.identity_on_null
        )

    @property
    def has_foreign_key_options(self) -> bool:
        return (
            self.foreign_key_name is not None
            or self.on_delete is not None
            or self.on_update is not None
            or self.deferrable is not None
            or self.initially_deferred
        )

    @property
    def has_enum_type_options(self) -> bool:
        return (
            self.enum_type_name is not None
            or self.enum_schema is not None
            or self.enum_type_comment is not None
        )

    @model_validator(mode="after")
    def validate_numeric_shape(self) -> TableColumn:
        if self.comment is not None:
            self.comment = self.comment.strip()
            if not self.comment:
                raise ValueError("column comment cannot be empty")
        precision_set = self.numeric_precision is not None
        scale_set = self.numeric_scale is not None
        if precision_set != scale_set:
            raise ValueError(
                "numeric_precision and numeric_scale must be provided together"
            )
        if self.autoincrement and self.has_identity:
            raise ValueError("autoincrement and identity cannot be enabled together")
        if (
            self.identity_always
            or self.identity_start is not None
            or self.identity_increment is not None
            or self.identity_min_value is not None
            or self.identity_max_value is not None
            or self.identity_no_min_value
            or self.identity_no_max_value
            or self.identity_cycle
            or self.identity_cache is not None
            or self.identity_order
            or self.identity_on_null
        ):
            self.identity = True
        if self.identity_increment == 0:
            raise ValueError("identity_increment cannot be zero")
        if self.identity_on_null and self.identity_always:
            raise ValueError("identity_on_null requires BY DEFAULT identity")
        if self.identity_no_min_value and self.identity_min_value is not None:
            raise ValueError(
                "identity_no_min_value cannot be combined with identity_min_value"
            )
        if self.identity_no_max_value and self.identity_max_value is not None:
            raise ValueError(
                "identity_no_max_value cannot be combined with identity_max_value"
            )
        if self.identity_cache is not None and self.identity_cache <= 0:
            raise ValueError("identity_cache must be positive")
        if (
            self.identity_min_value is not None
            and self.identity_max_value is not None
            and self.identity_min_value > self.identity_max_value
        ):
            raise ValueError("identity_min_value cannot exceed identity_max_value")
        if self.on_delete is not None:
            self.on_delete = normalized_foreign_key_action(self.on_delete)
        if self.on_update is not None:
            self.on_update = normalized_foreign_key_action(self.on_update)
        self.deferrable = normalized_constraint_timing(
            self.deferrable,
            initially_deferred=self.initially_deferred,
        )
        self.enum_type_name = normalized_enum_identifier(
            self.enum_type_name,
            option_name="enum_type_name",
        )
        self.enum_schema = normalized_enum_identifier(
            self.enum_schema,
            option_name="enum_schema",
        )
        if self.enum_type_comment is not None:
            self.enum_type_comment = self.enum_type_comment.strip()
            if not self.enum_type_comment:
                raise ValueError("enum_type_comment cannot be empty")
        self.sqlite_on_conflict_primary_key = normalized_sqlite_conflict(
            self.sqlite_on_conflict_primary_key,
            option_name="sqlite_on_conflict_primary_key",
        )
        self.sqlite_on_conflict_not_null = normalized_sqlite_conflict(
            self.sqlite_on_conflict_not_null,
            option_name="sqlite_on_conflict_not_null",
        )
        self.sqlite_on_conflict_unique = normalized_sqlite_conflict(
            self.sqlite_on_conflict_unique,
            option_name="sqlite_on_conflict_unique",
        )
        return self


def normalized_constraint_timing(
    deferrable: bool | None,
    *,
    initially_deferred: bool,
) -> bool | None:
    """Return validated constraint deferrability metadata."""
    if initially_deferred:
        if deferrable is False:
            raise ValueError("initially_deferred requires a deferrable constraint")
        return True
    return deferrable


def normalized_enum_identifier(value: str | None, *, option_name: str) -> str | None:
    """Return a validated custom enum identifier."""
    if value is None:
        return None
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{option_name} cannot be empty")
    if "." in normalized:
        raise ValueError(f"{option_name} cannot contain '.'")
    return normalized


def normalized_storage_identifier(value: str | None, *, option_name: str) -> str | None:
    """Return a validated table storage identifier."""
    return normalized_enum_identifier(value, option_name=option_name)


def normalized_storage_token(value: str | None, *, option_name: str) -> str | None:
    """Return a validated backend storage token rendered as SQL metadata."""
    normalized = normalized_storage_identifier(value, option_name=option_name)
    if normalized is None:
        return None
    if re.fullmatch(r"[A-Za-z0-9_$]+", normalized) is None:
        raise ValueError(
            f"{option_name} must contain only letters, numbers, underscores, or '$'"
        )
    return normalized


def normalized_storage_string(value: str | None, *, option_name: str) -> str | None:
    """Return a validated backend storage string rendered as a SQL literal."""
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"{option_name} must be a string")
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{option_name} cannot be empty")
    if "\x00" in normalized:
        raise ValueError(f"{option_name} cannot contain NUL bytes")
    return normalized


def normalized_storage_path(value: str | None, *, option_name: str) -> str | None:
    """Return a validated backend storage path rendered as a SQL string literal."""
    return normalized_storage_string(value, option_name=option_name)


def normalized_sequence_data_type(
    value: str | None, *, sequence_name: str
) -> str | None:
    """Return a safe sequence data type fragment for CREATE SEQUENCE AS clauses."""
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(
            f"sequence data_type for sequence '{sequence_name}' must be a string"
        )
    normalized = " ".join(value.strip().lower().split())
    if not normalized:
        raise ValueError(
            f"sequence data_type for sequence '{sequence_name}' cannot be empty"
        )
    type_pattern = (
        r"[a-z_][a-z0-9_$]*(?:\.[a-z_][a-z0-9_$]*)?"
        r"(?:\([0-9]{1,3}(?:,[ ]*0)?\))?"
        r"(?: (?:signed|unsigned))?"
    )
    if re.fullmatch(type_pattern, normalized) is None:
        raise ValueError(
            f"sequence data_type for sequence '{sequence_name}' must be a safe SQL type"
        )
    return normalized


def normalized_storage_identifier_list(
    values: Sequence[str] | None,
    *,
    option_name: str,
) -> list[str]:
    """Return validated backend storage identifiers rendered as SQL names."""
    if values is None:
        return []
    if isinstance(values, str) or not isinstance(values, Sequence):
        raise ValueError(f"{option_name} must be a list of strings")
    identifiers: list[str] = []
    seen: set[str] = set()
    for index, value in enumerate(values):
        if not isinstance(value, str):
            raise ValueError(f"{option_name} item {index} must be a string")
        identifier = normalized_storage_identifier(
            value,
            option_name=f"{option_name} item {index}",
        )
        if identifier in seen:
            raise ValueError(f"{option_name} cannot contain duplicate table names")
        seen.add(identifier or "")
        identifiers.append(identifier or "")
    return identifiers


def normalized_postgres_index_ops(
    values: Mapping[str, Any] | None,
    *,
    index_name: str,
) -> dict[str, str]:
    """Return validated PostgreSQL index operator-class metadata."""
    if values is None:
        return {}
    if not isinstance(values, Mapping):
        raise ValueError(
            f"PostgreSQL index operator classes for index '{index_name}' "
            "must be a mapping of column or expression to operator class"
        )
    ops: dict[str, str] = {}
    for item, opclass in values.items():
        item_name = str(item).strip()
        if not item_name:
            raise ValueError(
                f"PostgreSQL index operator class item for index '{index_name}' "
                "cannot be empty"
            )
        ops[item_name] = normalized_postgres_opclass(
            opclass,
            option_name=(
                f"PostgreSQL index operator class for index '{index_name}' "
                f"item '{item_name}'"
            ),
        )
    return ops


def normalized_postgres_exclusion_ops(
    values: Mapping[str, Any] | None,
    *,
    constraint_name: str,
) -> dict[str, str]:
    """Return validated PostgreSQL EXCLUDE operator-class metadata."""
    if values is None:
        return {}
    if not isinstance(values, Mapping):
        raise ValueError(
            f"PostgreSQL exclusion operator classes for constraint "
            f"'{constraint_name}' must be a mapping of column or expression "
            "to operator class"
        )
    ops: dict[str, str] = {}
    for item, opclass in values.items():
        item_name = str(item).strip()
        if not item_name:
            raise ValueError(
                f"PostgreSQL exclusion operator class item for constraint "
                f"'{constraint_name}' cannot be empty"
            )
        ops[item_name] = normalized_postgres_opclass(
            opclass,
            option_name=(
                f"PostgreSQL exclusion operator class for constraint "
                f"'{constraint_name}' item '{item_name}'"
            ),
        )
    return ops


def normalized_postgres_opclass(value: Any, *, option_name: str) -> str:
    """Return a validated PostgreSQL operator class identifier."""
    if not isinstance(value, str):
        raise ValueError(f"{option_name} must be a PostgreSQL operator class name")
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{option_name} cannot be empty")
    parts = normalized.split(".")
    if len(parts) > 2:
        raise ValueError(f"{option_name} can contain at most one '.'")
    normalized_parts = [
        normalized_storage_token(part, option_name=option_name) for part in parts
    ]
    return ".".join(part for part in normalized_parts if part is not None)


def normalized_mysql_index_prefix(value: str | None, *, index_name: str) -> str | None:
    """Return validated MySQL/MariaDB index class metadata."""
    normalized = normalized_storage_token(
        value,
        option_name=f"MySQL/MariaDB index prefix for index '{index_name}'",
    )
    if normalized is None:
        return None
    prefix = normalized.upper()
    if prefix not in {"FULLTEXT", "SPATIAL"}:
        raise ValueError(
            f"MySQL/MariaDB index prefix for index '{index_name}' must be "
            "FULLTEXT or SPATIAL"
        )
    return prefix


def normalized_mysql_index_lengths(
    values: Mapping[str, int] | None,
    *,
    index_name: str,
) -> dict[str, int]:
    """Return validated MySQL/MariaDB index prefix-length metadata."""
    if values is None:
        return {}
    if not isinstance(values, Mapping):
        raise ValueError(
            f"MySQL/MariaDB index prefix lengths for index '{index_name}' "
            "must be a mapping of column name to positive integer length"
        )
    lengths: dict[str, int] = {}
    for column, length in values.items():
        column_name = str(column).strip()
        if not column_name:
            raise ValueError(
                f"MySQL/MariaDB index prefix length column for index '{index_name}' "
                "cannot be empty"
            )
        if isinstance(length, bool) or not isinstance(length, int):
            raise ValueError(
                f"MySQL/MariaDB index prefix length for index '{index_name}' "
                f"column '{column_name}' must be a positive integer"
            )
        if length <= 0:
            raise ValueError(
                f"MySQL/MariaDB index prefix length for index '{index_name}' "
                f"column '{column_name}' must be positive"
            )
        lengths[column_name] = length
    return lengths


def normalized_positive_int(
    value: int | None,
    *,
    option_name: str,
) -> int | None:
    """Return validated positive integer metadata."""
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{option_name} must be a positive integer")
    if value <= 0:
        raise ValueError(f"{option_name} must be positive")
    return value


def normalized_bool(value: bool | None, *, option_name: str) -> bool | None:
    """Return validated optional boolean metadata."""
    if value is None:
        return None
    if not isinstance(value, bool):
        raise ValueError(f"{option_name} must be true or false")
    return value


def normalized_oracle_index_compress(
    value: int | bool | None,
    *,
    index_name: str | None = None,
    object_name: str | None = None,
) -> int | bool | None:
    """Return validated Oracle index compression metadata."""
    label = object_name or f"index '{index_name}'"
    if value is None or value is False:
        return None
    if value is True:
        return True
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(
            f"Oracle index compression for {label} must be true "
            "or a positive integer prefix length"
        )
    if value <= 0:
        raise ValueError(f"Oracle index compression for {label} must be positive")
    return value


def normalized_oracle_table_compress(
    value: int | bool | None,
    *,
    table_name: str,
) -> int | bool | None:
    """Return validated Oracle table compression metadata."""
    if value is None or value is False:
        return None
    if value is True:
        return True
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(
            "Oracle table compression for table "
            f"'{table_name}' must be true or a positive integer level"
        )
    if value <= 0:
        raise ValueError(
            f"Oracle table compression for table '{table_name}' must be positive"
        )
    return value


def normalized_postgres_storage_parameters(
    values: (
        Mapping[str, str | int | bool] | Sequence[tuple[str, str | int | bool]] | None
    ),
    *,
    table_name: str | None = None,
    object_name: str | None = None,
) -> list[tuple[str, str]]:
    """Return validated PostgreSQL storage parameters."""
    if not values:
        return []
    storage_target = object_name or f"table '{table_name}'"
    parameters: list[tuple[str, str]] = []
    items = values.items() if isinstance(values, Mapping) else values
    for raw_name, raw_value in sorted(items, key=lambda item: str(item[0])):
        name = raw_name.strip()
        option_name = f"PostgreSQL storage parameter for {storage_target}"
        if not name:
            raise ValueError(f"{option_name} cannot be empty")
        if (
            re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?", name)
            is None
        ):
            raise ValueError(
                f"{option_name} must be an identifier or dotted identifier"
            )
        if isinstance(raw_value, bool):
            value = "true" if raw_value else "false"
        else:
            value = str(raw_value).strip()
        if not value:
            raise ValueError(f"{option_name} value for '{name}' cannot be empty")
        if re.fullmatch(r"[A-Za-z0-9_.$+-]+", value) is None:
            raise ValueError(
                f"{option_name} value for '{name}' must contain only "
                "letters, numbers, underscores, '.', '$', '+', or '-'"
            )
        parameters.append((name, value))
    return parameters


def normalized_postgres_partition_by(
    value: str | None,
    *,
    table_name: str,
) -> str | None:
    """Return a validated PostgreSQL PARTITION BY clause body."""
    if value is None:
        return None
    normalized = " ".join(value.strip().split())
    option_name = f"PostgreSQL partition key for table '{table_name}'"
    if not normalized:
        raise ValueError(f"{option_name} cannot be empty")
    if any(token in normalized for token in (";", "--", "/*", "*/")):
        raise ValueError(f"{option_name} cannot contain SQL statement separators")
    match = re.fullmatch(r"(?i)(range|list|hash)\s*(\(.+\))", normalized)
    if match is None:
        raise ValueError(
            f"{option_name} must use RANGE (...), LIST (...), or HASH (...)"
        )
    body = match.group(2)
    if not _balanced_parentheses(body):
        raise ValueError(f"{option_name} must contain balanced parentheses")
    return f"{match.group(1).upper()} {body}"


def normalized_mysql_partition_by(
    value: str | None,
    *,
    table_name: str,
    option: str = "PARTITION BY",
) -> str | None:
    """Return a validated MySQL/MariaDB partition clause body."""
    if value is None:
        return None
    normalized = " ".join(value.strip().split())
    option_name = f"MySQL/MariaDB {option} for table '{table_name}'"
    if not normalized:
        raise ValueError(f"{option_name} cannot be empty")
    if any(token in normalized for token in (";", "--", "/*", "*/")):
        raise ValueError(f"{option_name} cannot contain SQL statement separators")
    match = re.fullmatch(
        r"(?i)(range|list|hash|key|linear\s+hash|linear\s+key)\s*(\(.+\))", normalized
    )
    if match is None:
        raise ValueError(
            f"{option_name} must use RANGE (...), LIST (...), HASH (...), "
            "KEY (...), LINEAR HASH (...), or LINEAR KEY (...)"
        )
    body = match.group(2)
    if not _balanced_parentheses(body):
        raise ValueError(f"{option_name} must contain balanced parentheses")
    return f"{match.group(1).upper()} {body}"


def normalized_postgres_partition_for(
    value: str | None,
    *,
    table_name: str,
) -> str | None:
    """Return a validated PostgreSQL partition bound clause."""
    if value is None:
        return None
    normalized = " ".join(value.strip().split())
    option_name = f"PostgreSQL partition bound for table '{table_name}'"
    if not normalized:
        raise ValueError(f"{option_name} cannot be empty")
    if any(token in normalized for token in (";", "--", "/*", "*/")):
        raise ValueError(f"{option_name} cannot contain SQL statement separators")
    if normalized.upper() == "DEFAULT":
        return "DEFAULT"
    bound = re.sub(r"(?i)^for\s+values\s+", "", normalized, count=1)
    bound_match = re.fullmatch(r"(?i)(in|with)\s*(\(.+\))", bound)
    if bound_match is not None:
        bound = f"{bound_match.group(1).upper()} {bound_match.group(2)}"
    else:
        bound_match = re.fullmatch(r"(?i)from\s*(\(.+\))\s+to\s*(\(.+\))", bound)
        if bound_match is not None:
            bound = f"FROM {bound_match.group(1)} TO {bound_match.group(2)}"
    if bound_match is None:
        raise ValueError(
            f"{option_name} must use IN (...), FROM (...) TO (...), "
            "WITH (...), or DEFAULT"
        )
    if not _balanced_parentheses(bound):
        raise ValueError(f"{option_name} must contain balanced parentheses")
    return f"FOR VALUES {bound}"


def _balanced_parentheses(value: str) -> bool:
    depth = 0
    in_single_quote = False
    in_double_quote = False
    index = 0
    while index < len(value):
        char = value[index]
        if char == "'" and not in_double_quote:
            if in_single_quote and index + 1 < len(value) and value[index + 1] == "'":
                index += 2
                continue
            in_single_quote = not in_single_quote
        elif char == '"' and not in_single_quote:
            in_double_quote = not in_double_quote
        elif not in_single_quote and not in_double_quote:
            if char == "(":
                depth += 1
            elif char == ")":
                depth -= 1
                if depth < 0:
                    return False
        index += 1
    return depth == 0 and not in_single_quote and not in_double_quote


def normalized_foreign_key_action(action: str) -> str:
    """Return a normalized foreign-key action name."""
    normalized = action.lower().replace(" ", "_")
    if normalized not in FOREIGN_KEY_ACTIONS:
        allowed = ", ".join(sorted(FOREIGN_KEY_ACTIONS))
        raise ValueError(f"foreign key action must be one of: {allowed}")
    return normalized


def normalized_foreign_key_match(match: str) -> str:
    """Return a normalized foreign-key match type."""
    normalized = match.lower().replace(" ", "_")
    if normalized not in FOREIGN_KEY_MATCH_TYPES:
        allowed = ", ".join(sorted(FOREIGN_KEY_MATCH_TYPES))
        raise ValueError(f"foreign key match type must be one of: {allowed}")
    return normalized


def normalized_sqlite_conflict(value: str | None, *, option_name: str) -> str | None:
    """Return a normalized SQLite constraint conflict algorithm."""
    if value is None:
        return None
    normalized = value.strip().lower().replace(" ", "_")
    if normalized not in SQLITE_CONFLICT_ALGORITHMS:
        allowed = ", ".join(sorted(SQLITE_CONFLICT_ALGORITHMS))
        raise ValueError(f"{option_name} must be one of: {allowed}")
    return normalized.upper()


class TableCheck(BaseModel):
    """Public metadata for a named table-level CHECK constraint."""

    name: str
    expression: str
    validated: bool = True
    no_inherit: bool = False
    comment: str | None = None

    @model_validator(mode="after")
    def validate_comment(self) -> TableCheck:
        if self.comment is not None:
            self.comment = self.comment.strip()
            if not self.comment:
                raise ValueError("table check constraint comment cannot be empty")
        return self


class TableUnique(BaseModel):
    """Public metadata for a named table-level UNIQUE constraint."""

    name: str
    columns: list[str]
    postgres_include: list[str] = Field(default_factory=list)
    deferrable: bool | None = None
    initially_deferred: bool = False
    nulls_not_distinct: bool = False
    sqlite_on_conflict: str | None = None
    mssql_filegroup: str | None = None
    mssql_clustered: bool | None = None
    oracle_tablespace: str | None = None
    oracle_compress: int | bool | None = None
    comment: str | None = None

    @model_validator(mode="after")
    def validate_columns(self) -> TableUnique:
        if not self.columns:
            raise ValueError(
                "table unique constraint must reference at least one column"
            )
        self.postgres_include = normalized_non_empty_string_list(
            self.postgres_include,
            option_name=f"PostgreSQL INCLUDE columns for unique constraint '{self.name}'",
        )
        self.deferrable = normalized_constraint_timing(
            self.deferrable,
            initially_deferred=self.initially_deferred,
        )
        self.sqlite_on_conflict = normalized_sqlite_conflict(
            self.sqlite_on_conflict,
            option_name="sqlite_on_conflict",
        )
        self.mssql_filegroup = normalized_storage_identifier(
            self.mssql_filegroup,
            option_name=f"SQL Server filegroup for unique constraint '{self.name}'",
        )
        self.oracle_tablespace = normalized_storage_identifier(
            self.oracle_tablespace,
            option_name=f"Oracle tablespace for unique constraint '{self.name}'",
        )
        self.oracle_compress = normalized_oracle_index_compress(
            self.oracle_compress,
            object_name=f"unique constraint '{self.name}'",
        )
        if self.comment is not None:
            self.comment = self.comment.strip()
            if not self.comment:
                raise ValueError("table unique constraint comment cannot be empty")
        return self


def normalized_non_empty_string_list(
    values: Sequence[str],
    *,
    option_name: str,
) -> list[str]:
    normalized: list[str] = []
    for value in values:
        item = str(value).strip()
        if not item:
            raise ValueError(f"{option_name} cannot contain empty values")
        normalized.append(item)
    return normalized


class TableForeignKey(BaseModel):
    """Public metadata for a named table-level FOREIGN KEY constraint."""

    name: str
    columns: list[str]
    foreign_table: str
    foreign_columns: list[str]
    on_delete: str | None = None
    on_update: str | None = None
    deferrable: bool | None = None
    initially_deferred: bool = False
    validated: bool = True
    match: str | None = None
    comment: str | None = None

    @model_validator(mode="after")
    def validate_shape(self) -> TableForeignKey:
        if not self.columns:
            raise ValueError(
                "table foreign key constraint must reference at least one column"
            )
        if len(self.columns) != len(self.foreign_columns):
            raise ValueError(
                "table foreign key columns and foreign_columns must have the same length"
            )
        if self.on_delete is not None:
            self.on_delete = normalized_foreign_key_action(self.on_delete)
        if self.on_update is not None:
            self.on_update = normalized_foreign_key_action(self.on_update)
        if self.match is not None:
            self.match = normalized_foreign_key_match(self.match)
        self.deferrable = normalized_constraint_timing(
            self.deferrable,
            initially_deferred=self.initially_deferred,
        )
        if self.comment is not None:
            self.comment = self.comment.strip()
            if not self.comment:
                raise ValueError("table foreign key constraint comment cannot be empty")
        return self


class TableExclusion(BaseModel):
    """Public metadata for a PostgreSQL EXCLUDE constraint."""

    name: str
    columns: list[tuple[str, str]] = Field(default_factory=list)
    expressions: list[tuple[str, str]] = Field(default_factory=list)
    ops: dict[str, str] = Field(default_factory=dict)
    using: str = "gist"
    where: str | None = None
    deferrable: bool | None = None
    initially_deferred: bool = False
    comment: str | None = None

    @model_validator(mode="before")
    @classmethod
    def normalize_backend_options(cls, data: Any) -> Any:
        if not isinstance(data, Mapping):
            return data
        values = dict(data)
        constraint_name = values.get("name") or "<unknown>"
        values["ops"] = normalized_postgres_exclusion_ops(
            values.get("ops"),
            constraint_name=str(constraint_name),
        )
        return values

    @model_validator(mode="after")
    def validate_shape(self) -> TableExclusion:
        if not self.columns and not self.expressions:
            raise ValueError(
                "table exclusion constraint must reference at least one column or SQL expression"
            )
        self.columns = [
            self._validated_element(column, operator, "column")
            for column, operator in self.columns
        ]
        self.expressions = [
            self._validated_element(expression, operator, "expression")
            for expression, operator in self.expressions
        ]
        if self.ops:
            element_names = {value for value, _operator in self.columns} | {
                value for value, _operator in self.expressions
            }
            unknown_items = sorted(set(self.ops) - element_names)
            if unknown_items:
                unknown = ", ".join(unknown_items)
                raise ValueError(
                    f"PostgreSQL exclusion operator classes for constraint "
                    f"'{self.name}' reference columns or expressions not present "
                    f"in the constraint: {unknown}"
                )
        self.using = self.using.strip()
        if not self.using:
            raise ValueError("table exclusion constraint using cannot be empty")
        if self.where is not None:
            self.where = self.where.strip()
            if not self.where:
                raise ValueError("table exclusion constraint where cannot be empty")
        self.deferrable = normalized_constraint_timing(
            self.deferrable,
            initially_deferred=self.initially_deferred,
        )
        if self.comment is not None:
            self.comment = self.comment.strip()
            if not self.comment:
                raise ValueError("table exclusion constraint comment cannot be empty")
        return self

    @staticmethod
    def _validated_element(
        value: str,
        operator: str,
        element_kind: str,
    ) -> tuple[str, str]:
        value = value.strip()
        operator = operator.strip()
        if not value:
            raise ValueError(
                f"table exclusion constraint {element_kind} cannot be empty"
            )
        if not operator:
            raise ValueError(
                f"table exclusion constraint {element_kind} operator cannot be empty"
            )
        return value, operator


class DatabaseNamespace(BaseModel):
    """Public metadata for a database namespace/schema managed by migrations."""

    name: str
    comment: str | None = None

    @model_validator(mode="after")
    def validate_options(self) -> DatabaseNamespace:
        self.name = (
            normalized_enum_identifier(self.name, option_name="namespace name") or ""
        )
        if self.comment is not None:
            self.comment = self.comment.strip()
            if not self.comment:
                raise ValueError("namespace comment cannot be empty")
        return self

    def to_runtime(self) -> tuple[str, str | None]:
        """Return a compact runtime descriptor for migration snapshots."""
        return (self.name, self.comment)


class DatabaseSequence(BaseModel):
    """Public metadata for a database sequence managed by migrations."""

    name: str
    schema_name: str | None = Field(default=None, alias="schema")
    data_type: str | None = None
    start: int | None = None
    increment: int | None = None
    min_value: int | None = None
    max_value: int | None = None
    no_min_value: bool = False
    no_max_value: bool = False
    cycle: bool = False
    cache: int | None = None
    comment: str | None = None
    order: bool = False

    @model_validator(mode="after")
    def validate_options(self) -> DatabaseSequence:
        self.name = (
            normalized_enum_identifier(self.name, option_name="sequence name") or ""
        )
        self.schema_name = normalized_enum_identifier(
            self.schema_name,
            option_name="sequence schema",
        )
        self.data_type = normalized_sequence_data_type(
            self.data_type,
            sequence_name=self.name,
        )
        if self.increment == 0:
            raise ValueError("sequence increment cannot be zero")
        if self.no_min_value and self.min_value is not None:
            raise ValueError("no_min_value cannot be combined with min_value")
        if self.no_max_value and self.max_value is not None:
            raise ValueError("no_max_value cannot be combined with max_value")
        if self.cache is not None and self.cache <= 0:
            raise ValueError("sequence cache must be positive")
        if (
            self.min_value is not None
            and self.max_value is not None
            and self.min_value > self.max_value
        ):
            raise ValueError("sequence min_value cannot exceed max_value")
        if self.comment is not None:
            self.comment = self.comment.strip()
            if not self.comment:
                raise ValueError("sequence comment cannot be empty")
        return self

    def to_runtime(
        self,
    ) -> tuple[
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
    ]:
        """Return a compact runtime descriptor for migration snapshots."""
        return (
            self.name,
            self.schema_name,
            self.start,
            self.increment,
            self.min_value,
            self.max_value,
            self.cycle,
            self.cache,
            self.comment,
            self.data_type,
            self.order,
            self.no_min_value,
            self.no_max_value,
        )


class DatabaseView(BaseModel):
    """Public metadata for a database view managed by migrations."""

    name: str
    definition: str
    schema_name: str | None = Field(default=None, alias="schema")
    materialized: bool = False
    comment: str | None = None

    @model_validator(mode="after")
    def validate_options(self) -> DatabaseView:
        self.name = normalized_enum_identifier(self.name, option_name="view name") or ""
        self.schema_name = normalized_enum_identifier(
            self.schema_name,
            option_name="view schema",
        )
        definition = self.definition.strip()
        if definition.endswith(";"):
            definition = definition[:-1].strip()
        if not definition:
            raise ValueError("view definition cannot be empty")
        self.definition = definition
        if self.comment is not None:
            self.comment = self.comment.strip()
            if not self.comment:
                raise ValueError("view comment cannot be empty")
        return self

    def to_runtime(self) -> tuple[str, str | None, str, bool, str | None]:
        """Return a compact runtime descriptor for migration snapshots."""
        return (
            self.name,
            self.schema_name,
            self.definition,
            self.materialized,
            self.comment,
        )


class OrmTable(BaseModel, Generic[ModelType]):
    """
    Class to store table information,
    including relationships,
    back references for many-to-many relationships.
    """

    model: Type[ModelType]
    tablename: str
    pk: str
    schema_name: str | None = None
    comment: str | None = None
    tablespace: str | None = None
    mysql_engine: str | None = None
    mysql_charset: str | None = None
    mysql_collation: str | None = None
    mysql_row_format: str | None = None
    mysql_key_block_size: int | None = None
    mysql_pack_keys: bool | None = None
    mysql_checksum: bool | None = None
    mysql_delay_key_write: bool | None = None
    mysql_stats_persistent: bool | None = None
    mysql_stats_auto_recalc: bool | None = None
    mysql_stats_sample_pages: int | None = None
    mysql_avg_row_length: int | None = None
    mysql_max_rows: int | None = None
    mysql_min_rows: int | None = None
    mysql_insert_method: str | None = None
    mysql_data_directory: str | None = None
    mysql_index_directory: str | None = None
    mysql_connection: str | None = None
    mysql_union: list[str] = Field(default_factory=list)
    mysql_partition_by: str | None = None
    mysql_partitions: int | None = None
    mysql_subpartition_by: str | None = None
    mysql_subpartitions: int | None = None
    mysql_auto_increment: int | None = None
    oracle_compress: int | bool | None = None
    postgres_inherits: list[str] = Field(default_factory=list)
    postgres_with: list[tuple[str, str]] = Field(default_factory=list)
    postgres_using: str | None = None
    postgres_unlogged: bool = False
    postgres_partition_by: str | None = None
    postgres_partition_of: str | None = None
    postgres_partition_for: str | None = None
    sqlite_strict: bool = False
    sqlite_without_rowid: bool = False
    indexed: list[str]
    unique: list[str]
    indexes: list[TableIndex] = Field(default_factory=list)
    column_options: dict[str, TableColumn] = Field(default_factory=dict)
    unique_constraints: list[list[str]]
    named_unique_constraints: list[TableUnique] = Field(default_factory=list)
    check_constraints: list[TableCheck] = Field(default_factory=list)
    foreign_key_constraints: list[TableForeignKey] = Field(default_factory=list)
    exclusion_constraints: list[TableExclusion] = Field(default_factory=list)
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
