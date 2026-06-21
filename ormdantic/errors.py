"""Public Ormdantic exception types and diagnostic helpers."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Type, TypeVar

REDACTED_VALUE = "<redacted>"
SENSITIVE_PARAMETER_TOKENS = frozenset(
    {
        "api_key",
        "apikey",
        "auth",
        "authorization",
        "cookie",
        "credential",
        "credentials",
        "key",
        "pass",
        "passwd",
        "password",
        "private_key",
        "secret",
        "session",
        "token",
    }
)

ErrorT = TypeVar("ErrorT", bound="OrmdanticError")


class OrmdanticError(ValueError):
    """Base class for typed Ormdantic runtime errors.

    The string message stays concise for users while structured metadata is
    available on ``context`` for logging and support tooling.
    """

    def __init__(
        self,
        msg: str,
        *,
        context: Mapping[str, Any] | None = None,
        cause: BaseException | None = None,
    ) -> None:
        self.context = dict(context or {})
        self.cause = cause
        self.native_message = str(cause) if cause is not None else None
        self.native_error_type = type(cause).__name__ if cause is not None else None
        if self.native_message is not None:
            self.context.setdefault("native_message", self.native_message)
            self.context.setdefault("native_error_type", self.native_error_type)
        super().__init__(msg)

    def with_context(self: ErrorT, **context: Any) -> ErrorT:
        """Attach additional context without replacing existing keys."""
        for key, value in context.items():
            if value is not None:
                self.context.setdefault(key, value)
        return self


class ConfigurationError(OrmdanticError):
    """Raised for mal-configured database models or schemas."""

    def __init__(self, msg: str):
        super().__init__(msg)


class QueryCompilationError(OrmdanticError):
    """Raised when Ormdantic cannot compile a query."""


class QueryExecutionError(OrmdanticError):
    """Raised when a compiled query fails during execution."""


class DatabaseConnectionError(OrmdanticError):
    """Raised when the native runtime cannot connect to the database."""


class SchemaError(OrmdanticError):
    """Raised for schema creation, validation, or DDL failures."""


class MigrationError(OrmdanticError):
    """Raised for migration planning, history, apply, or rollback failures."""


class ReflectionError(OrmdanticError):
    """Raised when database reflection fails."""


class RelationshipLoadingError(OrmdanticError):
    """Raised when eager or explicit relationship loading fails."""


class HydrationError(OrmdanticError):
    """Raised when native rows cannot be hydrated into model instances."""


class TransactionError(OrmdanticError):
    """Raised for transaction, savepoint, commit, or rollback failures."""


class UndefinedBackReferenceError(ConfigurationError):
    """Raised when a back reference is missing from a table."""

    def __init__(self, table_a: str, table_b: str, field: str) -> None:
        super().__init__(
            f'Many relation defined on "{table_a}.{field}" to table {table_b}" must be'
            f' defined with a back reference on "{table_a}".'
        )


class MismatchingBackReferenceError(ConfigurationError):
    """Raised when a back reference is typed incorrectly."""

    def __init__(
        self, table_a: str, table_b: str, field: str, back_reference: str
    ) -> None:
        super().__init__(
            f'Many relation defined on "{table_a}.{field}" to'
            f' "{table_b}.{back_reference}" must use the same model type'
            f" back-referenced."
        )


class MustUnionForeignKeyError(ConfigurationError):
    """Raised when a relation field doesn't allow for just foreign key."""

    def __init__(
        self,
        table_a: str,
        table_b: str,
        field: str,
        model_b: Type,  # type: ignore
        pk_type: Type,  # type: ignore
    ) -> None:
        super().__init__(
            f'Relation defined on "{table_a}.{field}" to "{table_b}" must be a union'
            f' type of "Model | model_pk_type" e.g. "{model_b.__name__} | {pk_type}"'
        )


class TypeConversionError(ConfigurationError):
    """Raised when a Python type fails to convert to SQL."""

    def __init__(self, type: Type) -> None:  # type: ignore
        super().__init__(f"Type {type} is not supported by Ormdantic.")


def is_sensitive_parameter(name: str) -> bool:
    """Return whether a bind or field name should have its value redacted."""
    normalized = name.lower().replace("-", "_")
    return any(token in normalized for token in SENSITIVE_PARAMETER_TOKENS)


def redact_parameter_values(
    values: Mapping[str, Any] | Sequence[Any] | None,
    *,
    bind_names: Sequence[str] | None = None,
) -> dict[str, Any] | list[Any] | None:
    """Redact sensitive values while preserving parameter shape."""
    if values is None:
        return None
    if isinstance(values, Mapping):
        return {
            str(name): REDACTED_VALUE if is_sensitive_parameter(str(name)) else value
            for name, value in values.items()
        }
    if bind_names is not None:
        return {
            str(name): REDACTED_VALUE if is_sensitive_parameter(str(name)) else value
            for name, value in zip(bind_names, values, strict=False)
        }
    return list(values)


def raise_with_context(
    error: BaseException,
    error_type: type[OrmdanticError],
    message: str,
    *,
    context: Mapping[str, Any] | None = None,
) -> OrmdanticError:
    """Convert an arbitrary exception into a typed Ormdantic error."""
    if isinstance(error, OrmdanticError):
        if context:
            error.with_context(**dict(context))
        return error
    return error_type(message, context=context, cause=error)


def classify_native_error(
    error: BaseException,
    *,
    default: type[OrmdanticError] = QueryExecutionError,
    message: str | None = None,
    context: Mapping[str, Any] | None = None,
) -> OrmdanticError:
    """Map native bridge errors to actionable Python exception classes."""
    if isinstance(error, OrmdanticError):
        if context:
            error.with_context(**dict(context))
        return error

    text = str(error).lower()
    error_type = default
    if any(
        token in text
        for token in (
            "could not connect",
            "connection",
            "connection refused",
            "no such host",
            "authentication failed",
            "password authentication",
            "network",
        )
    ):
        error_type = DatabaseConnectionError
    elif "transaction error" in text or "savepoint" in text:
        error_type = TransactionError
    elif "reflection error" in text or default is ReflectionError:
        error_type = ReflectionError
    elif "migration error" in text or default is MigrationError:
        error_type = MigrationError
    elif "schema diff error" in text or default is SchemaError:
        error_type = SchemaError
    elif "compile" in text or "unsupported filter" in text:
        error_type = QueryCompilationError

    base_message = message or _default_error_message(error_type, context)
    native_message = str(error)
    if native_message and native_message not in base_message:
        base_message = f"{base_message}: {native_message}"
    return error_type(
        base_message,
        context=context,
        cause=error,
    )


def _default_error_message(
    error_type: type[OrmdanticError],
    context: Mapping[str, Any] | None,
) -> str:
    operation = (context or {}).get("operation")
    table = (context or {}).get("table")
    backend = (context or {}).get("backend")
    target = f" for table '{table}'" if table else ""
    backend_suffix = f" on {backend}" if backend else ""
    if operation:
        return f"{operation} failed{target}{backend_suffix}"
    if error_type is DatabaseConnectionError:
        return f"database connection failed{backend_suffix}"
    if error_type is MigrationError:
        return "migration failed"
    if error_type is ReflectionError:
        return "reflection failed"
    if error_type is SchemaError:
        return "schema operation failed"
    if error_type is TransactionError:
        return "transaction operation failed"
    return f"{error_type.__name__} raised"
