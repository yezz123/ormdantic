"""Framework-independent domain errors for the Todo application."""

from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType


class TodoApplicationError(Exception):
    """A domain failure with a stable, client-safe representation."""

    def __init__(
        self,
        *,
        code: str,
        public_message: str,
        context: Mapping[str, str] | None = None,
    ) -> None:
        self.code = code
        self.public_message = public_message
        self.context: Mapping[str, str] = MappingProxyType(dict(context or {}))
        super().__init__(public_message)


class ResourceNotFound(TodoApplicationError):
    """Raised when a requested domain resource does not exist."""

    def __init__(self, resource: str, identifier: str) -> None:
        super().__init__(
            code="not_found",
            public_message=f"{resource} '{identifier}' was not found.",
            context={"resource": resource, "identifier": identifier},
        )


class ResourceConflict(TodoApplicationError):
    """Raised when a requested change conflicts with existing domain state."""

    def __init__(self, resource: str, detail: str = "already exists") -> None:
        safe_detail = (
            "already exists"
            if detail == "already exists"
            else "conflicts with an existing resource"
        )
        super().__init__(
            code="conflict",
            public_message=f"{resource} {safe_detail}.",
            context={"resource": resource},
        )


class DatabaseUnavailable(TodoApplicationError):
    """Raised when persistence is temporarily unreachable."""

    def __init__(self) -> None:
        super().__init__(
            code="database_unavailable",
            public_message="The database is temporarily unavailable.",
        )


def error_response(error: TodoApplicationError) -> tuple[int, dict[str, object]]:
    """Map a domain error to an HTTP-compatible status and safe payload."""
    if isinstance(error, ResourceNotFound):
        status = 404
    elif isinstance(error, ResourceConflict):
        status = 409
    elif isinstance(error, DatabaseUnavailable):
        status = 503
    else:
        return (
            500,
            {
                "error": {
                    "code": "internal_error",
                    "message": "An unexpected application error occurred.",
                    "context": {},
                }
            },
        )

    return (
        status,
        {
            "error": {
                "code": error.code,
                "message": error.public_message,
                "context": dict(error.context),
            }
        },
    )
