import pytest

from examples.todo_app.app import errors


def test_resource_not_found_has_stable_response() -> None:
    error = errors.ResourceNotFound("Project", "project-123")

    assert error.code == "not_found"
    assert str(error) == "Project 'project-123' was not found."
    assert errors.error_response(error) == (
        404,
        {
            "error": {
                "code": "not_found",
                "message": "Project 'project-123' was not found.",
                "context": {
                    "resource": "Project",
                    "identifier": "project-123",
                },
            }
        },
    )


def test_resource_conflict_has_stable_response() -> None:
    error = errors.ResourceConflict("Project")

    assert error.code == "conflict"
    assert str(error) == "Project already exists."
    assert errors.error_response(error) == (
        409,
        {
            "error": {
                "code": "conflict",
                "message": "Project already exists.",
                "context": {"resource": "Project"},
            }
        },
    )


def test_resource_conflict_never_echoes_custom_private_detail() -> None:
    secret = "postgresql://admin:secret@db/internal"
    error = errors.ResourceConflict("Project", secret)
    status, payload = errors.error_response(error)

    assert status == 409
    assert secret not in str(error)
    assert secret not in repr(error)
    assert secret not in repr(payload)


def test_database_unavailable_has_fixed_redacted_response() -> None:
    error = errors.DatabaseUnavailable()

    assert error.code == "database_unavailable"
    assert str(error) == "The database is temporarily unavailable."
    assert errors.error_response(error) == (
        503,
        {
            "error": {
                "code": "database_unavailable",
                "message": "The database is temporarily unavailable.",
                "context": {},
            }
        },
    )
    with pytest.raises(TypeError):
        errors.DatabaseUnavailable("postgresql://user:secret@db/internal")


def test_chained_private_cause_is_absent_from_public_error_views() -> None:
    secret = "postgresql://admin:super-secret@db/internal"
    caught_error: errors.DatabaseUnavailable | None = None

    try:
        raise errors.DatabaseUnavailable() from RuntimeError(secret)
    except errors.DatabaseUnavailable as error:
        caught_error = error
        status, payload = errors.error_response(caught_error)

    assert caught_error is not None
    assert status == 503
    assert secret not in str(caught_error)
    assert secret not in repr(caught_error)
    assert secret not in repr(payload)


def test_error_context_is_a_defensive_read_only_copy() -> None:
    source = {"field": "original"}
    error = errors.TodoApplicationError(
        code="safe_code",
        public_message="Safe message.",
        context=source,
    )

    source["field"] = "changed"

    assert error.context == {"field": "original"}
    with pytest.raises(TypeError):
        error.context["field"] = "mutated"


def test_unknown_application_error_maps_to_safe_generic_500() -> None:
    error = errors.TodoApplicationError(
        code="private_code",
        public_message="Private diagnostic data.",
        context={"database_url": "postgresql://admin:secret@db/internal"},
    )

    assert errors.error_response(error) == (
        500,
        {
            "error": {
                "code": "internal_error",
                "message": "An unexpected application error occurred.",
                "context": {},
            }
        },
    )
