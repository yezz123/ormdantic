from __future__ import annotations

from ormdantic.migrations import SchemaSnapshot
from ormdantic.playground.diagnostics import (
    Diagnostic,
    Severity,
    redact_text,
    redact_value,
)
from ormdantic.playground.state import (
    PlaygroundState,
    RefreshResult,
    RefreshStatus,
    SchemaState,
    accept_refresh,
)


def test_redact_text_hides_credentials_in_database_urls() -> None:
    source = (
        "failed postgresql://alice:top-secret@db.example:5432/app "
        "and mysql://bob:another-secret@mysql.example/app"
    )

    redacted = redact_text(source)

    assert "top-secret" not in redacted
    assert "another-secret" not in redacted
    assert "alice:<redacted>@db.example" in redacted
    assert "bob:<redacted>@mysql.example" in redacted


def test_redact_value_hides_secret_keys_recursively() -> None:
    redacted = redact_value(
        {
            "token": "abc123",
            "nested": {
                "password": "hunter2",
                "message": "oracle://system:oracle@localhost/FREEPDB1",
            },
            "safe": "visible",
        }
    )

    assert redacted == {
        "token": "<redacted>",
        "nested": {
            "password": "<redacted>",
            "message": "oracle://system:<redacted>@localhost/FREEPDB1",
        },
        "safe": "visible",
    }


def test_diagnostic_constructor_redacts_message_and_details() -> None:
    diagnostic = Diagnostic.create(
        Severity.ERROR,
        "database.connection",
        "could not reach sqlite:///tmp/app.sqlite3?token=private-token",
        details={"database_url": "postgresql://user:pass@localhost/app"},
    )

    assert "private-token" not in diagnostic.message
    assert diagnostic.details == {"database_url": "<redacted>"}


def test_newer_refresh_generation_replaces_schema_state() -> None:
    initial = PlaygroundState(environment="development", generation=2)
    schema = SchemaState(
        model_snapshot=SchemaSnapshot.empty(),
        live_snapshot=SchemaSnapshot.empty(),
    )
    result = RefreshResult(
        generation=3,
        status=RefreshStatus.HEALTHY,
        schema=schema,
    )

    updated = accept_refresh(initial, result)

    assert updated.generation == 3
    assert updated.status is RefreshStatus.HEALTHY
    assert updated.schema is schema


def test_older_refresh_generation_is_ignored() -> None:
    initial = PlaygroundState(
        environment="development",
        generation=5,
        status=RefreshStatus.HEALTHY,
    )
    result = RefreshResult(
        generation=4,
        status=RefreshStatus.ERROR,
        schema=SchemaState(),
    )

    assert accept_refresh(initial, result) is initial


def test_generation_readiness_survives_a_background_refresh() -> None:
    snapshot = SchemaSnapshot.empty()
    state = PlaygroundState(
        environment="development",
        status=RefreshStatus.RUNNING,
        schema=SchemaState(
            model_snapshot=snapshot,
            live_snapshot=snapshot,
            forward_sql=("CREATE TABLE users (id INTEGER)",),
        ),
    )

    assert state.schema.ready_for_generation is True


def test_playground_state_repr_never_contains_a_database_url() -> None:
    state = PlaygroundState(
        environment="development",
        connection_label="DATABASE_URL (postgresql://user:secret@localhost/app)",
        diagnostics=(
            Diagnostic.create(
                Severity.ERROR,
                "database",
                "postgresql://user:secret@localhost/app",
            ),
        ),
    )

    rendered = repr(state)

    assert "secret" not in rendered
    assert "<redacted>" in rendered
