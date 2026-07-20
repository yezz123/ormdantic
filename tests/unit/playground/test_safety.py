from __future__ import annotations

import pytest

from ormdantic.migrations import MigrationOperation, MigrationPlan
from ormdantic.playground.config import EnvironmentConfig
from ormdantic.playground.safety import (
    ActionRequest,
    PreflightContext,
    Risk,
    classify_plan,
    evaluate_action,
)


def environment(
    name: str,
    *,
    safety: str = "confirm",
    production: bool = False,
) -> EnvironmentConfig:
    return EnvironmentConfig(
        name=name,
        safety=safety,  # type: ignore[arg-type]
        production=production,
    )


def request(
    *,
    risk: Risk = Risk.WRITE,
    action: str = "apply",
    environment_name: str = "development",
    destructive_count: int = 0,
) -> ActionRequest:
    return ActionRequest(
        action=action,
        environment=environment_name,
        database_name="app_db",
        target="002_add_users",
        risk=risk,
        sql=("ALTER TABLE users ADD COLUMN name TEXT",),
        destructive_sql=tuple("DROP TABLE users" for _ in range(destructive_count)),
        artifact_checksum="checksum-1",
        reviewed_generation=4,
    )


def preflight(**changes: object) -> PreflightContext:
    values: dict[str, object] = {
        "connected": True,
        "target_imported": True,
        "dialect": "postgresql",
        "artifact_dialect": "postgresql",
        "history_readable": True,
        "history_dirty": False,
        "artifact_valid": True,
        "checksum_valid": True,
        "dependencies_valid": True,
        "revision_state_valid": True,
        "rollback_available": True,
        "snapshot_current": True,
        "operations_supported": True,
        "operation_running": False,
        "editor_valid": True,
        "editor_dirty": False,
        "sql_present": True,
        "destructive_reviewed": True,
        "artifact_checksum": "checksum-1",
        "generation": 4,
    }
    values.update(changes)
    return PreflightContext(**values)  # type: ignore[arg-type]


def test_read_only_action_needs_no_confirmation() -> None:
    decision = evaluate_action(
        request(risk=Risk.READ_ONLY, action="preview"),
        environment("development"),
        preflight(),
    )

    assert decision.allowed is True
    assert decision.phrase is None
    assert decision.reasons == ()


def test_safe_development_apply_uses_normal_confirmation() -> None:
    action = request()
    profile = environment("development")

    pending = evaluate_action(action, profile, preflight())
    confirmed = evaluate_action(action, profile, preflight(), confirmed=True)

    assert pending.allowed is False
    assert pending.phrase is None
    assert "Confirm apply" in pending.reasons[0]
    assert confirmed.allowed is True


def test_typed_environment_requires_database_and_revision_exactly() -> None:
    action = request(environment_name="staging")
    profile = environment("staging", safety="typed")

    pending = evaluate_action(action, profile, preflight(), confirmed=True)
    wrong = evaluate_action(
        action,
        profile,
        preflight(),
        confirmed=True,
        confirmation="app_db 002_add_users ",
    )
    accepted = evaluate_action(
        action,
        profile,
        preflight(),
        confirmed=True,
        confirmation="app_db 002_add_users",
    )

    assert pending.phrase == "app_db 002_add_users"
    assert pending.allowed is False
    assert wrong.allowed is False
    assert accepted.allowed is True


def test_production_destructive_phrase_contains_full_review_context() -> None:
    action = request(
        risk=Risk.DESTRUCTIVE,
        environment_name="production",
        destructive_count=2,
    )
    profile = environment("production", production=True)

    decision = evaluate_action(
        action,
        profile,
        preflight(),
        confirmed=True,
        confirmation="production app_db 002_add_users 2",
    )

    assert decision.phrase == "production app_db 002_add_users 2"
    assert decision.allowed is True


@pytest.mark.parametrize(
    ("change", "reason"),
    [
        ({"connected": False}, "database is not connected"),
        ({"target_imported": False}, "model target did not import"),
        ({"dialect": "mysql"}, "dialect does not match"),
        ({"history_readable": False}, "history is not readable"),
        ({"history_dirty": True}, "history is dirty"),
        ({"artifact_valid": False}, "artifact is invalid"),
        ({"checksum_valid": False}, "checksum is invalid"),
        ({"dependencies_valid": False}, "dependencies are invalid"),
        ({"revision_state_valid": False}, "revision state is not legal"),
        ({"snapshot_current": False}, "review is stale"),
        ({"operations_supported": False}, "unsupported operation"),
        ({"operation_running": True}, "another operation is running"),
        ({"editor_valid": False}, "editor document is invalid"),
        ({"editor_dirty": True}, "unsaved editor changes"),
        ({"sql_present": False}, "no SQL to execute"),
        ({"artifact_checksum": "changed"}, "artifact changed after review"),
        ({"generation": 5}, "schema changed after review"),
    ],
)
def test_apply_preflight_blocks_each_unsafe_state(
    change: dict[str, object],
    reason: str,
) -> None:
    decision = evaluate_action(
        request(),
        environment("development"),
        preflight(**change),
        confirmed=True,
    )

    assert decision.allowed is False
    assert any(reason in item for item in decision.reasons)


def test_rollback_requires_rollback_sql() -> None:
    decision = evaluate_action(
        request(action="rollback"),
        environment("development"),
        preflight(rollback_available=False),
        confirmed=True,
    )

    assert decision.allowed is False
    assert "rollback SQL is unavailable" in decision.reasons


def test_repair_is_allowed_to_target_dirty_history() -> None:
    decision = evaluate_action(
        request(action="repair", risk=Risk.HISTORY_REWRITE),
        environment("development"),
        preflight(history_dirty=True),
        confirmed=True,
        confirmation="app_db 002_add_users",
    )

    assert decision.allowed is True


def test_history_rewrites_always_require_an_exact_typed_phrase() -> None:
    action = request(action="squash", risk=Risk.HISTORY_REWRITE)

    pending = evaluate_action(
        action,
        environment("development"),
        preflight(),
        confirmed=True,
    )
    accepted = evaluate_action(
        action,
        environment("development"),
        preflight(),
        confirmed=True,
        confirmation="app_db 002_add_users",
    )

    assert pending.phrase == "app_db 002_add_users"
    assert pending.allowed is False
    assert accepted.allowed is True


def test_destructive_action_requires_operation_review() -> None:
    decision = evaluate_action(
        request(risk=Risk.DESTRUCTIVE, destructive_count=1),
        environment("development"),
        preflight(destructive_reviewed=False),
        confirmed=True,
    )

    assert decision.allowed is False
    assert "destructive SQL was not reviewed" in decision.reasons


def test_classify_plan_uses_operation_safety_metadata() -> None:
    assert classify_plan(MigrationPlan()) is Risk.READ_ONLY
    assert (
        classify_plan(
            MigrationPlan(operations=[MigrationOperation(sql="CREATE TABLE users")])
        )
        is Risk.WRITE
    )
    assert (
        classify_plan(
            MigrationPlan(
                operations=[
                    MigrationOperation(sql="DROP TABLE users", destructive=True)
                ]
            )
        )
        is Risk.DESTRUCTIVE
    )
