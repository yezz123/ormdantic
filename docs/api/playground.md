---
title: Extend playground services
contentType: Reference
---

# Extend playground services

The public playground service layer supports custom launchers, configuration tooling, schema refresh integrations, and migration workflow tests. Textual screen and widget classes remain implementation details.

## Configuration records and functions

Use these records to discover, parse, select, validate, and write playground configuration.

::: ormdantic.playground.config
    options:
      members:
        - ConfigError
        - ProjectConfig
        - EnvironmentConfig
        - PlaygroundConfig
        - EffectiveConfig
        - DatabaseUrlSource
        - discover_config
        - parse_config
        - parse_config_source
        - load_config
        - write_config
        - write_config_source
        - resolve_database_url

## Immutable state records

State snapshots keep UI rendering and background services independent.

::: ormdantic.playground.state
    options:
      members:
        - RefreshStatus
        - SchemaState
        - ArtifactSummary
        - MigrationState
        - OperationState
        - PlaygroundState
        - RefreshResult
        - accept_refresh

## Diagnostics and redaction

Create diagnostics through `Diagnostic.create` so messages and structured details are redacted before publication.

::: ormdantic.playground.diagnostics
    options:
      members:
        - Severity
        - Diagnostic
        - redact_text
        - redact_value

## Inspection and refresh services

`inspect_models` isolates imports in a child process. `RefreshService` coordinates model inspection, live reflection, planning, and history without blocking the TUI event loop.

::: ormdantic.playground.inspection
    options:
      members:
        - InspectionError
        - InspectionResult
        - inspect_models

::: ormdantic.playground.services
    options:
      members:
        - DatabaseRefresh
        - RefreshService
        - reflect_database
        - plan_migration

## Watch events

`SchemaWatcher` polls file metadata and database cadence. It emits coalesced, numbered events.

::: ormdantic.playground.watcher
    options:
      members:
        - WatchReason
        - WatchEvent
        - SchemaWatcher

## Workspace and safety records

Workspace functions parse, edit, convert, draft, and atomically save migration artifacts. Safety functions evaluate immutable action requests against current preflight state.

::: ormdantic.playground.workspace
    options:
      members:
        - ArtifactDocument
        - MigrationWorkspace
        - load_workspace
        - select_document
        - update_source
        - replace_operation_sql
        - convert_to_toml
        - save_document
        - draft_path
        - write_draft
        - recover_draft
        - discard_draft

::: ormdantic.playground.safety
    options:
      members:
        - Risk
        - ActionRequest
        - PreflightContext
        - SafetyDecision
        - classify_plan
        - evaluate_action

## Controller and operations

`PlaygroundController` owns immutable state and exposes intents. `MigrationOperations` enforces approved request identity before calling the migration manager.

::: ormdantic.playground.controller
    options:
      members:
        - ControllerActionOutcome
        - PlaygroundController

::: ormdantic.playground.operations
    options:
      members:
        - MigrationOperations
