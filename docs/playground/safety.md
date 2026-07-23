---
title: Understand playground migration safeguards
contentType: Conceptual
---

# Understand playground migration safeguards

The playground treats every review as a short-lived authorization for one environment, revision, checksum, schema generation, risk class, and SQL payload. It never caches confirmation across actions.

## Read the risk classes

The migration plan and operation metadata determine risk:

| Risk | Meaning | Example |
| --- | --- | --- |
| `read_only` | No database write | SQL preview |
| `write` | Non-destructive schema or history write | Create a table |
| `destructive` | A destructive operation or destructive diff | Drop a table |
| `history_rewrite` | Migration history or artifact-chain rewrite | Repair or squash |

Rollback uses the same diff, warning, safety, and SQL classification as the migration manager. The review cannot authorize a lower risk than execution detects.

## Complete every preflight check

Database actions remain disabled until all relevant checks pass:

- The selected environment matches the reviewed environment
- The database is connected
- The model target imports successfully
- The artifact dialect matches the live dialect
- Migration history is readable and legal for the action
- Dirty history blocks apply, rollback, and squash
- The artifact parses and its checksum is valid
- Dependencies and revision state are valid
- Rollback SQL exists for rollback
- Model and live snapshots are current
- Every operation is supported and contains SQL
- No other operation is running
- The editor is valid and saved
- The artifact checksum and schema generation still match the review
- Destructive SQL received an explicit review

Repair can target dirty history because clearing investigated dirty state is its purpose.

## Use confirmation policies

`safety = "confirm"` requires the action button confirmation but no phrase for ordinary writes. Destructive actions still require the destructive-review checkbox.

`safety = "typed"` requires this exact phrase:

```text
database_name revision
```

History rewrites always use the same typed phrase, even under `confirm` policy.

Production destructive actions require more context:

```text
environment database_name revision destructive_operation_count
```

Matching is case-sensitive. Leading or trailing whitespace fails.

## Protect secrets

Configuration stores the name of a URL variable, not its value. UI state stores a source label such as `DATABASE_URL`.

Diagnostics recursively redact credential passwords and secret query parameters. The playground does not render resolved URLs in review summaries or logs.

## Understand transaction limits

Migration transaction behavior depends on the database and statement type. Some data definition language (DDL) statements auto-commit or cannot roll back atomically.

Back up important databases and test every destructive artifact in staging. A rollback operation is executable SQL, not a guarantee that deleted data can be restored.

## Understand cancellation limits

Model inspection runs in a child process that the playground can terminate. Database reflection and migration calls run in worker threads because the current manager API is blocking at that boundary.

Cancelling the UI worker discards its result. A native database call already running may finish after cancellation. Check durable history and the live schema before retrying.

## Preserve review identity

Apply and rollback bind to the selected artifact checksum. Squash binds to a combined checksum of all input artifacts.

Any artifact edit, schema refresh, environment switch, or generation change invalidates the prior review. Reopen the dialog and inspect the new state.
