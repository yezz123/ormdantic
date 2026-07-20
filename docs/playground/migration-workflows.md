---
title: Run migration workflows in the playground
contentType: How-to
---

# Run migration workflows in the playground

Use the playground to generate, review, edit, apply, roll back, repair, and squash migration artifacts. Every database mutation rechecks the current environment, schema generation, history, artifact checksum, and complete SQL.

## Generate a TOML migration from drift

Generate only from a healthy, non-empty drift:

1. Open **Drift** and review every structured change
2. Read the complete forward SQL and safety classification
3. Select **Generate migration** or press `g`
4. Enter a revision using letters, numbers, dots, dashes, or underscores
5. Add an optional description
6. Select **Create TOML**

The generator sets `depends_on` to the current applied revision when one exists. It writes the artifact atomically and selects it in the editor.

## Review and edit an artifact

Open **Migrations** to inspect status, dependency metadata, checksum, format, and risk. Select **Open editor** to edit the complete TOML source or one SQL operation.

Press `ctrl+s` before a database action. Invalid TOML, invalid checksums, unsaved changes, and unsupported operations block execution.

See [Edit TOML and SQL](editor.md) for source synchronization and recovery drafts.

## Apply one migration

Apply a selected, pending artifact after a fresh schema review:

1. Select the artifact in **Migrations**
2. Press `a`
3. Confirm the environment, database name, revision, risk, and operation count
4. Read all SQL in the review dialog
5. Check the destructive-review box when it appears
6. Enter the exact phrase when the environment requires one
7. Select **Apply**

The dialog stays open when preflight or execution fails. A successful operation refreshes schema and durable history before the success message appears.

## Roll back the current migration

Rollback uses the selected artifact's `down` operations:

1. Select an applied artifact
2. Press `b`
3. Review the full rollback SQL and classification
4. Complete destructive review and typed confirmation when required
5. Select **Rollback**

Rollback remains disabled when no rollback SQL exists or the selected revision is not applied.

## Repair a dirty history row

Repair changes migration metadata; it does not run schema SQL. Investigate and correct the database before clearing dirty state.

1. Open **History & logs**
2. Select a row marked dirty
3. Select **Repair dirty row**
4. Confirm the retained status and the `clear dirty flag` change
5. Type the exact database and revision phrase
6. Select **Repair history**

History repair always requires typed confirmation, including development environments.

## Squash pending migrations

Squash creates a new artifact from every valid pending artifact shown in the workspace. It refuses applied, invalid, or unsaved inputs.

1. Open **Migrations** with at least two pending artifacts
2. Select **Squash pending**
3. Review every input filename and the combined SQL
4. Edit the proposed squash revision if needed
5. Type the exact database and new revision phrase
6. Select **Create squash**

The review binds to a combined checksum of every input artifact. If any input changes, reopen the review.

Squash does not delete the source files. Remove or archive them through your normal version-control review after verifying the new artifact.

## Inspect durable results

Open **History & logs** after an operation. The table shows status, revision, applied time, execution duration, and dirty state.

The log pane shows the latest operation and up to 20 recent diagnostics. Credentials embedded in URLs and secret query parameters are redacted.

## Keep automation non-interactive

Use scriptable commands for deployment and CI:

```console
ormdantic migrations status
ormdantic migrations apply-dir migrations
ormdantic migrations current
```

The playground and CLI use the same checksummed TOML and JSON artifacts.
