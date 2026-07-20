---
title: Edit migration TOML and SQL
contentType: How-to
---

# Edit migration TOML and SQL

The migration editor keeps the complete artifact source and per-operation SQL synchronized. TOML is the canonical editable format; JSON remains readable and executable until you convert it explicitly.

## Edit the complete TOML document

Select an artifact in **Migrations**, then open **Editor**. The left pane contains the complete document, including revision metadata, snapshots, operations, rollback operations, warnings, dependencies, and safety metadata.

TOML changes parse as you type. An invalid buffer preserves the last valid artifact in memory but disables SQL editing, saving, and migration execution.

## Edit one SQL operation

The right pane separates forward and rollback operations. Select an operation, edit its complete SQL, and inspect the regenerated TOML source.

Changing operation SQL recalculates the artifact checksum. Save before opening an action review.

## Save atomically

Press `ctrl+s` or select **Save**. The editor validates the artifact, recalculates its checksum, writes a sibling temporary file, calls `fsync`, and replaces the destination.

Invalid source never replaces the saved migration.

## Recover an interrupted edit

Dirty TOML buffers create recovery drafts after a 250 ms idle delay. Drafts live under:

```text
.ormdantic/drafts/revision_name.toml
```

Draft writes use the same atomic replacement strategy. The watcher ignores this directory, so draft creation does not start schema refresh.

The quit binding displays a confirmation screen while any editor document is dirty. Keep editing or confirm that you want to exit.

## Convert legacy JSON explicitly

JSON artifacts open in read-only mode. Select **Convert to TOML** to create a separate `.toml` file.

Conversion does not overwrite or remove the JSON source. Review the generated TOML, update version-control references, and remove the legacy file only after verification.

## Edit project configuration

Open **Settings** to edit the complete `ormdantic.toml` file with TOML syntax highlighting. **Save TOML** validates the whole document before replacing the configuration and rebuilding the selected environment controller.

An invalid edit leaves the previous configuration active and unchanged on disk.

## Avoid conflicting external edits

The watcher reloads migration files only when no workspace document is dirty. Save your buffer before expecting an external file change to appear.

Use `r` after resolving a conflict to refresh schema and history against the saved artifacts.
