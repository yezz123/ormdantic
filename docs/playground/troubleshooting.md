---
title: Diagnose playground problems
contentType: Troubleshooting
---

# Diagnose playground problems

Use the visible diagnostic code, status bar, and preflight reasons to locate a playground failure. Fix the underlying model, connection, artifact, or history state before retrying a mutation.

## The playground command cannot import Textual

Install the optional dependency in the environment that owns the `ormdantic` executable:

```console
python -m pip install 'ormdantic[playground]'
```

Confirm that both commands resolve through the same interpreter:

```console
python -m ormdantic --help
ormdantic playground --help
```

## No configuration is found

Launch from the project directory or pass the file explicitly:

```console
ormdantic playground --config path/to/ormdantic.toml
```

The launcher searches the current directory and parent directories. It does not search child directories.

## The model target fails to import

Use a `module:attribute` target that resolves from the configuration directory:

```toml
[project]
target = "app.database:db"

[environments.development]
url_env = "DATABASE_URL"
```

The attribute must contain an `Ormdantic` instance. Remove slow network calls and other unrelated import-time work from the target module.

## The database stays offline

Check the configured source without pasting its value into logs:

1. Confirm that `url_env` names the intended variable
2. Export that variable or define it in `env_file`
3. Check compiled support with `runtime_capabilities()`
4. Verify network, certificate, and database permissions
5. Press `r`

The status bar displays the source label and detected dialect after connection.

## Drift is stale or partial

Open **Overview** and **Schemas** to identify the missing side. A retained snapshot remains visible, but generation and database actions stay blocked until both sources refresh successfully.

Fix every error diagnostic, then press `r`. Do not treat retained SQL as current review.

## A migration action remains disabled

Read every line in the preflight panel. Common causes include:

| Message | Fix |
| --- | --- |
| `artifact changed after review` | Reopen the dialog after saving the artifact. |
| `live schema changed after review` | Reopen the dialog against the new generation. |
| `unsaved editor changes` | Press `ctrl+s`, then reopen the action. |
| `revision state is not legal` | Apply only pending revisions and roll back only applied revisions. |
| `history is dirty` | Investigate the failed migration, then use guarded repair. |
| `dialect does not match` | Generate the artifact for the selected environment. |
| `destructive SQL was not reviewed` | Read all SQL and select the destructive-review checkbox. |

Typed phrases require exact case and whitespace.

## TOML saves fail

The editor reports a parse or validation message and leaves the saved file unchanged. Fix the reported dotted key or TOML syntax before saving again.

Unknown keys fail validation. Check [Configure the playground](configuration.md) for supported fields.

## JSON cannot be edited

JSON migration artifacts are intentionally read-only. Select **Convert to TOML**, review the new file, then edit its source or operation SQL.

## External migration edits do not appear

The watcher does not replace a dirty editor buffer. Save or finish the local edit, then press `r`.

Confirm that the file matches a project-relative `watch` glob and is outside ignored directories.

## Quit reports a pending operation

Wait for the current action and its post-operation refresh. Cancelling a UI worker cannot guarantee cancellation of a native driver call already running in its thread.

Inspect **History & logs** and the live schema before retrying.

## A failed migration leaves dirty history

Do not clear dirty state until you understand whether each statement committed. Repair the database manually when needed, confirm its actual schema, then use **Repair dirty row**.

Back up important data before manual or destructive repair.
