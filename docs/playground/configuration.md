---
title: Configure the playground
contentType: Reference
---

# Configure the playground

`ormdantic.toml` selects the model target, migration workspace, watcher cadence, database URL source, and safety policy. Paths resolve from the directory containing the configuration file.

## Start with a complete configuration

The following example defines development, staging, and production environments without storing credentials:

```toml
[project]
target = "app.database:db"
migrations_dir = "migrations"
format = "toml"
watch = [
  "app/**/*.py",
  "migrations/**/*.toml",
  "migrations/**/*.json",
]
database_poll_seconds = 5.0
debounce_milliseconds = 300

[environments.development]
url_env = "DATABASE_URL"
env_file = ".env"
safety = "confirm"

[environments.staging]
url_env = "STAGING_DATABASE_URL"
env_file = ".env.staging"
safety = "typed"

[environments.production]
url_env = "PRODUCTION_DATABASE_URL"
env_file = ".env.production"
safety = "typed"
production = true
```

## Configure project keys

Project keys control model inspection and local files:

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `target` | string | required | Import target in `module:attribute` form. The attribute must be an `Ormdantic` instance. |
| `migrations_dir` | path | `migrations` | Directory containing top-level `.toml` and `.json` artifacts. |
| `format` | `toml` or `json` | `toml` | Preferred generation format. The playground generates TOML. |
| `watch` | array of globs | Python and migration globs | Project-relative files that trigger schema refresh. |
| `database_poll_seconds` | positive number | `5.0` | Interval between live database checks. |
| `debounce_milliseconds` | non-negative integer | `300` | File-change coalescing window. |

Absolute watch globs are ignored. The watcher also ignores `.git`, `.venv`, `__pycache__`, and `.ormdantic/drafts`.

## Configure environment keys

Each table under `environments` defines a connection source and policy:

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `url_env` | string | `DATABASE_URL` | Name of the environment variable that contains the database URL. |
| `env_file` | path or null | `.env` | Dotenv fallback. Set `null` to disable file lookup. |
| `safety` | `confirm` or `typed` | `confirm` | Confirmation policy for database writes. |
| `production` | boolean | `false` | Forces typed safety and the production destructive phrase. |

Production mode cannot be weakened through `safety = "confirm"`. The effective policy remains `typed`.

## Understand precedence

The playground resolves values in this order:

1. CLI options such as `--target` and `--migrations-dir`
2. The selected environment table
3. The project table
4. Built-in defaults

Environment selection defaults to `development`. Select another profile with `--environment`, `-e`, or the Settings screen.

## Resolve the database URL

The playground reads the variable named by `url_env` from the process environment. If it is absent or empty, the playground reads the configured `env_file`.

Only the source label, such as `DATABASE_URL`, enters application state. Diagnostic and operation messages redact credentials and secret query parameters.

## Discover configuration from nested directories

When `--config` is absent, the launcher searches the current directory and each parent for `ormdantic.toml`. The nearest file wins.

Specify an exact path when a repository contains multiple projects:

```console
ormdantic playground --config services/accounts/ormdantic.toml
```

## Override target and migration paths

CLI path and target overrides affect the current run. They do not rewrite `ormdantic.toml`.

```console
ormdantic playground \
  --target accounts.database:db \
  --migrations-dir schema/migrations
```

## Edit configuration inside the playground

The Settings screen embeds the complete TOML source. **Save TOML** parses and validates the entire document before an atomic replacement. Invalid TOML leaves the existing file unchanged and displays the dotted configuration key when possible.
