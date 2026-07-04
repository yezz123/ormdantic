---
hide:
  - navigation
---

# Contributing

This guide takes you from a fresh clone to a pull request that is ready to
review. Most commands run from the repository root.

Ormdantic has two main parts:

- A Python package in `ormdantic/`.
- A Rust extension in `rust/crates/` that is exposed to Python as
  `ormdantic._ormdantic`.

If your change touches behavior that crosses that boundary, rebuild the Rust
extension before running Python tests.

## Choose Your Path

Start by opening the section that matches your change.

<details markdown="1">
<summary><strong>I am changing documentation</strong></summary>

Use this path for edits under `docs/`, examples, API pages, and navigation.

```bash
uv sync --group docs
uv run --group docs zensical serve
```

Before you open the PR, build the docs:

```bash
bash scripts/docs_build.sh
```

If you add or move a page, update `mkdocs.yml` so it appears in the site
navigation.

</details>

<details markdown="1">
<summary><strong>I am changing Python behavior</strong></summary>

Use this path for changes in `ormdantic/`, the CLI, migrations, query helpers,
sessions, loading, serializers, or Python-side errors.

```bash
uv sync --group dev
uv run --group dev maturin develop
uv run --group testing pytest tests/unit -q
```

Then run the smallest integration test that covers the changed behavior. For a
full Python check, run:

```bash
bash scripts/test.sh
```

</details>

<details markdown="1">
<summary><strong>I am changing the Rust engine</strong></summary>

Use this path for changes under `rust/crates/`, SQL generation, native drivers,
hydration, or the Python extension boundary.

```bash
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --exclude ormdantic-py
uv run --group dev maturin develop
bash scripts/test.sh
```

Re-run `maturin develop` after Rust changes so Python imports the extension you built.

</details>

<details markdown="1">
<summary><strong>I am changing database dialect behavior</strong></summary>

Use this path for PostgreSQL, MySQL, MariaDB, SQL Server, Oracle, or
dialect-specific migration behavior.

SQLite is covered by the normal test suite. Server databases are covered by the
Docker matrix:

```bash
bash docker/databases/run-tests.sh
```

The matrix starts the database services, exports the `ORMDANTIC_TEST_*_URL` and
`ORMDANTIC_*_URL` environment variables, and runs the external Python and Rust
checks. See `docker/databases/README.md` for the exact URLs.

</details>

<details markdown="1">
<summary><strong>I am changing performance-sensitive code</strong></summary>

Use this path for serializer, hydration, query planning, and driver changes
that may affect throughput or allocations.

```bash
uv run --group dev maturin develop
uv run pytest tests/benchmarks
```

If the change is intended to improve performance, include the benchmark command
and result summary in the PR description.

</details>

## Setup Once

Install the tools you need:

- Python 3.10 or newer.
- A stable Rust toolchain with `cargo`.
- `uv`.
- Docker, only when you need the server database matrix.

Clone the repository and install the development environment:

```bash
git clone https://github.com/yezz123/ormdantic.git
cd ormdantic
uv sync --group dev
uv run --group dev maturin develop
```

`uv sync --group dev` installs the testing, linting, documentation, and native
build dependencies declared in `pyproject.toml`.

You can run commands through `uv run` without activating the virtual
environment. If you prefer an activated shell, use:

```bash
source .venv/bin/activate
```

On Windows PowerShell:

```powershell
.venv\Scripts\Activate.ps1
```

Install Git hooks after the first sync:

```bash
uv run --group linting pre-commit install
```

The hooks run the configured `pre-commit-hooks`, Ruff checks, and Ty type
checking before commits.

## Command Reference

| Goal | Command |
| --- | --- |
| Install all development dependencies | `uv sync --group dev` |
| Rebuild the Python extension | `uv run --group dev maturin develop` |
| Run Python tests with coverage | `bash scripts/test.sh` |
| Run one Python test file | `uv run --group testing pytest tests/unit/test_expression_api.py -q` |
| Type-check Python | `bash scripts/lint.sh` |
| Run pre-commit on every file | `bash scripts/format.sh` |
| Check Rust formatting | `cargo fmt --check` |
| Run Rust clippy | `cargo clippy --workspace --all-targets -- -D warnings` |
| Run Rust tests | `cargo test --workspace --exclude ormdantic-py` |
| Serve the docs locally | `uv run --group docs zensical serve` |
| Build the docs | `bash scripts/docs_build.sh` |
| Run the server database matrix | `bash docker/databases/run-tests.sh` |

## Development Loop

1. Create a focused branch:

    ```bash
    git checkout -b fix/short-description
    ```

2. Make the smallest change that solves the issue.

3. Add or update tests close to the behavior you changed.

4. Rebuild the extension if Rust changed:

    ```bash
    uv run --group dev maturin develop
    ```

5. Run the narrowest useful test first, then run the broader check that matches
   your change path.

6. Update documentation when behavior, configuration, commands, or public API
   changes.

## Pull Request Checklist

Use this as a final pass before opening a PR:

- The change is focused on one problem or feature.
- User-facing behavior has tests or a clear reason tests are not practical.
- Documentation is updated for new commands, APIs, options, or behavior.
- Rust changes were rebuilt with `uv run --group dev maturin develop`.
- Relevant Python, Rust, docs, or database checks pass locally.
- The PR description explains what changed, why it changed, and how it was
  tested.
- The contribution follows the [Code of Conduct](../faq/code_of_conduct.md).

## Troubleshooting

<details markdown="1">
<summary><strong>Python cannot import <code>ormdantic._ormdantic</code></strong></summary>

The native extension is missing or stale. Rebuild it from the repository root:

```bash
uv run --group dev maturin develop
```

</details>

<details markdown="1">
<summary><strong>A command is missing from the virtual environment</strong></summary>

Sync the dependency group that owns the command:

```bash
uv sync --group dev
uv sync --group docs
uv sync --group linting
uv sync --group testing
```

Prefer `uv run ...` so the command comes from the local environment.

</details>

<details markdown="1">
<summary><strong>Database tests cannot connect</strong></summary>

For PostgreSQL, MySQL, MariaDB, SQL Server, and Oracle tests, run the database
matrix script:

```bash
bash docker/databases/run-tests.sh
```

If you use your own database services, set the matching `ORMDANTIC_TEST_*_URL`
environment variables before running the external tests.

</details>

<details markdown="1">
<summary><strong>Docs build locally but the page is missing from navigation</strong></summary>

Add the page to the `nav` section in `mkdocs.yml`. The docs builder can render a
Markdown file even when the site navigation does not link to it.

</details>
