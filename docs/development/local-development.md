# Local Development

Use this guide when working from a source checkout. It covers the Python
environment, Rust toolchain, native extension build, and common local commands.

## Prerequisites

- Python 3.10 or newer.
- `uv` for Python dependency management and command execution.
- A stable Rust toolchain with `cargo`, `rustfmt`, and `clippy`.
- Docker only when you need the external database matrix.

Install or update Rust with `rustup`:

```bash
rustup toolchain install stable
rustup default stable
rustup component add rustfmt clippy
```

## Create the Environment

Install all development dependency groups:

```bash
uv sync --group dev
```

Build the Rust extension into the local virtual environment:

```bash
uv run --group dev maturin develop
```

Run `maturin develop` again after Rust changes, after changing Python versions,
or when `ormdantic._ormdantic` cannot import.

## Verify the Native Runtime

The installed extension exposes driver diagnostics without opening a database
connection:

```bash
uv run python -c "from ormdantic import runtime_capabilities; print(runtime_capabilities())"
```

Run the installed-package smoke check when validating a wheel, source
distribution, or local editable build:

```bash
uv run python scripts/smoke_installed_package.py
```

The smoke check imports `ormdantic._ormdantic`, verifies required native
symbols, checks runtime driver diagnostics, and executes `SELECT 1 AS ok`
through SQLite.

## Common Commands

The repository `Makefile` wraps the same scripts used by CI:

| Goal | Command |
| --- | --- |
| Rebuild the extension and run Python tests | `make test` |
| Run Rust formatting, Rust clippy, and Python type checks | `make lint` |
| Build the documentation site | `make docs` |
| Run Python and Rust coverage checks | `make coverage` |
| Run Python and Rust benchmarks | `make bench` |
| Run local formatters and pre-commit hooks | `make format` |

For narrower loops, run the underlying command directly:

```bash
uv run --group testing pytest tests/unit/test_expression_api.py -q
cargo test --workspace --exclude ormdantic-py
bash docker/databases/run-tests.sh
```

## Dependency Groups

`pyproject.toml` defines these `uv` groups:

- `testing` for pytest, coverage, and benchmark plugins.
- `linting` for pre-commit and Ty.
- `docs` for the documentation toolchain.
- `dev` for all contributor dependencies plus `maturin`.

The `sqlite` and `postgresql` optional extras are intentionally empty today
because runtime drivers are compiled into the Rust extension instead of
installed as Python packages.

## Troubleshooting

If the extension import fails, rebuild it:

```bash
uv sync --group dev
uv run --group dev maturin develop
```

If the failure started after switching Python versions or upgrading an
installed package, force a reinstall:

```bash
pip install --force-reinstall ormdantic
```
