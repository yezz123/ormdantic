# Installation

Install Ormdantic from PyPI, then verify that the native runtime contains the drivers you need. Most users only need the first command.

## Install the package

Use `uv` if your project already uses it:

```console
uv add ormdantic
```

Or use `pip`:

```console
pip install ormdantic
```

Ormdantic ships a Python package plus a Rust extension named `ormdantic._ormdantic`. The extension contains the SQL compiler, hydration planner, migration bridge, and native database drivers.

## Install the optional playground

Install the terminal playground when you want interactive schema watching, migration review, and TOML or SQL editing:

```console
uv add 'ormdantic[playground]'
```

```console
pip install 'ormdantic[playground]'
```

Launch it from a configured project:

```console
ormdantic playground
```

The extra installs Textual with syntax highlighting. Ordinary imports and `ormdantic migrations` commands do not require Textual. See [Explore schemas and migrations in the playground](playground/index.md) for setup and workflows.

## Install the Todo tutorial dependencies

The repository's FastAPI reference application uses a separate extra:

```console
uv sync --extra examples
```

This installs FastAPI, HTTPX, and Uvicorn without adding them to the core ORM
dependency set. Continue with the [Todo tutorial setup](tutorial/setup.md).

## Requirements

- Python 3.10 or newer
- Pydantic v2
- A wheel compatible with your platform, or a local Rust toolchain to build from source

## Check driver availability

Runtime driver support is compiled into the Rust extension. Check what your installed package contains:

```python
from ormdantic import runtime_capabilities

print(runtime_capabilities())
```

The result is a dictionary:

```python
{
    "sqlite": True,
    "postgresql": True,
    "mysql": True,
    "mariadb": True,
    "mssql": True,
    "oracle": True,
}
```

The exact values depend on how the wheel or local build was produced.

If a backend reports `False`, your installed extension was built without that driver. Use a wheel that includes the driver or build the extension locally with the needed Rust features.

## Connect to a database

Ormdantic accepts SQLAlchemy-style URLs, but execution uses Ormdantic's Rust drivers.

```python
Ormdantic("sqlite:///app.sqlite3")
Ormdantic("postgresql://postgres:postgres@localhost:5432/postgres")
Ormdantic("mysql://root:password@localhost:3306/app")
Ormdantic("mariadb://root:password@localhost:3306/app")
Ormdantic("mssql://sa:Password123@localhost:1433/master?trust_cert=true")
Ormdantic("oracle://system:oracle@localhost:1521/FREEPDB1")
```

See [Drivers](drivers/index.md) for backend-specific URL notes.

## Install for repository development

For repository development, install the dev dependencies and build the extension with maturin:

```console
uv sync --group dev
uv run --group dev maturin develop
```

Run the Python tests with:

```console
uv run pytest
```

Run the docs with:

```console
uv run --group docs zensical serve
```
