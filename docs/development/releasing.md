# Releasing

Ormdantic releases are driven by Git tags. A release publishes the Rust crates to crates.io first, then publishes the Python distribution to PyPI, then creates or updates the GitHub Release with the built artifacts.

## One-time setup

Configure these before the first release:

- Add a `CARGO_REGISTRY_TOKEN` repository secret with permission to publish all `ormdantic-*` crates on crates.io.
- Configure PyPI trusted publishing for the `ormdantic` project with the `.github/workflows/release.yml` workflow and the `release` environment.
- Protect the GitHub `release` environment if releases should require manual approval.

## Prepare a release

Use the bump script to keep the Python and Rust versions in sync:

```bash
uv run python scripts/bump_version.py 2.1.0
```

To preview a bump without writing files:

```bash
uv run python scripts/bump_version.py --part patch --dry-run
```

Before tagging, check these values all match:

```bash
uv run python scripts/bump_version.py --check
```

The script updates and checks:

- `ormdantic/__init__.py` `__version__`.
- `Cargo.toml` `[workspace.package]` `version`.
- Internal `ormdantic-*` entries in `Cargo.toml` `[workspace.dependencies]`.
- `Cargo.lock` package entries for the workspace crates.

## Required Checks

Before tagging, run or confirm these gates:

- `make lint` for Rust formatting, Rust clippy, and Python type checks.
- `make test` for the local editable extension and Python test suite.
- `make docs` for the documentation site.
- `make coverage` when preparing a release candidate or coverage-sensitive change.
- `make bench` for runtime, hydration, serializer, migration, or query-planning changes.
- `bash docker/databases/run-tests.sh` for backend-specific driver or dialect changes.
- `uv run python scripts/smoke_installed_package.py` after building or installing an artifact locally.

The release workflow repeats the release verification checks and smoke-tests
the built source distribution and wheels before uploading artifacts.

## Wheel and Source Distribution Matrix

The release workflow builds:

- one source distribution on Ubuntu with Python 3.12
- wheels on `ubuntu-latest`, `macos-latest`, and `windows-latest`
- wheels for Python 3.10, 3.11, 3.12, 3.13, and 3.14

Every built artifact is checked with `twine`. The wheel matrix installs the
wheel into a clean virtual environment and runs
`scripts/smoke_installed_package.py`. The source distribution job installs the
sdist into a clean virtual environment and runs the same smoke script, proving
the installed package imports and can execute a basic SQLite query without any
external database service.

## Publish

Create and push a tag that matches the version:

```bash
git tag v2.0.0
git push origin v2.0.0
```

The release workflow validates the tag, runs the release verification checks,
publishes crates in dependency order, publishes the Python package, and uploads
the artifacts to the GitHub Release.

For a non-publishing rehearsal, run the `Release` workflow manually with
`publish` set to `false`.
