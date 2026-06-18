# Releasing

Ormdantic releases are driven by Git tags. A release publishes the Rust crates to
crates.io first, then publishes the Python distribution to PyPI, then creates or
updates the GitHub Release with the built artifacts.

## One-Time Setup

Configure these before the first release:

- Add a `CARGO_REGISTRY_TOKEN` repository secret with permission to publish all
  `ormdantic-*` crates on crates.io.
- Configure PyPI trusted publishing for the `ormdantic` project with the
  `.github/workflows/release.yml` workflow and the `release` environment.
- Protect the GitHub `release` environment if releases should require manual
  approval.

## Prepare A Release

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
