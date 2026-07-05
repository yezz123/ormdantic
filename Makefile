.PHONY: test lint docs coverage bench format taplo-check

test:
	uv run --group dev maturin develop
	bash scripts/test.sh

lint:
	cargo fmt --check
	cargo clippy --workspace --all-targets -- -D warnings
	bash scripts/lint.sh

docs:
	bash scripts/docs_build.sh

coverage:
	uv run --group dev maturin develop
	bash scripts/test.sh
	bash scripts/coverage-rust.sh

bench:
	uv run --group dev maturin develop
	uv run --group testing pytest tests/benchmarks
	cargo bench --workspace

format:
	cargo fmt
	uv run --group linting pre-commit run --all-files --verbose --show-diff-on-failure

taplo-check:
	npx @taplo/cli fmt --config ./taplo/taplo.toml --check
