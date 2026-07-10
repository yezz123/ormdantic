.PHONY: test lint docs coverage coverage-combined bench benchmark-report benchmark-huge format taplo-check

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
	uv run --group testing python scripts/coverage_combined.py

coverage-combined:
	uv run --group testing python scripts/coverage_combined.py $(if $(COMBINED_COVERAGE_FAIL_UNDER),--fail-under $(COMBINED_COVERAGE_FAIL_UNDER),)

bench:
	uv run --group dev maturin develop
	uv run --group testing pytest tests/benchmarks
	cargo bench --workspace

benchmark-report:
	uv run --group dev maturin develop
	uv run --group benchmark python -m benchmark.run

benchmark-huge:
	uv run --group dev maturin develop
	uv run --group benchmark python -m benchmark.run --profile huge

format:
	cargo fmt
	uv run --group linting pre-commit run --all-files --verbose --show-diff-on-failure

taplo-check:
	npx @taplo/cli fmt --config ./taplo/taplo.toml --check
