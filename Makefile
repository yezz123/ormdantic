.PHONY: test lint docs coverage coverage-combined bench benchmark-smoke benchmark-sqlite-smoke benchmark-postgres-smoke benchmark-mysql-smoke benchmark-report benchmark-million benchmark-huge format taplo-check

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
	uv run --group dev maturin develop --release
	uv run --group testing pytest tests/benchmarks
	cargo bench --workspace

benchmark-smoke: benchmark-sqlite-smoke

benchmark-sqlite-smoke:
	uv run --group dev maturin develop --release
	uv run --group benchmark python -m benchmark.run --backend sqlite --profile smoke

benchmark-postgres-smoke:
	docker compose -p ormdantic-benchmark -f docker/databases/docker-compose.yaml up -d --wait postgres
	uv run --group dev maturin develop --release
	uv run --group benchmark python -m benchmark.run --backend postgres --profile smoke

benchmark-mysql-smoke:
	docker compose -p ormdantic-benchmark -f docker/databases/docker-compose.yaml up -d --wait mysql
	uv run --group dev maturin develop --release
	uv run --group benchmark python -m benchmark.run --backend mysql --profile smoke

benchmark-report:
	uv run --group dev maturin develop --release
	uv run --group benchmark python -m benchmark.run --backend sqlite --profile default

benchmark-million:
	uv run --group dev maturin develop --release
	uv run --group benchmark python -m benchmark.run --backend sqlite --profile million

benchmark-huge: benchmark-million

format:
	cargo fmt
	uv run --group linting pre-commit run --all-files --verbose --show-diff-on-failure

taplo-check:
	npx @taplo/cli fmt --config ./taplo/taplo.toml --check
