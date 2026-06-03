.PHONY: format taplo-check

format:
	cargo fmt
	cargo clippy --all -- -D warnings
	cargo clippy --tests --no-deps -- -D warnings

taplo-check:
	npx @taplo/cli fmt --config ./taplo/taplo.toml --check
