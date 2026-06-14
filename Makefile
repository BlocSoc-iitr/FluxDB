.PHONY: fmt fmt-check lint test build pr

fmt:
	cargo fmt

fmt-check:
	cargo fmt -- --check

lint:
	cargo clippy -- -D warnings

test:
	cargo test --verbose

build:
	cargo build --verbose

pr: fmt lint build test
