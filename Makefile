# Developer convenience targets. CI should mirror these.

.PHONY: check fmt fmt-check clippy test audit deny machete typos all ci

all: fmt-check clippy test typos

ci: fmt-check clippy test audit deny machete typos

fmt:
	cargo fmt --all

fmt-check:
	cargo fmt --all -- --check

clippy:
	cargo clippy --all-targets --all-features -- -D warnings

test:
	cargo test --all-features

audit:
	cargo audit

deny:
	cargo deny check

machete:
	cargo machete

typos:
	typos

semver:
	cargo semver-checks
