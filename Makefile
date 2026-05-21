# Developer convenience targets. CI should mirror these.
#
# `--locked` is passed to cargo invocations that resolve dependencies, so the
# build fails if `Cargo.lock` would be modified — i.e. if any upstream
# version has drifted from what we've reviewed and committed. This is the
# supply-chain backbone: without it, CI silently picks up new transitive
# crate versions between commits.

.PHONY: check fmt fmt-check clippy test audit deny machete typos all ci

all: fmt-check clippy test typos

ci: fmt-check clippy test audit deny machete typos

fmt:
	cargo fmt --all

fmt-check:
	cargo fmt --all -- --check

clippy:
	cargo clippy --locked --all-targets --all-features -- -D warnings

test:
	cargo test --locked --all-features

audit:
	cargo audit

deny:
	cargo deny check

machete:
	cargo machete

typos:
	typos

semver:
	cargo semver-checks --only-explicit-features
