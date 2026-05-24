# Contributing to Grimoire

Thanks for your interest. Grimoire is a solo project at the moment. I'll try to keep the process is light. This should be the only place you need to look in order to contribute.

## Prerequisites

- A Rust toolchain matching `rust-toolchain.toml` (currently 1.95.0). If you
  have `rustup` installed, running any `cargo` command in this repo will
  install the pinned toolchain automatically.
- `protoc` on your PATH (used by `build.rs` to compile the worker/peer
  protos).
- Optional dev tools: `cargo-deny`, `cargo-machete`, `cargo-audit`,
  `typos-cli`, `pre-commit`. CI installs these via `taiki-e/install-action`;
  locally, `cargo install` or your package manager works.

## The gate

Everything CI runs is mirrored in the `Makefile`. The two targets that
matter day-to-day:

```sh
make all   # fmt-check, clippy, test, typos. Run before pushing.
make ci    # the full CI gate including cargo-deny, cargo-machete
```

`cargo lint` (alias defined in `.cargo/config.toml`) is a shortcut for
`make clippy`.

Pre-commit hooks are configured in `.pre-commit-config.yaml`; run
`pre-commit install` once to enable them.

## Code style

- Formatting is enforced by `rustfmt`. Stable options live in `rustfmt.toml`;
  nightly-only options are staged in `.rustfmt.unstable.toml` and applied
  with `cargo fmt-unstable` (requires a nightly toolchain).
- Lints are declared in the `[lints]` table of `Cargo.toml`. Pedantic and
  nursery are warn-by-default; each `allow` has a comment explaining why.
- Comments document why, not what.

## Commits and PRs

- Use conventional-commit subjects (`feat:`, `fix:`, `chore:`, `docs:`,
  `refactor:`, `perf:`, `style:`, `test:`). Keep the subject line concise;
  the body is optional.
- Land small, focused changes. If your branch grows past a few related
  commits, split it.
- The `main` branch is the only long-lived branch; there is no release
  branch yet.

## Reporting bugs

Open a normal GitHub issue.
