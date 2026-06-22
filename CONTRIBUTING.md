# Contributing to Keyhole

Thanks for contributing! A few conventions keep the project healthy.

## Development setup

```sh
just setup                  # install the pinned Rust toolchain + components
docker compose up -d redis  # a local Redis for manual testing
just run                    # launch the TUI
```

## Run what CI runs

```sh
just fmt           # rustfmt
just lint          # clippy -D warnings (all targets/features)
just test          # unit + snapshot tests
just deny          # cargo-deny: advisories, licenses, bans, sources
just coverage      # line coverage + the enforced floor
just msrv          # build on the declared minimum supported Rust version
just test-int      # integration tests (brings up dockerized brokers)
just release-lint  # installer test suite + shellcheck + actionlint
```

## Tests are required

Per [`CLAUDE.md`](CLAUDE.md), every change ships with adequate test coverage and
the codebase as a whole stays covered. CI enforces a line-coverage floor via
`cargo llvm-cov --fail-under-lines` (see `.github/workflows/ci.yml`).

The floor is a **ratchet**: as coverage rises, raise the floor toward the
observed number — never lower it just to make a change pass. New code should
keep total coverage at or above the current value.

## Changelog & releases

- Record notable, user-facing changes under the `[Unreleased]` heading in
  [`CHANGELOG.md`](CHANGELOG.md) ([Keep a Changelog](https://keepachangelog.com/)
  format).
- Releases are cut with [`cargo-release`](https://github.com/crate-ci/cargo-release)
  via `just release <level>` (preview first; add `-x` to execute). The tag push
  triggers the signed release workflow. Config lives in `release.toml`.
- Publishing to crates.io is a separate, deliberate manual step (`cargo publish`).

## Style

Match the surrounding code. `rustfmt` and `clippy -D warnings` are the source of
truth; both run in CI and via `just lint`.
