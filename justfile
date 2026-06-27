# Keyhole developer tasks. Run `just` to list them.

# List available recipes.
default:
    @just --list

# Install the pinned Rust toolchain + components (clippy, rustfmt).
setup:
    # One --component per component: a bare second name is read as a toolchain.
    rustup toolchain install stable --component clippy --component rustfmt

# Run the TUI (forwards extra args: `just run -- --log-level debug`).
run *ARGS:
    cargo run -- {{ARGS}}

# Format all code.
fmt:
    cargo fmt --all

# Lint (check only): rustfmt --check + clippy, denying warnings.
lint:
    ./scripts/lint.sh

# Lint and auto-apply fixes: rustfmt + clippy --fix.
lint-fix:
    ./scripts/lint.sh --fix

# Unit + snapshot tests.
test:
    ./scripts/test.sh

# Integration tests against dockerized Redis + ActiveMQ + RabbitMQ.
test-int:
    docker compose --profile rabbitmq up -d redis activemq rabbitmq
    # ActiveMQ has no healthcheck; wait for its AMQP port to accept connections.
    bash -c 'for i in $(seq 1 60); do (echo > /dev/tcp/127.0.0.1/${KEYHOLE_ACTIVEMQ_PORT:-5674}) 2>/dev/null && break; sleep 2; done'
    # Wait until RabbitMQ accepts the keyhole credentials — i.e. the node is up
    # AND the default user is provisioned (a bare port/ping check races ahead of it).
    bash -c 'for i in $(seq 1 60); do docker compose exec -T rabbitmq rabbitmqctl -q authenticate_user keyhole keyhole >/dev/null 2>&1 && break; sleep 2; done'
    -cargo test --features integration -- --include-ignored
    docker compose --profile rabbitmq down

# Seed the local Redis with sample browse data.
seed:
    docker compose up -d redis
    bash -c 'for i in $(seq 1 30); do docker compose exec -T redis redis-cli ping >/dev/null 2>&1 && break; sleep 1; done'
    cargo run -- dev seed

# Bring up all brokers, seed Redis, then stream fake traffic into them until
# Ctrl-C. Connection parameters come from config.dev.toml; launch the TUI in
# another shell with `keyhole --config config.dev.toml --connect dev-redis`.
dev:
    docker compose --profile rabbitmq up -d redis activemq rabbitmq
    bash -c 'for i in $(seq 1 30); do docker compose exec -T redis redis-cli ping >/dev/null 2>&1 && break; sleep 1; done'
    bash -c 'for i in $(seq 1 60); do (echo > /dev/tcp/127.0.0.1/${KEYHOLE_ACTIVEMQ_PORT:-5674}) 2>/dev/null && break; sleep 2; done'
    bash -c 'for i in $(seq 1 60); do docker compose exec -T rabbitmq rabbitmqctl -q authenticate_user keyhole keyhole >/dev/null 2>&1 && break; sleep 2; done'
    cargo run -- dev seed
    KEYHOLE_DEV_RABBITMQ_PASSWORD=keyhole cargo run -- dev publish --broker all

# Record a real TUI session and render the marketing-site demo SVGs.
# Seeds Redis, streams fake traffic, drives the binary through a PTY, and renders
# scripts/demos/keyhole-demo.cast -> website/ + docs-site/ public SVGs. Re-run and
# commit to refresh the demo as the UI changes. Needs docker, python3, npx.
record-demo:
    ./scripts/record_demo.sh

# Optimized release build.
build-release:
    cargo build --release

# Build the distro-native packages (.deb + .rpm) locally, the way the release
# workflow does: a full glibc release build, the generated man page + completions,
# then cargo-deb / cargo-generate-rpm over Cargo.toml's [package.metadata.*].
# Outputs land in target/debian/ and target/generate-rpm/. The .deb leg needs
# dpkg-shlibdeps (Debian-only); requires `cargo install cargo-deb cargo-generate-rpm`.
package:
    cargo build --release
    mkdir -p dist-assets
    ./target/release/keyhole gen man --out dist-assets
    ./target/release/keyhole gen completions bash --out dist-assets
    ./target/release/keyhole gen completions zsh  --out dist-assets
    ./target/release/keyhole gen completions fish --out dist-assets
    cargo deb --no-build --no-strip
    cargo generate-rpm

# Cut a release via cargo-release: bump version, rewrite CHANGELOG.md, commit,
# tag `vX.Y.Z`, and push (the tag triggers .github/workflows/release.yml).
# cargo-release is dry-run by default — append `-x` to actually execute:
#   just release patch        # preview a patch bump
#   just release minor -x      # bump minor, tag, and push for real
# Requires `cargo install cargo-release`. See release.toml for the config.
release *ARGS:
    cargo release {{ARGS}}

# Lint the release plumbing the way CI does: run the installer + packaging test
# suites, shellcheck the scripts, and (if installed) actionlint the workflows.
release-lint:
    ./scripts/test_install.sh
    ./scripts/test_packaging.sh
    shellcheck scripts/install.sh scripts/test_install.sh scripts/gen_packaging.sh scripts/test_packaging.sh scripts/lib/srcinfo.sh scripts/record_demo.sh
    command -v actionlint >/dev/null && actionlint || echo "actionlint not installed; skipping workflow lint"

# Validate the Tier-2 community packaging artifacts (AUR / Homebrew / Nix):
# structure, version consistency, .SRCINFO sync, and the release generator
# round-trip. makepkg/brew/nix validators run too when those tools are present.
test-packaging:
    ./scripts/test_packaging.sh

# Refresh packaging/aur/*/.SRCINFO after hand-editing a PKGBUILD.
gen-srcinfo:
    ./scripts/gen_packaging.sh srcinfo

# Supply-chain audit (advisories/licenses/bans/sources); needs cargo-deny.
deny:
    cargo deny check

# Line coverage + enforced floor, unit+snapshot only (no docker); needs cargo-llvm-cov.
# (CI's coverage job adds the broker integration tests for the authoritative number.)
coverage:
    cargo llvm-cov --summary-only --fail-under-lines 85

# Build on the declared MSRV (Cargo.toml rust-version) to catch drift.
msrv:
    #!/usr/bin/env bash
    set -euo pipefail
    v=$(grep -m1 -E '^rust-version' Cargo.toml | sed -E 's/.*"([0-9.]+)".*/\1/')
    rustup toolchain install "$v" --profile minimal
    cargo "+$v" check --locked --all-features
