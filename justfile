# BrokerTUI developer tasks. Run `just` to list them.

# List available recipes.
default:
    @just --list

# Install the pinned Rust toolchain + components.
setup:
    rustup toolchain install stable --component clippy rustfmt

# Run the TUI (forwards extra args: `just run -- --log-level debug`).
run *ARGS:
    cargo run -- {{ARGS}}

# Format all code.
fmt:
    cargo fmt --all

# Check formatting without modifying files.
fmt-check:
    cargo fmt --all -- --check

# Lint, denying warnings.
lint:
    cargo clippy --all-targets --all-features -- -D warnings

# Unit + snapshot tests.
test:
    cargo test

# Integration tests against a dockerized Redis.
test-int:
    docker compose up -d redis
    -cargo test --features integration -- --include-ignored
    docker compose down

# Seed the local Redis with sample data (expanded in Phase 1).
seed:
    docker compose exec redis redis-cli ping

# Optimized release build.
build-release:
    cargo build --release

# Static, headless build (env-var secrets only; no keyring backend).
build-musl:
    rustup target add x86_64-unknown-linux-musl
    cargo build --release --target x86_64-unknown-linux-musl --no-default-features
