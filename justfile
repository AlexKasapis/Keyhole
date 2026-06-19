# BrokerTUI developer tasks. Run `just` to list them.

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

# Check formatting without modifying files.
fmt-check:
    cargo fmt --all -- --check

# Lint, denying warnings.
lint:
    cargo clippy --all-targets --all-features -- -D warnings

# Unit + snapshot tests.
test:
    cargo test

# Integration tests against dockerized Redis + ActiveMQ.
test-int:
    docker compose up -d redis activemq
    # Wait for ActiveMQ's AMQP port to start accepting connections (it boots slowly).
    bash -c 'for i in $(seq 1 60); do (echo > /dev/tcp/127.0.0.1/${BROKERTUI_ACTIVEMQ_PORT:-5674}) 2>/dev/null && break; sleep 2; done'
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
