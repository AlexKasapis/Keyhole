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

# Cut a release via cargo-release: bump version, rewrite CHANGELOG.md, commit,
# tag `vX.Y.Z`, and push (the tag triggers .github/workflows/release.yml).
# cargo-release is dry-run by default — append `-x` to actually execute:
#   just release patch        # preview a patch bump
#   just release minor -x      # bump minor, tag, and push for real
# Requires `cargo install cargo-release`. See release.toml for the config.
release *ARGS:
    cargo release {{ARGS}}

# Lint the release plumbing the way CI does: shellcheck the installer + its
# tests, run the installer test suite, and (if installed) actionlint the workflows.
release-lint:
    ./scripts/test_install.sh
    shellcheck scripts/install.sh scripts/test_install.sh
    command -v actionlint >/dev/null && actionlint || echo "actionlint not installed; skipping workflow lint"
