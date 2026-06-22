#!/usr/bin/env bash
# Run the BrokerTUI test suite (unit + snapshot tests).
#
# Mirrors the `test` justfile recipe / CI `Tests` step. Extra arguments are
# forwarded to `cargo test`, so the integration suite (which needs the docker
# broker stack up — see `just test-int`) can be run with, e.g.:
#
#   scripts/test.sh --features integration -- --include-ignored
#
# Usage:
#   scripts/test.sh [CARGO_TEST_ARGS...]   Run tests, forwarding extra args.
#   scripts/test.sh -h|--help              Show this help.
set -euo pipefail

# Run from the repo root regardless of the caller's cwd.
cd "$(dirname "$0")/.."

case "${1:-}" in
    -h | --help)
        # Print the leading comment block (after the shebang, up to the first
        # non-comment line) as help, stripping the leading "# ".
        awk 'NR==1 {next} /^#/ {sub(/^# ?/, ""); print; next} {exit}' "$0"
        exit 0
        ;;
esac

echo "==> cargo test $*"
cargo test "$@"

echo "test: OK"
