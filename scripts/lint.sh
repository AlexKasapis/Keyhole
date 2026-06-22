#!/usr/bin/env bash
# Lint Keyhole: rustfmt + clippy over all targets/features.
#
# Mirrors the `fmt-check`/`lint` (and `fmt`) justfile recipes and the CI
# `Format check` + `Clippy` steps, so a green run here means a green CI.
#
# Usage:
#   scripts/lint.sh            Check only: report formatting + clippy issues,
#                              change nothing, exit non-zero if anything fails.
#   scripts/lint.sh --fix      Apply fixes: `cargo fmt` then `cargo clippy --fix`,
#                              then re-check so the exit code still reflects any
#                              issue clippy could not fix automatically.
#   scripts/lint.sh -h|--help  Show this help.
set -euo pipefail

# Run from the repo root regardless of the caller's cwd.
cd "$(dirname "$0")/.."

# Print the leading comment block (everything after the shebang up to the first
# non-comment line) as help, stripping the leading "# ".
usage() {
    awk 'NR==1 {next} /^#/ {sub(/^# ?/, ""); print; next} {exit}' "$0"
}

FIX=0
for arg in "$@"; do
    case "$arg" in
        --fix) FIX=1 ;;
        -h | --help)
            usage
            exit 0
            ;;
        *)
            echo "lint.sh: unknown argument: $arg" >&2
            echo "Try 'scripts/lint.sh --help'." >&2
            exit 2
            ;;
    esac
done

if [[ "$FIX" -eq 1 ]]; then
    echo "==> cargo fmt --all"
    cargo fmt --all

    echo "==> cargo clippy --fix (machine-applicable suggestions)"
    # --allow-dirty/--allow-staged: don't refuse to write fixes over uncommitted
    # changes — applying lint fixes to a dirty tree is exactly the intent here.
    cargo clippy --fix --all-targets --all-features --allow-dirty --allow-staged -- -D warnings

    echo "==> re-checking after fixes"
fi

echo "==> cargo fmt --all -- --check"
cargo fmt --all -- --check

echo "==> cargo clippy --all-targets --all-features -- -D warnings"
cargo clippy --all-targets --all-features -- -D warnings

echo "lint: OK"
