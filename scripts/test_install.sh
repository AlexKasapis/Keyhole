#!/usr/bin/env bash
# Tests for scripts/install.sh — the prebuilt-binary installer.
#
# Covers target-triple detection (every supported os/arch/flavor + rejections)
# and the full download → checksum-verify → install flow against a locally
# staged fake release (no network), including the corrupted-checksum abort and
# the cosign signature paths (valid / invalid / unsigned), driven by a stub
# cosign so no real sigstore access is needed.
#
# Usage:
#   scripts/test_install.sh         Run all installer tests.
#   scripts/test_install.sh -h      Show this help.
set -euo pipefail

cd "$(dirname "$0")/.."
INSTALLER="scripts/install.sh"

case "${1:-}" in
    -h | --help)
        awk 'NR==1 {next} /^#/ {sub(/^# ?/, ""); print; next} {exit}' "$0"
        exit 0
        ;;
esac

fail=0
pass() { printf 'ok   - %s\n' "$1"; }
bad() {
    printf 'FAIL - %s\n' "$1" >&2
    fail=1
}

expect_target() {
    # $1=os $2=arch $3=flavor $4=expected-triple
    local got
    got=$(KEYHOLE_UNAME_S="$1" KEYHOLE_UNAME_M="$2" KEYHOLE_INSTALL_FLAVOR="$3" \
        sh "$INSTALLER" --print-target 2>/dev/null || true)
    if [ "$got" = "$4" ]; then
        pass "$1/$2/$3 -> $4"
    else
        bad "$1/$2/$3: expected '$4', got '$got'"
    fi
}

expect_reject() {
    # $1=description; remaining args are VAR=VAL overrides
    local desc="$1"
    shift
    if env "$@" sh "$INSTALLER" --print-target >/dev/null 2>&1; then
        bad "$desc: expected non-zero exit"
    else
        pass "$desc rejected"
    fi
}

echo "== target detection =="
expect_target Linux x86_64 gnu x86_64-unknown-linux-gnu
expect_target Linux x86_64 musl x86_64-unknown-linux-musl
expect_target Linux aarch64 gnu aarch64-unknown-linux-gnu
expect_target Linux aarch64 musl aarch64-unknown-linux-musl
expect_target Linux arm64 gnu aarch64-unknown-linux-gnu
expect_target Linux amd64 musl x86_64-unknown-linux-musl
# Flavor defaults to gnu when unset.
default_flavor=$(KEYHOLE_UNAME_S=Linux KEYHOLE_UNAME_M=x86_64 sh "$INSTALLER" --print-target 2>/dev/null || true)
if [ "$default_flavor" = "x86_64-unknown-linux-gnu" ]; then
    pass "flavor defaults to gnu"
else
    bad "flavor default: got '$default_flavor'"
fi

echo "== rejections =="
expect_reject "macOS" KEYHOLE_UNAME_S=Darwin KEYHOLE_UNAME_M=x86_64
expect_reject "unknown OS" KEYHOLE_UNAME_S=Plan9 KEYHOLE_UNAME_M=x86_64
expect_reject "unknown arch" KEYHOLE_UNAME_S=Linux KEYHOLE_UNAME_M=riscv64
expect_reject "unknown flavor" KEYHOLE_UNAME_S=Linux KEYHOLE_UNAME_M=x86_64 KEYHOLE_INSTALL_FLAVOR=static

echo "== end-to-end install against a staged local release =="
work=$(mktemp -d "${TMPDIR:-/tmp}/keyhole-install-test.XXXXXX")
trap 'rm -rf "$work"' EXIT
stage="$work/release"
payload="$work/payload"
mkdir -p "$stage" "$payload"
target="x86_64-unknown-linux-gnu"
archive="keyhole-${target}.tar.gz"

printf '#!/bin/sh\necho "keyhole 9.9.9-test"\n' >"$payload/keyhole"
chmod +x "$payload/keyhole"
echo '.TH keyhole 1' >"$payload/keyhole.1"
cp LICENSE-MIT LICENSE-APACHE "$payload/"
(cd "$payload" && tar -czf "$stage/$archive" .)
(cd "$stage" && sha256sum "$archive" >"$archive.sha256")

run_install() {
    KEYHOLE_UNAME_S=Linux KEYHOLE_UNAME_M=x86_64 \
        KEYHOLE_INSTALL_BASE="file://$stage" \
        KEYHOLE_INSTALL_DIR="$1/bin" \
        KEYHOLE_INSTALL_MANDIR="$1/man" \
        sh "$INSTALLER"
}

good="$work/good"
if run_install "$good" >/dev/null 2>&1; then
    if [ -x "$good/bin/keyhole" ]; then pass "binary installed and executable"; else bad "binary missing after install"; fi
    if [ -f "$good/man/keyhole.1" ]; then pass "man page installed"; else bad "man page missing after install"; fi
    if [ "$("$good/bin/keyhole")" = "keyhole 9.9.9-test" ]; then
        pass "installed binary runs"
    else
        bad "installed binary did not run as expected"
    fi
else
    bad "install with a valid checksum failed"
fi

echo "== cosign signature verification =="
# Stub cosign on PATH: the installer only calls `verify-blob`, so the stub just
# exits per STUB_COSIGN_EXIT — exercising the pass/fail branches without sigstore
# or network access.
mkdir -p "$work/stubbin"
cat >"$work/stubbin/cosign" <<'STUB'
#!/bin/sh
[ "$1" = "verify-blob" ] && exit "${STUB_COSIGN_EXIT:-0}"
exit 0
STUB
chmod +x "$work/stubbin/cosign"

# Publish a (dummy) detached signature + certificate next to the archive.
echo "dummy-signature" >"$stage/$archive.sig"
echo "dummy-certificate" >"$stage/$archive.pem"

cosign_install() {
    # $1 = install root, $2 = stub `verify-blob` exit code.
    env PATH="$work/stubbin:$PATH" STUB_COSIGN_EXIT="$2" \
        KEYHOLE_UNAME_S=Linux KEYHOLE_UNAME_M=x86_64 \
        KEYHOLE_INSTALL_BASE="file://$stage" \
        KEYHOLE_INSTALL_DIR="$1/bin" KEYHOLE_INSTALL_MANDIR="$1/man" \
        sh "$INSTALLER"
}

# (a) cosign present + valid signature -> installs.
cdir="$work/cosign-ok"
if cosign_install "$cdir" 0 >/dev/null 2>&1 && [ -x "$cdir/bin/keyhole" ]; then
    pass "valid cosign signature: installed"
else
    bad "valid cosign signature: expected a successful install"
fi

# (b) cosign present + invalid signature -> aborts, nothing installed.
cdir="$work/cosign-bad"
if cosign_install "$cdir" 1 >/dev/null 2>&1; then
    bad "invalid cosign signature: install should have aborted"
elif [ -e "$cdir/bin/keyhole" ]; then
    bad "invalid cosign signature: binary installed despite failed verification"
else
    pass "invalid cosign signature aborted, nothing installed"
fi

# (c) cosign present but NO signature published -> soft skip, still installs via
# the checksum. STUB_COSIGN_EXIT=1 proves the skip happens before cosign runs.
rm -f "$stage/$archive.sig" "$stage/$archive.pem"
cdir="$work/cosign-missing"
if cosign_install "$cdir" 1 >/dev/null 2>&1 && [ -x "$cdir/bin/keyhole" ]; then
    pass "missing signature: soft-skipped, installed via checksum"
else
    bad "missing signature: expected a successful (checksum-only) install"
fi

echo "== corrupted checksum aborts =="
echo "deadbeef  $archive" >"$stage/$archive.sha256"
bad_dir="$work/corrupt"
if run_install "$bad_dir" >/dev/null 2>&1; then
    bad "install proceeded despite a bad checksum"
elif [ -e "$bad_dir/bin/keyhole" ]; then
    bad "binary installed despite a bad checksum"
else
    pass "bad checksum aborted, nothing installed"
fi

echo
if [ "$fail" -eq 0 ]; then
    echo "install.sh: all tests passed"
else
    echo "install.sh: FAILURES above" >&2
    exit 1
fi
