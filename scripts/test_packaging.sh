#!/usr/bin/env bash
# Tests for the Tier-2 community packaging artifacts: the AUR PKGBUILDs
# (keyhole + keyhole-bin) and their .SRCINFO, the Homebrew formula, and the Nix
# flake. Tool-independent by default — it validates structure, version
# consistency, .SRCINFO sync, and the release-time generator round-trip using
# only bash + ruby. If makepkg / brew / nix happen to be installed, the relevant
# native validators run too; otherwise they are skipped (mirroring how the
# release-lint job treats actionlint).
#
# Usage:
#   scripts/test_packaging.sh        Run all packaging tests.
#   scripts/test_packaging.sh -h     Show this help.
set -euo pipefail

cd "$(dirname "$0")/.."
PKG=packaging
# shellcheck source=scripts/lib/srcinfo.sh
. scripts/lib/srcinfo.sh

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
skip() { printf 'skip - %s\n' "$1"; }

CARGO_VERSION=$(grep -m1 -E '^version = ' Cargo.toml | sed -E 's/.*"([^"]+)".*/\1/')
[ -n "$CARGO_VERSION" ] || {
    echo "could not read package version from Cargo.toml" >&2
    exit 1
}
echo "Cargo.toml package version: $CARGO_VERSION"

# Read a scalar variable from a PKGBUILD by sourcing it in a subshell, so the
# PKGBUILD's variables and functions never leak into the test process.
pkgbuild_var() {
    # $1 = PKGBUILD path, $2 = variable name
    (
        # shellcheck disable=SC1090  # path is a runtime argument
        source "$1"
        printf '%s' "${!2-}"
    )
}

echo "== AUR PKGBUILD structure =="
for pkg in keyhole keyhole-bin; do
    pb="$PKG/aur/$pkg/PKGBUILD"
    if [ ! -f "$pb" ]; then
        bad "$pkg: PKGBUILD missing"
        continue
    fi
    if bash -n "$pb" 2>/dev/null; then pass "$pkg: PKGBUILD is valid bash"; else bad "$pkg: PKGBUILD has a syntax error"; fi
    # Required metadata + a package() function.
    missing=""
    for v in pkgname pkgver pkgrel pkgdesc arch url license; do
        [ -n "$(pkgbuild_var "$pb" "$v")" ] || missing="$missing $v"
    done
    [ -z "$missing" ] && pass "$pkg: has required fields" || bad "$pkg: missing fields:$missing"
    if grep -qE '^[[:space:]]*package\(\)' "$pb"; then pass "$pkg: defines package()"; else bad "$pkg: no package() function"; fi
    # pkgname must equal the directory name.
    [ "$(pkgbuild_var "$pb" pkgname)" = "$pkg" ] && pass "$pkg: pkgname matches dir" || bad "$pkg: pkgname != $pkg"
done

echo "== version consistency =="
for pkg in keyhole keyhole-bin; do
    pv=$(pkgbuild_var "$PKG/aur/$pkg/PKGBUILD" pkgver)
    [ "$pv" = "$CARGO_VERSION" ] && pass "$pkg pkgver=$pv matches Cargo.toml" || bad "$pkg pkgver=$pv != $CARGO_VERSION"
done
hb_version=$(grep -m1 -E '^  version "' "$PKG/homebrew/keyhole.rb" | sed -E 's/.*"([^"]+)".*/\1/')
[ "$hb_version" = "$CARGO_VERSION" ] && pass "homebrew version=$hb_version matches Cargo.toml" || bad "homebrew version=$hb_version != $CARGO_VERSION"

echo "== .SRCINFO is in sync with PKGBUILD =="
for pkg in keyhole keyhole-bin; do
    pb="$PKG/aur/$pkg/PKGBUILD"
    si="$PKG/aur/$pkg/.SRCINFO"
    if [ ! -f "$si" ]; then
        bad "$pkg: .SRCINFO missing (run: scripts/gen_packaging.sh srcinfo)"
        continue
    fi
    if diff -u "$si" <(srcinfo_emit "$pb") >/dev/null 2>&1; then
        pass "$pkg: .SRCINFO matches PKGBUILD"
    else
        bad "$pkg: .SRCINFO is stale (run: scripts/gen_packaging.sh srcinfo)"
    fi
done

echo "== Homebrew formula =="
formula="$PKG/homebrew/keyhole.rb"
if ruby -c "$formula" >/dev/null 2>&1; then pass "formula is valid Ruby"; else bad "formula has a Ruby syntax error"; fi
for anchor in '@sha256:linux-x86_64' '@sha256:linux-aarch64' '@sha256:src'; do
    grep -q "$anchor" "$formula" && pass "formula has $anchor checksum anchor" || bad "formula missing $anchor anchor"
done
grep -qE '^  def install' "$formula" && pass "formula defines install" || bad "formula has no install method"
grep -qE '^  test do' "$formula" && pass "formula defines a test block" || bad "formula has no test block"

echo "== Nix flake =="
if [ -f flake.nix ] && [ -f flake.lock ]; then
    pass "flake.nix + flake.lock present"
else
    bad "flake.nix and/or flake.lock missing"
fi
for needle in 'buildRustPackage' 'cargoLock' 'mainProgram'; do
    grep -q "$needle" flake.nix && pass "flake.nix references $needle" || bad "flake.nix missing $needle"
done
if ruby -rjson -e 'JSON.parse(File.read("flake.lock"))' >/dev/null 2>&1; then
    pass "flake.lock is valid JSON"
else
    bad "flake.lock is not valid JSON"
fi

echo "== release generator round-trip =="
work=$(mktemp -d "${TMPDIR:-/tmp}/keyhole-pkg-test.XXXXXX")
trap 'rm -rf "$work"' EXIT
cp -r "$PKG" "$work/packaging"
H1=$(printf 'a%.0s' {1..64})
H2=$(printf 'b%.0s' {1..64})
H3=$(printf 'c%.0s' {1..64})
if scripts/gen_packaging.sh release \
    --version 9.9.9 --sha-x86_64 "$H1" --sha-aarch64 "$H2" --sha-src "$H3" \
    --dir "$work/packaging" >/dev/null 2>&1; then
    pass "generator ran"
else
    bad "generator failed"
fi

gbin="$work/packaging/aur/keyhole-bin/PKGBUILD"
gsrc="$work/packaging/aur/keyhole/PKGBUILD"
gformula="$work/packaging/homebrew/keyhole.rb"

# Version bumped everywhere.
[ "$(pkgbuild_var "$gbin" pkgver)" = "9.9.9" ] && pass "keyhole-bin version bumped" || bad "keyhole-bin version not bumped"
[ "$(pkgbuild_var "$gsrc" pkgver)" = "9.9.9" ] && pass "keyhole version bumped" || bad "keyhole version not bumped"
grep -q '^  version "9.9.9"' "$gformula" && pass "homebrew version bumped" || bad "homebrew version not bumped"

# Real checksums injected into the right slots.
grep -q "sha256sums_x86_64=('$H1')" "$gbin" && pass "keyhole-bin x86_64 sum injected" || bad "keyhole-bin x86_64 sum wrong"
grep -q "sha256sums_aarch64=('$H2')" "$gbin" && pass "keyhole-bin aarch64 sum injected" || bad "keyhole-bin aarch64 sum wrong"
grep -q "sha256sums=('$H3')" "$gsrc" && pass "keyhole source sum injected" || bad "keyhole source sum wrong"
grep -q "sha256 \"$H1\" # @sha256:linux-x86_64" "$gformula" && pass "homebrew x86_64 sum injected" || bad "homebrew x86_64 sum wrong"
grep -q "sha256 \"$H2\" # @sha256:linux-aarch64" "$gformula" && pass "homebrew aarch64 sum injected" || bad "homebrew aarch64 sum wrong"
grep -q "sha256 \"$H3\" # @sha256:src" "$gformula" && pass "homebrew src sum injected" || bad "homebrew src sum wrong"

# No placeholders survive, and the outputs are still well-formed. Match the
# checksum-array assignment form so the explanatory `'SKIP'` in the header
# comment is not counted.
if grep -qE "^sha256sums(_[a-z0-9]+)?=\('SKIP'\)" "$gbin" "$gsrc"; then bad "a 'SKIP' placeholder survived in a PKGBUILD"; else pass "no SKIP placeholders remain"; fi
if grep -q "0000000000000000" "$gformula"; then bad "a zero-hash placeholder survived in the formula"; else pass "no zero-hash placeholders remain"; fi
bash -n "$gbin" && bash -n "$gsrc" && pass "generated PKGBUILDs are valid bash" || bad "a generated PKGBUILD broke"
ruby -c "$gformula" >/dev/null 2>&1 && pass "generated formula is valid Ruby" || bad "generated formula broke"
# Regenerated .SRCINFO reflects the new version + sums.
diff -u "$work/packaging/aur/keyhole-bin/.SRCINFO" <(srcinfo_emit "$gbin") >/dev/null 2>&1 &&
    grep -q "pkgver = 9.9.9" "$work/packaging/aur/keyhole-bin/.SRCINFO" &&
    grep -q "sha256sums_x86_64 = $H1" "$work/packaging/aur/keyhole-bin/.SRCINFO" &&
    pass "generated .SRCINFO is consistent" || bad "generated .SRCINFO inconsistent"

echo "== optional native validators =="
if command -v makepkg >/dev/null 2>&1; then
    for pkg in keyhole keyhole-bin; do
        if diff -u "$PKG/aur/$pkg/.SRCINFO" <(cd "$PKG/aur/$pkg" && makepkg --printsrcinfo) >/dev/null 2>&1; then
            pass "$pkg: .SRCINFO matches makepkg --printsrcinfo"
        else
            bad "$pkg: .SRCINFO differs from makepkg --printsrcinfo"
        fi
    done
else
    skip "makepkg not installed (cannot cross-check .SRCINFO / build the package)"
fi
if command -v brew >/dev/null 2>&1; then
    brew audit --formula --strict "$formula" >/dev/null 2>&1 && pass "brew audit clean" || bad "brew audit reported issues"
else
    skip "brew not installed (cannot run brew audit)"
fi
if command -v nix >/dev/null 2>&1; then
    nix flake check --no-build >/dev/null 2>&1 && pass "nix flake check (eval) clean" || bad "nix flake check failed"
else
    skip "nix not installed (cannot evaluate/build the flake)"
fi

echo
if [ "$fail" -eq 0 ]; then
    echo "test_packaging: all tests passed"
else
    echo "test_packaging: FAILURES above" >&2
    exit 1
fi
