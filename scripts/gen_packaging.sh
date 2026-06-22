#!/usr/bin/env bash
# Generate / refresh the Tier-2 community packaging artifacts (AUR + Homebrew).
#
# These artifacts pin a version and the sha256 checksums of release tarballs,
# which only exist once a `vX.Y.Z` tag has been built. The committed files carry
# placeholders ('SKIP' for AUR, 64 zeros for Homebrew); this script fills in the
# real values at release time (driven by .github/workflows/release.yml) and
# regenerates the .SRCINFO files so they never drift from the PKGBUILDs.
#
# Subcommands:
#   srcinfo [--dir DIR]
#       Regenerate packaging/aur/*/.SRCINFO from their PKGBUILDs. Run after
#       hand-editing a PKGBUILD. This is also what test_packaging.sh checks.
#
#   release --version V --sha-x86_64 H --sha-aarch64 H --sha-src H [--dir DIR]
#       Set the version and inject the three release checksums (the x86_64/aarch64
#       glibc tarballs and the source-tag tarball) across both PKGBUILDs and the
#       Homebrew formula, then regenerate the .SRCINFO files.
#
#   --help    Show this help.
#
# --dir DIR overrides the packaging tree to operate on (default: ./packaging),
# so tests can run against a throwaway copy without dirtying the work tree.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
# shellcheck source=scripts/lib/srcinfo.sh
. "$ROOT/scripts/lib/srcinfo.sh"

PKG_DIR="$ROOT/packaging"

die() {
    printf 'gen_packaging: %s\n' "$*" >&2
    exit 1
}

usage() {
    awk 'NR==1 {next} /^#/ {sub(/^# ?/, ""); print; next} {exit}' "$0"
}

# Validate $1 is a lowercase 64-char hex sha256 digest.
require_sha256() {
    case "$1" in
        *[!0-9a-f]* | "") die "not a sha256 hex digest: '$1'" ;;
    esac
    [ "${#1}" -eq 64 ] || die "sha256 must be 64 hex chars, got ${#1}: '$1'"
}

# Regenerate both AUR .SRCINFO files from their PKGBUILDs.
regen_srcinfo() {
    local pkg
    for pkg in keyhole keyhole-bin; do
        local dir="$PKG_DIR/aur/$pkg"
        [ -f "$dir/PKGBUILD" ] || die "missing $dir/PKGBUILD"
        srcinfo_emit "$dir/PKGBUILD" >"$dir/.SRCINFO"
        printf 'wrote %s/.SRCINFO\n' "$dir"
    done
}

cmd_srcinfo() {
    while [ $# -gt 0 ]; do
        case "$1" in
            --dir)
                PKG_DIR="$2"
                shift 2
                ;;
            *) die "unknown argument to srcinfo: $1" ;;
        esac
    done
    regen_srcinfo
}

cmd_release() {
    local version="" sha_x86="" sha_arm="" sha_src=""
    while [ $# -gt 0 ]; do
        case "$1" in
            --version)
                version="$2"
                shift 2
                ;;
            --sha-x86_64)
                sha_x86="$2"
                shift 2
                ;;
            --sha-aarch64)
                sha_arm="$2"
                shift 2
                ;;
            --sha-src)
                sha_src="$2"
                shift 2
                ;;
            --dir)
                PKG_DIR="$2"
                shift 2
                ;;
            *) die "unknown argument to release: $1" ;;
        esac
    done

    [ -n "$version" ] || die "release: --version is required"
    [ -n "$sha_x86" ] || die "release: --sha-x86_64 is required"
    [ -n "$sha_arm" ] || die "release: --sha-aarch64 is required"
    [ -n "$sha_src" ] || die "release: --sha-src is required"
    # A leading 'v' is a common slip when passing a tag; normalise it away.
    version="${version#v}"
    require_sha256 "$sha_x86"
    require_sha256 "$sha_arm"
    require_sha256 "$sha_src"

    local bin="$PKG_DIR/aur/keyhole-bin/PKGBUILD"
    local src="$PKG_DIR/aur/keyhole/PKGBUILD"
    local formula="$PKG_DIR/homebrew/keyhole.rb"
    local f
    for f in "$bin" "$src" "$formula"; do
        [ -f "$f" ] || die "missing $f"
    done

    # AUR keyhole-bin: version + the two prebuilt-tarball checksums. The RHS is
    # replaced wholesale (matching any prior value), so re-running is idempotent.
    sed -i -E \
        -e "s/^pkgver=.*/pkgver=$version/" \
        -e "s/^sha256sums_x86_64=\(.*\)/sha256sums_x86_64=('$sha_x86')/" \
        -e "s/^sha256sums_aarch64=\(.*\)/sha256sums_aarch64=('$sha_arm')/" \
        "$bin"

    # AUR keyhole (from source): version + the source-tag tarball checksum.
    sed -i -E \
        -e "s/^pkgver=.*/pkgver=$version/" \
        -e "s/^sha256sums=\(.*\)/sha256sums=('$sha_src')/" \
        "$src"

    # Homebrew: the formula version + the three sha256 lines, keyed by their
    # trailing `# @sha256:...` anchor comments.
    sed -i -E \
        -e "s/^  version \".*\"/  version \"$version\"/" \
        -e "s/sha256 \"[0-9a-f]*\"( # @sha256:linux-x86_64)/sha256 \"$sha_x86\"\1/" \
        -e "s/sha256 \"[0-9a-f]*\"( # @sha256:linux-aarch64)/sha256 \"$sha_arm\"\1/" \
        -e "s/sha256 \"[0-9a-f]*\"( # @sha256:src)/sha256 \"$sha_src\"\1/" \
        "$formula"

    regen_srcinfo
    printf 'gen_packaging: filled version %s and checksums into AUR + Homebrew artifacts\n' "$version"
}

case "${1:-}" in
    -h | --help | "")
        usage
        exit 0
        ;;
    srcinfo)
        shift
        cmd_srcinfo "$@"
        ;;
    release)
        shift
        cmd_release "$@"
        ;;
    *) die "unknown subcommand '$1' (try --help)" ;;
esac
