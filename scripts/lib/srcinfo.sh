#!/usr/bin/env bash
# Shared helper: render a PKGBUILD's .SRCINFO, the way `makepkg --printsrcinfo`
# does, without needing makepkg installed (it is unavailable off Arch and on the
# CI runners). Sourcing a PKGBUILD to read its fields is exactly how makepkg
# itself derives .SRCINFO, so the output matches for the field set these
# packages use (scalars, simple arrays, and per-arch source/checksum arrays).
#
# Usage:  source scripts/lib/srcinfo.sh; srcinfo_emit path/to/PKGBUILD
#
# Both scripts/gen_packaging.sh (to refresh committed .SRCINFO) and
# scripts/test_packaging.sh (to assert they are in sync) use this single source
# of truth, so the two can never disagree.

# Print one indented "key = value" line.
_srcinfo_kv() { printf '\t%s = %s\n' "$1" "$2"; }

# Emit every element of array variable $2 as "key = element" lines (no-op if the
# variable is unset). Uses a nameref so the array is expanded by reference.
_srcinfo_arr() {
    local key="$1" name="$2"
    declare -p "$name" >/dev/null 2>&1 || return 0
    local -n _ref="$name"
    local v
    for v in "${_ref[@]}"; do
        _srcinfo_kv "$key" "$v"
    done
}

# Emit scalar variable $2 as a single "key = value" line (no-op if unset/empty).
_srcinfo_scalar() {
    local key="$1" name="$2"
    declare -p "$name" >/dev/null 2>&1 || return 0
    local -n _ref="$name"
    [ -n "${_ref:-}" ] && _srcinfo_kv "$key" "$_ref"
}

# Print the .SRCINFO body from the PKGBUILD variables currently in scope. Field
# order mirrors makepkg's: scalar metadata, arch, license, the *depends family,
# provides/conflicts, options, then source/checksum arrays (arch-agnostic first,
# then each declared architecture's source immediately followed by its sums).
# pkgname/pkgver/arch/... are defined by the PKGBUILD that srcinfo_emit sources,
# so shellcheck cannot see their assignments (disable applies to the whole fn).
# shellcheck disable=SC2154
_srcinfo_print() {
    printf 'pkgbase = %s\n' "$pkgname"
    _srcinfo_scalar pkgdesc pkgdesc
    _srcinfo_scalar pkgver pkgver
    _srcinfo_scalar pkgrel pkgrel
    _srcinfo_scalar epoch epoch
    _srcinfo_scalar url url
    _srcinfo_arr arch arch
    _srcinfo_arr groups groups
    _srcinfo_arr license license
    _srcinfo_arr checkdepends checkdepends
    _srcinfo_arr makedepends makedepends
    _srcinfo_arr depends depends
    _srcinfo_arr optdepends optdepends
    _srcinfo_arr provides provides
    _srcinfo_arr conflicts conflicts
    _srcinfo_arr replaces replaces
    _srcinfo_arr options options
    _srcinfo_arr source source
    _srcinfo_arr sha256sums sha256sums
    local a
    for a in "${arch[@]}"; do
        _srcinfo_arr "source_$a" "source_$a"
        _srcinfo_arr "sha256sums_$a" "sha256sums_$a"
    done
    printf '\n'
    printf 'pkgname = %s\n' "$pkgname"
}

# srcinfo_emit <PKGBUILD>: source the PKGBUILD in a subshell (so its variables
# and functions never leak into the caller) and print its .SRCINFO to stdout.
srcinfo_emit() {
    local pkgbuild="$1"
    (
        # shellcheck disable=SC1090  # path is a runtime argument
        source "$pkgbuild"
        _srcinfo_print
    )
}
