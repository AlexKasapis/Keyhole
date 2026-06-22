#!/bin/sh
# Keyhole installer — download a prebuilt release tarball and install the binary.
#
# Usage (the hosted one-liner pipes this straight into sh):
#
#   curl --proto '=https' --tlsv1.2 -LsSf \
#     https://github.com/AlexKasapis/Keyhole/releases/latest/download/keyhole-installer.sh | sh
#
# It detects your OS/arch, downloads the matching tarball + its .sha256, verifies
# the checksum, and installs `keyhole` (plus the man page, best-effort).
#
# Two build flavors are published per architecture (see the README "Build
# variants" table). The default is the full glibc build (keyring + AMQP +
# RabbitMQ); the `musl` flavor is a dependency-free static binary (Redis only,
# env-var secrets), for headless/minimal hosts:
#
#   KEYHOLE_INSTALL_FLAVOR=musl sh keyhole-installer.sh
#
# Environment overrides:
#   KEYHOLE_INSTALL_FLAVOR  gnu (default) | musl       — which build to fetch
#   KEYHOLE_INSTALL_DIR     install prefix for the binary (default ~/.local/bin)
#   KEYHOLE_INSTALL_BASE    release asset base URL
#                           (default: .../releases/latest/download)
#   KEYHOLE_UNAME_S/_M      override `uname -s`/`uname -m` (testing only)
#
# Flags:
#   --print-target   print the resolved Rust target triple and exit (no install)
#   -h, --help       show this help and exit
set -eu

REPO="AlexKasapis/Keyhole"
BIN="keyhole"
DEFAULT_BASE="https://github.com/${REPO}/releases/latest/download"

say() { printf '%s\n' "$*"; }
err() { printf 'keyhole-installer: %s\n' "$*" >&2; }
die() {
    err "$*"
    exit 1
}

usage() {
    # Print the leading comment block (everything after the shebang up to the
    # first non-comment line) as help, stripping the leading "# ".
    awk 'NR==1 {next} /^#/ {sub(/^# ?/, ""); print; next} {exit}' "$0"
}

# Resolve the Rust target triple for this host + selected flavor. Honours the
# KEYHOLE_UNAME_* overrides so the detection logic is unit-testable without
# actually running on every platform.
resolve_target() {
    os="${KEYHOLE_UNAME_S:-$(uname -s)}"
    arch="${KEYHOLE_UNAME_M:-$(uname -m)}"
    flavor="${KEYHOLE_INSTALL_FLAVOR:-gnu}"

    case "$os" in
        Linux) ;;
        Darwin) die "macOS is not published yet; build from source: cargo install ${BIN}" ;;
        *) die "unsupported OS '${os}' (only Linux is published today)" ;;
    esac

    case "$arch" in
        x86_64 | amd64) rust_arch="x86_64" ;;
        aarch64 | arm64) rust_arch="aarch64" ;;
        *) die "unsupported architecture '${arch}' (x86_64 and aarch64 are published)" ;;
    esac

    case "$flavor" in
        gnu | musl) ;;
        *) die "unknown KEYHOLE_INSTALL_FLAVOR '${flavor}' (expected 'gnu' or 'musl')" ;;
    esac

    printf '%s-unknown-linux-%s' "$rust_arch" "$flavor"
}

# Download $1 to $2 using whichever of curl/wget is available. A local path or
# file:// URL (e.g. KEYHOLE_INSTALL_BASE pointing at a downloaded mirror) is
# copied directly — handy for air-gapped installs and for testing.
fetch() {
    case "$1" in
        file://*) cp "${1#file://}" "$2" && return 0 || return 1 ;;
        /*) cp "$1" "$2" && return 0 || return 1 ;;
    esac
    if command -v curl >/dev/null 2>&1; then
        curl --proto '=https' --tlsv1.2 -fLsS "$1" -o "$2"
    elif command -v wget >/dev/null 2>&1; then
        wget -qO "$2" "$1"
    else
        die "need curl or wget to download releases"
    fi
}

# Verify that file $1 matches the sha256 in checksum-file $2 (format: "<hex>  <name>").
verify_sha256() {
    file="$1"
    sumfile="$2"
    expected=$(awk '{print $1; exit}' "$sumfile")
    [ -n "$expected" ] || die "empty checksum file for $(basename "$file")"
    if command -v sha256sum >/dev/null 2>&1; then
        actual=$(sha256sum "$file" | awk '{print $1}')
    elif command -v shasum >/dev/null 2>&1; then
        actual=$(shasum -a 256 "$file" | awk '{print $1}')
    else
        err "no sha256 tool (sha256sum/shasum) found; skipping checksum verification"
        return 0
    fi
    [ "$actual" = "$expected" ] ||
        die "checksum mismatch for $(basename "$file"): expected ${expected}, got ${actual}"
}

main() {
    target=$(resolve_target)
    base="${KEYHOLE_INSTALL_BASE:-$DEFAULT_BASE}"
    install_dir="${KEYHOLE_INSTALL_DIR:-$HOME/.local/bin}"
    archive="${BIN}-${target}.tar.gz"
    url="${base}/${archive}"

    tmp=$(mktemp -d "${TMPDIR:-/tmp}/keyhole-install.XXXXXX") ||
        die "could not create a temp directory"
    # shellcheck disable=SC2064  # expand $tmp now, at trap-install time.
    trap "rm -rf '$tmp'" EXIT INT TERM

    say "Downloading ${archive} ..."
    fetch "$url" "${tmp}/${archive}"
    if fetch "${url}.sha256" "${tmp}/${archive}.sha256" 2>/dev/null; then
        verify_sha256 "${tmp}/${archive}" "${tmp}/${archive}.sha256"
        say "Checksum OK."
    else
        err "no .sha256 published for ${archive}; skipping checksum verification"
    fi

    (cd "$tmp" && tar -xzf "$archive") || die "failed to extract ${archive}"

    binsrc=$(find "$tmp" -type f -name "$BIN" -perm -u+x 2>/dev/null | head -n1)
    [ -n "$binsrc" ] || binsrc=$(find "$tmp" -type f -name "$BIN" 2>/dev/null | head -n1)
    [ -n "$binsrc" ] || die "archive did not contain the ${BIN} binary"

    mkdir -p "$install_dir" || die "could not create install dir ${install_dir}"
    install -m 0755 "$binsrc" "${install_dir}/${BIN}" 2>/dev/null ||
        { cp "$binsrc" "${install_dir}/${BIN}" && chmod 0755 "${install_dir}/${BIN}"; } ||
        die "could not install ${BIN} into ${install_dir}"
    say "Installed ${BIN} -> ${install_dir}/${BIN}"

    # Best-effort man page install; never fail the run over it.
    mansrc=$(find "$tmp" -type f -name "${BIN}.1" 2>/dev/null | head -n1)
    if [ -n "$mansrc" ]; then
        mandir="${KEYHOLE_INSTALL_MANDIR:-$HOME/.local/share/man/man1}"
        if mkdir -p "$mandir" 2>/dev/null && cp "$mansrc" "${mandir}/${BIN}.1" 2>/dev/null; then
            say "Installed man page -> ${mandir}/${BIN}.1"
        fi
    fi

    case ":${PATH}:" in
        *":${install_dir}:"*) ;;
        *) say "Note: ${install_dir} is not on your PATH. Add it, e.g.:
    export PATH=\"${install_dir}:\$PATH\"" ;;
    esac

    say "Done. Run '${BIN} --version' to check."
}

case "${1:-}" in
    -h | --help)
        usage
        exit 0
        ;;
    --print-target)
        resolve_target
        echo
        exit 0
        ;;
    "") main ;;
    *) die "unknown argument '$1' (try --help)" ;;
esac
