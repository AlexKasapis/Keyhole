# Keyhole — Linux Distribution Plan

## Context

Keyhole is a well-structured Rust binary crate (`keyhole`) with a clap CLI, but it has **zero distribution infrastructure**: no LICENSE files (despite `license = "MIT OR Apache-2.0"` in `Cargo.toml`), incomplete package metadata, no release automation (CI builds and tests but never uploads artifacts or triggers on tags), and no installation path for end users. The goal is to make it installable on Linux through every realistic channel — downloadable binaries from a website (GitHub Releases), `cargo install`, community package managers (AUR, Homebrew, Nix), distro-native packages (`.rpm` for zypper/dnf, `.deb` for apt), and eventually official distro repositories.

Repo remote is `github.com:AlexKasapis/Keyhole`, so GitHub Releases is the natural artifact host.

**Key technical nuance that shapes everything below:** the default build links glibc and depends on D-Bus / Secret Service at runtime (the `keyring` feature → `zbus`). The `--no-default-features` build (already wired in CI for `x86_64-unknown-linux-musl`) is fully static, has no D-Bus dependency, but drops keyring, AMQP, and RabbitMQ support. So every package must choose a feature profile and declare runtime deps accordingly.

The work is organized in tiers by effort and by how much is in our control. Tiers 0–2 are fully self-serve and should ship first; Tier 3 produces installable distro packages hosted on Releases; Tier 4 is official-repo inclusion (partly gated by external maintainers, with self-serve stopgaps).

---

## Tier 0 — Foundations (prerequisite for every channel)

These are cheap and unblock everything else.

1. **LICENSE files** — add `LICENSE-MIT` and `LICENSE-APACHE` at repo root (standard dual-license texts). `Cargo.toml` already declares the SPDX expression; the files are mandatory for crates.io, every distro packager, and the AUR/Nix/Homebrew formulas.
2. **`Cargo.toml` metadata** — add the fields packagers and crates.io expect, currently all missing:
   - `authors = ["Alex Kasapis <akasapis@trigonongroup.com>"]`
   - `repository = "https://github.com/AlexKasapis/Keyhole"`
   - `homepage = "https://github.com/AlexKasapis/Keyhole"`
   - `documentation` (docs.rs auto-populates for libs; for a bin, point at the repo or README)
   - optional `include = [...]` to keep the published crate slim (exclude `docker-compose.yml`, `scripts/tui_smoke.py`, snapshot fixtures if large).
3. **`CHANGELOG.md`** — Keep-a-Changelog format. cargo-dist extracts release notes from it; distro packagers reference it.
4. **README "Installation" section** — replace the dev-only quick-start with a real install section (cargo install, distro one-liners, download links). Start as placeholders; fill URLs as each channel goes live.
5. **Man page + shell completions generation** — add a small build path using `clap_mangen` and `clap_complete` (a `build.rs` or a `cargo xtask gen-man`/`gen-completions` binary that introspects the existing `cli.rs` clap `Command`). These artifacts are reused by the `.deb`/`.rpm`/AUR/Homebrew/Nix packages and improve quality across the board. The CLI is already a clean derive-based clap definition in `src/cli.rs`, so this is mechanical.

**Files:** new `LICENSE-MIT`, `LICENSE-APACHE`, `CHANGELOG.md`; edit `Cargo.toml`, `README.md`; new `build.rs` or `xtask/` for man/completions; reuse the clap `Command` from `src/cli.rs`.

---

## Tier 1 — Self-serve, ship first

### 1a. crates.io (`cargo install keyhole`)
- After Tier 0, run `cargo publish --dry-run`, fix any warnings, then `cargo publish`.
- Gives Rust users `cargo install keyhole` (builds default features → needs glibc + D-Bus headers at build time; document the `--no-default-features` escape hatch for headless installs).
- **Testing:** `cargo publish --dry-run`, then `cargo install --path .` in a clean container to confirm a from-scratch build+install works.

### 1b. GitHub Releases via cargo-dist (`dist`) — the "download from a website" path
- Adopt **cargo-dist** (`dist init`) — the modern all-in-one release tool. It generates `.github/workflows/release.yml` and a `[workspace.metadata.dist]` / `dist-workspace.toml` config, and on a `v*` tag push it cross-compiles, builds tarballs + `.sha256` checksums, generates a `curl --proto '=https' --tlsv1.2 -LsSf <url> | sh` shell installer, and creates the GitHub Release with notes pulled from `CHANGELOG.md`.
- **Target matrix:** `x86_64-unknown-linux-gnu`, `aarch64-unknown-linux-gnu` (full features), plus `x86_64-unknown-linux-musl` and `aarch64-unknown-linux-musl` (static, `--no-default-features`). Configure per-target features in the dist config so musl targets drop `keyring`/`amqp`/`rabbitmq` — mirrors the existing CI `musl` job (`.github/workflows/ci.yml:88`).
- cargo-dist can also emit a **Homebrew formula** (Tier 2) and `.deb`/`.rpm` (Tier 3) from the same config, so it becomes the hub for most channels.
- **Release process:** introduce `cargo-release` (or a `just release` recipe) to bump version, update `CHANGELOG.md`, tag `vX.Y.Z`, and push — which triggers the release workflow.
- **Testing:** push a `v0.1.0-rc.1` prerelease tag to a fork/branch, confirm artifacts + installer land on the Release page, then run the generated installer in a clean container and launch `keyhole --version`.

**Files:** new `.github/workflows/release.yml` (generated), `dist-workspace.toml` / `[workspace.metadata.dist]` in `Cargo.toml`, optional `release.toml`, `just release` recipe in `justfile`. Leave the existing `ci.yml` as the test/lint gate.

---

## Tier 2 — Community package managers (self-serve)

### 2a. AUR (Arch)
- Two packages, the Arch convention: `keyhole` (builds from source via `cargo`) and `keyhole-bin` (repackages the GitHub Release glibc tarball). Write `PKGBUILD` + `.SRCINFO`, publish to the AUR git remote. Declare runtime dep on `dbus`/secret-service for the keyring feature. Install the generated man page + completions.

### 2b. Homebrew (also works on Linux via Linuxbrew)
- Let cargo-dist generate and push the formula to a `AlexKasapis/homebrew-tap` repo. Users: `brew install AlexKasapis/tap/keyhole`.

### 2c. Nix
- Add a `flake.nix` exposing a `rustPlatform.buildRustPackage` derivation (with `nativeBuildInputs`/`buildInputs` for D-Bus when keyring is on). Gives `nix run github:AlexKasapis/Keyhole` and `nix profile install` immediately. Optionally submit to **nixpkgs** later (a lightly-reviewed PR, much faster than Debian/Fedora).

**Files:** new `aur/PKGBUILD` (+ published AUR repo), `homebrew-tap` repo (cargo-dist-managed), `flake.nix` + `flake.lock`.

---

## Tier 3 — Distro-native packages on GitHub Releases (zypper / dnf / apt from a file)

This gives openSUSE/Fedora/Debian users a real `zypper install ./keyhole.rpm` / `dnf install` / `apt install ./keyhole.deb` experience **without** waiting on official-repo inclusion.

- **`.rpm`** (zypper + dnf): `cargo-generate-rpm` or cargo-dist's RPM support. Bundle the binary, man page, completions, LICENSE files; declare runtime `Requires` (e.g. `dbus-1`/`libsecret` for the keyring feature, or ship a no-keyring rpm variant).
- **`.deb`** (apt): `cargo-deb`. Same contents; set `Depends` on `libdbus-1-3` etc.
- Wire both into the release workflow so each tag uploads `.rpm` + `.deb` alongside the tarballs.
- Decide the feature policy for these packages: recommended default is the **full glibc build** (keyring/amqp/rabbitmq on) with declared D-Bus deps, since distro users have a normal desktop/session bus; offer the static musl tarball as the dependency-free fallback.
- **Testing:** in openSUSE/Fedora/Debian containers, install the produced package, run `keyhole --version` and `man keyhole`, confirm dependency resolution.

**Files:** `[package.metadata.deb]` and `[package.metadata.generate-rpm]` sections in `Cargo.toml`; extend the release workflow.

---

## Tier 4 — Official distro repositories (gated; self-serve stopgaps first)

Honest framing: native inclusion in Debian/Ubuntu and Fedora is months-long and depends on external maintainers/review. openSUSE is the most accessible of the three. Pursue the self-serve build-service repos first — they let users `addrepo` and get automatic updates.

- **openSUSE (zypper, native):** use the **openSUSE Build Service (OBS)** on build.opensuse.org — create a project, add a `keyhole.spec`, OBS builds the RPM and publishes a repo users add via `zypper addrepo`. Self-serve. Submitting to Tumbleweed/Factory adds a review step.
- **Fedora (dnf):** self-serve via **COPR** (`dnf copr enable AlexKasapis/keyhole`) immediately; official Fedora requires a package review (Bugzilla) + sponsor.
- **Debian/Ubuntu (apt):** self-serve via an **Ubuntu PPA** (or a hosted apt repo) immediately; official Debian requires an ITP bug + a Debian Developer sponsor + Debian-format packaging.
- The `.spec` (OBS/COPR) and Debian packaging reuse everything from Tier 0/3 (LICENSE, man page, completions, dependency declarations).

**Files:** `packaging/keyhole.spec` (OBS/COPR), `packaging/debian/` tree (PPA/Debian), each maintained in its respective external service.

---

## Recommended sequencing

1. **Tier 0** (LICENSE, metadata, CHANGELOG, man/completions) — unblocks all.
2. **Tier 1** (crates.io + cargo-dist GitHub Releases) — immediate "download + `cargo install`".
3. **Tier 2** (AUR, Homebrew, Nix) — broad self-serve reach, mostly cargo-dist-driven.
4. **Tier 3** (`.rpm`/`.deb` on Releases) — covers zypper/dnf/apt by file install.
5. **Tier 4** (OBS/COPR/PPA, then official repos) — long-tail, partly external.

## Cross-cutting decisions to settle during implementation

- **Feature policy per artifact:** full glibc builds carry keyring/amqp/rabbitmq + declared D-Bus deps; static musl builds are `--no-default-features`. Document both clearly so users pick the right one.
- **Versioning/release flow:** adopt `cargo-release` or a `just release` recipe so a single tag drives every downstream channel.
- **Per CLAUDE.md, ship tests with each step:** add CI jobs that build the `.deb`/`.rpm` and smoke-install them in distro containers; verify the generated man page/completions; keep `cargo publish --dry-run` green. Reuse the existing `scripts/tui_smoke.py` PTY harness to smoke-launch installed binaries.

## Verification (end-to-end)

- `cargo publish --dry-run` clean; `cargo install --path .` works in a clean glibc container.
- Prerelease tag (`v0.1.0-rc.1`) produces a GitHub Release with tarballs, checksums, shell installer, `.deb`, `.rpm`; the `curl | sh` installer installs a runnable binary.
- In openSUSE / Fedora / Debian containers: install the native package, run `keyhole --version`, `keyhole --help`, `man keyhole`, and a `tui_smoke.py` launch; confirm dependency resolution.
- `nix run github:AlexKasapis/Keyhole` and `brew install AlexKasapis/tap/keyhole` (Linuxbrew) both launch the TUI.
