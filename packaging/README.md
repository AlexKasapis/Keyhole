# Packaging (community + distro packages)

These are the source-of-truth definitions for Keyhole's packages. They live in
the main repo so they version together with the code and are validated in CI.

| Channel  | Tier | Source of truth                        | Install command |
|----------|:----:|----------------------------------------|-----------------|
| AUR      | 2    | `aur/keyhole/`, `aur/keyhole-bin/`     | `paru -S keyhole` / `keyhole-bin` |
| Homebrew | 2    | `homebrew/keyhole.rb`                  | `brew install AlexKasapis/tap/keyhole` |
| Nix      | 2    | `../flake.nix`, `../flake.lock`        | `nix run github:AlexKasapis/Keyhole` |
| Debian / Ubuntu (`.deb`) | 3 | `[package.metadata.deb]` in `../Cargo.toml` | `apt install ./keyhole_*_amd64.deb` |
| openSUSE / Fedora (`.rpm`) | 3 | `[package.metadata.generate-rpm]` in `../Cargo.toml` | `zypper`/`dnf install ./keyhole-*.x86_64.rpm` |

The **Tier 2** community packages (AUR/Homebrew/Nix) need a per-release publish
step to their external homes. The **Tier 3** distro packages are built and
uploaded to the GitHub Release automatically — no follow-up — so users install
them straight from a file.

## Feature policy

The AUR `keyhole`/`keyhole-bin`, the Homebrew/Nix builds, and the Tier 3
`.deb`/`.rpm` all ship the **full feature set** (keyring + AMQP + RabbitMQ),
matching the release tarballs — these are always built in. The OS-keyring
backend is pure-Rust zbus, so it needs a Secret Service provider (e.g.
`gnome-keyring`) on the session bus at runtime, declared as an optional /
recommended dependency rather than a hard one.

## AUR

Two packages, per Arch convention:

- **`keyhole`** — builds from the GitHub source-tag tarball with `cargo`.
- **`keyhole-bin`** — repackages the prebuilt glibc release tarball (no compile).

Each ships the binary, the generated man page, shell completions (bash/zsh/fish),
and the licenses. `.SRCINFO` is generated from the `PKGBUILD`; never edit it by
hand — run `just gen-srcinfo` (or `scripts/gen_packaging.sh srcinfo`) after
changing a `PKGBUILD`.

The committed checksums are `'SKIP'` placeholders. At release time
`scripts/gen_packaging.sh` fills the real digests and the workflow attaches the
finished files to the GitHub Release (`keyhole.PKGBUILD`, `keyhole.SRCINFO`,
`keyhole-bin.PKGBUILD`, `keyhole-bin.SRCINFO`).

**Publish** (first time): create the AUR packages and push.

```sh
git clone ssh://aur@aur.archlinux.org/keyhole.git aur-keyhole
# copy the release's keyhole.PKGBUILD + keyhole.SRCINFO in as PKGBUILD/.SRCINFO
cd aur-keyhole && git add PKGBUILD .SRCINFO && git commit -m "keyhole X.Y.Z" && git push
# …and the same for keyhole-bin.
```

## Homebrew

`homebrew/keyhole.rb` uses the prebuilt glibc tarballs on Linux and builds from
source on macOS (no prebuilt macOS binary is published). The `version` and the
three `# @sha256:…`-tagged lines are filled at release time by the generator.

**Publish:** copy the release's `keyhole.rb` into the
[`AlexKasapis/homebrew-tap`](https://github.com/AlexKasapis/homebrew-tap) repo
under `Formula/keyhole.rb` and push. Then `brew install AlexKasapis/tap/keyhole`.

## Nix

The flake at the repo root exposes the package, an app (`nix run`), and a dev
shell. It reads the version from `Cargo.toml` and builds against the committed
`Cargo.lock`, so no release-time edits are needed — `nix run
github:AlexKasapis/Keyhole` works off any tagged commit immediately.

```sh
nix run    github:AlexKasapis/Keyhole          # run without installing
nix profile install github:AlexKasapis/Keyhole # install into your profile
nix build  github:AlexKasapis/Keyhole          # ./result/bin/keyhole
```

Submitting to **nixpkgs** later is an optional, lightly-reviewed follow-up.

## Updating the pinned nixpkgs

```sh
nix flake update            # bumps flake.lock to the latest nixos-unstable
nix flake check && nix build .#default
```

## Distro packages — `.deb` / `.rpm` (Tier 3)

The native distro packages have no files under `packaging/`; their definitions
are the `[package.metadata.deb]` (cargo-deb) and `[package.metadata.generate-rpm]`
(cargo-generate-rpm) sections in the top-level `Cargo.toml`. Both packagers read
the name/version/license/description straight from `[package]`, so — unlike the
AUR/Homebrew artifacts — there is nothing to version-stamp at release time.

Each package bundles the binary, the generated man page + bash/zsh/fish
completions, and the licenses, laid out per distro convention (e.g. zsh
completions under Debian's `vendor-completions` vs. Fedora/openSUSE's
`site-functions`). Hard dependencies are auto-detected from the ELF — cargo-deb
via `dpkg-shlibdeps` (`$auto`), cargo-generate-rpm via its builtin scan — so the
glibc floor is exact; the OS-keyring Secret Service daemon is a weak
`Recommends`/`gnome-keyring`, not a hard dependency.

The release workflow's `packages` job builds both (x86_64, full glibc features),
signs + checksums them alongside the tarballs, and uploads them to the Release.
Build them locally after a release build:

```sh
cargo build --release && keyhole gen man --out dist-assets   # + gen completions
just package                                                 # → target/debian, target/generate-rpm
```

Other architectures are served by the prebuilt `gnu` tarball or `cargo install`;
adding an aarch64 `.deb`/`.rpm` means a native `ubuntu-24.04-arm` workflow leg
(a cross-built package would carry a wrong dependency floor).

## Testing

`scripts/test_packaging.sh` (via `just test-packaging`, and in CI's
`release-lint` job) validates every artifact: PKGBUILD/formula structure,
version consistency with `Cargo.toml`, `.SRCINFO` sync, the release generator's
round-trip, and the `.deb`/`.rpm` metadata (payload coverage, per-distro paths,
dependency policy). It uses only bash + ruby, and additionally runs
`makepkg`/`brew`/`nix` and `cargo deb`/`cargo generate-rpm` validators when those
tools (and a release build) are present. CI's dedicated `nix` and `packages`
jobs build the real artifacts and smoke-install them — the `packages` job
`apt`/`dnf install`s the `.deb`/`.rpm` in Debian + Fedora and runs the binary.
