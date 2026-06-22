# Packaging (Tier 2 — community package managers)

These are the source-of-truth definitions for Keyhole's community packages. They
live in the main repo so they version together with the code and are validated
in CI; publishing them to their external homes is a per-release follow-up.

| Channel  | Files                                  | Install command |
|----------|----------------------------------------|-----------------|
| AUR      | `aur/keyhole/`, `aur/keyhole-bin/`     | `paru -S keyhole` / `keyhole-bin` |
| Homebrew | `homebrew/keyhole.rb`                  | `brew install AlexKasapis/tap/keyhole` |
| Nix      | `../flake.nix`, `../flake.lock`        | `nix run github:AlexKasapis/Keyhole` |

## Feature policy

The AUR `keyhole`/`keyhole-bin` and the Homebrew/Nix builds all ship the **full
glibc feature set** (keyring + AMQP + RabbitMQ), matching the `gnu` release
tarballs. The dependency-free static build is the `musl` release tarball — see
the README "Prebuilt binaries" section. The OS-keyring backend is pure-Rust
zbus, so it needs a Secret Service provider (e.g. `gnome-keyring`) on the session
bus at runtime, declared as an optional dependency rather than a hard one.

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

## Testing

`scripts/test_packaging.sh` (via `just test-packaging`, and in CI's
`release-lint` job) validates every artifact: PKGBUILD/formula structure,
version consistency with `Cargo.toml`, `.SRCINFO` sync, and the release
generator's round-trip. It uses only bash + ruby, and additionally runs
`makepkg`/`brew`/`nix` validators when those tools are installed. CI's dedicated
`nix` job evaluates the flake and builds + smoke-runs the package.
