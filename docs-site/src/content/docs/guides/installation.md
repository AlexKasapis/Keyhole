---
title: Installation
description: Install Keyhole via the install script, Cargo, Nix, or distro packages.
---

Keyhole is a single self-contained binary. Pick the channel that fits your setup.
The [repository README][readme] is the authoritative reference, including the full
verification commands.

## Install script (quickest)

The canonical, verifiable one-liner points straight at the GitHub release:

```sh
curl --proto '=https' --tlsv1.2 -LsSf \
  https://github.com/AlexKasapis/Keyhole/releases/latest/download/keyhole-installer.sh | sh
```

It detects your OS/arch, downloads the matching tarball plus its checksum,
verifies the checksum (and the cosign signature, if `cosign` is installed), then
installs `keyhole`.

A shorter, convenient form is also served from the project domain — it `302`s to
the exact release asset above, so the verification story is identical:

```sh
curl --proto '=https' --tlsv1.2 -LsSf https://keyholetui.com/install.sh | sh
```

## Cargo

```sh
cargo install keyhole   # builds with keyring + AMQP + RabbitMQ
```

## Nix

The repository is a flake:

```sh
nix run github:AlexKasapis/Keyhole               # try it without installing
nix profile install github:AlexKasapis/Keyhole   # install into your profile
```

## Distro packages (.deb / .rpm)

Each release attaches a Debian/Ubuntu `.deb` and an openSUSE/Fedora `.rpm`
(x86_64). Install with your package manager so dependencies resolve:

```sh
sudo apt install ./keyhole_*_amd64.deb       # Debian / Ubuntu
sudo zypper install ./keyhole-*.x86_64.rpm   # openSUSE
sudo dnf install ./keyhole-*.x86_64.rpm      # Fedora
```

## Verifying a download

Every release artifact ships with a SHA-256 checksum, a sigstore/cosign keyless
signature, a SLSA build-provenance attestation, and a CycloneDX SBOM. See
[Verifying a download][verify] in the README for the exact `cosign verify-blob`
and `gh attestation verify` commands.

[readme]: https://github.com/AlexKasapis/Keyhole#readme
[verify]: https://github.com/AlexKasapis/Keyhole#verifying-a-download
