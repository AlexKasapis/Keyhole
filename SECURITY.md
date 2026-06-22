# Security Policy

## Supported versions

Keyhole is pre-1.0. Security fixes target the latest released `0.x` version and
the `main` branch.

| Version       | Supported |
|---------------|:---------:|
| latest `0.x`  |     ✓     |
| older         |     ✗     |

## Reporting a vulnerability

Please report security issues **privately** — do not open a public issue.

- Preferred: GitHub [private vulnerability reporting][advisory]
  (the repo's *Security → Report a vulnerability*).
- Or email: apkasapis@gmail.com.

Expect an initial response within a few days. Once a fix is ready it is released
and a public advisory is published.

## Scope notes

- All broker operations are read-only / non-destructive by design.
- Broker passwords are never written to the config file in plaintext — they are
  resolved at connect time from an env var, the OS keyring, or an interactive
  prompt. Reports of any path that could leak a credential to disk or to the log
  files are especially welcome.

## Verifying releases

Release artifacts are checksummed, signed (sigstore/cosign keyless), and carry
SLSA build-provenance attestations, alongside a CycloneDX SBOM. See the
[README "Verifying a download"][verify] section for the exact commands.

[advisory]: https://github.com/AlexKasapis/Keyhole/security/advisories/new
[verify]: README.md#verifying-a-download
