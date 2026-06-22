# Changelog

All notable changes to Keyhole are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0]

Initial release.

### Added

- **Redis** broker support: keyspace browser with an inline server-statistics
  band, value inspector, read-only command console, and realtime tails
  (pub/sub, pattern pub/sub, streams, keyspace notifications, and `MONITOR`).
- **AMQP 1.0** support (ActiveMQ / Amazon MQ / RabbitMQ 4.x): non-destructive
  topic and queue tailing, with `amqps://` TLS. Optional `amqp` feature.
- **RabbitMQ** (AMQP 0.9.1) support: non-destructive exchange taps via a
  temporary, auto-deleting bound queue; virtual hosts and `amqps://` TLS.
  Optional `rabbitmq` feature.
- **Recording**: record any live tail to a lossless JSONL file, toggleable
  while it runs; export a finished recording to CSV.
- **Headless mode**: `keyhole record` and `keyhole export` reuse the broker and
  recording stack without a terminal.
- Connection profiles stored in TOML with comment-preserving in-app edits;
  secrets are resolved at connect time via env var, OS keyring, or interactive
  prompt (never stored in plaintext).
- Command palette, help overlay, sidebar connection/keyspace tree, mouse
  support, `dark`/`light` theming with per-style overrides (honours `NO_COLOR`),
  and file-only logging.

[Unreleased]: https://github.com/AlexKasapis/Keyhole/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/AlexKasapis/Keyhole/releases/tag/v0.1.0
