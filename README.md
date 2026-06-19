# BrokerTUI

A terminal UI to connect to message/data brokers — **Redis** first, **AMQP/RabbitMQ**
later — to browse their data, watch realtime activity, and **record live streams to
disk** for later inspection. One self-contained binary you can run locally or over SSH.

> **Status:** Phase 0 scaffold (terminal lifecycle, async event loop, file logging).
> See the implementation plan for the full roadmap. v1 (Phases 0–2) = connect →
> browse Redis fully → watch & record realtime.

## Quick start

```sh
just setup          # install the pinned Rust toolchain
docker compose up -d redis
just run            # cargo run
```

Press `q` or `Ctrl-C` to quit. Logs are written to
`~/.local/share/brokertui/logs/` (never stdout, since the TUI owns the terminal).

## Development

```sh
just fmt            # rustfmt
just lint           # clippy -D warnings
just test           # unit + snapshot tests
just test-int       # integration tests against a dockerized Redis
```

## License

MIT OR Apache-2.0
