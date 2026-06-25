---
title: Quick start
description: Launch the Keyhole TUI and connect to your first broker.
---

## Launch

```sh
keyhole
```

This opens the terminal UI. Quit with the usual `q` / `Esc` flow shown in the
footer key hints.

## Connection profiles

Connections are saved as TOML profiles in:

```
~/.config/keyhole/config.toml
```

Each profile names a broker and how to reach it. Broker passwords are **never**
written to this file in plaintext — they are resolved at connect time from an
environment variable, the OS keyring, or an interactive prompt.

A minimal Redis profile looks like:

```toml
[[connections]]
name  = "local-redis"
kind  = "redis"
url   = "redis://127.0.0.1:6379"
```

For AMQP 1.0 (ActiveMQ / Amazon MQ / RabbitMQ 4.x) and RabbitMQ (AMQP 0.9.1),
use the matching `kind` and an `amqp://` / `amqps://` URL. When an AMQP 1.0
profile also names its broker's web console (default port `8161`), Keyhole
enriches the browser with destination discovery over the Jolokia management API.

:::tip
Logs are written to `~/.local/share/keyhole/logs/` — useful when a connection
won't establish.
:::

## Where to go next

- [Supported brokers](/guides/brokers/) — what Keyhole can do per broker.
- [Recording streams](/guides/recording/) — capture a live tail to disk.
