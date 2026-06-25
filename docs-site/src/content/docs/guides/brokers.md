---
title: Supported brokers
description: What Keyhole can do with Redis, AMQP 1.0 and RabbitMQ.
---

Keyhole speaks to three broker families, all built into the same binary:

- **Redis**
- **AMQP 1.0** — ActiveMQ, Amazon MQ, RabbitMQ 4.x
- **RabbitMQ** — AMQP 0.9.1 (all versions)

## Capability matrix

| Capability                          | Redis | AMQP 1.0 | RabbitMQ |
| ----------------------------------- | :---: | :------: | :------: |
| Keyspace browser + value inspector  |  ✅   |    —     |    —     |
| Read-only command console           |  ✅   |    —     |    —     |
| Realtime tails                      |  ✅   |    ✅    |    ✅    |
| Record live tail → JSONL            |  ✅   |    ✅    |    ✅    |

## Redis

Browse the keyspace and inspect values, watch a live server-statistics band, and
run read-only commands from a pinned console. Realtime tails cover pub/sub,
pattern pub/sub, streams, keyspace events, and `MONITOR`.

## AMQP 1.0

Tail topics and queues. Destinations cannot be enumerated over the AMQP 1.0 wire,
so when a profile names its broker's web console Keyhole discovers destinations
over the Jolokia HTTP management API.

## RabbitMQ (AMQP 0.9.1)

Tap exchanges to watch messages flow in realtime.

:::note
Every read path is non-destructive. The only write is a deliberate,
capability-gated AMQP publish driven by an explicit keystroke.
:::
