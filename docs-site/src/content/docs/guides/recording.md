---
title: Recording streams
description: Capture a live tail to a JSONL file and replay it in the in-app viewer.
---

Any live tail in Keyhole — Redis pub/sub, streams, keyspace events or `MONITOR`;
AMQP 1.0 topic/queue tails; RabbitMQ exchange taps — can be recorded to disk.

## How it works

While a realtime tail is running, start a recording and Keyhole writes every
event to a **lossless JSONL** file (one JSON object per line, with RFC 3339
timestamps). Nothing is sampled or summarised, so the capture is a faithful
replay of what crossed the wire.

## Browsing recordings

Recorded files are browsable from the in-app **Recordings** viewer: pick a file
from the list pane and step through its events in the viewer pane. This makes a
recording reusable long after the live session has ended — for debugging,
sharing a repro, or comparing runs.

:::tip
Because recordings are plain JSONL, they also play nicely with command-line
tools — e.g. `jq` over the file outside Keyhole.
:::
