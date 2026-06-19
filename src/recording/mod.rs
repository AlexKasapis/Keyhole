//! Recording subsystem.
//!
//! A `Recorder` consumes a broker's `BrokerEvent` stream — the same stream the
//! live view shows — and writes a binary-safe JSONL envelope to disk. The
//! recorder path is lossless (awaited), unlike the lossy UI path. Arrives in
//! Phase 2.
