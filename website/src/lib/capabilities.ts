// The per-broker capability matrix shown on the landing page.
//
// Each capability records support for [Redis, AMQP 1.0, RabbitMQ] and a detail
// line revealed on row hover.

export interface Capability {
  name: string;
  detail: string;
  /** Support for [Redis, AMQP 1.0, RabbitMQ]. */
  support: readonly [boolean, boolean, boolean];
}

export const CAPABILITIES: readonly Capability[] = [
  {
    name: 'Keyspace browser + value inspector',
    support: [true, false, false],
    detail:
      'Walk the Redis keyspace and inspect every value, with a live server-statistics band running alongside.',
  },
  {
    name: 'Read-only command console',
    support: [true, false, false],
    detail:
      'A pinned, read-only command console — every read path stays non-destructive, so you can probe safely in production.',
  },
  {
    name: 'Realtime tails',
    support: [true, true, true],
    detail:
      'Pub/sub, pattern pub/sub, streams, keyspace events and MONITOR for Redis; topic & queue tails for AMQP 1.0; exchange taps for RabbitMQ.',
  },
  {
    name: 'Record live tail → JSONL',
    support: [true, true, true],
    detail:
      'Record any live tail to a lossless JSONL file — one JSON object per line, RFC 3339 timestamps, browsable in-app and friendly to jq.',
  },
];

export const CELL_SUPPORTED = '✓';
export const CELL_UNSUPPORTED = '—';
/** Accent yellow for a supported cell. */
export const CELL_COLOR_SUPPORTED = '#fabd2f';
/** Muted tone for an unsupported cell. */
export const CELL_COLOR_UNSUPPORTED = '#504945';

export interface Cell {
  /** `✓` when supported, `—` otherwise. */
  mark: string;
  color: string;
}

/** Map a support boolean to the mark + color shown in a matrix cell. */
export function cell(supported: boolean): Cell {
  return supported
    ? { mark: CELL_SUPPORTED, color: CELL_COLOR_SUPPORTED }
    : { mark: CELL_UNSUPPORTED, color: CELL_COLOR_UNSUPPORTED };
}
