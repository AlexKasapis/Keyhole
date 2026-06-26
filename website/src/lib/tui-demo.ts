// Pure logic for the embedded, animated Keyhole TUI demo.
//
// This is a *mock* of the terminal app: it fabricates `orders.*` pattern-tail
// events on a timer. All functions are deterministic given an injected random
// source (`rng`, a `() => number` in [0, 1)) and clock (`now`, a `Date`), so the
// behaviour can be unit-tested without stubbing globals. The browser entry point
// (KeyholeTui.astro) injects `Math.random` and `new Date()`; tests inject their
// own. The server pre-render injects a seeded rng + fixed clock for a stable,
// reproducible first paint.

export interface TuiEvent {
  /** `HH:MM:SS.mmm`, generated at event creation. */
  ts: string;
  /** Channel name, right-padded / truncated to {@link SOURCE_WIDTH} cols. */
  source: string;
  /** JSON payload string. */
  payload: string;
}

export interface TuiState {
  /** Rolling buffer, newest last, capped at {@link MAX_EVENTS}. */
  events: TuiEvent[];
  received: number;
  recRecords: number;
  recBytes: number;
}

/** Rolling buffer size — only the most recent N events are kept. */
export const MAX_EVENTS = 7;
/** Source channel column width (mirrors the app's `pad_end(truncate(., 18), 18)`). */
export const SOURCE_WIDTH = 18;

export interface Channel {
  source: string;
  pay: (rng: () => number) => string;
}

/** The five `orders.*` channel templates the demo draws from. */
export const CHANNELS: readonly Channel[] = [
  {
    source: 'orders.created',
    pay: (r) => `{"id":${48000 + ((r() * 900) | 0)},"total":${(r() * 240 + 9).toFixed(2)},"cur":"EUR"}`,
  },
  {
    source: 'orders.created',
    pay: (r) => `{"id":${48000 + ((r() * 900) | 0)},"total":${(r() * 60 + 4).toFixed(2)},"cur":"GBP"}`,
  },
  {
    source: 'orders.paid',
    pay: (r) => `{"id":${48000 + ((r() * 900) | 0)},"ok":true}`,
  },
  {
    source: 'orders.shipped',
    pay: (r) => `{"id":${48000 + ((r() * 900) | 0)},"carrier":"dhl","eta":"2d"}`,
  },
  {
    source: 'orders.cancelled',
    pay: (r) => `{"id":${48000 + ((r() * 900) | 0)},"reason":"timeout"}`,
  },
];

/** Footer keybinding hints: `[key, action]` pairs. */
export const HINTS: ReadonlyArray<readonly [string, string]> = [
  ['↑↓→', 'nav'],
  ['z', 'all'],
  ['/', 'filter'],
  ['oO', 'sort'],
  ['Tab/Ctrl-↓', 'panel'],
  [':', 'palette'],
  ['?', 'help'],
  ['Esc', 'back'],
];

/** Counter starting values (match the design reference). */
export const INITIAL_STATE: TuiState = {
  events: [],
  received: 1284,
  recRecords: 312,
  recBytes: 18841,
};

/** Format a clock as `HH:MM:SS.mmm`, zero-padded. */
export function fmtTime(now: Date): string {
  const p = (n: number, l = 2): string => String(n).padStart(l, '0');
  return `${p(now.getHours())}:${p(now.getMinutes())}:${p(now.getSeconds())}.${p(now.getMilliseconds(), 3)}`;
}

/** Right-pad with spaces, or truncate with `…`, to exactly {@link SOURCE_WIDTH} cols. */
export function padSource(source: string): string {
  return source.length > SOURCE_WIDTH
    ? source.slice(0, SOURCE_WIDTH - 1) + '…'
    : source.padEnd(SOURCE_WIDTH, ' ');
}

/** Fabricate one event, drawing a channel at random. */
export function mkEvent(rng: () => number, now: Date): TuiEvent {
  const c = CHANNELS[Math.floor(rng() * CHANNELS.length)];
  return { ts: fmtTime(now), source: padSource(c.source), payload: c.pay(rng) };
}

/** Seed the initial rolling buffer with {@link MAX_EVENTS} events. */
export function seedEvents(rng: () => number, now: () => Date): TuiEvent[] {
  const seed: TuiEvent[] = [];
  for (let i = 0; i < MAX_EVENTS; i++) seed.push(mkEvent(rng, now()));
  return seed;
}

/** Advance one tick: append an event (keep the last {@link MAX_EVENTS}) and bump counters. */
export function step(state: TuiState, rng: () => number, now: Date): TuiState {
  return {
    events: [...state.events, mkEvent(rng, now)].slice(-MAX_EVENTS),
    received: state.received + 1,
    recRecords: state.recRecords + 1,
    recBytes: state.recBytes + (52 + ((rng() * 28) | 0)),
  };
}

/** Random loop delay in ms, 650–1600. */
export function nextDelay(rng: () => number): number {
  return 650 + rng() * 950;
}

/** `received` count with thousands separators. */
export function formatReceived(received: number): string {
  return received.toLocaleString('en-US');
}

/** Recorded bytes as KB, to one decimal. */
export function formatKb(recBytes: number): string {
  return (recBytes / 1024).toFixed(1);
}

/** The red recording label, e.g. `● REC 312 (18.4 KB) `. */
export function recLabel(recRecords: number, recBytes: number): string {
  return `● REC ${recRecords} (${formatKb(recBytes)} KB) `;
}

export interface Hint {
  /** Leading separator: a space for the first hint, ` · ` thereafter. */
  sep: string;
  key: string;
  act: string;
}

/** Hints with their leading separators. */
export function renderHints(): Hint[] {
  return HINTS.map(([key, act], i) => ({ sep: i === 0 ? ' ' : ' · ', key, act }));
}

/**
 * A small deterministic PRNG (mulberry32). Used for the server-rendered first
 * paint so the build output is reproducible; the client re-seeds with
 * `Math.random` on mount. Also handy in tests.
 */
export function makeSeededRng(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}
