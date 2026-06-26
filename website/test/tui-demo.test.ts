import { describe, it, expect } from 'vitest';
import {
  CHANNELS,
  HINTS,
  INITIAL_STATE,
  MAX_EVENTS,
  SOURCE_WIDTH,
  fmtTime,
  padSource,
  mkEvent,
  seedEvents,
  step,
  nextDelay,
  formatReceived,
  formatKb,
  recLabel,
  renderHints,
  makeSeededRng,
  type TuiState,
} from '../src/lib/tui-demo';

const zero = () => 0;
const half = () => 0.5;

describe('fmtTime', () => {
  it('zero-pads to HH:MM:SS.mmm', () => {
    // Local-time constructor so the assertion is timezone-independent.
    expect(fmtTime(new Date(2026, 5, 26, 4, 2, 9, 5))).toBe('04:02:09.005');
    expect(fmtTime(new Date(2026, 0, 1, 23, 59, 59, 999))).toBe('23:59:59.999');
  });
});

describe('padSource', () => {
  it('right-pads a short channel to exactly SOURCE_WIDTH', () => {
    const out = padSource('orders.paid');
    expect(out).toBe('orders.paid       ');
    expect(out).toHaveLength(SOURCE_WIDTH);
  });

  it('truncates a long channel to 17 chars + an ellipsis', () => {
    const out = padSource('orders.created.eu.west.1.replica');
    expect(out).toHaveLength(SOURCE_WIDTH);
    expect(out.endsWith('…')).toBe(true);
    expect(out).toBe('orders.created.eu…');
  });
});

describe('mkEvent', () => {
  it('is deterministic given an injected rng + clock', () => {
    const now = new Date(2026, 5, 26, 14, 2, 9, 0);
    const ev = mkEvent(zero, now);
    expect(ev.ts).toBe('14:02:09.000');
    expect(ev.source).toBe(padSource('orders.created'));
    expect(ev.payload).toBe('{"id":48000,"total":9.00,"cur":"EUR"}');
  });

  it('selects the channel by floor(rng * CHANNELS.length)', () => {
    const ev = mkEvent(half, new Date(2026, 5, 26, 0, 0, 0, 0));
    expect(Math.floor(0.5 * CHANNELS.length)).toBe(2); // orders.paid
    expect(ev.source).toBe(padSource('orders.paid'));
    expect(ev.payload).toBe('{"id":48450,"ok":true}');
  });
});

describe('seedEvents', () => {
  it('produces exactly MAX_EVENTS events', () => {
    const events = seedEvents(makeSeededRng(1), () => new Date(2026, 5, 26, 12, 0, 0, 0));
    expect(events).toHaveLength(MAX_EVENTS);
    for (const ev of events) expect(ev.source).toHaveLength(SOURCE_WIDTH);
  });
});

describe('step', () => {
  const baseNow = new Date(2026, 5, 26, 14, 2, 9, 0);

  it('appends one event and bumps every counter', () => {
    const next = step(INITIAL_STATE, zero, baseNow);
    expect(next.events).toHaveLength(1);
    expect(next.received).toBe(INITIAL_STATE.received + 1);
    expect(next.recRecords).toBe(INITIAL_STATE.recRecords + 1);
    // rng()=0 → recBytes increment is exactly 52 (52 + (0*28|0)).
    expect(next.recBytes).toBe(INITIAL_STATE.recBytes + 52);
  });

  it('caps the rolling buffer at MAX_EVENTS, dropping the oldest', () => {
    const full: TuiState = {
      ...INITIAL_STATE,
      events: seedEvents(makeSeededRng(7), () => baseNow),
    };
    const marker = full.events[1];
    const next = step(full, zero, baseNow);
    expect(next.events).toHaveLength(MAX_EVENTS);
    // The first (oldest) event is dropped; the previous 2nd is now first.
    expect(next.events[0]).toBe(marker);
    expect(next.events[MAX_EVENTS - 1].payload).toBe('{"id":48000,"total":9.00,"cur":"EUR"}');
  });

  it('increments recorded bytes by 52–79', () => {
    for (const r of [0, 0.25, 0.5, 0.999]) {
      const next = step(INITIAL_STATE, () => r, baseNow);
      const delta = next.recBytes - INITIAL_STATE.recBytes;
      expect(delta).toBeGreaterThanOrEqual(52);
      expect(delta).toBeLessThanOrEqual(79);
    }
  });
});

describe('nextDelay', () => {
  it('stays within 650–1600 ms', () => {
    expect(nextDelay(zero)).toBe(650);
    expect(nextDelay(() => 1)).toBeCloseTo(1600);
    expect(nextDelay(half)).toBe(1125);
  });
});

describe('counter formatting', () => {
  it('formats received with thousands separators', () => {
    expect(formatReceived(1284)).toBe('1,284');
    expect(formatReceived(1000000)).toBe('1,000,000');
    expect(formatReceived(7)).toBe('7');
  });

  it('formats recorded bytes as KB to one decimal', () => {
    expect(formatKb(18841)).toBe('18.4');
    expect(formatKb(1024)).toBe('1.0');
  });

  it('builds the recording label', () => {
    expect(recLabel(312, 18841)).toBe('● REC 312 (18.4 KB) ');
  });
});

describe('renderHints', () => {
  it('mirrors HINTS with a leading separator per item', () => {
    const hints = renderHints();
    expect(hints).toHaveLength(HINTS.length);
    expect(hints[0]).toEqual({ sep: ' ', key: '↑↓→', act: 'nav' });
    expect(hints[1].sep).toBe(' · ');
    expect(hints.every((h, i) => h.sep === (i === 0 ? ' ' : ' · '))).toBe(true);
  });
});

describe('makeSeededRng', () => {
  it('is deterministic for a given seed', () => {
    const a = makeSeededRng(12345);
    const b = makeSeededRng(12345);
    const seqA = [a(), a(), a()];
    const seqB = [b(), b(), b()];
    expect(seqA).toEqual(seqB);
  });

  it('returns values in [0, 1)', () => {
    const rng = makeSeededRng(99);
    for (let i = 0; i < 200; i++) {
      const v = rng();
      expect(v).toBeGreaterThanOrEqual(0);
      expect(v).toBeLessThan(1);
    }
  });

  it('produces different sequences for different seeds', () => {
    expect(makeSeededRng(1)()).not.toBe(makeSeededRng(2)());
  });
});
