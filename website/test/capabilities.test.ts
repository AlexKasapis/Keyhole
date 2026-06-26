import { describe, it, expect } from 'vitest';
import { CAPABILITIES, cell } from '../src/lib/capabilities';

describe('capability cell mapping', () => {
  it('maps a supported cell to ✓ in accent yellow', () => {
    expect(cell(true)).toEqual({ mark: '✓', color: '#fabd2f' });
  });

  it('maps an unsupported cell to — in the muted tone', () => {
    expect(cell(false)).toEqual({ mark: '—', color: '#504945' });
  });
});

describe('capability data', () => {
  it('lists the four capabilities', () => {
    expect(CAPABILITIES).toHaveLength(4);
  });

  it('marks the keyspace browser and command console as Redis-only', () => {
    const redisOnly = CAPABILITIES.filter((c) => c.support[0] && !c.support[1] && !c.support[2]);
    expect(redisOnly.map((c) => c.name)).toEqual([
      'Keyspace browser + value inspector',
      'Read-only command console',
    ]);
  });

  it('marks tails and recording as supported across all three brokers', () => {
    const allThree = CAPABILITIES.filter((c) => c.support.every(Boolean));
    expect(allThree.map((c) => c.name)).toEqual([
      'Realtime tails',
      'Record live tail → JSONL',
    ]);
  });

  it('gives every capability a non-empty detail line', () => {
    for (const c of CAPABILITIES) {
      expect(c.detail.length).toBeGreaterThan(0);
    }
  });
});
