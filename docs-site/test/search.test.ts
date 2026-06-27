import { describe, it, expect } from 'vitest';
import { searchPages } from '../src/lib/search';

describe('searchPages', () => {
  it('returns nothing for an empty or whitespace query', () => {
    expect(searchPages('')).toEqual([]);
    expect(searchPages('   ')).toEqual([]);
  });

  it('matches on the page label, case-insensitively', () => {
    const results = searchPages('INSTALL');
    expect(results.map((r) => r.id)).toEqual(['installation']);
  });

  it('matches on the keyword index, not just the visible label', () => {
    // "jsonl" appears only in the Recording page's keywords.
    expect(searchPages('jsonl').map((r) => r.id)).toEqual(['recording']);
    // "toml" → Configuration; "pubsub" → Realtime tails.
    expect(searchPages('toml').map((r) => r.id)).toEqual(['config']);
    expect(searchPages('pubsub').map((r) => r.id)).toEqual(['tails']);
  });

  it('returns every matching page in sidebar order', () => {
    // "tail" is a keyword of both Quickstart and Realtime tails.
    expect(searchPages('tail').map((r) => r.id)).toEqual(['quickstart', 'tails']);
  });

  it('returns the fields the dropdown renders', () => {
    const [result] = searchPages('palette');
    expect(result).toEqual({
      id: 'palette',
      label: 'Command palette',
      group: 'Reference',
      href: '/palette',
    });
  });

  it('returns an empty list when nothing matches', () => {
    expect(searchPages('xyzzy')).toEqual([]);
  });
});
