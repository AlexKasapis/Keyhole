import { describe, it, expect } from 'vitest';
import { KEYBINDINGS, filterKeybindings } from '../src/lib/keybindings';

describe('keybinding data', () => {
  it('has the source-verified groups, slugs matching the page TOC', () => {
    expect(KEYBINDINGS.map((g) => g.slug)).toEqual([
      'global',
      'nav',
      'browse',
      'feeds',
      'console',
      'manage',
    ]);
  });

  it('gives every binding at least one keycap and a description', () => {
    for (const group of KEYBINDINGS) {
      for (const b of group.bindings) {
        expect(b.keys.length).toBeGreaterThan(0);
        expect(b.desc.length).toBeGreaterThan(0);
      }
    }
  });
});

describe('filterKeybindings', () => {
  it('returns every group unfiltered for an empty query', () => {
    const all = filterKeybindings('');
    expect(all).toHaveLength(KEYBINDINGS.length);
    expect(all.map((g) => g.bindings.length)).toEqual(KEYBINDINGS.map((g) => g.bindings.length));
  });

  it('matches on the description, dropping groups with no surviving rows', () => {
    const groups = filterKeybindings('pause');
    expect(groups.map((g) => g.slug)).toEqual(['feeds']);
    expect(groups[0].bindings.map((b) => b.desc)).toEqual(['Play / pause the feed']);
  });

  it('matches on the keycap labels too', () => {
    const groups = filterKeybindings('esc');
    expect(groups.map((g) => g.slug)).toEqual(['global']);
    expect(groups[0].bindings).toHaveLength(1);
    expect(groups[0].bindings[0].desc.startsWith('Back')).toBe(true);
  });

  it('is case-insensitive', () => {
    expect(filterKeybindings('QUIT').map((g) => g.slug)).toEqual(['global']);
  });

  it('returns nothing when no binding matches', () => {
    expect(filterKeybindings('zzz')).toEqual([]);
  });
});
