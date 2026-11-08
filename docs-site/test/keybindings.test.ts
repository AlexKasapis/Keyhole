import { describe, it, expect } from 'vitest';
import { KEYBINDINGS, filterKeybindings, type Broker } from '../src/lib/keybindings';
import { PAGES, getPage } from '../src/lib/nav';

describe('keybinding data', () => {
  it('has the source-verified groups, slugs matching the page TOC', () => {
    expect(KEYBINDINGS.map((g) => g.slug)).toEqual([
      'global',
      'nav',
      'browse',
      'amqp',
      'feeds',
      'console',
      'manage',
    ]);
  });

  it('lists each group in the page TOC, in order and under its title', () => {
    expect(getPage('keybindings').toc).toEqual(KEYBINDINGS.map((g) => [g.slug, g.title]));
  });

  it('gives every binding at least one keycap and a description', () => {
    for (const group of KEYBINDINGS) {
      for (const b of group.bindings) {
        expect(b.keys.length).toBeGreaterThan(0);
        expect(b.desc.length).toBeGreaterThan(0);
      }
    }
  });

  it("gives each row in a group its own description (the page's filter keys rows on it)", () => {
    for (const group of KEYBINDINGS) {
      const descs = group.bindings.map((b) => b.desc);
      expect(new Set(descs).size, group.slug).toBe(descs.length);
    }
  });

  it('tags every destination-browser binding with its brokers, and nothing else', () => {
    for (const group of KEYBINDINGS) {
      for (const b of group.bindings) {
        if (group.slug === 'amqp') expect(b.brokers?.length, b.desc).toBeGreaterThan(0);
        else expect(b.brokers, b.desc).toBeUndefined();
      }
    }
  });

  it('links only to sections the docs have', () => {
    const anchors = PAGES.flatMap((p) => p.toc.map(([slug]) => `${p.href}#${slug}`));
    for (const b of KEYBINDINGS.flatMap((g) => g.bindings)) {
      if (b.more) expect(anchors).toContain(b.more.href);
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

  it('matches on broker tags, keeping the rows shared by both brokers', () => {
    const dest = KEYBINDINGS.find((g) => g.slug === 'amqp')!;
    const brokers: Broker[] = ['AMQP 1.0', 'RabbitMQ'];
    for (const broker of brokers) {
      const kept = filterKeybindings(broker.toLowerCase());
      expect(kept.map((g) => g.slug)).toEqual(['amqp']);
      expect(kept[0].bindings).toEqual(dest.bindings.filter((b) => b.brokers?.includes(broker)));
      expect(kept[0].bindings.some((b) => b.brokers?.length === brokers.length)).toBe(true);
    }
  });
});
