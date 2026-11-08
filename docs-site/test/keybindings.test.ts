import { describe, it, expect } from 'vitest';
import { KEYBINDINGS, filterKeybindings } from '../src/lib/keybindings';
import { getPage } from '../src/lib/nav';

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

describe('AMQP 1.0 & RabbitMQ bindings', () => {
  const group = KEYBINDINGS.find((g) => g.slug === 'amqp')!;
  const descOf = (...keys: string[]) =>
    group.bindings.find((b) => b.keys.join(' ') === keys.join(' '))?.desc;

  it('covers the destination-browser keys from the help overlay', () => {
    expect(group.bindings.map((b) => b.keys.join(' '))).toEqual([
      'Enter',
      't',
      'Ctrl →',
      'Ctrl ←',
      '/',
      'P',
      'a',
      'r',
      'x d',
      '↑ ↓',
      'Home End',
    ]);
  });

  it('gives each row its own description (the page filter keys rows on it)', () => {
    const descs = group.bindings.map((b) => b.desc);
    expect(new Set(descs).size).toBe(descs.length);
  });

  it('spells out both brokers where a key differs between them', () => {
    for (const keys of [['Enter'], ['t'], ['r']]) {
      expect(descOf(...keys)).toContain('(AMQP 1.0)');
      expect(descOf(...keys)).toContain('(RabbitMQ');
    }
    expect(descOf('r')).toContain('Discover');
    expect(descOf('r')).toContain('refresh');
  });

  it('tags the curated-list keys AMQP 1.0 and the roster scrolling RabbitMQ', () => {
    for (const keys of [['a'], ['x', 'd']]) {
      expect(descOf(...keys)).toMatch(/\(AMQP 1\.0\)$/);
    }
    for (const keys of [['↑', '↓'], ['Home', 'End']]) {
      expect(descOf(...keys)).toMatch(/Connections or Consumers tab \(RabbitMQ\)$/);
    }
  });

  it('filters to the broker named in the query', () => {
    // "rabbitmq" doesn't contain "amqp", so each query keeps its own rows.
    const rabbit = filterKeybindings('rabbitmq');
    expect(rabbit.map((g) => g.slug)).toEqual(['amqp']);
    expect(rabbit[0].bindings.map((b) => b.keys.join(' '))).toEqual([
      'Enter',
      't',
      'P',
      'r',
      '↑ ↓',
      'Home End',
    ]);
    const amqp = filterKeybindings('amqp 1.0');
    expect(amqp.map((g) => g.slug)).toEqual(['amqp']);
    expect(amqp[0].bindings.map((b) => b.keys.join(' '))).toEqual(['Enter', 't', 'a', 'r', 'x d']);
  });

  it('finds the publish and peek rows by what they do', () => {
    expect(filterKeybindings('publish').map((g) => g.slug)).toEqual(['amqp']);
    expect(filterKeybindings('peek')[0].bindings.map((b) => b.keys.join(' '))).toEqual([
      'Enter',
      'Ctrl →',
      '/',
    ]);
  });
});
