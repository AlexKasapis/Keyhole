import { describe, it, expect } from 'vitest';
import { NAV, PAGES, getPage } from '../src/lib/nav';
import { KEYBINDINGS } from '../src/lib/keybindings';

// Every page's source, keyed by file path, to check its anchors against the TOC.
const PAGE_SOURCES = import.meta.glob<string>('../src/pages/*.astro', {
  query: '?raw',
  import: 'default',
  eager: true,
});

describe('nav model', () => {
  it('describes the ten documented pages', () => {
    expect(PAGES).toHaveLength(10);
  });

  it('roots Installation at the docs index', () => {
    expect(getPage('installation').href).toBe('/');
  });

  it('gives every page a unique route', () => {
    const hrefs = PAGES.map((p) => p.href);
    expect(new Set(hrefs).size).toBe(hrefs.length);
  });

  it('every sidebar entry resolves to a real page', () => {
    for (const group of NAV) {
      for (const id of group.pageIds) {
        expect(() => getPage(id)).not.toThrow();
      }
    }
  });

  it('the sidebar covers every page exactly once', () => {
    const sidebarIds = NAV.flatMap((g) => g.pageIds);
    expect([...sidebarIds].sort()).toEqual(PAGES.map((p) => p.id).sort());
  });

  it("each page's eyebrow group matches the sidebar group it sits in", () => {
    for (const group of NAV) {
      for (const id of group.pageIds) {
        expect(getPage(id).group).toBe(group.title);
      }
    }
  });

  it('gives every page a non-empty TOC with unique slugs', () => {
    for (const page of PAGES) {
      expect(page.toc.length).toBeGreaterThan(0);
      const slugs = page.toc.map(([slug]) => slug);
      expect(new Set(slugs).size).toBe(slugs.length);
    }
  });

  it("matches every page's TOC to the data-toc anchors in its source", () => {
    const seen: string[] = [];
    for (const [path, source] of Object.entries(PAGE_SOURCES)) {
      const id = source.match(/<DocsLayout pageId="([^"]+)"/)?.[1];
      expect(id, path).toBeDefined();
      seen.push(id!);
      // The Keybindings page renders one anchor per KEYBINDINGS group.
      const anchors =
        id === 'keybindings' && source.includes('data-toc={g.slug}')
          ? KEYBINDINGS.map((g) => g.slug)
          : [...source.matchAll(/data-toc="([^"]+)"/g)].map((m) => m[1]);
      expect(anchors, path).toEqual(getPage(id!).toc.map(([slug]) => slug));
    }
    expect(seen.sort()).toEqual(PAGES.map((p) => p.id).sort());
  });

  it('throws on an unknown page id', () => {
    expect(() => getPage('nope')).toThrow();
  });
});
