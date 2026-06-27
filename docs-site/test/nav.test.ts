import { describe, it, expect } from 'vitest';
import { NAV, PAGES, getPage } from '../src/lib/nav';

describe('nav model', () => {
  it('describes the nine documented pages', () => {
    expect(PAGES).toHaveLength(9);
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

  it('throws on an unknown page id', () => {
    expect(() => getPage('nope')).toThrow();
  });
});
