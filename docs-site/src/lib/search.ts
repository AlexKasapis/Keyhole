// Live docs search — the logic behind the nav-bar search box.
//
// Mirrors the design reference: a page matches when the query is a substring of
// its label OR of its keyword string. Pure and synchronous so it can run in the
// client search handler and be unit-tested directly.

import { PAGES } from './nav';

export interface SearchResult {
  id: string;
  /** Page title shown as the result's primary line. */
  label: string;
  /** Sidebar group, shown as the result's secondary line. */
  group: string;
  /** Route to navigate to on click. */
  href: string;
}

/** Pages matching `query`, in sidebar order. Empty query → no results. */
export function searchPages(query: string): SearchResult[] {
  const q = query.trim().toLowerCase();
  if (!q) return [];
  return PAGES.filter(
    (p) => p.label.toLowerCase().includes(q) || p.keywords.includes(q),
  ).map((p) => ({ id: p.id, label: p.label, group: p.group, href: p.href }));
}
