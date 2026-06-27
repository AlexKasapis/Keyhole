// Central navigation model for the docs site.
//
// One entry per page: its sidebar group, route, document title, search keywords,
// and the on-this-page TOC (the H3 `data-toc` anchors, in document order). The
// layout renders the sidebar / right-rail TOC from this data, the client search
// box queries it (see ./search), and the page files cross-check their own TOC
// against it in tests. Page order here is the order the handoff lists them in.

/** A page's on-this-page entry: `[anchor slug, label]`. The slug matches the
 * `data-toc="…"` attribute on the page's H3 (or group header). */
export type TocEntry = readonly [slug: string, label: string];

export interface PageMeta {
  /** Stable id, also used as the sidebar/search key. */
  id: string;
  /** Sidebar + H1 label, and the search result title. */
  label: string;
  /** Sidebar group title; doubles as the page eyebrow. */
  group: string;
  /** Route. Installation lives at the docs root (`/`). */
  href: string;
  /** Document `<title>`. */
  title: string;
  /** Space-joined search keywords (already lower-case). */
  keywords: string;
  /** On-this-page anchors, in document order. */
  toc: readonly TocEntry[];
}

export interface NavGroup {
  /** Stable id, used as the per-group collapse key. */
  id: string;
  title: string;
  /** Page ids in this group, in display order. */
  pageIds: readonly string[];
}

/** Sidebar groups, top to bottom. */
export const NAV: readonly NavGroup[] = [
  { id: 'gs', title: 'Getting started', pageIds: ['installation', 'quickstart'] },
  { id: 'conn', title: 'Connecting', pageIds: ['connecting'] },
  { id: 'use', title: 'Using Keyhole', pageIds: ['keyspace', 'tails', 'recording'] },
  { id: 'ref', title: 'Reference', pageIds: ['keybindings', 'config', 'palette'] },
];

/** Every page, keyed for lookup; array order is sidebar/document order. */
export const PAGES: readonly PageMeta[] = [
  {
    id: 'installation',
    label: 'Installation',
    group: 'Getting started',
    href: '/',
    title: 'Installation · Keyhole docs',
    keywords: 'install curl cargo brew nix deb rpm package source build verify',
    toc: [
      ['quick', 'Quick install'],
      ['pkg', 'Package managers'],
      ['source', 'From source'],
      ['verify', 'Verify'],
    ],
  },
  {
    id: 'quickstart',
    label: 'Quickstart',
    group: 'Getting started',
    href: '/quickstart',
    title: 'Quickstart · Keyhole docs',
    keywords: 'start launch connect first tutorial getting started tail',
    toc: [
      ['launch', 'Launch'],
      ['connect', 'Connect'],
      ['browse', 'Browse keys'],
      ['tail', 'Start a tail'],
      ['next', 'Next steps'],
    ],
  },
  {
    id: 'connecting',
    label: 'Connect a broker',
    group: 'Connecting',
    href: '/connecting',
    title: 'Connect a broker · Keyhole docs',
    keywords: 'redis amqp rabbitmq connection string url tls auth password vhost',
    toc: [
      ['define', 'Defining a connection'],
      ['redis', 'Redis'],
      ['amqp', 'AMQP 1.0'],
      ['rabbit', 'RabbitMQ'],
      ['tls', 'TLS & auth'],
    ],
  },
  {
    id: 'keyspace',
    label: 'Keyspace & values',
    group: 'Using Keyhole',
    href: '/keyspace',
    title: 'Keyspace & values · Keyhole docs',
    keywords: 'keys browser value inspector tree filter sort console read only hash stream',
    toc: [
      ['keys', 'The keys pane'],
      ['value', 'Value inspector'],
      ['filter', 'Filter & sort'],
      ['console', 'Read-only console'],
    ],
  },
  {
    id: 'tails',
    label: 'Realtime tails',
    group: 'Using Keyhole',
    href: '/tails',
    title: 'Realtime tails · Keyhole docs',
    keywords: 'tail pubsub pattern stream monitor keyspace events realtime live',
    toc: [
      ['types', 'Tail types'],
      ['start', 'Starting a tail'],
      ['pattern', 'Pattern tails'],
      ['scrollback', 'Pause & scrollback'],
    ],
  },
  {
    id: 'recording',
    label: 'Recording to JSONL',
    group: 'Using Keyhole',
    href: '/recording',
    title: 'Recording to JSONL · Keyhole docs',
    keywords: 'record jsonl jq replay export capture file',
    toc: [
      ['rec', 'Recording a tail'],
      ['format', 'JSONL format'],
      ['jq', 'Working with jq'],
      ['browse', 'Browse recordings'],
    ],
  },
  {
    id: 'keybindings',
    label: 'Keybindings',
    group: 'Reference',
    href: '/keybindings',
    title: 'Keybindings · Keyhole docs',
    keywords: 'keys shortcuts bindings hotkeys nav cheat sheet',
    toc: [
      ['global', 'Global'],
      ['nav', 'Navigation'],
      ['browse', 'Browsing & values'],
      ['feeds', 'Tails & recording'],
      ['console', 'Console'],
      ['manage', 'Connections & recordings'],
    ],
  },
  {
    id: 'config',
    label: 'Configuration',
    group: 'Reference',
    href: '/config',
    title: 'Configuration · Keyhole docs',
    keywords: 'config file flags toml profile settings locations',
    toc: [
      ['file', 'Config file'],
      ['loc', 'Locations'],
      ['flags', 'Flags'],
      ['profiles', 'Profiles'],
    ],
  },
  {
    id: 'palette',
    label: 'Command palette',
    group: 'Reference',
    href: '/palette',
    title: 'Command palette · Keyhole docs',
    keywords: 'command palette fuzzy search actions colon',
    toc: [
      ['open', 'Opening the palette'],
      ['cmds', 'Commands'],
      ['settings', 'Settings'],
    ],
  },
];

const PAGES_BY_ID = new Map(PAGES.map((p) => [p.id, p]));

/** Look up a page by id, throwing if it is unknown (a programming error). */
export function getPage(id: string): PageMeta {
  const page = PAGES_BY_ID.get(id);
  if (!page) throw new Error(`Unknown docs page id: ${id}`);
  return page;
}
