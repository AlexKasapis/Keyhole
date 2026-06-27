// The interactive keybinding cheat-sheet on the Keybindings page.
//
// These are the real, source-verified bindings (src/app/input.rs, action.rs,
// console.rs, settings.rs). Keyhole is context-sensitive: globals fire only in
// non-modal panes, and some keys mean different things per pane — the grouping
// below reflects that. Each group's `slug` matches its `data-toc` anchor (and
// the page TOC). `filterKeybindings` powers the live filter input: a binding
// matches when the query is a substring of its description OR of its
// space-joined keycaps; groups with no surviving rows drop out. Pure, so it runs
// in the client filter handler and is unit-tested.

export interface Binding {
  /** Keycaps, rendered as individual <kbd> chips. */
  keys: readonly string[];
  desc: string;
}

export interface KeybindingGroup {
  title: string;
  /** Anchor slug; matches the TOC entry and the group's `data-toc`. */
  slug: string;
  bindings: readonly Binding[];
}

export const KEYBINDINGS: readonly KeybindingGroup[] = [
  {
    title: 'Global',
    slug: 'global',
    bindings: [
      { keys: ['Ctrl', 'C'], desc: 'Quit' },
      { keys: ['Esc'], desc: 'Back / leave the screen (twice on Home to quit)' },
      { keys: [':'], desc: 'Open the command palette' },
      { keys: ['?'], desc: 'Toggle the help overlay' },
      { keys: ['m'], desc: 'Toggle mouse capture' },
    ],
  },
  {
    title: 'Navigation',
    slug: 'nav',
    bindings: [
      { keys: ['↑', '↓'], desc: 'Move the selection' },
      { keys: ['Home', 'End'], desc: 'Jump to top / bottom' },
      { keys: ['Tab', 'Shift', 'Tab'], desc: 'Cycle panels and subpanel tabs' },
      { keys: ['Ctrl', '↑'], desc: 'Focus the keys / body pane' },
      { keys: ['Ctrl', '↓'], desc: 'Focus the bottom panel' },
    ],
  },
  {
    title: 'Browsing & values',
    slug: 'browse',
    bindings: [
      { keys: ['Enter'], desc: 'Fold / unfold the selected group' },
      { keys: ['l'], desc: 'Fold / unfold the selected group' },
      { keys: ['z'], desc: 'Fold / unfold all groups' },
      { keys: ['/'], desc: 'Filter keys' },
      { keys: ['o'], desc: 'Cycle the sort column' },
      { keys: ['O'], desc: 'Toggle the sort direction' },
    ],
  },
  {
    title: 'Tails & recording',
    slug: 'feeds',
    bindings: [
      { keys: ['Enter'], desc: 'Start a tail from the Pub/Sub or Tail anchor' },
      { keys: ['p'], desc: 'Play / pause the feed' },
      { keys: ['r'], desc: 'Start / stop recording the tail' },
      { keys: ['x'], desc: 'Close the active tail tab' },
    ],
  },
  {
    title: 'Console',
    slug: 'console',
    bindings: [
      { keys: ['Enter'], desc: 'Run the typed command' },
      { keys: ['Ctrl', 'P'], desc: 'Previous command in history' },
      { keys: ['Ctrl', 'N'], desc: 'Next command in history' },
      { keys: ['Ctrl', 'L'], desc: 'Clear the console' },
    ],
  },
  {
    title: 'Connections & recordings',
    slug: 'manage',
    bindings: [
      { keys: ['Enter'], desc: 'Connect to the selected profile' },
      { keys: ['a'], desc: 'Add a connection' },
      { keys: ['e'], desc: 'Edit the selected connection' },
      { keys: ['x'], desc: 'Disconnect the selected profile' },
      { keys: ['r'], desc: 'Rename the selected recording' },
      { keys: ['d', 'd'], desc: 'Delete the selected recording (press twice)' },
    ],
  },
];

/** Groups (and rows) surviving `query`. Empty query → every group, unfiltered. */
export function filterKeybindings(query: string): KeybindingGroup[] {
  const q = query.trim().toLowerCase();
  return KEYBINDINGS.map((g) => ({
    ...g,
    bindings: g.bindings.filter(
      (b) =>
        !q ||
        b.desc.toLowerCase().includes(q) ||
        b.keys.join(' ').toLowerCase().includes(q),
    ),
  })).filter((g) => g.bindings.length > 0);
}
