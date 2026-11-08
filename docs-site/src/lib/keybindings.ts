// The interactive keybinding cheat-sheet on the Keybindings page.
//
// These are the real, source-verified bindings (src/app/input.rs, action.rs,
// console.rs, settings.rs). Keyhole is context-sensitive: globals fire only in
// non-modal panes, and some keys mean different things per pane — the grouping
// below reflects that. Each group's `slug` matches its `data-toc` anchor (and
// the page TOC). `filterKeybindings` powers the live filter input: a binding
// matches when the query is a substring of its description, of its
// space-joined keycaps, of its link's label OR of one of its broker tags; groups
// with no surviving rows drop out. Pure, so it runs in the client filter handler and is unit-tested. The
// page tells rows apart by description, so a group's descriptions are unique.

/** A broker a destination-browser binding applies to, shown as a tag. */
export type Broker = 'AMQP 1.0' | 'RabbitMQ';

export interface Binding {
  /** Keycaps, rendered as individual <kbd> chips. */
  keys: readonly string[];
  desc: string;
  /** The brokers a destination-browser binding works on, shown as tags. A key
   * that does different things per broker gets a row for each. */
  brokers?: readonly Broker[];
  /** A docs section with more detail, linked after the description. */
  more?: { label: string; href: string };
}

export interface KeybindingGroup {
  title: string;
  /** Anchor slug; matches the TOC entry and the group's `data-toc`. */
  slug: string;
  bindings: readonly Binding[];
}

const AMQP: Broker = 'AMQP 1.0';
const RABBITMQ: Broker = 'RabbitMQ';
const BOTH: readonly Broker[] = [AMQP, RABBITMQ];

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
      { keys: ['l'], desc: 'Fold / unfold the selected group, same as Enter' },
      { keys: ['z'], desc: 'Fold / unfold all groups' },
      { keys: ['/'], desc: 'Filter keys' },
      { keys: ['o'], desc: 'Cycle the sort column' },
      { keys: ['O'], desc: 'Toggle the sort direction' },
    ],
  },
  {
    // The destination browser: handle_destination_key, handle_message_key and
    // handle_details_key in src/app/input.rs.
    title: 'AMQP 1.0 & RabbitMQ',
    slug: 'amqp',
    bindings: [
      {
        keys: ['Enter'],
        desc: 'Peek the queue per peek_mode; browse leaves messages in place',
        brokers: [AMQP],
      },
      {
        keys: ['Enter'],
        desc: 'Peek the queue per peek_mode; browse takes messages and puts them back',
        brokers: [RABBITMQ],
        more: { label: 'Peek modes', href: '/rabbitmq#modes' },
      },
      { keys: ['Enter'], desc: 'Tail the selected topic', brokers: [AMQP] },
      { keys: ['Enter'], desc: 'Tap the selected exchange, same as t', brokers: [RABBITMQ] },
      { keys: ['t'], desc: 'Tail the selected topic or queue', brokers: [AMQP] },
      {
        keys: ['t'],
        desc: 'Tap the selected exchange; a direct one opens the Tail anchor',
        brokers: [RABBITMQ],
      },
      {
        keys: ['Ctrl', '→'],
        desc: "Step into the peeked queue's messages (or Enter again)",
        brokers: BOTH,
      },
      { keys: ['Ctrl', '←'], desc: 'Back to the destination list', brokers: BOTH },
      {
        keys: ['/'],
        desc: 'Filter the peeked messages (in the message list)',
        brokers: BOTH,
      },
      { keys: ['P'], desc: 'Publish a message to the selected topic or queue', brokers: [AMQP] },
      {
        keys: ['P'],
        desc: 'Publish; exchanges other than fanout and headers ask for a routing key',
        brokers: [RABBITMQ],
      },
      { keys: ['a'], desc: 'Add a topic or queue to the list', brokers: [AMQP] },
      { keys: ['r'], desc: 'Discover topics and queues from management_url', brokers: [AMQP] },
      {
        keys: ['r'],
        desc: 'Refresh queues and exchanges, retrying a failed API',
        brokers: [RABBITMQ],
      },
      {
        keys: ['x'],
        desc: 'Remove the selected topic or queue from the list',
        brokers: [AMQP],
      },
      { keys: ['d'], desc: 'Remove the selected topic or queue, same as x', brokers: [AMQP] },
      { keys: ['↑', '↓'], desc: 'Scroll the Connections or Consumers tab', brokers: [RABBITMQ] },
      {
        keys: ['Home', 'End'],
        desc: 'Jump to top / bottom of the Connections or Consumers tab',
        brokers: [RABBITMQ],
      },
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
        b.keys.join(' ').toLowerCase().includes(q) ||
        (b.more?.label.toLowerCase().includes(q) ?? false) ||
        (b.brokers ?? []).some((br) => br.toLowerCase().includes(q)),
    ),
  })).filter((g) => g.bindings.length > 0);
}
