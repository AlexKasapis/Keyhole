//! UI-facing application state types owned by [`crate::app::App`].

use std::cmp::Ordering;
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use ratatui::widgets::TableState;
use time::OffsetDateTime;

use crate::broker::actor::ConnHandle;
use crate::broker::{
    BrokerEvent, BrokerType, BrowsePage, BrowseReq, Capabilities, ConnId, EntryMeta, ServerStats,
    SubSpec, Ttl, ValueType, ValueView,
};
use crate::config::ConnectionConfig;

/// Which top-level screen is showing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Screen {
    /// The landing area: a single box whose tab strip switches between the saved
    /// connections list and the on-disk recordings (paired with [`Screen::Recordings`]).
    Home,
    /// Key browser + value inspector for the active connection. For brokers with
    /// server statistics (Redis) it also carries a compact stats band up top —
    /// the former standalone Dashboard, now merged into this main panel. Brokers
    /// with a read-only command console (Redis) also carry a tabbed panel pinned
    /// to the bottom: the read-only command console plus one tab per live tail
    /// (pub/sub, streams, keyspace, MONITOR) — the former standalone Console and
    /// Realtime screens, now folded in and cycled with Tab / Shift-Tab.
    Browser,
    /// On-disk recordings.
    Recordings,
}

/// A command in the command palette (opened with `:`). Selecting one runs it;
/// the palette is the discoverable entry point for actions that don't warrant a
/// dedicated key. Today it holds a single command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PaletteCommand {
    /// Open the settings page.
    OpenSettings,
}

impl PaletteCommand {
    /// Every command, in the order shown in the palette.
    pub fn all() -> &'static [PaletteCommand] {
        &[PaletteCommand::OpenSettings]
    }

    /// The label shown for this command in the palette list.
    pub fn label(self) -> &'static str {
        match self {
            PaletteCommand::OpenSettings => "Settings page",
        }
    }
}

/// The command-palette overlay's state: which command is highlighted. The list
/// of commands is fixed ([`PaletteCommand::all`]); only the cursor moves.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PaletteState {
    /// Index of the highlighted command in [`PaletteCommand::all`].
    pub selected: usize,
}

/// A row on the settings page, in display order. ↑/↓ move the highlight between
/// rows ([`SettingsState::selected`]); ←/→ cycle the highlighted row's value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SettingsRow {
    /// The colour theme base (`dark` / `light` / `gruvbox`).
    Theme,
    /// Whether UI animations play (`on` / `off`).
    Animations,
    /// How peeking an AMQP queue reads it (`browse` / `skip` / `destructive`).
    PeekMode,
}

impl SettingsRow {
    /// Every row, in the order shown on the settings page.
    pub fn all() -> &'static [SettingsRow] {
        &[
            SettingsRow::Theme,
            SettingsRow::Animations,
            SettingsRow::PeekMode,
        ]
    }

    /// The label shown in this row's left column.
    pub fn label(self) -> &'static str {
        match self {
            SettingsRow::Theme => "Theme",
            SettingsRow::Animations => "Animations",
            SettingsRow::PeekMode => "AMQP peek mode",
        }
    }
}

/// The settings-page overlay's state: which row is highlighted. The rows are
/// fixed ([`SettingsRow::all`]); only the cursor moves. ↑/↓ move it, ←/→ cycle
/// the highlighted row's value.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SettingsState {
    /// Index of the highlighted row in [`SettingsRow::all`].
    pub selected: usize,
}

/// Keyboard input mode (text-entry modes capture raw keys).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputMode {
    Normal,
    Filter,
    Form,
    /// Entering a subscription spec on the Pub/Sub or Tail anchor tab. Which spec
    /// is built on submit follows the focused tab, not the buffer.
    Subscribe,
    /// Typing a command in the read-only console.
    Command,
    /// Editing the name of the selected recording on the Recordings tab.
    Rename,
    /// Typing a destination spec (`topic:name` / `queue:name`) to add to an AMQP
    /// connection's curated destination list (the left pane).
    AddDestination,
    /// Typing a message body to publish to the selected AMQP destination.
    Publish,
    /// Typing a substring filter over the AMQP peek message list.
    PeekFilter,
}

/// Which Browser pane currently owns the keyboard. Independent of which bottom
/// subpanel tab is *selected*: the selected tab stays visible (and its feed
/// live) regardless of focus — only the focused pane receives keybinds, so the
/// key list keeps Space / `j` / `k` while a text subpanel is merely on screen.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PaneFocus {
    /// The key list (and the value pane it drives). The default on Browser entry.
    #[default]
    Keys,
    /// The bottom subpanel (Console / Monitor / Keyspace / Pub-Sub / Tail / tails).
    Bottom,
}

/// Which Recordings-tab pane currently owns the keyboard. The left pane is the
/// recordings list (↑/↓ move the selection); the right pane is the viewer (↑/↓
/// scroll the loaded recording). Ctrl-←/→ move focus between them. Reset to
/// [`RecordingsFocus::List`] on every entry to the tab.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RecordingsFocus {
    /// The recordings list (left). The default on entering the tab.
    #[default]
    List,
    /// The recording viewer (right).
    Viewer,
}

/// One tab in the Browser's bottom panel. The first six are fixed and always
/// present; [`PanelTab::Sub`] is one tab per live pub/sub or stream tail, placed
/// immediately after its anchor ([`PanelTab::PubSub`] for pub/sub channels and
/// patterns, [`PanelTab::Tail`] for streams). Every tab is reached only by
/// cycling with Tab / Shift-Tab. MONITOR and keyspace tails have no tab of their
/// own — they live under their anchor and run only while it is focused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PanelTab {
    /// Server overview: time-series graphs (ops/sec, keys) and the connected
    /// client list. The leftmost tab; passive (no input, no feed).
    ServerDetails,
    /// Read-only command console: type a command, Enter to run.
    Console,
    /// Server-wide MONITOR feed; live only while this tab is focused.
    Monitor,
    /// Keyspace-notification feed for the active db; live only while focused.
    Keyspace,
    /// Pub/sub anchor: an always-shown input to subscribe to a channel or pattern.
    PubSub,
    /// Stream-tail anchor: an always-shown input to tail a stream key.
    Tail,
    /// A live pub/sub or stream tail at `subs[idx]`.
    Sub(usize),
}

/// How a status-bar notification behaves over time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatusKind {
    /// An ordinary notification: it self-dismisses after a few seconds (see
    /// `STATUS_TTL`) or as soon as a newer notification replaces it.
    Transient,
    /// A confirmation prompt tied to an armed key chord (e.g. "Press d again to
    /// delete"). It stays put while the chord is armed and is cleared the
    /// instant the chord is broken — with no replacement message — rather than
    /// timing out on its own.
    Confirm,
}

/// A status-bar notification shown in the bottom-right of the footer.
pub struct Status {
    pub message: String,
    pub is_error: bool,
    /// What dismisses this notification (timeout vs. chord resolution).
    pub kind: StatusKind,
    /// The tick-clock time ([`crate::app::App`]'s `now`) at which this was
    /// shown. A `Transient` notification expires once `STATUS_TTL` has elapsed
    /// since this instant.
    pub shown_at: OffsetDateTime,
}

/// Health of the active broker connection, surfaced as a coloured dot in the
/// Browser's Server band. `Connected` is derived from whether a connection
/// is active (see [`crate::app::App::conn_health`]); the remaining variants
/// describe the no-connection situation — nothing started yet (`Offline`), a
/// connect in flight (`Connecting`), or a failed attempt / dropped connection
/// (`Error`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnHealth {
    Offline,
    Connecting,
    Connected,
    Error,
}

/// Lifecycle of a subscription/tail tab.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubState {
    /// Tail requested; waiting for the actor to confirm.
    Connecting,
    /// Tail established and receiving.
    Active,
    /// Tail stopped (source closed, failed, or stopped by the user).
    Ended(Option<String>),
}

/// UI-side recording state for a tail, mirrored from `RecordingUpdate` events.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecordState {
    Off,
    On {
        records: u64,
        bytes: u64,
        path: PathBuf,
    },
}

impl RecordState {
    pub fn is_on(&self) -> bool {
        matches!(self, RecordState::On { .. })
    }
}

/// One live tail: a capped scrollback ring buffer plus recording state.
pub struct Subscription {
    pub sub_id: u32,
    pub spec: SubSpec,
    pub label: String,
    pub state: SubState,
    /// Newest-last ring buffer of received events (older ones are evicted).
    pub events: VecDeque<BrokerEvent>,
    /// Ring-buffer capacity.
    pub capacity: usize,
    /// Total events received (including ones evicted from the ring).
    pub received: u64,
    pub recording: RecordState,
    /// Stick to the newest event; disabled while the user scrolls up. Purely a
    /// viewport concern — events keep accumulating while scrolled up (see
    /// [`Subscription::push`]). Distinct from [`Self::paused`].
    pub follow: bool,
    /// Explicitly paused (the `p` key). While paused the tail stays subscribed
    /// at the broker, but incoming events are dropped instead of tracked, so the
    /// scrollback and the `received` tally freeze. Unlike [`Self::follow`], this
    /// stops collection, not just the viewport.
    pub paused: bool,
    /// How many events back from the newest the viewport bottom sits
    /// (`0` == following the newest event).
    pub offset: usize,
    /// A non-fatal advisory for this tail (e.g. keyspace notifications are
    /// disabled server-side), shown as a banner. UI-only — never recorded.
    pub notice: Option<String>,
}

impl Subscription {
    pub fn new(sub_id: u32, spec: SubSpec, capacity: usize) -> Self {
        let label = spec.label();
        Self {
            sub_id,
            spec,
            label,
            state: SubState::Connecting,
            events: VecDeque::new(),
            capacity: capacity.max(1),
            received: 0,
            recording: RecordState::Off,
            follow: true,
            paused: false,
            offset: 0,
            notice: None,
        }
    }

    /// Append an event, evicting the oldest if at capacity.
    pub fn push(&mut self, event: BrokerEvent) {
        if self.events.len() == self.capacity {
            self.events.pop_front();
        }
        self.events.push_back(event);
        self.received += 1;
        // When scrolled up, keep the viewport anchored on the same older events
        // as newer ones arrive (offset is measured from the newest end).
        if !self.follow {
            let max = self.events.len().saturating_sub(1);
            self.offset = (self.offset + 1).min(max);
        }
    }

    /// Count a received event toward the tally without storing it for display.
    /// The paced Monitor feed calls this once its per-frame reveal budget is
    /// spent: the event still counts (so the tally tracks true throughput) but
    /// is dropped from the on-screen scrollback (the recording keeps it).
    pub(crate) fn skip(&mut self) {
        self.received += 1;
    }
}

/// A recording file on disk, listed in the Recordings view. The full path is
/// `recordings_dir / name`, so only the leaf name is retained here.
pub struct RecordingFile {
    pub name: String,
    pub size: u64,
    pub modified: Option<OffsetDateTime>,
}

/// One executed console command and its rendered reply (or error).
pub struct ConsoleEntry {
    pub command: String,
    pub output: String,
    pub is_error: bool,
}

/// Per-connection read-only command console state.
#[derive(Default)]
pub struct Console {
    /// The command line currently being typed.
    pub input: String,
    /// Executed commands with their replies (oldest first).
    pub entries: Vec<ConsoleEntry>,
    /// Submitted commands, for up/down recall (oldest first).
    pub history: Vec<String>,
    /// Recall cursor into `history`; `None` while editing a fresh line.
    pub history_pos: Option<usize>,
    /// Output scroll offset (lines from the top); large == follow the tail.
    pub scroll: u16,
    /// A command sent to the actor and awaiting its reply.
    pub pending: Option<String>,
}

impl Console {
    /// Record a submitted command in the history (de-duplicating an immediate
    /// repeat) and reset the recall cursor.
    pub fn remember(&mut self, command: &str) {
        if self.history.last().map(String::as_str) != Some(command) {
            self.history.push(command.to_string());
        }
        self.history_pos = None;
    }

    /// Replace the input with the previous history entry (older).
    pub fn recall_prev(&mut self) {
        if self.history.is_empty() {
            return;
        }
        let pos = match self.history_pos {
            Some(0) => 0,
            Some(p) => p - 1,
            None => self.history.len() - 1,
        };
        self.history_pos = Some(pos);
        self.input = self.history[pos].clone();
    }

    /// Replace the input with the next history entry (newer), or clear past the end.
    pub fn recall_next(&mut self) {
        let Some(pos) = self.history_pos else {
            return;
        };
        if pos + 1 < self.history.len() {
            self.history_pos = Some(pos + 1);
            self.input = self.history[pos + 1].clone();
        } else {
            self.history_pos = None;
            self.input.clear();
        }
    }
}

/// The column the key browser is ordered by.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortKey {
    /// Lexicographic by key name.
    Name,
    /// By value type.
    Type,
    /// By time-to-live.
    Ttl,
    /// By approximate memory footprint.
    Size,
}

impl SortKey {
    /// The next sort key in the cycle (Name → Type → Ttl → Size → Name).
    pub fn next(self) -> Self {
        match self {
            SortKey::Name => SortKey::Type,
            SortKey::Type => SortKey::Ttl,
            SortKey::Ttl => SortKey::Size,
            SortKey::Size => SortKey::Name,
        }
    }

    /// Short label for the info bar.
    pub fn label(self) -> &'static str {
        match self {
            SortKey::Name => "name",
            SortKey::Type => "type",
            SortKey::Ttl => "ttl",
            SortKey::Size => "size",
        }
    }
}

/// A single rendered row of the key browser: a collapsible namespace group
/// header or a key identified by its index into [`Connection::keys`]. Keys are
/// grouped by their `:`-delimited namespace at *every* level, so groups nest
/// (`user` → `user:1000` → `user:1000:name`); `depth` carries the nesting level
/// for indentation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ViewRow {
    /// A namespace group header. `path` is the full colon-joined prefix (e.g.
    /// `user:1000`) — the unique collapse key — `depth` is the nesting level
    /// (`0` at the top), and `count` is the number of keys in the whole subtree.
    Group {
        path: String,
        depth: usize,
        count: usize,
    },
    /// A key entry: `idx` points into [`Connection::keys`]; `depth` is the
    /// indentation level (its parent group's depth + 1).
    Entry { idx: usize, depth: usize },
}

/// Separator that delimits Redis key namespaces (`user:1000:name` → `user`).
pub const PREFIX_SEPARATOR: char = ':';

/// The immediate parent group path of a key: everything before the *last*
/// [`PREFIX_SEPARATOR`], or `""` (the root "no prefix" group) when the key has
/// none. `user:1000:name` → `user:1000`, `user:1` → `user`, `loose` → ``.
fn parent_path(key: &str) -> String {
    match key.rsplit_once(PREFIX_SEPARATOR) {
        Some((head, _)) => head.to_string(),
        None => String::new(),
    }
}

/// Every ancestor group path of a key, shallowest first. A key with no
/// separator belongs to the single root "no prefix" group (`""`); otherwise it
/// nests under one group per separator: `a:b:c` → `["a", "a:b"]`.
fn prefix_paths(key: &str) -> Vec<String> {
    let segs: Vec<&str> = key.split(PREFIX_SEPARATOR).collect();
    if segs.len() <= 1 {
        return vec![String::new()];
    }
    (1..segs.len()).map(|d| segs[..d].join(":")).collect()
}

/// Every distinct group path across `keys`, at all nesting depths — including
/// groups not currently visible because an ancestor is folded. Used to
/// collapse / expand or pre-collapse the whole tree.
fn all_group_paths(keys: &[EntryMeta]) -> HashSet<String> {
    let mut paths = HashSet::new();
    for e in keys {
        paths.extend(prefix_paths(&e.key));
    }
    paths
}

/// The first `n` colon-separated segments of `key`, re-joined.
/// `prefix_path("a:b:c", 2)` → `"a:b"`.
fn prefix_path(key: &str, n: usize) -> String {
    key.split(PREFIX_SEPARATOR)
        .take(n)
        .collect::<Vec<_>>()
        .join(":")
}

/// The segment of `key` at nesting `depth`, and whether the key continues past
/// it (has a deeper segment). `("user:1000:name", 1)` → `("1000", true)`;
/// `("user:1", 1)` → `("1", false)`.
fn segment_at(key: &str, depth: usize) -> (&str, bool) {
    let mut rest = key;
    for _ in 0..depth {
        match rest.split_once(PREFIX_SEPARATOR) {
            Some((_, tail)) => rest = tail,
            None => return ("", false),
        }
    }
    match rest.split_once(PREFIX_SEPARATOR) {
        Some((head, _)) => (head, true),
        None => (rest, false),
    }
}

/// Stable display order of value types (used when sorting by type).
fn type_rank(t: ValueType) -> u8 {
    match t {
        ValueType::String => 0,
        ValueType::List => 1,
        ValueType::Set => 2,
        ValueType::Hash => 3,
        ValueType::ZSet => 4,
        ValueType::Stream => 5,
        ValueType::None => 6,
        ValueType::Unknown => 7,
    }
}

/// Ascending TTL rank: soonest expiry first, then no-expire, then unknown.
fn ttl_rank(t: Ttl) -> (u8, i64) {
    match t {
        Ttl::Seconds(s) => (0, s),
        Ttl::NoExpire => (1, 0),
        Ttl::Unknown => (2, 0),
    }
}

/// Ascending size rank: smallest first, with unknown sizes sorted last.
fn size_rank(s: Option<u64>) -> (u8, u64) {
    match s {
        Some(n) => (0, n),
        None => (1, 0),
    }
}

/// Ascending comparison of two entries by `sort`, with the key name as a
/// stable tiebreak so equal-ranked rows keep a deterministic order.
fn entry_cmp(a: &EntryMeta, b: &EntryMeta, sort: SortKey) -> Ordering {
    let primary = match sort {
        SortKey::Name => Ordering::Equal,
        SortKey::Type => type_rank(a.vtype).cmp(&type_rank(b.vtype)),
        SortKey::Ttl => ttl_rank(a.ttl).cmp(&ttl_rank(b.ttl)),
        SortKey::Size => size_rank(a.size).cmp(&size_rank(b.size)),
    };
    primary.then_with(|| a.key.cmp(&b.key))
}

/// Build the ordered, prefix-grouped list of display rows over `keys`.
///
/// Keys are bucketed by their `:`-delimited namespace at every level, so groups
/// nest: a [`ViewRow::Group`] header is emitted for each namespace, followed
/// (unless its `path` is in `collapsed`) by its subgroups and then its own keys.
/// Groups are always listed alphabetically by segment; `desc` reverses only the
/// order of the keys within a group, not the groups themselves.
pub fn build_view(
    keys: &[EntryMeta],
    sort: SortKey,
    desc: bool,
    collapsed: &HashSet<String>,
) -> Vec<ViewRow> {
    let cmp = |a: usize, b: usize| {
        let o = entry_cmp(&keys[a], &keys[b], sort);
        if desc {
            o.reverse()
        } else {
            o
        }
    };
    let mut rows = Vec::new();
    emit_level(
        keys,
        (0..keys.len()).collect(),
        0,
        &cmp,
        collapsed,
        &mut rows,
    );
    rows
}

/// Emit the rows for one nesting level. `indices` are the keys under the current
/// parent (the whole keyspace at the root). At the root, keys with no separator
/// collect into a single "no prefix" group (`""`); below the root they render as
/// direct entries beneath their parent. Subgroups come before this level's own
/// keys; groups stay alphabetical while `desc` reverses only the keys.
fn emit_level(
    keys: &[EntryMeta],
    indices: Vec<usize>,
    depth: usize,
    cmp: &impl Fn(usize, usize) -> Ordering,
    collapsed: &HashSet<String>,
    rows: &mut Vec<ViewRow>,
) {
    let mut branches: BTreeMap<&str, Vec<usize>> = BTreeMap::new();
    let mut terminals: Vec<usize> = Vec::new();
    for i in indices {
        let (seg, more) = segment_at(&keys[i].key, depth);
        if more {
            branches.entry(seg).or_default().push(i);
        } else {
            terminals.push(i);
        }
    }

    if depth == 0 {
        // Root: keys with no separator collect into the "no prefix" group (""),
        // which — sorting first — leads the named groups (BTreeMap order).
        if !terminals.is_empty() {
            branches.entry("").or_default().extend(terminals);
        }
        for (head, members) in branches {
            emit_group(keys, head.to_string(), members, 0, cmp, collapsed, rows);
        }
    } else {
        // Subgroups first (alphabetical), then this level's own keys (sorted).
        for members in branches.values() {
            let path = prefix_path(&keys[members[0]].key, depth + 1);
            emit_group(keys, path, members.clone(), depth, cmp, collapsed, rows);
        }
        terminals.sort_by(|&a, &b| cmp(a, b));
        for idx in terminals {
            rows.push(ViewRow::Entry { idx, depth });
        }
    }
}

/// Emit a group header at `depth` and, unless it is collapsed, recurse into its
/// members at `depth + 1`.
fn emit_group(
    keys: &[EntryMeta],
    path: String,
    members: Vec<usize>,
    depth: usize,
    cmp: &impl Fn(usize, usize) -> Ordering,
    collapsed: &HashSet<String>,
    rows: &mut Vec<ViewRow>,
) {
    let expanded = !collapsed.contains(&path);
    rows.push(ViewRow::Group {
        path,
        depth,
        count: members.len(),
    });
    if expanded {
        emit_level(keys, members, depth + 1, cmp, collapsed, rows);
    }
}

/// Identity of the selected row, captured before a [`Connection::rebuild_view`]
/// so the highlight can follow the same key/group across a re-sort or regroup.
enum SelAnchor {
    Entry(String),
    Group(String),
    None,
}

/// What [`Connection::apply_page`] determined should happen after folding one
/// [`BrowsePage`] into the scan in progress.
#[derive(Debug)]
pub enum ScanStep {
    /// The page belonged to a superseded scan (or another DB) and was ignored.
    Stale,
    /// The scan continues; send this next [`BrowseReq`] to fetch the next page.
    Continue(BrowseReq),
    /// The scan finished; [`Connection::keys`] is now up to date.
    Done,
}

/// The keyspace-browser state for one connection: the scanned keys, the derived
/// grouped/sorted view and its selection, the scan-in-progress bookkeeping, the
/// match pattern, and the auto-refresh timer. Owns the browse half of a
/// [`Connection`]; the scan/view methods live on [`Connection`] since they also
/// coordinate the value inspector and the connection's selected database.
pub struct KeyBrowser {
    /// SCAN match pattern (`*` by default).
    pub pattern: String,
    /// The keys currently shown — the result of the most recently *completed*
    /// keyspace scan. A background refresh accumulates into [`Self::scan_buf`]
    /// and only swaps in here once finished, so the list never flickers or
    /// empties mid-refresh.
    pub keys: Vec<EntryMeta>,
    /// SCAN cursor for the scan in progress (`0` once it finishes).
    pub next_cursor: u64,
    /// Whether the most recent scan has finished (drives the "scanning…" hint).
    /// A background refresh sets this `false` while it runs.
    pub complete: bool,
    /// Generation of the current/most-recently-started scan. Stamped onto every
    /// [`BrowseReq`] of that scan; pages whose epoch no longer matches are from
    /// a superseded scan (DB switch, new filter, fresh refresh) and discarded.
    pub scan_epoch: u64,
    /// True while a scan's pages are still arriving (used to avoid launching an
    /// overlapping background refresh).
    pub scanning: bool,
    /// Pages of an in-flight *background* refresh accumulate here and replace
    /// [`Self::keys`] atomically when the scan completes. Unused for a live
    /// (foreground) scan, which writes straight to `keys` so the user sees keys
    /// appear as they load.
    pub scan_buf: Vec<EntryMeta>,
    /// Whether the in-flight scan writes progressively into [`Self::keys`]
    /// (foreground: initial load, DB/filter change, explicit refresh) or stages
    /// into [`Self::scan_buf`] for an atomic swap (background auto-refresh).
    pub scan_live: bool,
    /// Column the key list is ordered by.
    pub sort: SortKey,
    /// Descending order when set (otherwise ascending).
    pub sort_desc: bool,
    /// Prefixes whose namespace-prefix groups are currently collapsed (hidden
    /// keys). Keys are always grouped by prefix; collapsing only hides a group's
    /// entries, leaving its header.
    pub collapsed: HashSet<String>,
    /// Rendered rows (group headers + keys) derived from `keys`, `sort`,
    /// `sort_desc`, and `collapsed`. The table's selected index points into
    /// this, not into `keys`. Rebuilt via [`Connection::rebuild_view`].
    pub view: Vec<ViewRow>,
    /// When [`Self::view`] was last rebuilt during the scan in progress, used to
    /// throttle progressive rebuilds (see [`Connection::rebuild_view_throttled`]).
    /// Reset at the start of every scan; `None` forces the first page to build.
    last_view_build: Option<Instant>,
    /// Whether the one-time "start fully collapsed" fold has run. Set when the
    /// first scan of this connection completes (see
    /// [`Connection::collapse_groups_on_first_load`]); later scans then leave the
    /// user's expand/collapse choices alone.
    did_initial_collapse: bool,
    pub table: TableState,
    /// Ticks elapsed since the last automatic key-browser refresh.
    pub browse_ticks: u32,
}

impl KeyBrowser {
    fn new() -> Self {
        let mut table = TableState::default();
        table.select(Some(0));
        Self {
            pattern: "*".to_string(),
            keys: Vec::new(),
            next_cursor: 0,
            complete: false,
            scan_epoch: 0,
            scanning: false,
            scan_buf: Vec::new(),
            scan_live: false,
            sort: SortKey::Name,
            sort_desc: false,
            collapsed: HashSet::new(),
            view: Vec::new(),
            last_view_build: None,
            did_initial_collapse: false,
            table,
            browse_ticks: 0,
        }
    }
}

/// The value-inspector pane for one connection: the loaded value, the key it
/// belongs to, and the pane's scroll offset.
#[derive(Default)]
pub struct ValueInspector {
    pub value: Option<ValueView>,
    pub value_key: Option<String>,
    pub value_scroll: u16,
}

impl ValueInspector {
    /// Clamp the scroll offset so paging can't run past the end of the value.
    /// Called from the render path because the bound (`max`) depends on the
    /// rendered pane height, which the update phase has no way to know — see the
    /// note on viewport-derived writes in `ui::views::browser`.
    pub fn clamp_scroll(&mut self, max: u16) {
        self.value_scroll = self.value_scroll.min(max);
    }
}

/// Whether a curated AMQP destination is a topic or a queue. Decides the source
/// spec built when peeking or tailing it (and the tag shown in the list).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DestKind {
    Topic,
    Queue,
}

impl DestKind {
    /// The lower-case tag shown beside the name in the destination list.
    pub fn tag(self) -> &'static str {
        match self {
            DestKind::Topic => "topic",
            DestKind::Queue => "queue",
        }
    }
}

/// One curated AMQP destination: a topic or queue the user has added to the
/// browser. AMQP 1.0 can't enumerate destinations, so the user maintains this
/// list themselves (persisted in the profile).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Destination {
    pub name: String,
    pub kind: DestKind,
}

impl Destination {
    /// The source spec this destination tails/peeks as.
    pub fn spec(&self) -> SubSpec {
        match self.kind {
            DestKind::Topic => SubSpec::Topic(self.name.clone()),
            DestKind::Queue => SubSpec::Queue(self.name.clone()),
        }
    }

    /// Build a destination from a source spec, if it is an AMQP one (`topic:` /
    /// `queue:`). Other specs have no destination-list representation.
    pub fn from_spec(spec: &SubSpec) -> Option<Self> {
        match spec {
            SubSpec::Topic(name) => Some(Destination {
                name: name.clone(),
                kind: DestKind::Topic,
            }),
            SubSpec::Queue(name) => Some(Destination {
                name: name.clone(),
                kind: DestKind::Queue,
            }),
            _ => None,
        }
    }

    /// The canonical `topic:name` / `queue:name` string persisted in the profile.
    pub fn canonical(&self) -> String {
        self.spec().label()
    }
}

/// The left-pane destination browser for an AMQP connection: the curated list
/// plus its selection. The AMQP analog of [`KeyBrowser`] (no scan/sort/grouping —
/// the list is user-curated, not discovered).
#[derive(Default)]
pub struct DestinationBrowser {
    pub items: Vec<Destination>,
    pub table: TableState,
}

/// The right-pane peek inspector for an AMQP connection: the messages from the
/// last queue peek, which destination they belong to (a staleness guard, like
/// [`ValueInspector::value_key`]), the scroll offset, and whether a peek is in
/// flight.
///
/// The pane has two states — a navigable *message list* and, for the selected
/// message, an expanded *detail* view (full body + all metadata) — plus an
/// optional substring `filter` over the list. Keyboard focus moves into the
/// pane (`focused`) so `j`/`k` drive the message cursor rather than the
/// destination list.
#[derive(Default)]
pub struct PeekInspector {
    pub events: Vec<BrokerEvent>,
    /// The destination the current `events` are from. A late-arriving peek for a
    /// different destination (the user moved on) is discarded by comparing this.
    pub peeked: Option<SubSpec>,
    pub scroll: u16,
    pub pending: bool,
    /// Whether the message pane (right) holds the keyboard. When false the
    /// destination list (left) does. Reset to false whenever the selection or
    /// destination changes.
    pub focused: bool,
    /// Index of the highlighted message within the *filtered* list.
    pub selected: usize,
    /// Whether the expanded single-message detail view is open.
    pub detail: bool,
    /// Case-insensitive substring filter over message bodies and metadata; empty
    /// means show everything.
    pub filter: String,
    /// Whether the last peek returned a full batch (the limit), so the view may
    /// be truncated — surfaced in the count line.
    pub limit_hit: bool,
}

impl PeekInspector {
    /// Whether a message matches the current `filter` (case-insensitive substring
    /// over the payload text and every metadata key/value). An empty filter
    /// matches everything.
    pub fn matches(filter: &str, ev: &BrokerEvent) -> bool {
        if filter.is_empty() {
            return true;
        }
        let needle = filter.to_lowercase();
        if ev.payload.as_text().to_lowercase().contains(&needle) {
            return true;
        }
        ev.meta
            .iter()
            .any(|(k, v)| k.to_lowercase().contains(&needle) || v.to_lowercase().contains(&needle))
    }

    /// The indices of `events` that pass the current filter, in order.
    pub fn filtered_indices(&self) -> Vec<usize> {
        self.events
            .iter()
            .enumerate()
            .filter(|(_, ev)| Self::matches(&self.filter, ev))
            .map(|(i, _)| i)
            .collect()
    }

    /// How many messages pass the current filter.
    pub fn filtered_len(&self) -> usize {
        self.events
            .iter()
            .filter(|ev| Self::matches(&self.filter, ev))
            .count()
    }

    /// The currently selected message (within the filtered list), if any.
    pub fn selected_event(&self) -> Option<&BrokerEvent> {
        self.filtered_indices()
            .get(self.selected)
            .map(|&i| &self.events[i])
    }

    /// Clamp the selection to the filtered-list bounds, so a shrinking filter
    /// never leaves the cursor past the end.
    pub fn clamp_selection(&mut self) {
        let len = self.filtered_len();
        if len == 0 {
            self.selected = 0;
        } else if self.selected >= len {
            self.selected = len - 1;
        }
    }

    /// Move the message cursor by `delta` (clamped), within the filtered list.
    pub fn move_selection(&mut self, delta: i32) {
        let len = self.filtered_len();
        if len == 0 {
            self.selected = 0;
            return;
        }
        let next = (self.selected as i32 + delta).clamp(0, len as i32 - 1);
        self.selected = next as usize;
    }
}

/// Maximum samples retained in the Server Details time-series graphs. Stats
/// refresh every `STATS_REFRESH_TICKS` ticks (~2s), so this holds roughly the
/// last `STATS_HISTORY × 2s` ≈ 8 minutes of ops/network history; older samples
/// are evicted from the front as new ones arrive.
pub const STATS_HISTORY: usize = 240;

/// The server-statistics state for one connection: the latest stats and the
/// tick counter that paces their refresh (both feeding the Browser's Server
/// band), plus the rolling ops/network history and client-list scroll that drive
/// the Server Details tab.
#[derive(Default)]
pub struct StatsPanel {
    pub stats: Option<ServerStats>,
    pub stat_ticks: u32,
    /// Instantaneous ops/sec at each refresh, oldest first, capped at
    /// [`STATS_HISTORY`]. Feeds the Server Details "Ops/sec" graph.
    pub ops_history: VecDeque<u64>,
    /// Total network throughput in bytes/sec (read + write) at each refresh,
    /// oldest first, capped at [`STATS_HISTORY`]. Feeds the Server Details
    /// "Net" graph.
    pub net_history: VecDeque<u64>,
    /// Scroll offset (rows from the top) of the Server Details client list.
    pub details_scroll: u16,
}

impl StatsPanel {
    /// Fold a fresh stats reply into the panel: append its ops/sec and total
    /// network throughput to the rolling history (evicting the oldest once
    /// [`STATS_HISTORY`] is reached), then store the reply. A metric absent from
    /// the reply records a `0` sample so the two series stay evenly spaced in
    /// time.
    pub fn record(&mut self, stats: ServerStats) {
        let ops = stats.instantaneous_ops_per_sec.unwrap_or(0);
        // INFO reports the rates in KB/s; track bytes/sec so the graph caption
        // can reuse the byte formatter.
        let in_bps = stats.instantaneous_input_kbps.unwrap_or(0.0) * 1024.0;
        let out_bps = stats.instantaneous_output_kbps.unwrap_or(0.0) * 1024.0;
        let net = (in_bps + out_bps).round() as u64;
        push_capped(&mut self.ops_history, ops);
        push_capped(&mut self.net_history, net);
        self.stats = Some(stats);
    }
}

/// Append `value` to a [`STATS_HISTORY`]-capped ring buffer, evicting the oldest
/// sample once full.
fn push_capped(buf: &mut VecDeque<u64>, value: u64) {
    if buf.len() == STATS_HISTORY {
        buf.pop_front();
    }
    buf.push_back(value);
}

/// An open connection plus its per-connection browse / inspect / dashboard
/// state, each grouped into a cohesive sub-struct.
pub struct Connection {
    pub id: ConnId,
    pub name: String,
    pub caps: Capabilities,
    pub db: u32,
    /// Keyspace browser (keys, view, scan, sort) — populated for Redis.
    pub browser: KeyBrowser,
    /// Value inspector pane — populated for Redis.
    pub inspector: ValueInspector,
    /// Server-statistics dashboard band — populated for Redis.
    pub dashboard: StatsPanel,
    /// Curated destination list (left pane) — populated for AMQP.
    pub destinations: DestinationBrowser,
    /// Queue message peek (right pane) — populated for AMQP.
    pub peek: PeekInspector,
    /// Live tails for this connection. Pub/sub and stream tails each get their
    /// own tab in the Browser's bottom panel (after the Pub/Sub and Tail anchors
    /// respectively); MONITOR and keyspace tails live under their fixed anchor
    /// tab and run only while it is focused. See [`Self::panel_slots`].
    pub subs: Vec<Subscription>,
    /// Active tab in the Browser's bottom panel: an index into the computed
    /// [`Self::panel_slots`] list. Cycled with Tab / Shift-Tab. Which tab is
    /// *selected* (visible) is separate from which pane has the keyboard —
    /// see [`Self::focus`].
    pub panel_tab: usize,
    /// Which Browser pane has the keyboard for this connection. Resets to `Keys`
    /// on entry; moved with Tab / Shift-Tab (→ bottom) and Ctrl-↑ / Ctrl-↓.
    pub focus: PaneFocus,
    /// Read-only command console state for this connection.
    pub console: Console,
    pub handle: ConnHandle,
}

impl Connection {
    pub fn new(handle: ConnHandle) -> Self {
        Self {
            id: handle.id,
            name: handle.name.clone(),
            caps: handle.caps.clone(),
            db: 0,
            browser: KeyBrowser::new(),
            inspector: ValueInspector::default(),
            dashboard: StatsPanel::default(),
            destinations: DestinationBrowser::default(),
            peek: PeekInspector::default(),
            subs: Vec::new(),
            panel_tab: 0,
            focus: PaneFocus::Keys,
            console: Console::default(),
            handle,
        }
    }

    /// The currently highlighted key, if a key row (not a group header) is
    /// selected.
    pub fn selected(&self) -> Option<&EntryMeta> {
        match self
            .browser
            .table
            .selected()
            .and_then(|i| self.browser.view.get(i))
        {
            Some(ViewRow::Entry { idx, .. }) => self.browser.keys.get(*idx),
            _ => None,
        }
    }

    /// The highlighted AMQP destination, if one is selected (AMQP browser).
    pub fn selected_destination(&self) -> Option<&Destination> {
        self.destinations
            .table
            .selected()
            .and_then(|i| self.destinations.items.get(i))
    }

    /// Add `dest` to the curated list (deduped) and select it. Returns `true`
    /// when it was newly added, `false` when it was already present (the existing
    /// row is selected either way).
    pub fn add_destination(&mut self, dest: Destination) -> bool {
        if let Some(i) = self.destinations.items.iter().position(|d| *d == dest) {
            self.destinations.table.select(Some(i));
            return false;
        }
        self.destinations.items.push(dest);
        let last = self.destinations.items.len() - 1;
        self.destinations.table.select(Some(last));
        true
    }

    /// Remove the highlighted destination, reselecting a neighbour. Returns the
    /// removed entry, or `None` when nothing was selected.
    pub fn remove_selected_destination(&mut self) -> Option<Destination> {
        let sel = self.destinations.table.selected()?;
        if sel >= self.destinations.items.len() {
            return None;
        }
        let removed = self.destinations.items.remove(sel);
        let len = self.destinations.items.len();
        self.destinations
            .table
            .select((len > 0).then(|| sel.min(len - 1)));
        Some(removed)
    }

    /// The prefix of the group the cursor is *in*: the highlighted group
    /// header's prefix, or — when a key row is highlighted — the prefix of that
    /// key's group. `None` only when nothing is selected. This is what lets
    /// collapse/expand act on the current group from anywhere inside it, not
    /// just from the header row.
    pub fn cursor_group_prefix(&self) -> Option<String> {
        match self
            .browser
            .table
            .selected()
            .and_then(|i| self.browser.view.get(i))
        {
            Some(ViewRow::Group { path, .. }) => Some(path.clone()),
            Some(ViewRow::Entry { idx, .. }) => {
                self.browser.keys.get(*idx).map(|e| parent_path(&e.key))
            }
            None => None,
        }
    }

    /// Rebuild [`Self::view`] from the current keys and sort settings, keeping
    /// the highlight on the same key/group where possible (and clamping it into
    /// range when that row no longer exists).
    pub fn rebuild_view(&mut self) {
        let anchor = self.selected_anchor();
        self.browser.view = build_view(
            &self.browser.keys,
            self.browser.sort,
            self.browser.sort_desc,
            &self.browser.collapsed,
        );
        self.restore_selection(anchor);
        self.browser.last_view_build = Some(Instant::now());
    }

    /// Rebuild the view during a progressive (foreground) scan, but at most once
    /// per `min_interval`. A large keyspace arrives over many SCAN pages; if each
    /// page triggered a full re-sort the cost would be quadratic in the key count
    /// and the load would crawl. Throttling keeps the visible list updating
    /// smoothly (~live) while bounding the total rebuild work to the scan's
    /// wall-clock duration. The first page of a scan (`last_view_build == None`,
    /// reset in [`Self::begin_scan`]) always rebuilds so keys appear at once; the
    /// final page rebuilds unconditionally via [`Self::rebuild_view`] so the
    /// finished list is always exact.
    pub fn rebuild_view_throttled(&mut self, min_interval: Duration) {
        let due = match self.browser.last_view_build {
            Some(last) => last.elapsed() >= min_interval,
            None => true,
        };
        if due {
            self.rebuild_view();
        }
    }

    /// Begin a fresh keyspace scan, returning the first [`BrowseReq`] to send.
    ///
    /// `live` chooses how results surface. A *foreground* scan (`true`: the
    /// initial load, a DB or filter change, an explicit refresh) clears the
    /// list and writes pages straight into [`Self::keys`] so keys appear as
    /// they load. A *background* scan (`false`: the periodic auto-refresh)
    /// keeps the current list on screen and stages pages into
    /// [`Self::scan_buf`], swapping the fresh set in only once the scan
    /// completes — so a routine refresh never flickers or empties the list.
    ///
    /// Either way the scan epoch is bumped, so pages still in flight from a
    /// previous scan are recognised as stale when they arrive.
    pub fn begin_scan(&mut self, live: bool, page_size: usize) -> BrowseReq {
        self.browser.scan_epoch = self.browser.scan_epoch.wrapping_add(1);
        self.browser.scanning = true;
        self.browser.complete = false;
        self.browser.next_cursor = 0;
        self.browser.scan_buf.clear();
        self.browser.scan_live = live;
        if live {
            // The whole result set is being replaced; drop the old selection and
            // value so nothing briefly points at a key from the previous result.
            self.browser.keys.clear();
            self.inspector.value = None;
            self.inspector.value_key = None;
            self.inspector.value_scroll = 0;
            self.rebuild_view();
        }
        // A fresh scan: force the first arriving page to rebuild immediately,
        // then throttle subsequent progressive rebuilds. This must come *after*
        // the live-clear `rebuild_view()` above, which would otherwise stamp
        // `last_view_build` and make the throttle skip the first page — leaving
        // a non-empty key set with an empty view (a render-time invariant break).
        self.browser.last_view_build = None;
        BrowseReq {
            db: self.db,
            pattern: self.browser.pattern.clone(),
            cursor: 0,
            page_size,
            epoch: self.browser.scan_epoch,
        }
    }

    /// Fold one [`BrowsePage`] into the scan in progress, reporting whether it
    /// was stale, whether another page should be fetched, or that the scan is
    /// complete. `page_size` is the `COUNT` hint for any continuation request.
    pub fn apply_page(&mut self, page: BrowsePage, page_size: usize) -> ScanStep {
        // A page whose epoch no longer matches (or that targets a DB we have
        // since left) belongs to a scan we have abandoned — ignore it so it
        // can't contaminate the current scan's results.
        if page.epoch != self.browser.scan_epoch || page.db != self.db {
            return ScanStep::Stale;
        }
        self.browser.next_cursor = page.next_cursor;
        if self.browser.scan_live {
            // Foreground: reveal keys as they load.
            self.browser.keys.extend(page.entries);
        } else {
            // Background: stage until complete, leaving the visible list intact.
            self.browser.scan_buf.extend(page.entries);
        }
        // The view rebuild is deliberately *not* done here. The caller decides:
        // a foreground scan rebuilds the view progressively but throttled (so a
        // many-page scan isn't quadratic), and either kind rebuilds once the scan
        // completes. See `App::on_keys_page`.
        if page.next_cursor != 0 {
            return ScanStep::Continue(BrowseReq {
                db: self.db,
                pattern: self.browser.pattern.clone(),
                cursor: page.next_cursor,
                page_size,
                epoch: self.browser.scan_epoch,
            });
        }
        self.browser.scanning = false;
        self.browser.complete = true;
        if !self.browser.scan_live {
            // Atomically swap in the freshly scanned set; the caller's
            // completion rebuild then reflects it, keeping the highlight on the
            // same key/group where it still exists.
            self.browser.keys = std::mem::take(&mut self.browser.scan_buf);
        }
        ScanStep::Done
    }

    /// Capture the identity of the currently selected row so it can be re-found
    /// after the view is rebuilt.
    fn selected_anchor(&self) -> SelAnchor {
        match self
            .browser
            .table
            .selected()
            .and_then(|i| self.browser.view.get(i))
        {
            Some(ViewRow::Entry { idx, .. }) => self
                .browser
                .keys
                .get(*idx)
                .map(|e| SelAnchor::Entry(e.key.clone()))
                .unwrap_or(SelAnchor::None),
            Some(ViewRow::Group { path, .. }) => SelAnchor::Group(path.clone()),
            None => SelAnchor::None,
        }
    }

    /// Re-select the row matching `anchor` in the freshly built view, falling
    /// back to the clamped previous index, or `None` when the view is empty.
    fn restore_selection(&mut self, anchor: SelAnchor) {
        if self.browser.view.is_empty() {
            self.browser.table.select(None);
            return;
        }
        let keys = &self.browser.keys;
        let found = match anchor {
            SelAnchor::Entry(key) => self.browser.view.iter().position(|r| match r {
                ViewRow::Entry { idx, .. } => keys[*idx].key == key,
                ViewRow::Group { .. } => false,
            }),
            SelAnchor::Group(path) => self
                .browser
                .view
                .iter()
                .position(|r| matches!(r, ViewRow::Group { path: p, .. } if *p == path)),
            SelAnchor::None => None,
        };
        let idx = found.unwrap_or_else(|| {
            self.browser
                .table
                .selected()
                .unwrap_or(0)
                .min(self.browser.view.len() - 1)
        });
        self.browser.table.select(Some(idx));
    }

    /// Advance the sort key to the next column and re-sort.
    pub fn cycle_sort(&mut self) {
        self.browser.sort = self.browser.sort.next();
        self.rebuild_view();
    }

    /// Flip between ascending and descending order and re-sort.
    pub fn toggle_sort_dir(&mut self) {
        self.browser.sort_desc = !self.browser.sort_desc;
        self.rebuild_view();
    }

    /// Collapse or expand the group at the cursor — whether the cursor is on the
    /// group header itself or on a key within that group. Returns `true` if a
    /// group was toggled, `false` when there is none to act on (nothing
    /// selected).
    ///
    /// The highlight always lands on the toggled group's header afterwards:
    /// when collapsing from a key inside the group, that key's row disappears,
    /// so anchoring to the header keeps the cursor on a visible, sensible row.
    pub fn toggle_selected_group(&mut self) -> bool {
        let Some(prefix) = self.cursor_group_prefix() else {
            return false;
        };
        if !self.browser.collapsed.remove(&prefix) {
            self.browser.collapsed.insert(prefix.clone());
        }
        self.rebuild_view();
        if let Some(idx) = self
            .browser
            .view
            .iter()
            .position(|r| matches!(r, ViewRow::Group { path: p, .. } if *p == prefix))
        {
            self.browser.table.select(Some(idx));
        }
        true
    }

    /// On the first completed scan of this connection, fold every group so
    /// entering the browser shows just the top-level namespaces. Idempotent: it
    /// runs once, then later scans (DB switch, auto-refresh) leave the user's
    /// expand/collapse choices untouched.
    pub fn collapse_groups_on_first_load(&mut self) {
        if self.browser.did_initial_collapse {
            return;
        }
        self.browser.did_initial_collapse = true;
        self.browser
            .collapsed
            .extend(all_group_paths(&self.browser.keys));
    }

    /// Collapse every group when any is currently expanded, otherwise expand
    /// them all. Acts on the whole nested tree — including groups hidden inside a
    /// folded ancestor — so "expand all" reveals every level at once. No-op when
    /// there are no groups.
    pub fn toggle_all_groups(&mut self) {
        let paths = all_group_paths(&self.browser.keys);
        if paths.is_empty() {
            return;
        }
        let any_expanded = paths.iter().any(|p| !self.browser.collapsed.contains(p));
        if any_expanded {
            self.browser.collapsed.extend(paths);
        } else {
            for p in &paths {
                self.browser.collapsed.remove(p);
            }
        }
        self.rebuild_view();
    }

    /// The ordered list of bottom-panel tabs: the six fixed anchors (Server
    /// Details leftmost, then Console, Monitor, Keyspace, Pub/Sub, Tail) plus one
    /// tab per pub/sub tail (after the Pub/Sub anchor) and one per stream tail
    /// (after the Tail anchor). MONITOR/keyspace tails are *not* listed — they
    /// render under their anchor — so their focus-driven lifecycle never shifts
    /// the tab indices. [`Self::panel_tab`] indexes into this.
    pub fn panel_slots(&self) -> Vec<PanelTab> {
        // AMQP has no Redis anchors (no stats/console/monitor/keyspace). Its
        // bottom panel is a single Tail anchor (an input to start a topic/queue
        // tail) followed by one tab per live tail.
        if !self.caps.uses_key_scan() {
            let mut slots = vec![PanelTab::Tail];
            slots.extend((0..self.subs.len()).map(PanelTab::Sub));
            return slots;
        }
        let mut slots = vec![
            PanelTab::ServerDetails,
            PanelTab::Console,
            PanelTab::Monitor,
            PanelTab::Keyspace,
            PanelTab::PubSub,
        ];
        for (i, s) in self.subs.iter().enumerate() {
            if matches!(s.spec, SubSpec::Channel(_) | SubSpec::Pattern(_)) {
                slots.push(PanelTab::Sub(i));
            }
        }
        slots.push(PanelTab::Tail);
        for (i, s) in self.subs.iter().enumerate() {
            if matches!(s.spec, SubSpec::Stream { .. }) {
                slots.push(PanelTab::Sub(i));
            }
        }
        slots
    }

    /// Number of tabs in the bottom panel. AMQP: a single Tail anchor plus one
    /// per live tail. Redis: the six fixed anchors plus one per pub/sub or stream
    /// tail (MONITOR/keyspace tails have no tab of their own).
    pub fn panel_tab_count(&self) -> usize {
        if !self.caps.uses_key_scan() {
            return 1 + self.subs.len();
        }
        6 + self
            .subs
            .iter()
            .filter(|s| {
                matches!(
                    s.spec,
                    SubSpec::Channel(_) | SubSpec::Pattern(_) | SubSpec::Stream { .. }
                )
            })
            .count()
    }

    /// The currently focused tab, clamping a stale index back into range.
    pub fn active_panel(&self) -> PanelTab {
        let slots = self.panel_slots();
        slots[self.panel_tab.min(slots.len() - 1)]
    }

    /// The MONITOR tail, if one is live (only while its tab is focused).
    pub fn monitor_sub(&self) -> Option<&Subscription> {
        self.subs
            .iter()
            .find(|s| matches!(s.spec, SubSpec::Monitor))
    }

    /// The keyspace tail, if one is live (only while its tab is focused).
    pub fn keyspace_sub(&self) -> Option<&Subscription> {
        self.subs
            .iter()
            .find(|s| matches!(s.spec, SubSpec::Keyspace { .. }))
    }

    /// The subscription the active tab shows, if any: the MONITOR/keyspace
    /// singleton under its anchor, or the pub/sub / stream tail of a `Sub` tab.
    pub fn panel_subscription(&self) -> Option<&Subscription> {
        match self.active_panel() {
            PanelTab::Monitor => self.monitor_sub(),
            PanelTab::Keyspace => self.keyspace_sub(),
            PanelTab::Sub(i) => self.subs.get(i),
            PanelTab::ServerDetails | PanelTab::Console | PanelTab::PubSub | PanelTab::Tail => None,
        }
    }

    /// Mutable [`Self::panel_subscription`] — for the play/pause and recording
    /// toggles that act on the focused feed.
    pub fn panel_subscription_mut(&mut self) -> Option<&mut Subscription> {
        match self.active_panel() {
            PanelTab::Monitor => self
                .subs
                .iter_mut()
                .find(|s| matches!(s.spec, SubSpec::Monitor)),
            PanelTab::Keyspace => self
                .subs
                .iter_mut()
                .find(|s| matches!(s.spec, SubSpec::Keyspace { .. })),
            PanelTab::Sub(i) => self.subs.get_mut(i),
            PanelTab::ServerDetails | PanelTab::Console | PanelTab::PubSub | PanelTab::Tail => None,
        }
    }

    /// The focused tail, if any — alias for [`Self::panel_subscription`], kept
    /// for the tail/recording call sites that don't care it lives in a panel.
    pub fn active_subscription(&self) -> Option<&Subscription> {
        self.panel_subscription()
    }

    /// Cycle the bottom panel's active tab by `delta`, wrapping around the whole
    /// tab list.
    pub fn cycle_panel(&mut self, delta: i32) {
        let n = self.panel_tab_count() as i32;
        self.panel_tab = (self.panel_tab as i32 + delta).rem_euclid(n) as usize;
    }

    /// Focus the bottom panel on the tail at `sub_idx` (an index into `subs`),
    /// landing on whichever tab slot now holds it.
    pub fn focus_sub(&mut self, sub_idx: usize) {
        if let Some(pos) = self
            .panel_slots()
            .iter()
            .position(|t| *t == PanelTab::Sub(sub_idx))
        {
            self.panel_tab = pos;
        }
    }

    /// Find a tail by id (mutable).
    pub fn sub_by_id_mut(&mut self, sub_id: u32) -> Option<&mut Subscription> {
        self.subs.iter_mut().find(|s| s.sub_id == sub_id)
    }
}

/// The type-dependent defaults for the connection form's variable fields, kept
/// in one table (see [`ConnForm::type_defaults`]) so each broker's row is
/// defined once rather than spread across parallel per-field matches.
struct TypeDefaults {
    /// Prefilled Port field.
    port: &'static str,
    /// Prefilled slot-3 value (Redis DB index / RabbitMQ vhost / unused).
    slot3: &'static str,
    /// Label shown for the slot-3 field.
    slot3_label: &'static str,
    /// One-line note shown beneath the form to describe this broker type.
    note: &'static str,
}

/// The add/edit-connection modal. Fields are plain strings edited in place; the
/// password field accepts a *spec* (`env:VAR`, `keyring`, `prompt`) or a literal
/// (used for the session only, never persisted in plaintext).
pub struct ConnForm {
    pub fields: [String; ConnForm::FIELD_COUNT],
    pub tls: bool,
    /// Which broker the new connection talks to (cycled with the Type toggle).
    pub r#type: BrokerType,
    pub focus: usize,
    pub error: Option<String>,
    /// When `Some(idx)`, the form edits the existing profile at that index in
    /// `App::profiles` (and the parallel `config.connections`) rather than
    /// creating a new one. `None` is the add-connection form.
    pub editing: Option<usize>,
    /// Whether a delete (Ctrl-D, edit mode only) is armed and awaiting a
    /// confirming second press. Any other key disarms it.
    pub confirm_delete: bool,
}

impl ConnForm {
    pub const FIELD_COUNT: usize = 6;
    /// Index of the shared slot that carries a Redis DB index or a RabbitMQ
    /// vhost, relabelled per type (see [`Self::slot3_label`]).
    pub const SLOT3_FIELD: usize = 3;
    /// Index of the synthetic "TLS toggle" focus position.
    pub const TLS_FOCUS: usize = Self::FIELD_COUNT;
    /// Index of the synthetic "broker type toggle" focus position.
    pub const TYPE_FOCUS: usize = Self::FIELD_COUNT + 1;
    /// Total number of focusable positions (fields + TLS + type toggles).
    pub const FOCUS_COUNT: usize = Self::FIELD_COUNT + 2;

    pub const LABELS: [&'static str; Self::FIELD_COUNT] =
        ["Name", "Host", "Port", "DB", "Username", "Password"];

    /// The order focus moves through (↑/↓) and the order the rows
    /// are rendered. Type sits directly under Name — it drives the other
    /// fields' defaults, so it reads first — while TLS stays at the end. The
    /// values are the focus indices (text-field slots plus the two synthetic
    /// toggles); the slot-3 (DB / Vhost) row is skipped when hidden (AMQP).
    pub const FOCUS_ORDER: [usize; Self::FOCUS_COUNT] = [
        0,                 // Name
        Self::TYPE_FOCUS,  // Type
        1,                 // Host
        2,                 // Port
        Self::SLOT3_FIELD, // DB / Vhost
        4,                 // Username
        5,                 // Password
        Self::TLS_FOCUS,   // TLS
    ];

    pub fn new() -> Self {
        Self {
            fields: [
                String::new(),
                "127.0.0.1".to_string(),
                "6379".to_string(),
                "0".to_string(),
                String::new(),
                String::new(),
            ],
            tls: false,
            r#type: BrokerType::Redis,
            focus: 0,
            error: None,
            editing: None,
            confirm_delete: false,
        }
    }

    /// Build an edit form pre-filled from an existing saved profile. `idx` is the
    /// profile's position in `App::profiles` (and the parallel
    /// `config.connections`), recorded so the submit path replaces that entry
    /// rather than appending a new one.
    pub fn edit(idx: usize, profile: &ConnectionConfig) -> Self {
        let mut form = Self::new();
        form.editing = Some(idx);
        let (r#type, host, port, slot3, username, password, tls) = match profile {
            ConnectionConfig::Redis(p) => (
                BrokerType::Redis,
                p.host.clone(),
                p.port,
                p.db.to_string(),
                p.username.clone(),
                p.password.clone(),
                p.tls,
            ),
            ConnectionConfig::Amqp(p) => (
                BrokerType::Amqp,
                p.host.clone(),
                p.port,
                String::new(),
                p.username.clone(),
                p.password.clone(),
                p.tls,
            ),
            ConnectionConfig::Rabbitmq(p) => (
                BrokerType::Rabbitmq,
                p.host.clone(),
                p.port,
                p.vhost.clone(),
                p.username.clone(),
                p.password.clone(),
                p.tls,
            ),
        };
        form.r#type = r#type;
        form.fields = [
            profile.name().to_string(),
            host,
            port.to_string(),
            slot3,
            username.unwrap_or_default(),
            // The persisted password is a spec (`env:VAR` / `keyring` / `prompt`)
            // or empty — never a plaintext literal — so it is safe to prefill.
            password.unwrap_or_default(),
        ];
        form.tls = tls;
        form
    }

    /// Cycle the broker type one step, fixing up the type-dependent form fields.
    /// `forward` advances Redis → AMQP → RabbitMQ → Redis (Right arrow); the
    /// reverse runs Redis → RabbitMQ → AMQP → Redis (Left arrow). The **Port**
    /// means the same thing for every broker, so a value the user has customised
    /// is preserved (only a value still holding the previous type's default is
    /// re-defaulted). **Slot 3's** meaning *changes* with the type (a Redis DB
    /// index vs a RabbitMQ vhost vs unused for AMQP), so a carried-over value
    /// would be nonsensical — it is always reset to the new type's default rather
    /// than bleeding across types.
    pub fn cycle_type(&mut self, forward: bool) {
        let prev = self.r#type;
        self.r#type = if forward {
            match prev {
                BrokerType::Redis => BrokerType::Amqp,
                BrokerType::Amqp => BrokerType::Rabbitmq,
                BrokerType::Rabbitmq => BrokerType::Redis,
            }
        } else {
            match prev {
                BrokerType::Redis => BrokerType::Rabbitmq,
                BrokerType::Rabbitmq => BrokerType::Amqp,
                BrokerType::Amqp => BrokerType::Redis,
            }
        };
        let prev_def = Self::type_defaults(prev);
        let new_def = Self::type_defaults(self.r#type);
        if self.fields[2] == prev_def.port {
            self.fields[2] = new_def.port.to_string();
        }
        self.fields[Self::SLOT3_FIELD] = new_def.slot3.to_string();
    }

    /// The type-dependent form defaults in one place: the prefilled Port, the
    /// slot-3 value, and the slot-3 label (a Redis DB index vs a RabbitMQ vhost
    /// vs unused for AMQP). Adding a broker means adding one row here.
    fn type_defaults(r#type: BrokerType) -> TypeDefaults {
        match r#type {
            BrokerType::Redis => TypeDefaults {
                port: "6379",
                slot3: "0",
                slot3_label: "DB",
                note: "Redis: DB selects the database index (default 0); port 6379.",
            },
            // AMQP is not database-scoped, so it has no slot-3 row (the label is
            // unused). `slot3` stays empty and the field is hidden — see
            // `slot3_shown`.
            BrokerType::Amqp => TypeDefaults {
                port: "5672",
                slot3: "",
                slot3_label: "DB",
                note: "AMQP 1.0: not database-scoped; port 5672, or 5671 with TLS.",
            },
            BrokerType::Rabbitmq => TypeDefaults {
                port: "5672",
                slot3: "/",
                slot3_label: "Vhost",
                note: "RabbitMQ: Vhost defaults to /; port 5672, or 5671 with TLS.",
            },
        }
    }

    /// The label shown for the shared slot-3 field, which carries a Redis
    /// database index or a RabbitMQ vhost depending on the selected type.
    pub fn slot3_label(r#type: BrokerType) -> &'static str {
        Self::type_defaults(r#type).slot3_label
    }

    /// The one-line note describing the selected broker type, shown beneath the
    /// form. Consolidated here so each type's blurb lives beside its defaults.
    pub fn type_note(r#type: BrokerType) -> &'static str {
        Self::type_defaults(r#type).note
    }

    /// Whether the shared slot-3 field (DB / Vhost) is shown for this type.
    /// AMQP 1.0 is not database-scoped, so it has no slot-3 row at all and the
    /// field is skipped both when rendering and when cycling focus.
    pub fn slot3_shown(r#type: BrokerType) -> bool {
        !matches!(r#type, BrokerType::Amqp)
    }

    pub fn focus_next(&mut self) {
        self.step_focus(1);
    }

    pub fn focus_prev(&mut self) {
        self.step_focus(-1);
    }

    /// Move `delta` (+1 forward / -1 back) steps through [`Self::FOCUS_ORDER`],
    /// wrapping around and hopping over the slot-3 (DB / Vhost) position when it
    /// is hidden (AMQP). Only ever one hidden slot, flanked by visible rows, so a
    /// single step always lands on a shown position.
    fn step_focus(&mut self, delta: isize) {
        let order = Self::FOCUS_ORDER;
        let n = order.len() as isize;
        let mut at = order.iter().position(|&f| f == self.focus).unwrap_or(0) as isize;
        loop {
            at = (at + delta).rem_euclid(n);
            let next = order[at as usize];
            if next == Self::SLOT3_FIELD && !Self::slot3_shown(self.r#type) {
                continue;
            }
            self.focus = next;
            break;
        }
    }
}

impl Default for ConnForm {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::broker::Payload;

    fn ev(tag: &str) -> BrokerEvent {
        BrokerEvent {
            ts: OffsetDateTime::UNIX_EPOCH,
            source: tag.to_string(),
            payload: Payload::Utf8(tag.to_string()),
            meta: Vec::new(),
        }
    }

    fn meta(key: &str, vtype: ValueType, ttl: Ttl, size: Option<u64>) -> EntryMeta {
        EntryMeta {
            key: key.to_string(),
            vtype,
            ttl,
            size,
        }
    }

    /// The key names of the [`ViewRow::Entry`] rows, in view order.
    fn entry_keys(view: &[ViewRow], keys: &[EntryMeta]) -> Vec<String> {
        view.iter()
            .filter_map(|r| match r {
                ViewRow::Entry { idx, .. } => Some(keys[*idx].key.clone()),
                ViewRow::Group { .. } => None,
            })
            .collect()
    }

    /// The `(path, count)` of each [`ViewRow::Group`] header, in view order.
    fn group_headers(view: &[ViewRow]) -> Vec<(String, usize)> {
        view.iter()
            .filter_map(|r| match r {
                ViewRow::Group { path, count, .. } => Some((path.clone(), *count)),
                ViewRow::Entry { .. } => None,
            })
            .collect()
    }

    #[test]
    fn build_view_sorts_by_name_both_directions() {
        let keys = vec![
            meta("banana", ValueType::String, Ttl::NoExpire, None),
            meta("apple", ValueType::String, Ttl::NoExpire, None),
            meta("cherry", ValueType::String, Ttl::NoExpire, None),
        ];
        let empty = HashSet::new();
        let asc = build_view(&keys, SortKey::Name, false, &empty);
        assert_eq!(entry_keys(&asc, &keys), ["apple", "banana", "cherry"]);
        let desc = build_view(&keys, SortKey::Name, true, &empty);
        assert_eq!(entry_keys(&desc, &keys), ["cherry", "banana", "apple"]);
    }

    #[test]
    fn build_view_sorts_by_type_then_name() {
        let keys = vec![
            meta("z", ValueType::Hash, Ttl::NoExpire, None),
            meta("a", ValueType::String, Ttl::NoExpire, None),
            meta("b", ValueType::String, Ttl::NoExpire, None),
        ];
        let view = build_view(&keys, SortKey::Type, false, &HashSet::new());
        // strings (rank 0) before hash (rank 3); ties broken by name.
        assert_eq!(entry_keys(&view, &keys), ["a", "b", "z"]);
    }

    #[test]
    fn build_view_sorts_by_ttl_soonest_first_then_no_expire_then_unknown() {
        let keys = vec![
            meta("never", ValueType::String, Ttl::NoExpire, None),
            meta("soon", ValueType::String, Ttl::Seconds(10), None),
            meta("later", ValueType::String, Ttl::Seconds(100), None),
            meta("gone", ValueType::String, Ttl::Unknown, None),
        ];
        let view = build_view(&keys, SortKey::Ttl, false, &HashSet::new());
        assert_eq!(entry_keys(&view, &keys), ["soon", "later", "never", "gone"]);
    }

    #[test]
    fn build_view_sorts_by_size_smallest_first_unknown_last() {
        let keys = vec![
            meta("big", ValueType::String, Ttl::NoExpire, Some(1000)),
            meta("small", ValueType::String, Ttl::NoExpire, Some(10)),
            meta("unknown", ValueType::String, Ttl::NoExpire, None),
        ];
        let view = build_view(&keys, SortKey::Size, false, &HashSet::new());
        assert_eq!(entry_keys(&view, &keys), ["small", "big", "unknown"]);
    }

    #[test]
    fn build_view_groups_by_prefix_with_headers_and_no_prefix_bucket() {
        let keys = vec![
            meta("user:2", ValueType::String, Ttl::NoExpire, None),
            meta("cache:x", ValueType::String, Ttl::NoExpire, None),
            meta("user:1", ValueType::String, Ttl::NoExpire, None),
            meta("loose", ValueType::String, Ttl::NoExpire, None),
        ];
        let view = build_view(&keys, SortKey::Name, false, &HashSet::new());
        // Groups are alphabetical by prefix: "" (no prefix) sorts first, then
        // cache, then user. Each header carries its key count.
        assert_eq!(
            group_headers(&view),
            [
                ("".to_string(), 1),
                ("cache".to_string(), 1),
                ("user".to_string(), 2),
            ]
        );
        // Keys within a group are name-sorted (user:1 before user:2).
        assert_eq!(
            entry_keys(&view, &keys),
            ["loose", "cache:x", "user:1", "user:2"]
        );
    }

    #[test]
    fn build_view_collapsed_group_keeps_header_but_hides_entries() {
        let keys = vec![
            meta("user:1", ValueType::String, Ttl::NoExpire, None),
            meta("user:2", ValueType::String, Ttl::NoExpire, None),
            meta("cache:x", ValueType::String, Ttl::NoExpire, None),
        ];
        let mut collapsed = HashSet::new();
        collapsed.insert("user".to_string());
        let view = build_view(&keys, SortKey::Name, false, &collapsed);
        // Both headers remain; the collapsed group's keys are gone.
        assert_eq!(
            group_headers(&view),
            [("cache".to_string(), 1), ("user".to_string(), 2)]
        );
        assert_eq!(entry_keys(&view, &keys), ["cache:x"]);
    }

    /// One short tag per row: `G path@depth(count)` for a group, `E key@depth`
    /// for an entry — enough to pin the whole nested structure in one assert.
    fn row_tags(view: &[ViewRow], keys: &[EntryMeta]) -> Vec<String> {
        view.iter()
            .map(|r| match r {
                ViewRow::Group { path, depth, count } => format!("G {path}@{depth}({count})"),
                ViewRow::Entry { idx, depth } => format!("E {}@{depth}", keys[*idx].key),
            })
            .collect()
    }

    #[test]
    fn build_view_nests_groups_at_every_separator() {
        // Multi-segment keys nest: `user` holds the `user:1000` subgroup (with its
        // own keys at depth 2) plus user's own direct keys. Subgroups lead their
        // level's direct entries; group counts are the whole subtree.
        let keys = vec![
            meta("user:1", ValueType::String, Ttl::NoExpire, None),
            meta("user:1000:name", ValueType::String, Ttl::NoExpire, None),
            meta("user:1000:email", ValueType::String, Ttl::NoExpire, None),
            meta("user:2", ValueType::String, Ttl::NoExpire, None),
            meta("cache:x", ValueType::String, Ttl::NoExpire, None),
            meta("loose", ValueType::String, Ttl::NoExpire, None),
        ];
        let view = build_view(&keys, SortKey::Name, false, &HashSet::new());
        assert_eq!(
            row_tags(&view, &keys),
            [
                "G @0(1)", // (no prefix) — colonless keys, sorts first
                "E loose@1",
                "G cache@0(1)",
                "E cache:x@1",
                "G user@0(4)",         // subtree count includes the nested keys
                "G user:1000@1(2)",    // subgroup before user's own direct keys
                "E user:1000:email@2", // name-asc within the subgroup
                "E user:1000:name@2",
                "E user:1@1",
                "E user:2@1",
            ]
        );
    }

    #[test]
    fn build_view_collapsing_a_parent_hides_its_subgroups() {
        let keys = vec![
            meta("user:1000:name", ValueType::String, Ttl::NoExpire, None),
            meta("user:1", ValueType::String, Ttl::NoExpire, None),
        ];
        // Folding the inner `user:1000` group hides only its keys; `user` and its
        // direct key `user:1` stay.
        let mut collapsed = HashSet::new();
        collapsed.insert("user:1000".to_string());
        let view = build_view(&keys, SortKey::Name, false, &collapsed);
        assert_eq!(
            row_tags(&view, &keys),
            ["G user@0(2)", "G user:1000@1(1)", "E user:1@1"]
        );
        // Folding the outer `user` group hides the whole subtree, subgroup and all.
        collapsed.insert("user".to_string());
        let view = build_view(&keys, SortKey::Name, false, &collapsed);
        assert_eq!(row_tags(&view, &keys), ["G user@0(2)"]);
    }

    #[test]
    fn ring_buffer_caps_and_counts() {
        let mut s = Subscription::new(1, SubSpec::Channel("c".into()), 3);
        for i in 0..5 {
            s.push(ev(&format!("m{i}")));
        }
        assert_eq!(s.events.len(), 3, "capped at capacity");
        assert_eq!(s.received, 5, "received counts every event");
        let sources: Vec<&str> = s.events.iter().map(|e| e.source.as_str()).collect();
        assert_eq!(sources, vec!["m2", "m3", "m4"], "oldest evicted");
        // Following by default keeps the viewport pinned to newest.
        assert!(s.follow);
        assert_eq!(s.offset, 0);
    }

    #[test]
    fn paused_viewport_anchors_on_new_events() {
        let mut s = Subscription::new(1, SubSpec::Channel("c".into()), 10);
        for i in 0..5 {
            s.push(ev(&format!("m{i}")));
        }
        // Scroll up two events (offset measured back from the newest).
        s.follow = false;
        s.offset = 2;
        s.push(ev("m5"));
        // To keep the same older events in view, offset grows with new arrivals.
        assert_eq!(s.offset, 3);
        assert!(!s.follow);
    }

    #[test]
    fn record_state_is_on() {
        assert!(!RecordState::Off.is_on());
        assert!(RecordState::On {
            records: 1,
            bytes: 2,
            path: std::path::PathBuf::from("x")
        }
        .is_on());
    }

    #[test]
    fn console_history_recall_walks_both_ways() {
        let mut c = Console::default();
        c.remember("GET a");
        c.remember("GET b");
        // De-dupes an immediate repeat.
        c.remember("GET b");
        assert_eq!(c.history, vec!["GET a", "GET b"]);

        // Up from a fresh line lands on the newest, then walks back.
        c.recall_prev();
        assert_eq!(c.input, "GET b");
        c.recall_prev();
        assert_eq!(c.input, "GET a");
        c.recall_prev();
        assert_eq!(c.input, "GET a", "clamped at the oldest");

        // Down walks forward, then clears past the newest.
        c.recall_next();
        assert_eq!(c.input, "GET b");
        c.recall_next();
        assert_eq!(c.input, "", "past the newest clears the line");
        assert_eq!(c.history_pos, None);
    }

    #[test]
    fn console_recall_next_without_position_is_noop() {
        let mut c = Console::default();
        c.remember("PING");
        c.input = "typing".into();
        c.recall_next(); // history_pos is None
        assert_eq!(c.input, "typing");
    }

    #[test]
    fn subscription_starts_without_notice() {
        let s = Subscription::new(1, SubSpec::Keyspace { db: 0 }, 10);
        assert!(s.notice.is_none());
        assert_eq!(s.label, "keyspace:db0");
    }

    #[test]
    fn connform_cycle_type_forward_cycles_and_tracks_field_defaults() {
        let mut form = ConnForm::new();
        // Fresh form starts as Redis with its defaults.
        assert_eq!(form.r#type, BrokerType::Redis);
        assert_eq!(form.fields[2], "6379");
        assert_eq!(form.fields[ConnForm::SLOT3_FIELD], "0");

        // Redis → AMQP: port and the DB slot move to AMQP's defaults.
        form.cycle_type(true);
        assert_eq!(form.r#type, BrokerType::Amqp);
        assert_eq!(form.fields[2], "5672");
        assert_eq!(
            form.fields[ConnForm::SLOT3_FIELD],
            "",
            "AMQP ignores the slot"
        );

        // AMQP → RabbitMQ: port stays 5672, the slot becomes the vhost default.
        form.cycle_type(true);
        assert_eq!(form.r#type, BrokerType::Rabbitmq);
        assert_eq!(form.fields[2], "5672");
        assert_eq!(form.fields[ConnForm::SLOT3_FIELD], "/");
        assert_eq!(ConnForm::slot3_label(form.r#type), "Vhost");

        // RabbitMQ → Redis: back to the Redis defaults — full cycle restored.
        form.cycle_type(true);
        assert_eq!(form.r#type, BrokerType::Redis);
        assert_eq!(form.fields[2], "6379");
        assert_eq!(form.fields[ConnForm::SLOT3_FIELD], "0");
        assert_eq!(ConnForm::slot3_label(form.r#type), "DB");
    }

    #[test]
    fn connform_cycle_type_backward_is_the_reverse_order() {
        // Left arrow walks the cycle the other way: Redis → RabbitMQ → AMQP →
        // Redis, the mirror of the forward (Right) direction.
        let mut form = ConnForm::new();
        assert_eq!(form.r#type, BrokerType::Redis);

        form.cycle_type(false);
        assert_eq!(form.r#type, BrokerType::Rabbitmq);
        assert_eq!(form.fields[2], "5672");
        assert_eq!(form.fields[ConnForm::SLOT3_FIELD], "/");

        form.cycle_type(false);
        assert_eq!(form.r#type, BrokerType::Amqp);

        form.cycle_type(false);
        assert_eq!(form.r#type, BrokerType::Redis);

        // Forward then back from a fresh form returns to the start.
        let mut there_and_back = ConnForm::new();
        there_and_back.cycle_type(true);
        there_and_back.cycle_type(false);
        assert_eq!(there_and_back.r#type, BrokerType::Redis);
    }

    #[test]
    fn connform_cycle_type_preserves_custom_port_but_resets_slot3() {
        let mut form = ConnForm::new();
        // Port means the same across brokers, so a user-typed value (no longer
        // the previous type's default) survives a type switch …
        form.fields[2] = "7000".to_string();
        // … but slot-3's meaning changes per type (a Redis DB index vs a vhost),
        // so a value there must NOT bleed across types — it resets to the new
        // type's default. (Finding: a stray DB value otherwise became a vhost.)
        form.fields[ConnForm::SLOT3_FIELD] = "3".to_string();
        form.cycle_type(true); // Redis -> AMQP
        assert_eq!(form.fields[2], "7000", "custom port preserved");
        assert_eq!(
            form.fields[ConnForm::SLOT3_FIELD],
            "",
            "slot-3 reset to the new type's default, not carried over"
        );
    }

    #[test]
    fn connform_focus_order_places_type_directly_after_name() {
        // Focus order mirrors the rendered layout: Name → Type → Host → … → TLS,
        // wrapping back to Name.
        let mut form = ConnForm::new();
        assert_eq!(form.focus, 0, "starts on Name");
        form.focus_next();
        assert_eq!(form.focus, ConnForm::TYPE_FOCUS, "Type follows Name");
        form.focus_next();
        assert_eq!(form.focus, 1, "Host follows Type");

        // Stepping back (Up) from Name wraps to the last row (TLS).
        form.focus = 0;
        form.focus_prev();
        assert_eq!(form.focus, ConnForm::TLS_FOCUS, "wrap back to TLS");

        // And stepping back from Type lands back on Name.
        form.focus = ConnForm::TYPE_FOCUS;
        form.focus_prev();
        assert_eq!(form.focus, 0, "Type steps back to Name");
    }

    #[test]
    fn connform_edit_prefills_fields_from_profile() {
        use crate::config::RabbitmqProfile;
        let profile = ConnectionConfig::Rabbitmq(RabbitmqProfile {
            name: "prod".to_string(),
            host: "broker.example".to_string(),
            port: 5671,
            vhost: "/app".to_string(),
            username: Some("svc".to_string()),
            password: Some("env:RMQ_PW".to_string()),
            tls: true,
        });
        let form = ConnForm::edit(3, &profile);
        assert_eq!(form.editing, Some(3), "remembers the profile index");
        assert_eq!(form.r#type, BrokerType::Rabbitmq);
        assert_eq!(form.fields[0], "prod");
        assert_eq!(form.fields[1], "broker.example");
        assert_eq!(form.fields[2], "5671");
        assert_eq!(form.fields[ConnForm::SLOT3_FIELD], "/app");
        assert_eq!(form.fields[4], "svc");
        assert_eq!(form.fields[5], "env:RMQ_PW", "password spec prefilled");
        assert!(form.tls);
    }

    #[test]
    fn connform_slot3_shown_only_when_database_scoped() {
        assert!(ConnForm::slot3_shown(BrokerType::Redis), "DB row");
        assert!(ConnForm::slot3_shown(BrokerType::Rabbitmq), "Vhost row");
        assert!(
            !ConnForm::slot3_shown(BrokerType::Amqp),
            "AMQP is not database-scoped: no slot-3 row"
        );
    }

    #[test]
    fn connform_focus_skips_hidden_db_row_on_amqp() {
        let mut form = ConnForm::new();
        form.cycle_type(true); // -> AMQP, where slot 3 (DB) is hidden
        assert_eq!(form.r#type, BrokerType::Amqp);

        // Down: Port (2) → Username (4), hopping over the hidden DB slot (3).
        form.focus = 2;
        form.focus_next();
        assert_eq!(form.focus, 4, "Down skips the hidden DB row going forward");

        // Up: Username (4) → Port (2), again hopping over slot 3.
        form.focus_prev();
        assert_eq!(form.focus, 2, "Up skips the hidden DB row going back");
    }

    #[test]
    fn connform_focus_lands_on_db_row_when_shown() {
        // Redis is database-scoped, so the DB slot is a normal focus stop.
        let mut form = ConnForm::new();
        form.focus = 2; // Port
        form.focus_next();
        assert_eq!(
            form.focus,
            ConnForm::SLOT3_FIELD,
            "Redis stops on the DB row"
        );
    }

    #[test]
    fn connform_type_note_is_distinct_per_type() {
        let redis = ConnForm::type_note(BrokerType::Redis);
        let amqp = ConnForm::type_note(BrokerType::Amqp);
        let rabbit = ConnForm::type_note(BrokerType::Rabbitmq);
        assert!(redis.contains("DB"));
        assert!(amqp.contains("not database-scoped"));
        assert!(rabbit.contains("Vhost"));
        assert_ne!(redis, amqp);
        assert_ne!(amqp, rabbit);
    }

    #[test]
    fn connection_substructs_start_with_documented_defaults() {
        // Browse state lives under `browser`, the value pane under `inspector`,
        // and the dashboard under `dashboard`. Pin the defaults so the grouping
        // can't silently drift.
        let b = KeyBrowser::new();
        assert_eq!(b.pattern, "*");
        assert!(matches!(b.sort, SortKey::Name));
        assert!(!b.sort_desc);
        assert_eq!(b.table.selected(), Some(0));
        assert!(b.keys.is_empty() && b.view.is_empty());
        assert!(!b.complete && !b.scanning);

        let i = ValueInspector::default();
        assert!(i.value.is_none());
        assert!(i.value_key.is_none());
        assert_eq!(i.value_scroll, 0);

        let d = StatsPanel::default();
        assert!(d.stats.is_none());
        assert_eq!(d.stat_ticks, 0);
        assert!(d.ops_history.is_empty() && d.net_history.is_empty());
        assert_eq!(d.details_scroll, 0);
    }

    #[test]
    fn stats_panel_record_tracks_ops_and_net_history() {
        let mut panel = StatsPanel::default();
        let stats = |ops: u64, in_kbps: f64, out_kbps: f64| ServerStats {
            instantaneous_ops_per_sec: Some(ops),
            instantaneous_input_kbps: Some(in_kbps),
            instantaneous_output_kbps: Some(out_kbps),
            ..Default::default()
        };
        panel.record(stats(7, 1.0, 1.0));
        panel.record(stats(9, 2.0, 0.5));
        // Each sample appends the ops/sec and the summed read+write throughput in
        // bytes/sec (KB/s × 1024), oldest first.
        assert_eq!(panel.ops_history, [7, 9]);
        assert_eq!(panel.net_history, [2048, 2560]);
        // The latest reply is retained for the band.
        assert_eq!(
            panel.stats.as_ref().unwrap().instantaneous_ops_per_sec,
            Some(9)
        );

        // A reply missing the metrics records a 0 so the series stay aligned.
        panel.record(ServerStats::default());
        assert_eq!(panel.ops_history, [7, 9, 0]);
        assert_eq!(panel.net_history, [2048, 2560, 0]);
    }

    #[test]
    fn stats_panel_history_is_capped() {
        let mut panel = StatsPanel::default();
        for i in 0..(STATS_HISTORY as u64 + 10) {
            panel.record(ServerStats {
                instantaneous_ops_per_sec: Some(i),
                ..Default::default()
            });
        }
        // The ring buffer holds exactly the cap, dropping the oldest samples.
        assert_eq!(panel.ops_history.len(), STATS_HISTORY);
        assert_eq!(*panel.ops_history.front().unwrap(), 10);
        assert_eq!(*panel.ops_history.back().unwrap(), STATS_HISTORY as u64 + 9);
    }

    /// Regression: a foreground (live) scan that spans more than one SCAN page
    /// must have a built view the instant its first page lands. `begin_scan`
    /// clears the keys and rebuilds an (empty) view to wipe the old list — but
    /// that rebuild must not stamp `last_view_build`, or the throttled rebuild
    /// on the first arriving page is skipped, leaving a non-empty key set with
    /// an empty view. That state trips the render-time invariant in
    /// `ui::views::browser` (`debug_assert!`) and panics the app on connect.
    #[tokio::test]
    async fn first_page_of_live_scan_builds_view_then_throttles_the_rest() {
        let handle = crate::broker::actor::mock::handle(1, "prod", 16).await;
        let mut conn = Connection::new(handle);

        // Start a live scan: list cleared, empty view.
        let _req = conn.begin_scan(true, 100);
        assert!(conn.browser.keys.is_empty() && conn.browser.view.is_empty());

        // First page lands with more to come (`next_cursor != 0` → Continue).
        let first = BrowsePage {
            db: conn.db,
            entries: vec![
                meta("alpha", ValueType::String, Ttl::NoExpire, None),
                meta("beta", ValueType::String, Ttl::NoExpire, None),
            ],
            next_cursor: 42,
            epoch: conn.browser.scan_epoch,
        };
        assert!(matches!(conn.apply_page(first, 100), ScanStep::Continue(_)));

        // Mirror `App::on_keys_page` for a live, mid-scan page. A huge interval
        // means the only way the view can be built is the forced first-page
        // rebuild (`last_view_build == None`) — the exact path the bug broke.
        conn.rebuild_view_throttled(Duration::from_secs(3600));
        assert_eq!(conn.browser.keys.len(), 2);
        assert!(
            !conn.browser.view.is_empty(),
            "first page of a live scan must build the view before render"
        );
        let after_first = conn.browser.view.len();

        // The throttle must still bound the *rest*: a second mid-scan page under
        // the same huge interval should NOT rebuild, so the view stays put while
        // keys keep growing (this is the quadratic-scan guard we must not lose).
        let second = BrowsePage {
            db: conn.db,
            entries: vec![
                meta("gamma", ValueType::String, Ttl::NoExpire, None),
                meta("delta", ValueType::String, Ttl::NoExpire, None),
            ],
            next_cursor: 99,
            epoch: conn.browser.scan_epoch,
        };
        assert!(matches!(
            conn.apply_page(second, 100),
            ScanStep::Continue(_)
        ));
        conn.rebuild_view_throttled(Duration::from_secs(3600));
        assert_eq!(conn.browser.keys.len(), 4);
        assert_eq!(
            conn.browser.view.len(),
            after_first,
            "subsequent pages stay throttled until the interval elapses"
        );
    }

    // -- AMQP peek inspector -------------------------------------------------

    fn peek_event(body: &str, meta: &[(&str, &str)]) -> crate::broker::BrokerEvent {
        crate::broker::BrokerEvent {
            ts: time::OffsetDateTime::UNIX_EPOCH,
            source: "q".into(),
            payload: Payload::Utf8(body.into()),
            meta: meta
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        }
    }

    fn peek_with(bodies: &[&str]) -> PeekInspector {
        PeekInspector {
            events: bodies.iter().map(|b| peek_event(b, &[])).collect(),
            ..Default::default()
        }
    }

    #[test]
    fn peek_filter_matches_body_and_meta_case_insensitively() {
        let ev = peek_event("Hello World", &[("subject", "Orders")]);
        assert!(PeekInspector::matches("", &ev), "empty filter matches all");
        assert!(PeekInspector::matches("hello", &ev), "body substring");
        assert!(PeekInspector::matches("ORD", &ev), "meta value substring");
        assert!(PeekInspector::matches("subject", &ev), "meta key substring");
        assert!(!PeekInspector::matches("missing", &ev));
    }

    #[test]
    fn peek_filtered_indices_track_the_filter() {
        let mut p = peek_with(&["alpha", "beta", "gamma"]);
        assert_eq!(p.filtered_indices(), vec![0, 1, 2]);
        assert_eq!(p.filtered_len(), 3);
        p.filter = "mm".into();
        assert_eq!(p.filtered_indices(), vec![2]);
        assert_eq!(p.filtered_len(), 1);
        // The selected index addresses the filtered list, so 0 → the match.
        p.selected = 0;
        assert_eq!(
            p.selected_event().map(|e| e.payload.as_text()),
            Some("gamma".into())
        );
    }

    #[test]
    fn peek_move_and_clamp_selection_stay_in_bounds() {
        let mut p = peek_with(&["a", "b", "c"]);
        p.move_selection(1);
        assert_eq!(p.selected, 1);
        p.move_selection(10);
        assert_eq!(p.selected, 2, "clamped at the last message");
        p.move_selection(-10);
        assert_eq!(p.selected, 0, "clamped at the first message");

        // A filter that shrinks the list re-clamps the cursor.
        p.selected = 2;
        p.filter = "a".into(); // matches only "a"
        p.clamp_selection();
        assert_eq!(p.selected, 0);

        // An empty list parks the cursor at 0 and yields no selected event.
        let mut empty = peek_with(&[]);
        empty.move_selection(1);
        assert_eq!(empty.selected, 0);
        assert!(empty.selected_event().is_none());
    }
}
