//! UI-facing application state types owned by [`crate::app::App`].

use std::cmp::Ordering;
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use ratatui::widgets::TableState;
use time::OffsetDateTime;

use crate::broker::actor::ConnHandle;
use crate::broker::{
    BrokerEvent, BrokerKind, BrowsePage, BrowseReq, Capabilities, ConnId, EntryMeta, ServerStats,
    SubSpec, Ttl, ValueType, ValueView,
};

/// Which top-level screen is showing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Screen {
    Connections,
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

/// Keyboard input mode (text-entry modes capture raw keys).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputMode {
    Normal,
    Filter,
    Form,
    /// Entering a subscription spec (`pubsub:ch`, `psub:ch.*`, `stream:key`).
    Subscribe,
    /// Typing a command in the read-only console.
    Command,
}

/// A transient status-bar message.
pub struct Status {
    pub message: String,
    pub is_error: bool,
}

/// Health of the active broker connection, surfaced as a coloured dot in the
/// header's top-right corner. `Connected` is derived from whether a connection
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
    /// Stick to the newest event; disabled while the user scrolls up.
    pub follow: bool,
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

/// A single rendered row of the key browser: either a collapsible namespace
/// group header (when grouping is on) or a key identified by its index into
/// [`Connection::keys`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ViewRow {
    /// A namespace-prefix group header and the number of keys it holds.
    Group { prefix: String, count: usize },
    /// A key entry; the index points into [`Connection::keys`].
    Entry(usize),
}

/// Separator that delimits Redis key namespaces (`user:1000:name` → `user`).
pub const PREFIX_SEPARATOR: char = ':';

/// The grouping prefix of a key: everything before the first
/// [`PREFIX_SEPARATOR`], or `""` when the key has no separator (such keys
/// collect into a single "no prefix" group).
fn key_prefix(key: &str) -> &str {
    match key.split_once(PREFIX_SEPARATOR) {
        Some((head, _)) => head,
        None => "",
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
/// Keys are always bucketed by their namespace prefix; each bucket yields a
/// [`ViewRow::Group`] header followed (unless the prefix is in `collapsed`) by
/// its entries in sorted order. Groups are always listed alphabetically by
/// prefix; `desc` reverses only the order of keys within a group, not the
/// groups themselves.
pub fn build_view(
    keys: &[EntryMeta],
    sort: SortKey,
    desc: bool,
    collapsed: &HashSet<String>,
) -> Vec<ViewRow> {
    let order = |a: usize, b: usize| {
        let o = entry_cmp(&keys[a], &keys[b], sort);
        if desc {
            o.reverse()
        } else {
            o
        }
    };

    let mut buckets: BTreeMap<&str, Vec<usize>> = BTreeMap::new();
    for (i, e) in keys.iter().enumerate() {
        buckets.entry(key_prefix(&e.key)).or_default().push(i);
    }
    let mut rows = Vec::new();
    for (prefix, mut members) in buckets {
        members.sort_by(|&a, &b| order(a, b));
        rows.push(ViewRow::Group {
            prefix: prefix.to_string(),
            count: members.len(),
        });
        if !collapsed.contains(prefix) {
            rows.extend(members.into_iter().map(ViewRow::Entry));
        }
    }
    rows
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

/// An open connection plus its per-connection browse/inspect/dashboard state.
pub struct Connection {
    pub id: ConnId,
    pub name: String,
    pub caps: Capabilities,
    pub db: u32,
    pub pattern: String,
    /// The keys currently shown in the browser — the result of the most
    /// recently *completed* keyspace scan. A background refresh accumulates
    /// into [`Self::scan_buf`] and only swaps in here once finished, so the
    /// list never flickers or empties mid-refresh.
    pub keys: Vec<EntryMeta>,
    /// SCAN cursor for the scan in progress (`0` once it finishes).
    pub next_cursor: u64,
    /// Whether the most recent scan has finished (drives the "scanning…"
    /// hint). A background refresh sets this `false` while it runs.
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
    /// this, not into `keys`. Rebuilt via [`Self::rebuild_view`].
    pub view: Vec<ViewRow>,
    /// When [`Self::view`] was last rebuilt during the scan in progress, used to
    /// throttle progressive rebuilds (see [`Self::rebuild_view_throttled`]).
    /// Reset at the start of every scan; `None` forces the first page to build.
    last_view_build: Option<Instant>,
    pub table: TableState,
    pub value: Option<ValueView>,
    pub value_key: Option<String>,
    pub value_scroll: u16,
    pub stats: Option<ServerStats>,
    pub stat_ticks: u32,
    /// Ticks elapsed since the last automatic key-browser refresh.
    pub browse_ticks: u32,
    /// Live tails for this connection, shown as tabs in the Browser's bottom
    /// panel (after the Console tab).
    pub subs: Vec<Subscription>,
    /// Active tab in the Browser's bottom panel: `0` is the read-only Console,
    /// and `1..=subs.len()` select `subs[panel_tab - 1]`. Cycled with Tab /
    /// Shift-Tab. (Replaces the former standalone Realtime screen's tab index.)
    pub panel_tab: usize,
    /// Read-only command console state for this connection.
    pub console: Console,
    pub handle: ConnHandle,
}

impl Connection {
    pub fn new(handle: ConnHandle) -> Self {
        let mut table = TableState::default();
        table.select(Some(0));
        Self {
            id: handle.id,
            name: handle.name.clone(),
            caps: handle.caps.clone(),
            db: 0,
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
            table,
            value: None,
            value_key: None,
            value_scroll: 0,
            stats: None,
            stat_ticks: 0,
            browse_ticks: 0,
            subs: Vec::new(),
            panel_tab: 0,
            console: Console::default(),
            handle,
        }
    }

    /// The currently highlighted key, if a key row (not a group header) is
    /// selected.
    pub fn selected(&self) -> Option<&EntryMeta> {
        match self.table.selected().and_then(|i| self.view.get(i)) {
            Some(ViewRow::Entry(i)) => self.keys.get(*i),
            _ => None,
        }
    }

    /// The prefix of the group the cursor is *in*: the highlighted group
    /// header's prefix, or — when a key row is highlighted — the prefix of that
    /// key's group. `None` only when nothing is selected. This is what lets
    /// collapse/expand act on the current group from anywhere inside it, not
    /// just from the header row.
    pub fn cursor_group_prefix(&self) -> Option<String> {
        match self.table.selected().and_then(|i| self.view.get(i)) {
            Some(ViewRow::Group { prefix, .. }) => Some(prefix.clone()),
            Some(ViewRow::Entry(i)) => self.keys.get(*i).map(|e| key_prefix(&e.key).to_string()),
            None => None,
        }
    }

    /// Rebuild [`Self::view`] from the current keys and sort settings, keeping
    /// the highlight on the same key/group where possible (and clamping it into
    /// range when that row no longer exists).
    pub fn rebuild_view(&mut self) {
        let anchor = self.selected_anchor();
        self.view = build_view(&self.keys, self.sort, self.sort_desc, &self.collapsed);
        self.restore_selection(anchor);
        self.last_view_build = Some(Instant::now());
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
        let due = match self.last_view_build {
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
        self.scan_epoch = self.scan_epoch.wrapping_add(1);
        self.scanning = true;
        self.complete = false;
        self.next_cursor = 0;
        self.scan_buf.clear();
        self.scan_live = live;
        // A fresh scan: force the first arriving page to rebuild immediately,
        // then throttle subsequent progressive rebuilds.
        self.last_view_build = None;
        if live {
            // The whole result set is being replaced; drop the old selection and
            // value so nothing briefly points at a key from the previous result.
            self.keys.clear();
            self.value = None;
            self.value_key = None;
            self.value_scroll = 0;
            self.rebuild_view();
        }
        BrowseReq {
            db: self.db,
            pattern: self.pattern.clone(),
            cursor: 0,
            page_size,
            epoch: self.scan_epoch,
        }
    }

    /// Fold one [`BrowsePage`] into the scan in progress, reporting whether it
    /// was stale, whether another page should be fetched, or that the scan is
    /// complete. `page_size` is the `COUNT` hint for any continuation request.
    pub fn apply_page(&mut self, page: BrowsePage, page_size: usize) -> ScanStep {
        // A page whose epoch no longer matches (or that targets a DB we have
        // since left) belongs to a scan we have abandoned — ignore it so it
        // can't contaminate the current scan's results.
        if page.epoch != self.scan_epoch || page.db != self.db {
            return ScanStep::Stale;
        }
        self.next_cursor = page.next_cursor;
        if self.scan_live {
            // Foreground: reveal keys as they load.
            self.keys.extend(page.entries);
        } else {
            // Background: stage until complete, leaving the visible list intact.
            self.scan_buf.extend(page.entries);
        }
        // The view rebuild is deliberately *not* done here. The caller decides:
        // a foreground scan rebuilds the view progressively but throttled (so a
        // many-page scan isn't quadratic), and either kind rebuilds once the scan
        // completes. See `App::on_keys_page`.
        if page.next_cursor != 0 {
            return ScanStep::Continue(BrowseReq {
                db: self.db,
                pattern: self.pattern.clone(),
                cursor: page.next_cursor,
                page_size,
                epoch: self.scan_epoch,
            });
        }
        self.scanning = false;
        self.complete = true;
        if !self.scan_live {
            // Atomically swap in the freshly scanned set; the caller's
            // completion rebuild then reflects it, keeping the highlight on the
            // same key/group where it still exists.
            self.keys = std::mem::take(&mut self.scan_buf);
        }
        ScanStep::Done
    }

    /// Capture the identity of the currently selected row so it can be re-found
    /// after the view is rebuilt.
    fn selected_anchor(&self) -> SelAnchor {
        match self.table.selected().and_then(|i| self.view.get(i)) {
            Some(ViewRow::Entry(i)) => self
                .keys
                .get(*i)
                .map(|e| SelAnchor::Entry(e.key.clone()))
                .unwrap_or(SelAnchor::None),
            Some(ViewRow::Group { prefix, .. }) => SelAnchor::Group(prefix.clone()),
            None => SelAnchor::None,
        }
    }

    /// Re-select the row matching `anchor` in the freshly built view, falling
    /// back to the clamped previous index, or `None` when the view is empty.
    fn restore_selection(&mut self, anchor: SelAnchor) {
        if self.view.is_empty() {
            self.table.select(None);
            return;
        }
        let keys = &self.keys;
        let found = match anchor {
            SelAnchor::Entry(key) => self.view.iter().position(|r| match r {
                ViewRow::Entry(i) => keys[*i].key == key,
                ViewRow::Group { .. } => false,
            }),
            SelAnchor::Group(prefix) => self
                .view
                .iter()
                .position(|r| matches!(r, ViewRow::Group { prefix: p, .. } if *p == prefix)),
            SelAnchor::None => None,
        };
        let idx =
            found.unwrap_or_else(|| self.table.selected().unwrap_or(0).min(self.view.len() - 1));
        self.table.select(Some(idx));
    }

    /// Advance the sort key to the next column and re-sort.
    pub fn cycle_sort(&mut self) {
        self.sort = self.sort.next();
        self.rebuild_view();
    }

    /// Flip between ascending and descending order and re-sort.
    pub fn toggle_sort_dir(&mut self) {
        self.sort_desc = !self.sort_desc;
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
        if !self.collapsed.remove(&prefix) {
            self.collapsed.insert(prefix.clone());
        }
        self.rebuild_view();
        if let Some(idx) = self
            .view
            .iter()
            .position(|r| matches!(r, ViewRow::Group { prefix: p, .. } if *p == prefix))
        {
            self.table.select(Some(idx));
        }
        true
    }

    /// Collapse every group when any is currently expanded, otherwise expand
    /// them all. No-op when grouping is off.
    pub fn toggle_all_groups(&mut self) {
        let prefixes: Vec<String> = self
            .view
            .iter()
            .filter_map(|r| match r {
                ViewRow::Group { prefix, .. } => Some(prefix.clone()),
                ViewRow::Entry(_) => None,
            })
            .collect();
        if prefixes.is_empty() {
            return;
        }
        let any_expanded = prefixes.iter().any(|p| !self.collapsed.contains(p));
        if any_expanded {
            self.collapsed.extend(prefixes);
        } else {
            for p in &prefixes {
                self.collapsed.remove(p);
            }
        }
        self.rebuild_view();
    }

    /// Number of tabs in the Browser's bottom panel: the Console tab plus one
    /// tab per live tail.
    pub fn panel_tab_count(&self) -> usize {
        1 + self.subs.len()
    }

    /// True when the bottom panel's Console tab (tab `0`) is the active one.
    pub fn panel_is_console(&self) -> bool {
        self.panel_tab == 0
    }

    /// The tail shown in the bottom panel, if a tail tab (not the Console) is
    /// active and still present. `panel_tab` of `0` is the Console; `n >= 1`
    /// selects `subs[n - 1]`.
    pub fn panel_subscription(&self) -> Option<&Subscription> {
        self.panel_tab.checked_sub(1).and_then(|i| self.subs.get(i))
    }

    /// The focused tail, if any — alias for [`Self::panel_subscription`], kept
    /// for the tail/recording call sites that don't care it lives in a panel.
    pub fn active_subscription(&self) -> Option<&Subscription> {
        self.panel_subscription()
    }

    /// Cycle the bottom panel's active tab by `delta`, wrapping around the
    /// Console tab and the live tails.
    pub fn cycle_panel(&mut self, delta: i32) {
        let n = self.panel_tab_count() as i32;
        self.panel_tab = (self.panel_tab as i32 + delta).rem_euclid(n) as usize;
    }

    /// Focus the bottom panel on the tail at `sub_idx` (an index into `subs`).
    pub fn focus_tail(&mut self, sub_idx: usize) {
        self.panel_tab = sub_idx + 1;
    }

    /// Find a tail by id (mutable).
    pub fn sub_by_id_mut(&mut self, sub_id: u32) -> Option<&mut Subscription> {
        self.subs.iter_mut().find(|s| s.sub_id == sub_id)
    }

    /// A short status-bar label: `name (dbN)` for Redis (database-scoped),
    /// `name [amqp]` for brokers where a database index is meaningless.
    pub fn label(&self) -> String {
        if self.caps.kind.uses_database() {
            format!("{} (db{})", self.name, self.db)
        } else {
            // The AMQP brokers are not database-scoped, so just tag the kind.
            format!("{} [{}]", self.name, self.caps.kind.label())
        }
    }
}

/// The kind-dependent defaults for the connection form's variable fields, kept
/// in one table (see [`ConnForm::kind_defaults`]) so each broker's row is
/// defined once rather than spread across parallel per-field matches.
struct KindDefaults {
    /// Prefilled Port field.
    port: &'static str,
    /// Prefilled slot-3 value (Redis DB index / RabbitMQ vhost / unused).
    slot3: &'static str,
    /// Label shown for the slot-3 field.
    slot3_label: &'static str,
}

/// The add-connection modal. Fields are plain strings edited in place; the
/// password field accepts a *spec* (`env:VAR`, `keyring`, `prompt`) or a literal
/// (used for the session only, never persisted in plaintext).
pub struct ConnForm {
    pub fields: [String; ConnForm::FIELD_COUNT],
    pub tls: bool,
    /// Which broker the new connection talks to (cycled with the Kind toggle).
    pub kind: BrokerKind,
    pub focus: usize,
    pub error: Option<String>,
}

impl ConnForm {
    pub const FIELD_COUNT: usize = 6;
    /// Index of the shared slot that carries a Redis DB index or a RabbitMQ
    /// vhost, relabelled per kind (see [`Self::slot3_label`]).
    pub const SLOT3_FIELD: usize = 3;
    /// Index of the synthetic "TLS toggle" focus position.
    pub const TLS_FOCUS: usize = Self::FIELD_COUNT;
    /// Index of the synthetic "broker kind toggle" focus position.
    pub const KIND_FOCUS: usize = Self::FIELD_COUNT + 1;
    /// Total number of focusable positions (fields + TLS + kind toggles).
    pub const FOCUS_COUNT: usize = Self::FIELD_COUNT + 2;

    pub const LABELS: [&'static str; Self::FIELD_COUNT] =
        ["Name", "Host", "Port", "DB", "Username", "Password"];

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
            kind: BrokerKind::Redis,
            focus: 0,
            error: None,
        }
    }

    /// Cycle the broker kind Redis → AMQP → RabbitMQ → Redis, fixing up the
    /// kind-dependent form fields. The **Port** means the same thing for every
    /// broker, so a value the user has customised is preserved (only a value
    /// still holding the previous kind's default is re-defaulted). **Slot 3's**
    /// meaning *changes* with the kind (a Redis DB index vs a RabbitMQ vhost vs
    /// unused for AMQP), so a carried-over value would be nonsensical — it is
    /// always reset to the new kind's default rather than bleeding across kinds.
    pub fn toggle_kind(&mut self) {
        let prev = self.kind;
        self.kind = match prev {
            BrokerKind::Redis => BrokerKind::Amqp,
            BrokerKind::Amqp => BrokerKind::Rabbitmq,
            BrokerKind::Rabbitmq => BrokerKind::Redis,
        };
        let prev_def = Self::kind_defaults(prev);
        let new_def = Self::kind_defaults(self.kind);
        if self.fields[2] == prev_def.port {
            self.fields[2] = new_def.port.to_string();
        }
        self.fields[Self::SLOT3_FIELD] = new_def.slot3.to_string();
    }

    /// The kind-dependent form defaults in one place: the prefilled Port, the
    /// slot-3 value, and the slot-3 label (a Redis DB index vs a RabbitMQ vhost
    /// vs unused for AMQP). Adding a broker means adding one row here.
    fn kind_defaults(kind: BrokerKind) -> KindDefaults {
        match kind {
            BrokerKind::Redis => KindDefaults {
                port: "6379",
                slot3: "0",
                slot3_label: "DB",
            },
            BrokerKind::Amqp => KindDefaults {
                port: "5672",
                slot3: "",
                slot3_label: "DB",
            },
            BrokerKind::Rabbitmq => KindDefaults {
                port: "5672",
                slot3: "/",
                slot3_label: "Vhost",
            },
        }
    }

    /// The label shown for the shared slot-3 field, which carries a Redis
    /// database index or a RabbitMQ vhost depending on the selected kind.
    pub fn slot3_label(kind: BrokerKind) -> &'static str {
        Self::kind_defaults(kind).slot3_label
    }

    pub fn focus_next(&mut self) {
        self.focus = (self.focus + 1) % Self::FOCUS_COUNT;
    }

    pub fn focus_prev(&mut self) {
        self.focus = (self.focus + Self::FOCUS_COUNT - 1) % Self::FOCUS_COUNT;
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
                ViewRow::Entry(i) => Some(keys[*i].key.clone()),
                ViewRow::Group { .. } => None,
            })
            .collect()
    }

    /// The `(prefix, count)` of each [`ViewRow::Group`] header, in view order.
    fn group_headers(view: &[ViewRow]) -> Vec<(String, usize)> {
        view.iter()
            .filter_map(|r| match r {
                ViewRow::Group { prefix, count } => Some((prefix.clone(), *count)),
                ViewRow::Entry(_) => None,
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
    fn connform_toggle_kind_cycles_and_tracks_field_defaults() {
        let mut form = ConnForm::new();
        // Fresh form starts as Redis with its defaults.
        assert_eq!(form.kind, BrokerKind::Redis);
        assert_eq!(form.fields[2], "6379");
        assert_eq!(form.fields[ConnForm::SLOT3_FIELD], "0");

        // Redis → AMQP: port and the DB slot move to AMQP's defaults.
        form.toggle_kind();
        assert_eq!(form.kind, BrokerKind::Amqp);
        assert_eq!(form.fields[2], "5672");
        assert_eq!(
            form.fields[ConnForm::SLOT3_FIELD],
            "",
            "AMQP ignores the slot"
        );

        // AMQP → RabbitMQ: port stays 5672, the slot becomes the vhost default.
        form.toggle_kind();
        assert_eq!(form.kind, BrokerKind::Rabbitmq);
        assert_eq!(form.fields[2], "5672");
        assert_eq!(form.fields[ConnForm::SLOT3_FIELD], "/");
        assert_eq!(ConnForm::slot3_label(form.kind), "Vhost");

        // RabbitMQ → Redis: back to the Redis defaults — full cycle restored.
        form.toggle_kind();
        assert_eq!(form.kind, BrokerKind::Redis);
        assert_eq!(form.fields[2], "6379");
        assert_eq!(form.fields[ConnForm::SLOT3_FIELD], "0");
        assert_eq!(ConnForm::slot3_label(form.kind), "DB");
    }

    #[test]
    fn connform_toggle_kind_preserves_custom_port_but_resets_slot3() {
        let mut form = ConnForm::new();
        // Port means the same across brokers, so a user-typed value (no longer
        // the previous kind's default) survives a kind switch …
        form.fields[2] = "7000".to_string();
        // … but slot-3's meaning changes per kind (a Redis DB index vs a vhost),
        // so a value there must NOT bleed across kinds — it resets to the new
        // kind's default. (Finding: a stray DB value otherwise became a vhost.)
        form.fields[ConnForm::SLOT3_FIELD] = "3".to_string();
        form.toggle_kind(); // Redis -> AMQP
        assert_eq!(form.fields[2], "7000", "custom port preserved");
        assert_eq!(
            form.fields[ConnForm::SLOT3_FIELD],
            "",
            "slot-3 reset to the new kind's default, not carried over"
        );
    }
}
