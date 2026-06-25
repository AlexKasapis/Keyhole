//! Shared test harness for the `app` module's unit tests, plus the per-area
//! submodules. The helpers here are the common prelude; each submodule pulls
//! them in with `use super::*`. Split out of a single 3.9k-line file.

use super::*;
use crate::broker::actor::mock;
use crate::broker::{EntryMeta, Payload, Ttl};
use crossterm::event::{KeyEventState, KeyModifiers};
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::mpsc::{self, Receiver};

mod amqp;
mod browser;
mod connection;
mod console;
mod forms;
mod navigation;
mod notifications;
mod pure_helpers;
mod realtime;
mod recordings;

fn build_app(
    config: Config,
    config_path: PathBuf,
    connect: Option<String>,
) -> (App, Receiver<AppEvent>) {
    let (tx, rx) = mpsc::channel::<AppEvent>(64);
    let app = App::new(
        config,
        config_path,
        std::env::temp_dir(),
        tx,
        TaskTracker::new(),
        CancellationToken::new(),
        connect,
    );
    (app, rx)
}

fn test_app() -> (App, Receiver<AppEvent>) {
    build_app(
        Config::default(),
        PathBuf::from("/nonexistent/keyhole/config.toml"),
        None,
    )
}

fn profile(name: &str) -> RedisProfile {
    RedisProfile {
        name: name.into(),
        host: "127.0.0.1".into(),
        port: 6399,
        db: 0,
        username: None,
        password: None,
        tls: false,
    }
}

fn config_with(names: &[&str]) -> Config {
    Config {
        connections: names
            .iter()
            .map(|n| ConnectionConfig::Redis(profile(n)))
            .collect(),
        ..Default::default()
    }
}

fn unique_config_path() -> PathBuf {
    static N: AtomicU64 = AtomicU64::new(0);
    let n = N.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("keyhole-app-{}-{n}.toml", std::process::id()))
}

/// Attach a live mock-backed connection and return its id.
async fn connect(app: &mut App, id: u32, name: &str) -> ConnId {
    let handle = mock::handle(id, name).await;
    app.handle_event(AppEvent::Connected { handle });
    ConnId(id)
}

/// Give the keyboard to a fixed anchor tab in the bottom subpanel and reconcile
/// the panel (mode + focus-scoped feeds), mirroring what Tab-cycling there does:
/// the bottom pane takes focus and lands on the given tab.
fn focus_panel(app: &mut App, tab: PanelTab) {
    let pos = app
        .active_conn()
        .unwrap()
        .panel_slots()
        .iter()
        .position(|t| *t == tab)
        .expect("panel tab present");
    let conn = app.active_conn_mut().unwrap();
    conn.panel_tab = pos;
    conn.focus = PaneFocus::Bottom;
    app.sync_panel_focus();
}

fn key(code: KeyCode) -> KeyEvent {
    KeyEvent::new(code, KeyModifiers::NONE)
}

fn ch(c: char) -> KeyEvent {
    key(KeyCode::Char(c))
}

fn ctrl_ch(c: char) -> KeyEvent {
    KeyEvent::new(KeyCode::Char(c), KeyModifiers::CONTROL)
}

fn ctrl_key(code: KeyCode) -> KeyEvent {
    KeyEvent::new(code, KeyModifiers::CONTROL)
}

fn broker_event(body: &str) -> BrokerEvent {
    BrokerEvent {
        ts: OffsetDateTime::UNIX_EPOCH,
        source: "c".into(),
        payload: Payload::Utf8(body.into()),
        meta: Vec::new(),
    }
}

fn stream_entry(name: &str, vtype: ValueType) -> EntryMeta {
    EntryMeta {
        key: name.into(),
        vtype,
        ttl: Ttl::NoExpire,
        size: None,
    }
}

/// Completes the connection's initial (foreground) scan with `entries`,
/// leaving the browser idle and showing those keys.
fn finish_initial_scan(app: &mut App, id: ConnId, entries: Vec<EntryMeta>) {
    let epoch = app.active_conn().unwrap().browser.scan_epoch;
    app.handle_event(AppEvent::KeysPage {
        id,
        page: BrowsePage {
            db: 0,
            entries,
            next_cursor: 0,
            epoch,
        },
    });
    assert!(app.active_conn().unwrap().browser.phase == ScanPhase::Complete);
    assert!(app.active_conn().unwrap().browser.phase != ScanPhase::InProgress);
}

/// The key names of the Entry rows in a connection's current view order.
fn view_keys(conn: &Connection) -> Vec<String> {
    conn.browser
        .view
        .iter()
        .filter_map(|r| match r {
            ViewRow::Entry { idx, .. } => Some(conn.browser.keys[*idx].key.clone()),
            ViewRow::Group { .. } => None,
        })
        .collect()
}

async fn browser_with_keys(keys: Vec<EntryMeta>) -> (App, Receiver<AppEvent>) {
    let (mut app, rx) = test_app();
    connect(&mut app, 1, "prod").await;
    app.screen = Screen::Browser;
    app.connections[0].browser.keys = keys;
    app.connections[0].rebuild_view();
    (app, rx)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drain_events_bounds_the_batch_under_a_sustained_flood() {
    // A drain that ran "until empty" would never return while a producer keeps
    // the channel full — the render loop would freeze for the whole burst, then
    // lurch forward in one jump (the 1–2s "rough" updates). Pin the fix: the
    // batch is bounded to the backlog present on entry (at most the channel
    // capacity), so drain returns even under a flood that never stops.
    const CAP: usize = 256;
    let (mut app, _rx) = test_app();
    let id = connect(&mut app, 1, "prod").await;
    app.start_subscribe(SubSpec::Channel("c".into()));
    let sub_id = app.connections[0].subs[0].sub_id;

    let (tx, mut rx) = mpsc::channel::<AppEvent>(CAP);
    // Fill the channel before draining, so the entry snapshot is the full CAP.
    for _ in 0..CAP {
        tx.try_send(AppEvent::Realtime {
            id,
            sub_id,
            event: broker_event("x"),
        })
        .unwrap();
    }
    // A producer that keeps topping the channel up from another worker thread,
    // racing the drain. Finite (so a regression fails instead of hanging) but
    // far larger than CAP.
    let flood = tokio::spawn(async move {
        for _ in 0..(CAP * 50) {
            if tx
                .send(AppEvent::Realtime {
                    id,
                    sub_id,
                    event: broker_event("x"),
                })
                .await
                .is_err()
            {
                break;
            }
        }
    });

    app.drain_events(&mut rx);
    flood.abort();

    // Bounded to the entry backlog: at most CAP, never the whole flood.
    assert!(
        app.connections[0].subs[0].received <= CAP as u64,
        "drain must bound the batch to the entry backlog ({CAP}), got {}",
        app.connections[0].subs[0].received
    );
}

/// A valid recording line for a record at a fixed timestamp.
fn recording_line(seq: u64, connection: &str, source: &str, payload: &str) -> String {
    format!(
        r#"{{"seq":{seq},"ts":"2026-06-19T09:08:07Z","connection":"{connection}","source":"{source}","source_type":"pubsub","encoding":"utf8","payload":"{payload}","meta":[]}}"#
    )
}

/// Switch to the Recordings tab pointed at a fresh temp dir holding `files`
/// (each a `(name, body)`), returning the dir for cleanup. Built on the real
/// Tab key path so the test exercises the home-tab switch and the scan.
fn open_recordings(app: &mut App, tag: &str, files: &[(&str, String)]) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("keyhole-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    for (name, body) in files {
        std::fs::write(dir.join(name), body).unwrap();
    }
    app.recordings_dir = dir.clone();
    app.apply(Action::NextTab); // Connections -> Recordings, scans on entry
    assert_eq!(app.screen, Screen::Recordings);
    dir
}

/// Focus the Monitor tab and resume it, returning its sub id ready to receive.
fn live_monitor(app: &mut App) -> u32 {
    focus_panel(app, PanelTab::Monitor);
    app.toggle_play_pause(); // it starts paused; resume tracking
    app.active_conn().unwrap().monitor_sub().unwrap().sub_id
}

/// Attach a live AMQP-capability mock connection.
async fn connect_amqp(app: &mut App, id: u32, name: &str) -> ConnId {
    let handle = mock::amqp_handle(id, name).await;
    app.handle_event(AppEvent::Connected { handle });
    ConnId(id)
}

/// Build an app whose config holds a single AMQP profile (so destinations
/// persist to a real temp file), with `management_url` optionally set. Returns
/// the app, its config path, and the event receiver (kept alive by the caller).
fn amqp_app_with_profile(
    name: &str,
    management_url: Option<&str>,
) -> (App, PathBuf, Receiver<AppEvent>) {
    let path = unique_config_path();
    let config = Config {
        connections: vec![ConnectionConfig::Amqp(AmqpProfile {
            name: name.into(),
            host: "127.0.0.1".into(),
            port: 5672,
            username: None,
            password: None,
            tls: false,
            destinations: Vec::new(),
            management_url: management_url.map(str::to_string),
            management_username: None,
            management_password: None,
        })],
        ..Default::default()
    };
    let (app, rx) = build_app(config, path.clone(), None);
    (app, path, rx)
}

/// Connect an AMQP mock, add `queue`, and load `bodies` as its peeked messages
/// (bypassing the async peek round-trip). Leaves the queue selected with the
/// keyboard on the destination list.
async fn amqp_with_messages(app: &mut App, bodies: &[&str]) {
    connect_amqp(app, 1, "mq").await;
    app.add_amqp_destination(SubSpec::Queue("orders".into()));
    let events: Vec<BrokerEvent> = bodies.iter().map(|b| broker_event(b)).collect();
    let conn = &mut app.connections[0];
    conn.peek.events = events;
    conn.peek.pending = false;
    conn.peek.peeked = Some(SubSpec::Queue("orders".into()));
    conn.peek.selected = 0;
    conn.peek.focused = false;
    conn.peek.detail = false;
}
