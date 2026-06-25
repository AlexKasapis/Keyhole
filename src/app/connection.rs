//! `App` connection lifecycle (connect/form submit) and the broker requests
//! it issues (scan, value, stats, filter). Part of the `app` module.

use super::*;

impl App {
    // -- connection lifecycle ------------------------------------------------

    pub(super) fn connect_selected_profile(&mut self) {
        let Some(sel) = self.profile_state.selected() else {
            return;
        };
        let Some(profile) = self.profiles.get(sel).cloned() else {
            return;
        };
        if let Some(idx) = self
            .connections
            .iter()
            .position(|c| c.name == profile.name())
        {
            self.active = Some(idx);
            self.screen = initial_screen(&self.connections[idx].caps);
            self.note_browser_view();
            return;
        }
        self.start_connect(profile, None);
    }

    pub(super) fn start_connect(
        &mut self,
        profile: ConnectionConfig,
        override_password: Option<String>,
    ) {
        let id = ConnId(self.next_id);
        self.next_id += 1;
        let events = self.events.clone();
        let tracker = self.tracker.clone();
        let cancel = self.cancel.clone();
        let preview = self.preview_bytes;
        let recordings_dir = self.recordings_dir.clone();
        let name = profile.name().to_string();
        let addr = profile.address();
        self.health = ConnHealth::Connecting;
        self.set_status(format!("Connecting to {name}…"), false);

        tokio::spawn(async move {
            // Resolve the secret off the render thread (keyring access can block).
            let (spec, account) = profile.secret_account();
            let password = match override_password {
                Some(pw) => Some(pw),
                None => match config::resolve_secret_async(spec, account).await {
                    Ok(pw) => pw,
                    Err(e) => {
                        let _ = events
                            .send(AppEvent::ConnError {
                                id,
                                context: "auth".to_string(),
                                error: e.to_string(),
                            })
                            .await;
                        return;
                    }
                },
            };
            let conn = match connection_for(profile, password, preview) {
                Ok(conn) => conn,
                Err(e) => {
                    let _ = events
                        .send(AppEvent::ConnError {
                            id,
                            context: "connect".to_string(),
                            error: e.to_string(),
                        })
                        .await;
                    return;
                }
            };
            match spawn_connection(SpawnParams {
                id,
                name,
                addr,
                conn,
                events: events.clone(),
                tracker: &tracker,
                parent_cancel: &cancel,
                recordings_dir,
            })
            .await
            {
                Ok(handle) => {
                    let _ = events.send(AppEvent::Connected { handle }).await;
                }
                Err(e) => {
                    let _ = events
                        .send(AppEvent::ConnError {
                            id,
                            context: "connect".to_string(),
                            // `{:#}` surfaces the full cause chain (e.g. a RabbitMQ
                            // connect's context plus the broker's reply detail).
                            error: format!("{e:#}"),
                        })
                        .await;
                }
            }
        });
    }

    pub(super) fn submit_form(&mut self) {
        let Some(form) = self.form.as_ref() else {
            return;
        };

        let name = form.fields[0].trim().to_string();
        if name.is_empty() {
            self.form_error("name is required");
            return;
        }
        let host = {
            let h = form.fields[1].trim();
            if h.is_empty() {
                "127.0.0.1"
            } else {
                h
            }
        }
        .to_string();
        let port: u16 = match form.fields[2].trim().parse() {
            Ok(p) => p,
            Err(_) => return self.form_error("port must be a number 0-65535"),
        };
        let username = {
            let u = form.fields[4].trim();
            if u.is_empty() {
                None
            } else {
                Some(u.to_string())
            }
        };
        let (saved_spec, session_password) = classify_password(form.fields[5].trim());
        let tls = form.tls;
        // `Some(idx)` edits the existing profile at that index; `None` adds a new one.
        let editing = form.editing;

        let profile = match form.r#type {
            BrokerType::Redis => {
                let db: u32 = match form.fields[3].trim().parse() {
                    Ok(d) => d,
                    Err(_) => return self.form_error("db must be a number"),
                };
                ConnectionConfig::Redis(RedisProfile {
                    name,
                    host,
                    port,
                    db,
                    username,
                    password: saved_spec,
                    tls,
                })
            }
            BrokerType::Amqp => {
                // The form doesn't edit the curated destinations or the
                // management-API fields, so carry them over from the existing
                // profile on edit — otherwise saving an edit would wipe them. A
                // brand-new connection starts with none.
                let (destinations, management_url, management_username, management_password) =
                    editing
                        .and_then(|idx| self.profiles.get(idx))
                        .and_then(|p| match p {
                            ConnectionConfig::Amqp(a) => Some((
                                a.destinations.clone(),
                                a.management_url.clone(),
                                a.management_username.clone(),
                                a.management_password.clone(),
                            )),
                            _ => None,
                        })
                        .unwrap_or_default();
                ConnectionConfig::Amqp(AmqpProfile {
                    name,
                    host,
                    port,
                    username,
                    password: saved_spec,
                    tls,
                    destinations,
                    management_url,
                    management_username,
                    management_password,
                })
            }
            BrokerType::Rabbitmq => {
                // The DB slot is relabelled "Vhost" for RabbitMQ; empty → default "/".
                let vhost = {
                    let v = form.fields[3].trim();
                    if v.is_empty() {
                        "/".to_string()
                    } else {
                        v.to_string()
                    }
                };
                ConnectionConfig::Rabbitmq(RabbitmqProfile {
                    name,
                    host,
                    port,
                    vhost,
                    username,
                    password: saved_spec,
                    tls,
                })
            }
        };

        self.form = None;
        self.mode = InputMode::Normal;
        match editing {
            Some(idx) => self.save_edited_profile(idx, profile, session_password),
            None => self.save_new_profile(profile, session_password),
        }
    }

    /// Persist a brand-new profile (best effort) and open a connection to it.
    fn save_new_profile(&mut self, profile: ConnectionConfig, session_password: Option<String>) {
        // Append to the on-disk config and keep the in-memory list in sync. On a
        // write failure the profile isn't persisted but still connects for this
        // session; the pop avoids a duplicate if the user retries.
        self.config.connections.push(profile.clone());
        match config::save(&self.config_path, &self.config) {
            Ok(()) => self.profiles.push(profile.clone()),
            Err(e) => {
                self.config.connections.pop();
                self.set_status(format!("could not save config: {e}"), true);
            }
        }
        self.start_connect(profile, session_password);
    }

    /// Replace the profile at `idx` with the edited one, persist (best effort),
    /// and — if that profile currently has a live session — tear it down and
    /// reconnect with the new settings so the changes take effect at once. An
    /// offline profile is just saved.
    fn save_edited_profile(
        &mut self,
        idx: usize,
        profile: ConnectionConfig,
        session_password: Option<String>,
    ) {
        // The index was captured when the form opened and the modal blocks other
        // mutations, so it still addresses the same profile; fall back to an add
        // if it has somehow gone out of range.
        if idx >= self.profiles.len() {
            return self.save_new_profile(profile, session_password);
        }
        let old_name = self.profiles[idx].name().to_string();
        let was_connected = self.is_connected(&old_name);

        // An in-place replace can't duplicate, so unlike the add path it needs no
        // rollback: the edit applies in memory regardless, and a write failure
        // only means it isn't persisted to disk.
        self.config.connections[idx] = profile.clone();
        self.profiles[idx] = profile.clone();
        let saved = config::save(&self.config_path, &self.config);
        if let Err(e) = &saved {
            self.set_status(format!("could not save config: {e}"), true);
        }

        if was_connected {
            self.teardown_connection(&old_name);
            self.start_connect(profile, session_password);
        } else if saved.is_ok() {
            self.set_status(format!("Saved connection {}", profile.name()), false);
        }
    }

    /// Open the edit form for the selected saved connection, pre-filled from its
    /// profile. A no-op when nothing is selected.
    pub(super) fn edit_selected_profile(&mut self) {
        let Some(sel) = self.profile_state.selected() else {
            return;
        };
        let Some(profile) = self.profiles.get(sel) else {
            return;
        };
        self.form = Some(ConnForm::edit(sel, profile));
        self.mode = InputMode::Form;
    }

    /// Disconnect the selected profile's live session (the `x` key on the
    /// Connections tab). Reports whether anything was actually connected.
    pub(super) fn disconnect_selected_profile(&mut self) {
        let Some(sel) = self.profile_state.selected() else {
            return;
        };
        let Some(profile) = self.profiles.get(sel) else {
            return;
        };
        let name = profile.name().to_string();
        if self.teardown_connection(&name) {
            self.set_status(format!("Disconnected {name}"), false);
        } else {
            self.set_status(format!("{name} is not connected"), false);
        }
    }

    /// Handle a delete request from the edit form (Ctrl-D). The first press arms
    /// a confirmation; a confirming second press actually deletes. A no-op on the
    /// add form (nothing to delete).
    pub(super) fn form_delete_request(&mut self) {
        let Some(form) = self.form.as_mut() else {
            return;
        };
        let Some(idx) = form.editing else {
            return;
        };
        if !form.confirm_delete {
            form.confirm_delete = true;
            return;
        }
        self.delete_profile(idx);
    }

    /// Delete the saved profile at `idx`: drop any live session, forget it on
    /// disk and in memory, and close the form.
    fn delete_profile(&mut self, idx: usize) {
        if idx >= self.profiles.len() {
            return;
        }
        let name = self.profiles[idx].name().to_string();
        self.teardown_connection(&name);
        self.config.connections.remove(idx);
        self.profiles.remove(idx);
        let saved = config::save(&self.config_path, &self.config);
        self.form = None;
        self.mode = InputMode::Normal;
        self.clamp_profile_selection();
        match saved {
            Ok(()) => self.set_status(format!("Deleted connection {name}"), false),
            Err(e) => self.set_status(format!("Deleted {name} (could not save config: {e})"), true),
        }
    }

    /// Tear down a live connection by profile name (user-initiated). Mirrors the
    /// cleanup in [`Self::on_disconnected`] but leaves the status line to the
    /// caller, so disconnect / edit-reconnect / delete can each phrase their own.
    /// Returns whether a live connection was found and closed.
    fn teardown_connection(&mut self, name: &str) -> bool {
        let Some(idx) = self.connections.iter().position(|c| c.name == name) else {
            return false;
        };
        let id = self.connections[idx].id;
        self.connections[idx].handle.shutdown();
        self.connections.remove(idx);
        // Forget a closed connection as the `b` target.
        if self.last_browser == Some(id) {
            self.last_browser = None;
        }
        self.active = if self.connections.is_empty() {
            None
        } else {
            Some(0)
        };
        if self.connections.is_empty() {
            // A clean, user-initiated close: return to the home screen and read
            // as offline (not the error health a dropped link records).
            self.screen = Screen::Home;
            self.health = ConnHealth::Offline;
        }
        true
    }

    /// Keep the Connections-list cursor within range after a profile is removed.
    fn clamp_profile_selection(&mut self) {
        let len = self.profiles.len();
        let sel = if len == 0 {
            None
        } else {
            Some(self.profile_state.selected().unwrap_or(0).min(len - 1))
        };
        self.profile_state.select(sel);
    }

    pub(super) fn form_error(&mut self, message: &str) {
        if let Some(form) = &mut self.form {
            form.error = Some(message.to_string());
        }
    }

    // -- broker requests -----------------------------------------------------

    /// Start a fresh keyspace scan for connection `id`, decoupled from
    /// navigation. `live` is forwarded to [`Connection::begin_scan`]: a
    /// foreground scan (initial load, DB/filter change, explicit refresh)
    /// reveals keys as they load and clears the previous result first; a
    /// background scan (the auto-refresh) keeps the current list visible and
    /// swaps the fresh set in atomically once complete. The scan then drives
    /// itself page by page to completion in [`Self::on_keys_page`].
    pub(super) fn start_scan(&mut self, id: ConnId, live: bool) {
        let page_size = self.scan_count;
        if let Some(conn) = self.conn_by_id_mut(id) {
            let req = conn.begin_scan(live, page_size);
            conn.handle.send(ConnCommand::Browse(req));
        }
    }

    pub(super) fn request_selected_value(&mut self, id: ConnId) {
        if let Some(conn) = self.conn_by_id_mut(id) {
            if let Some(entry) = conn.selected().cloned() {
                if conn.inspector.value_key.as_deref() != Some(entry.key.as_str()) {
                    conn.inspector.value = None;
                    conn.inspector.value_key = Some(entry.key.clone());
                    conn.inspector.value_scroll = 0;
                    conn.handle.send(ConnCommand::Inspect(InspectReq {
                        db: conn.db,
                        key: entry.key,
                        offset: 0,
                        limit: VALUE_LIMIT,
                    }));
                }
            }
        }
    }

    pub(super) fn request_stats(&mut self, id: ConnId) {
        if let Some(conn) = self.conn_by_id(id) {
            conn.handle.send(ConnCommand::RefreshStats);
        }
    }

    // -- AMQP destination browser -------------------------------------------

    /// Peek the connection's currently selected destination (AMQP). A queue is
    /// peeked per the configured [`Self::peek_mode`]; a topic (which doesn't
    /// retain) or `Skip` mode clears the inspector instead of issuing a read.
    pub(super) fn request_peek(&mut self, id: ConnId) {
        let mode = self.peek_mode();
        if let Some(conn) = self.conn_by_id_mut(id) {
            // Selecting a destination always returns focus to the list and drops
            // any open detail view / search filter from the previous selection.
            conn.peek.focused = false;
            conn.peek.detail = false;
            conn.peek.selected = 0;
            conn.peek.filter.clear();
            conn.peek.limit_hit = false;
            let Some(dest) = conn.selected_destination().cloned() else {
                // Empty list / nothing selected: nothing to show.
                conn.peek.events.clear();
                conn.peek.peeked = None;
                conn.peek.pending = false;
                return;
            };
            let spec = dest.spec();
            let peekable = matches!(dest.kind, DestKind::Queue) && mode != config::PeekMode::Skip;
            // Reset the inspector to the freshly selected destination.
            conn.peek.peeked = Some(spec.clone());
            conn.peek.events.clear();
            conn.peek.scroll = 0;
            conn.peek.pending = peekable;
            if peekable {
                conn.handle.send(ConnCommand::Peek {
                    spec,
                    mode,
                    limit: VALUE_LIMIT,
                });
            }
        }
    }

    /// Populate an AMQP connection's destination list from its saved profile's
    /// curated `destinations`, selecting the first. A no-op when the profile has
    /// no saved destinations.
    pub(super) fn seed_destinations(&mut self, id: ConnId) {
        let Some(name) = self.conn_by_id(id).map(|c| c.name.clone()) else {
            return;
        };
        let specs: Vec<String> = self
            .profiles
            .iter()
            .find_map(|p| match p {
                ConnectionConfig::Amqp(a) if a.name == name => Some(a.destinations.clone()),
                _ => None,
            })
            .unwrap_or_default();
        if specs.is_empty() {
            return;
        }
        if let Some(conn) = self.conn_by_id_mut(id) {
            for spec in &specs {
                // Tolerate a malformed saved spec rather than failing the whole
                // seed; a bad entry is simply skipped.
                if let Ok(parsed) = SubSpec::parse(spec, 0) {
                    if let Some(dest) = Destination::from_spec(&parsed) {
                        conn.add_destination(dest);
                    }
                }
            }
            if !conn.destinations.items.is_empty() {
                conn.destinations.table.select(Some(0));
            }
        }
    }

    /// Add a curated destination (from a parsed source spec) to the active AMQP
    /// connection, persist the list, and peek the new selection.
    pub(super) fn add_amqp_destination(&mut self, spec: SubSpec) {
        let Some(dest) = Destination::from_spec(&spec) else {
            self.set_status(
                "not an AMQP destination — use topic:name or queue:name".to_string(),
                true,
            );
            return;
        };
        let Some(id) = self.active_id() else {
            return;
        };
        let label = dest.canonical();
        let newly = match self.conn_by_id_mut(id) {
            Some(conn) => conn.add_destination(dest),
            None => return,
        };
        if newly {
            self.persist_destinations(id);
            self.set_status(format!("Added {label}"), false);
        } else {
            self.set_status(format!("{label} is already in the list"), false);
        }
        self.request_peek(id);
    }

    /// Publish `body` to the active AMQP connection's selected destination. The
    /// browser's only write: gated on the connection's [`can_publish`] capability
    /// and a selected destination, with the result reported via
    /// [`AppEvent::Published`](crate::event::AppEvent::Published).
    pub(super) fn publish_to_selected(&mut self, body: String) {
        let Some(id) = self.active_id() else {
            return;
        };
        let outcome = match self.conn_by_id(id) {
            Some(conn) if !conn.caps.can_publish => {
                Err("this connection cannot publish".to_string())
            }
            Some(conn) => match conn.selected_destination().map(|d| d.spec()) {
                Some(spec) => {
                    let label = spec.label();
                    conn.handle.send(ConnCommand::Publish {
                        spec,
                        body: body.into_bytes(),
                    });
                    Ok(label)
                }
                None => Err("no destination selected to publish to".to_string()),
            },
            None => return,
        };
        match outcome {
            Ok(label) => self.set_status(format!("Publishing to {label}…"), false),
            Err(e) => self.set_status(e, true),
        }
    }

    /// Remove the highlighted destination from the active AMQP connection,
    /// persist the list, and peek the new selection.
    pub(super) fn delete_selected_destination(&mut self) {
        let Some(id) = self.active_id() else {
            return;
        };
        let removed = match self.conn_by_id_mut(id) {
            Some(conn) => conn.remove_selected_destination(),
            None => return,
        };
        if let Some(dest) = removed {
            let label = dest.canonical();
            self.persist_destinations(id);
            self.set_status(format!("Removed {label}"), false);
            self.request_peek(id);
        }
    }

    /// Write the connection's current destination list back into its saved AMQP
    /// profile (config + in-memory list) and persist. Best-effort: a write
    /// failure surfaces as a footer status but the in-memory list still changes.
    pub(super) fn persist_destinations(&mut self, id: ConnId) {
        let Some((name, specs)) = self.conn_by_id(id).map(|c| {
            let specs: Vec<String> = c.destinations.items.iter().map(|d| d.canonical()).collect();
            (c.name.clone(), specs)
        }) else {
            return;
        };
        let mut changed = false;
        for p in self.config.connections.iter_mut() {
            if let ConnectionConfig::Amqp(a) = p {
                if a.name == name {
                    a.destinations = specs.clone();
                    changed = true;
                }
            }
        }
        for p in self.profiles.iter_mut() {
            if let ConnectionConfig::Amqp(a) = p {
                if a.name == name {
                    a.destinations = specs.clone();
                }
            }
        }
        if changed {
            if let Err(e) = config::save(&self.config_path, &self.config) {
                self.set_status(format!("could not save destinations: {e}"), true);
            }
        }
    }

    /// Discover the broker's topics/queues over the ActiveMQ management API and
    /// merge them into the connection's destination browser (see
    /// [`crate::broker::jolokia`]). A no-op unless the connection is AMQP *and*
    /// its profile names a `management_url`. `announce` controls user feedback:
    /// the manual refresh key sets it (so "discovering…"/"not configured" shows),
    /// while the auto-trigger on connect runs silently. The actual discovery runs
    /// off the render thread and reports back via
    /// [`AppEvent::DestinationsDiscovered`].
    pub(super) fn discover_destinations(&mut self, id: ConnId, announce: bool) {
        let Some(name) = self.conn_by_id(id).map(|c| c.name.clone()) else {
            return;
        };
        let profile = self.profiles.iter().find_map(|p| match p {
            ConnectionConfig::Amqp(a) if a.name == name => Some(a.clone()),
            _ => None,
        });
        let Some(profile) = profile else {
            if announce {
                self.set_status(
                    "destination discovery is only available for AMQP connections".to_string(),
                    true,
                );
            }
            return;
        };
        let Some(base_url) = profile.management_url.clone() else {
            if announce {
                self.set_status(
                    "set management_url on this connection to discover destinations".to_string(),
                    true,
                );
            }
            return;
        };
        let username = profile.management_username.clone();
        let spec = profile.management_password_spec();
        // Keyring lookups key off an account; fall back to the connection name
        // when no management username is set.
        let account = username.clone().unwrap_or_else(|| name.clone());
        let events = self.events.clone();
        if announce {
            self.set_status(format!("Discovering destinations on {base_url}…"), false);
        }
        tokio::spawn(async move {
            // Resolve the management secret off the render thread (keyring blocks).
            let password = match config::resolve_secret_async(spec, account).await {
                Ok(pw) => pw,
                Err(e) => {
                    let _ = events
                        .send(AppEvent::DestinationsDiscovered {
                            id,
                            result: Err(format!("auth: {e}")),
                        })
                        .await;
                    return;
                }
            };
            // The Jolokia client is blocking, so run it off the async runtime.
            let result = tokio::task::spawn_blocking(move || {
                crate::broker::jolokia::discover(
                    &base_url,
                    username.as_deref(),
                    password.as_deref(),
                )
            })
            .await
            .map_err(|e| format!("discovery task failed: {e}"))
            .and_then(|r| r.map_err(|e| format!("{e:#}")));
            let _ = events
                .send(AppEvent::DestinationsDiscovered { id, result })
                .await;
        });
    }

    pub(super) fn apply_filter(&mut self) {
        let raw = self.filter.trim().to_string();
        let pattern = if raw.is_empty() {
            "*".to_string()
        } else if raw.contains(['*', '?', '[']) {
            raw
        } else {
            format!("*{raw}*")
        };
        if let Some(conn) = self.active_conn_mut() {
            conn.browser.pattern = pattern;
        }
        if let Some(id) = self.active_id() {
            self.start_scan(id, true);
        }
    }
}
