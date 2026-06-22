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
            match spawn_connection(
                id,
                name,
                conn,
                events.clone(),
                &tracker,
                &cancel,
                recordings_dir,
            )
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

        let profile = match form.kind {
            BrokerKind::Redis => {
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
            BrokerKind::Amqp => ConnectionConfig::Amqp(AmqpProfile {
                name,
                host,
                port,
                username,
                password: saved_spec,
                tls,
            }),
            BrokerKind::Rabbitmq => {
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

        // Persist (best effort) and keep the in-memory profile list in sync.
        self.config.connections.push(profile.clone());
        match config::save(&self.config_path, &self.config) {
            Ok(()) => self.profiles.push(profile.clone()),
            Err(e) => {
                self.config.connections.pop();
                self.set_status(format!("could not save config: {e}"), true);
            }
        }

        self.form = None;
        self.mode = InputMode::Normal;
        self.start_connect(profile, session_password);
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
                if conn.value_key.as_deref() != Some(entry.key.as_str()) {
                    conn.value = None;
                    conn.value_key = Some(entry.key.clone());
                    conn.value_scroll = 0;
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
            conn.pattern = pattern;
        }
        if let Some(id) = self.active_id() {
            self.start_scan(id, true);
        }
    }
}
