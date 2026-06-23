//! Pure parsers for the Redis `INFO` and `CLIENT LIST` replies. Kept free of
//! I/O so they can be unit tested with fixtures.

use crate::broker::{ClientInfo, ServerStats};

/// Parse an `INFO` reply into [`ServerStats`].
///
/// The reply is a series of `# Section` headers and `key:value` lines separated
/// by `\r\n`. The `# Keyspace` section has `dbN:keys=...,expires=...` lines.
pub fn parse_info(text: &str) -> ServerStats {
    let mut stats = ServerStats::default();
    let mut section_name = String::new();
    let mut section_pairs: Vec<(String, String)> = Vec::new();

    for raw_line in text.lines() {
        let line = raw_line.trim_end(); // strip trailing \r
        if line.is_empty() {
            continue;
        }
        if let Some(name) = line.strip_prefix("# ") {
            if !section_name.is_empty() {
                stats
                    .sections
                    .push((section_name.clone(), std::mem::take(&mut section_pairs)));
            }
            section_name = name.trim().to_string();
            continue;
        }
        if let Some((key, value)) = line.split_once(':') {
            section_pairs.push((key.to_string(), value.to_string()));
            stats.raw.insert(key.to_string(), value.to_string());
        }
    }
    if !section_name.is_empty() {
        stats.sections.push((section_name, section_pairs));
    }

    let num = |k: &str| stats.raw.get(k).and_then(|v| v.parse::<u64>().ok());
    stats.redis_version = stats.raw.get("redis_version").cloned();
    stats.uptime_seconds = num("uptime_in_seconds");
    stats.connected_clients = num("connected_clients");
    stats.used_memory = num("used_memory");
    stats.used_memory_peak = num("used_memory_peak");
    stats.maxmemory = num("maxmemory");
    stats.instantaneous_ops_per_sec = num("instantaneous_ops_per_sec");
    stats.keyspace_hits = num("keyspace_hits");
    stats.keyspace_misses = num("keyspace_misses");

    let mut db_keys = Vec::new();
    for (key, value) in &stats.raw {
        if let Some(index) = key.strip_prefix("db") {
            if let Ok(db) = index.parse::<u32>() {
                if let Some(keys) = value
                    .split(',')
                    .find_map(|part| part.strip_prefix("keys="))
                    .and_then(|n| n.parse::<u64>().ok())
                {
                    db_keys.push((db, keys));
                }
            }
        }
    }
    db_keys.sort_by_key(|(db, _)| *db);
    stats.db_keys = db_keys;

    stats
}

/// Parse a Redis `CLIENT LIST` reply into [`ClientInfo`] rows, one per line.
///
/// Each line is a run of space-separated `key=value` fields
/// (`id=7 addr=127.0.0.1:6379 name=web age=12 idle=0 ... cmd=get`). Only the
/// fields keyhole surfaces are kept; anything else (and any field that fails to
/// parse) falls back to its type's default. Blank lines are ignored.
pub fn parse_client_list(text: &str) -> Vec<ClientInfo> {
    text.lines()
        .filter(|line| !line.trim().is_empty())
        .map(parse_client_line)
        .collect()
}

/// Parse one `CLIENT LIST` line into a [`ClientInfo`].
fn parse_client_line(line: &str) -> ClientInfo {
    let mut client = ClientInfo::default();
    for field in line.split_ascii_whitespace() {
        let Some((key, value)) = field.split_once('=') else {
            continue;
        };
        match key {
            "id" => client.id = value.parse().unwrap_or(0),
            "name" => client.name = value.to_string(),
            "addr" => client.addr = value.to_string(),
            "age" => client.age = value.parse().unwrap_or(0),
            "idle" => client.idle = value.parse().unwrap_or(0),
            "cmd" => client.last_cmd = value.to_string(),
            _ => {}
        }
    }
    client
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = "# Server\r\nredis_version:7.4.0\r\nuptime_in_seconds:12345\r\n\r\n# Clients\r\nconnected_clients:3\r\n\r\n# Memory\r\nused_memory:1048576\r\nused_memory_peak:2097152\r\nmaxmemory:0\r\n\r\n# Stats\r\ninstantaneous_ops_per_sec:7\r\nkeyspace_hits:90\r\nkeyspace_misses:10\r\n\r\n# Keyspace\r\ndb0:keys=9,expires=1,avg_ttl=0\r\ndb1:keys=1,expires=0,avg_ttl=0\r\n";

    #[test]
    fn parses_metrics() {
        let s = parse_info(SAMPLE);
        assert_eq!(s.redis_version.as_deref(), Some("7.4.0"));
        assert_eq!(s.uptime_seconds, Some(12345));
        assert_eq!(s.connected_clients, Some(3));
        assert_eq!(s.used_memory, Some(1_048_576));
        assert_eq!(s.used_memory_peak, Some(2_097_152));
        assert_eq!(s.instantaneous_ops_per_sec, Some(7));
        assert_eq!(s.keyspace_hits, Some(90));
        assert_eq!(s.keyspace_misses, Some(10));
    }

    #[test]
    fn parses_keyspace_and_sections() {
        let s = parse_info(SAMPLE);
        assert_eq!(s.db_keys, vec![(0, 9), (1, 1)]);
        let section_names: Vec<&str> = s.sections.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(
            section_names,
            vec!["Server", "Clients", "Memory", "Stats", "Keyspace"]
        );
    }

    #[test]
    fn computes_hit_ratio() {
        let s = parse_info(SAMPLE);
        assert_eq!(s.hit_ratio(), Some(0.9));
    }

    #[test]
    fn tolerates_empty_and_garbage() {
        let s = parse_info("");
        assert!(s.sections.is_empty());
        let s = parse_info("not an info reply\nrandom text");
        assert!(s.redis_version.is_none());
    }

    const CLIENTS: &str = "id=7 addr=127.0.0.1:50912 laddr=127.0.0.1:6379 fd=8 name=web age=42 idle=3 flags=N db=0 sub=0 cmd=get user=default\n\
id=9 addr=10.0.0.2:33344 laddr=127.0.0.1:6379 fd=12 name= age=5 idle=0 flags=N db=1 sub=1 cmd=client|list user=default\n";

    #[test]
    fn parses_client_list_fields() {
        let clients = parse_client_list(CLIENTS);
        assert_eq!(clients.len(), 2);
        assert_eq!(clients[0].id, 7);
        assert_eq!(clients[0].name, "web");
        assert_eq!(clients[0].addr, "127.0.0.1:50912");
        assert_eq!(clients[0].age, 42);
        assert_eq!(clients[0].idle, 3);
        assert_eq!(clients[0].last_cmd, "get");
        // An empty `name=` stays empty; multi-word commands keep their `|`.
        assert_eq!(clients[1].name, "");
        assert_eq!(clients[1].last_cmd, "client|list");
    }

    #[test]
    fn client_list_tolerates_blank_lines_and_missing_fields() {
        // A trailing/blank line yields no row; a line missing fields fills the
        // gaps with defaults rather than panicking or being dropped.
        let clients = parse_client_list("\nid=1 addr=x:1\n\n");
        assert_eq!(clients.len(), 1);
        assert_eq!(clients[0].id, 1);
        assert_eq!(clients[0].addr, "x:1");
        assert_eq!(clients[0].age, 0);
        assert_eq!(clients[0].name, "");
        assert!(parse_client_list("").is_empty());
    }
}
