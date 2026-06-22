//! Read-only command console: validation and reply rendering.
//!
//! The v1 contract is **read + record only** — the console must never mutate
//! data or touch the server's admin surface. Safety is enforced in two layers:
//!
//! 1. [`validate_readonly`] — a *deny-by-default* static gate. Only commands on
//!    an explicit read-only allowlist (and, for subcommand-bearing commands like
//!    `CONFIG`, only specific safe subcommands) are accepted; everything else is
//!    rejected before a single byte hits the socket.
//! 2. [`ensure_server_readonly`] — a defense-in-depth check that asks the server
//!    (`COMMAND INFO`) for the command's own flags and refuses anything flagged
//!    `write`/`admin`/`blocking`/`pubsub`. This can only further restrict what
//!    the allowlist permits, never widen it.
//!
//! Parsing and rendering are pure and unit-tested; the server check is exercised
//! by the integration suite.

use redis::aio::ConnectionManager;
use redis::Value;

use base64::Engine as _;

/// Commands that are read-only and safe regardless of their arguments.
///
/// Deliberately conservative: anything that writes, blocks, runs scripts, or
/// alters connection/server state is omitted (and would also be caught by
/// [`ensure_server_readonly`]). Notably absent: `SORT` (can `STORE`), `GETEX`/
/// `GETDEL` (mutate), `SUBSCRIBE`/`MONITOR` (use the realtime tails), `SELECT`/
/// `MULTI`/`HELLO`/`AUTH` (stateful), and `DUMP` (read-only, but returns opaque
/// RDB-serialized bytes that are useless in a text console). `OBJECT` is gated
/// per-subcommand instead (see [`SUBCOMMAND_COMMANDS`]).
///
/// Each `// group` comment heads the lines that follow it.
const READONLY_COMMANDS: &[&str] = &[
    // strings
    "GET",
    "GETRANGE",
    "SUBSTR",
    "STRLEN",
    "MGET",
    // generic / keyspace
    "EXISTS",
    "TYPE",
    "TTL",
    "PTTL",
    "EXPIRETIME",
    "PEXPIRETIME",
    "KEYS",
    "SCAN",
    "RANDOMKEY",
    "DBSIZE",
    // bitmaps
    "GETBIT",
    "BITCOUNT",
    "BITPOS",
    "BITFIELD_RO",
    // hashes
    "HGET",
    "HMGET",
    "HGETALL",
    "HKEYS",
    "HVALS",
    "HLEN",
    "HEXISTS",
    "HSCAN",
    "HSTRLEN",
    "HRANDFIELD",
    // lists
    "LRANGE",
    "LLEN",
    "LINDEX",
    "LPOS",
    // sets
    "SMEMBERS",
    "SISMEMBER",
    "SMISMEMBER",
    "SCARD",
    "SSCAN",
    "SRANDMEMBER",
    "SINTER",
    "SUNION",
    "SDIFF",
    "SINTERCARD",
    // sorted sets
    "ZRANGE",
    "ZRANGEBYSCORE",
    "ZREVRANGE",
    "ZREVRANGEBYSCORE",
    "ZRANGEBYLEX",
    "ZREVRANGEBYLEX",
    "ZCARD",
    "ZCOUNT",
    "ZSCORE",
    "ZMSCORE",
    "ZRANK",
    "ZREVRANK",
    "ZSCAN",
    "ZRANDMEMBER",
    "ZLEXCOUNT",
    "ZUNION",
    "ZINTER",
    "ZDIFF",
    "ZINTERCARD",
    // streams
    "XRANGE",
    "XREVRANGE",
    "XLEN",
    "XPENDING",
    // hyperloglog
    "PFCOUNT",
    // geo (read-only forms)
    "GEOPOS",
    "GEODIST",
    "GEOHASH",
    "GEOSEARCH",
    "GEORADIUS_RO",
    "GEORADIUSBYMEMBER_RO",
    // sort (read-only form)
    "SORT_RO",
    // server / connection (read-only)
    "INFO",
    "TIME",
    "LASTSAVE",
    "LOLWUT",
    "PING",
    "ECHO",
];

/// Commands whose safety depends on their first subcommand. The key is the
/// uppercased command; only the listed subcommands are accepted.
fn allowed_subcommand(cmd: &str, sub: &str) -> bool {
    let sub = sub.to_ascii_uppercase();
    let allowed: &[&str] = match cmd {
        "CONFIG" => &["GET"],
        "CLIENT" => &["ID", "GETNAME", "INFO", "LIST"],
        "CLUSTER" => &[
            "INFO",
            "NODES",
            "SLOTS",
            "SHARDS",
            "MYID",
            "LINKS",
            "KEYSLOT",
            "COUNTKEYSINSLOT",
            "GETKEYSINSLOT",
        ],
        "COMMAND" => &["COUNT", "DOCS", "INFO", "GETKEYS", "LIST"],
        "OBJECT" => &["ENCODING", "REFCOUNT", "IDLETIME", "FREQ", "HELP"],
        "MEMORY" => &["USAGE", "STATS", "DOCTOR"],
        "LATENCY" => &["HISTORY", "LATEST", "DOCTOR"],
        "SLOWLOG" => &["GET", "LEN"],
        "XINFO" => &["STREAM", "GROUPS", "CONSUMERS", "HELP"],
        "PUBSUB" => &[
            "CHANNELS",
            "NUMSUB",
            "NUMPAT",
            "SHARDCHANNELS",
            "SHARDNUMSUB",
        ],
        _ => return false,
    };
    allowed.contains(&sub.as_str())
}

/// Commands that require a subcommand check (see [`allowed_subcommand`]).
const SUBCOMMAND_COMMANDS: &[&str] = &[
    "CONFIG", "CLIENT", "CLUSTER", "COMMAND", "OBJECT", "MEMORY", "LATENCY", "SLOWLOG", "XINFO",
    "PUBSUB",
];

/// Validate `input` against the read-only allowlist, returning its parsed argv
/// (command + arguments) on success. Deny-by-default: an unrecognised command is
/// rejected.
pub fn validate_readonly(input: &str) -> anyhow::Result<Vec<String>> {
    let parts = tokenize(input)?;
    let Some(first) = parts.first() else {
        anyhow::bail!("enter a command, e.g. `GET key`");
    };
    let cmd = first.to_ascii_uppercase();

    if SUBCOMMAND_COMMANDS.contains(&cmd.as_str()) {
        let sub = parts.get(1).map(String::as_str).unwrap_or("");
        if sub.is_empty() {
            anyhow::bail!("`{cmd}` needs a read-only subcommand (e.g. `{cmd} GET`)");
        }
        if !allowed_subcommand(&cmd, sub) {
            anyhow::bail!("`{cmd} {sub}` is not allowed in the read-only console");
        }
        return Ok(parts);
    }

    if READONLY_COMMANDS.contains(&cmd.as_str()) {
        return Ok(parts);
    }
    anyhow::bail!(
        "`{cmd}` is not on the read-only allowlist — this console refuses writes and admin commands"
    )
}

/// Defense in depth: confirm the server itself classifies the command as
/// read-only. Rejects anything `COMMAND INFO` flags as `write`/`admin`/
/// `blocking`/`pubsub`. Skipped for subcommand-bearing commands (e.g. `CONFIG`
/// is flagged `admin` as a whole, yet `CONFIG GET` is read-only and already
/// gated by the static subcommand allowlist).
pub async fn ensure_server_readonly(
    conn: &mut ConnectionManager,
    parts: &[String],
) -> anyhow::Result<()> {
    let cmd = parts[0].to_ascii_uppercase();
    if SUBCOMMAND_COMMANDS.contains(&cmd.as_str()) {
        return Ok(());
    }
    let info: Value = redis::cmd("COMMAND")
        .arg("INFO")
        .arg(&parts[0])
        .query_async(conn)
        .await
        .map_err(|e| anyhow::anyhow!("checking command flags: {e}"))?;
    let flags = extract_flags(&info);
    for unsafe_flag in ["write", "admin", "blocking", "pubsub"] {
        if flags.iter().any(|f| f.eq_ignore_ascii_case(unsafe_flag)) {
            anyhow::bail!("server reports `{cmd}` as `{unsafe_flag}`; refusing to run it");
        }
    }
    Ok(())
}

/// Pull the flag list out of a `COMMAND INFO <cmd>` reply. The reply is an array
/// with one entry per command; entry layout is `[name, arity, [flags…], …]`. An
/// unknown command yields `Nil`, so the flags come back empty (the static
/// allowlist remains the gate in that case).
fn extract_flags(reply: &Value) -> Vec<String> {
    let Value::Array(entries) = reply else {
        return Vec::new();
    };
    let Some(Value::Array(entry)) = entries.first() else {
        return Vec::new();
    };
    match entry.get(2) {
        Some(Value::Array(flags)) | Some(Value::Set(flags)) => {
            flags.iter().filter_map(as_string).collect()
        }
        _ => Vec::new(),
    }
}

/// Extract a displayable string from a scalar reply value, if it is one.
fn as_string(v: &Value) -> Option<String> {
    match v {
        Value::SimpleString(s) => Some(s.clone()),
        Value::BulkString(b) => Some(String::from_utf8_lossy(b).into_owned()),
        Value::VerbatimString { text, .. } => Some(text.clone()),
        Value::Okay => Some("OK".to_string()),
        _ => None,
    }
}

/// Render a reply value for display in the console output pane (redis-cli-ish:
/// one element per line, `N)`-indexed arrays, binary bulk strings base64-tagged).
pub fn render_reply(value: &Value) -> String {
    let mut lines = Vec::new();
    render_into(value, 0, &mut lines);
    if lines.is_empty() {
        "(nil)".to_string()
    } else {
        lines.join("\n")
    }
}

fn render_into(value: &Value, indent: usize, lines: &mut Vec<String>) {
    let pad = "  ".repeat(indent);
    match value {
        Value::Nil => lines.push(format!("{pad}(nil)")),
        Value::Int(n) => lines.push(format!("{pad}(integer) {n}")),
        Value::Double(d) => lines.push(format!("{pad}(double) {d}")),
        Value::Boolean(b) => lines.push(format!("{pad}{b}")),
        Value::SimpleString(s) => lines.push(format!("{pad}{s}")),
        Value::Okay => lines.push(format!("{pad}OK")),
        Value::BulkString(bytes) => lines.push(format!("{pad}{}", bulk_display(bytes))),
        Value::VerbatimString { text, .. } => {
            for line in text.lines() {
                lines.push(format!("{pad}{line}"));
            }
        }
        Value::Array(items) | Value::Set(items) => {
            if items.is_empty() {
                lines.push(format!("{pad}(empty)"));
            }
            for (i, item) in items.iter().enumerate() {
                render_indexed(i + 1, item, indent, lines);
            }
        }
        Value::Map(pairs) => {
            if pairs.is_empty() {
                lines.push(format!("{pad}(empty)"));
            }
            for (k, v) in pairs {
                let key = as_string(k).unwrap_or_else(|| format!("{k:?}"));
                match v {
                    Value::Array(_) | Value::Set(_) | Value::Map(_) => {
                        lines.push(format!("{pad}{key}:"));
                        render_into(v, indent + 1, lines);
                    }
                    scalar => {
                        let mut tmp = Vec::new();
                        render_into(scalar, 0, &mut tmp);
                        lines.push(format!("{pad}{key}: {}", tmp.join(" ")));
                    }
                }
            }
        }
        // ServerError / Push / BigNumber / Attribute and any future variants:
        // a faithful debug rendering is good enough for the v1 console.
        other => lines.push(format!("{pad}{other:?}")),
    }
}

/// Render one indexed array element: scalars inline after `N)`, nested
/// collections on the following indented lines.
fn render_indexed(index: usize, value: &Value, indent: usize, lines: &mut Vec<String>) {
    let pad = "  ".repeat(indent);
    match value {
        Value::Array(_) | Value::Set(_) | Value::Map(_) => {
            lines.push(format!("{pad}{index})"));
            render_into(value, indent + 1, lines);
        }
        scalar => {
            let mut tmp = Vec::new();
            render_into(scalar, 0, &mut tmp);
            lines.push(format!("{pad}{index}) {}", tmp.join(" ")));
        }
    }
}

/// Display a bulk string: verbatim if valid UTF-8, else base64 with a tag.
fn bulk_display(bytes: &[u8]) -> String {
    match std::str::from_utf8(bytes) {
        Ok(s) => s.to_string(),
        Err(_) => format!(
            "base64:{}",
            base64::engine::general_purpose::STANDARD.encode(bytes)
        ),
    }
}

/// Tokenize a command line, honouring double and single quotes (double quotes
/// process `\"`, `\\`, `\n`, `\t`, `\r`; single quotes are literal except `\'`).
fn tokenize(input: &str) -> anyhow::Result<Vec<String>> {
    let mut tokens = Vec::new();
    let mut chars = input.chars().peekable();
    loop {
        // Skip inter-token whitespace.
        while matches!(chars.peek(), Some(c) if c.is_whitespace()) {
            chars.next();
        }
        let Some(&c) = chars.peek() else { break };
        let mut token = String::new();
        match c {
            '"' => {
                chars.next();
                loop {
                    match chars.next() {
                        Some('"') => break,
                        Some('\\') => match chars.next() {
                            Some('n') => token.push('\n'),
                            Some('t') => token.push('\t'),
                            Some('r') => token.push('\r'),
                            Some(other) => token.push(other), // \" \\ and any other
                            None => anyhow::bail!("unterminated quoted string"),
                        },
                        Some(other) => token.push(other),
                        None => anyhow::bail!("unterminated quoted string"),
                    }
                }
            }
            '\'' => {
                chars.next();
                loop {
                    match chars.next() {
                        Some('\'') => break,
                        Some('\\') if matches!(chars.peek(), Some('\'')) => {
                            chars.next();
                            token.push('\'');
                        }
                        Some(other) => token.push(other),
                        None => anyhow::bail!("unterminated quoted string"),
                    }
                }
            }
            _ => {
                while let Some(&c) = chars.peek() {
                    if c.is_whitespace() {
                        break;
                    }
                    token.push(c);
                    chars.next();
                }
            }
        }
        tokens.push(token);
    }
    Ok(tokens)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allows_simple_read_commands() {
        assert_eq!(
            validate_readonly("GET mykey").unwrap(),
            vec!["GET", "mykey"]
        );
        // Command names are case-insensitive for the gate, preserved for exec.
        assert_eq!(
            validate_readonly("hgetall h").unwrap(),
            vec!["hgetall", "h"]
        );
        assert!(validate_readonly("LRANGE l 0 -1").is_ok());
        assert!(validate_readonly("PING").is_ok());
        assert!(validate_readonly("SCAN 0 MATCH user:* COUNT 100").is_ok());
    }

    #[test]
    fn rejects_writes_and_admin() {
        for bad in [
            "SET k v",
            "DEL k",
            "FLUSHALL",
            "FLUSHDB",
            "EXPIRE k 10",
            "HSET h f v",
            "LPUSH l x",
            "ZADD z 1 m",
            "XADD s * f v",
            "RENAME a b",
            "SHUTDOWN",
            "DEBUG SLEEP 1",
            "SUBSCRIBE ch",
            "PSUBSCRIBE ch.*",
            "MONITOR",
            "EVAL \"return 1\" 0",
            "SCRIPT LOAD x",
            "FUNCTION LIST",
            "MULTI",
            "SELECT 1",
            "HELLO 3",
            "AUTH pw",
            "GETDEL k",
            "GETEX k EX 10",
            "SORT mylist",
            "ACL WHOAMI",
        ] {
            assert!(
                validate_readonly(bad).is_err(),
                "must reject non-read command: {bad}"
            );
        }
    }

    #[test]
    fn subcommand_gate_allows_only_safe_subcommands() {
        assert!(validate_readonly("CONFIG GET maxmemory").is_ok());
        assert!(validate_readonly("config get maxmemory").is_ok());
        assert!(
            validate_readonly("CONFIG SET maxmemory 0").is_err(),
            "CONFIG SET is a write"
        );
        assert!(
            validate_readonly("CONFIG REWRITE").is_err(),
            "CONFIG REWRITE is admin"
        );
        assert!(validate_readonly("CLIENT LIST").is_ok());
        assert!(
            validate_readonly("CLIENT KILL 1.2.3.4:5").is_err(),
            "CLIENT KILL is dangerous"
        );
        assert!(validate_readonly("OBJECT ENCODING k").is_ok());
        assert!(validate_readonly("XINFO STREAM s").is_ok());
        assert!(
            validate_readonly("CONFIG").is_err(),
            "a subcommand command with no subcommand is rejected"
        );
    }

    #[test]
    fn subcommand_gate_covers_every_subcommand_command() {
        // Subcommand-bearing commands have NO server-side backstop
        // (`ensure_server_readonly` skips them), so the static table is the only
        // gate — exercise a safe and an unsafe subcommand for each.
        for ok in [
            "CONFIG GET maxmemory",
            "CLIENT INFO",
            "CLUSTER INFO",
            "COMMAND COUNT",
            "OBJECT ENCODING k",
            "MEMORY USAGE k",
            "LATENCY LATEST",
            "SLOWLOG GET",
            "XINFO STREAM s",
            "PUBSUB CHANNELS",
        ] {
            assert!(validate_readonly(ok).is_ok(), "should allow `{ok}`");
        }
        for bad in [
            "CONFIG RESETSTAT", // resets server stats
            "CLIENT KILL 1.2.3.4:5",
            "CLUSTER RESET",
            "CLUSTER FORGET nodeid",
            "COMMAND BOGUS", // unknown subcommand
            "OBJECT FREQX k",
            "MEMORY PURGE",        // frees memory (admin)
            "MEMORY MALLOC-STATS", // not on the allowlist
            "LATENCY RESET",       // clears latency history
            "SLOWLOG RESET",       // clears the slow log
            "XINFO HELPER s",
            "PUBSUB SHARDNUMSUBX",
        ] {
            assert!(validate_readonly(bad).is_err(), "should refuse `{bad}`");
        }
    }

    #[test]
    fn dump_is_no_longer_allowed() {
        // DUMP is read-only but returns opaque RDB bytes; it was dropped from the
        // allowlist as part of keeping the console deliberately conservative.
        assert!(validate_readonly("DUMP k").is_err());
    }

    #[test]
    fn empty_input_is_rejected() {
        assert!(validate_readonly("").is_err());
        assert!(validate_readonly("   ").is_err());
    }

    #[test]
    fn tokenize_handles_quotes_and_escapes() {
        assert_eq!(tokenize("GET foo").unwrap(), vec!["GET", "foo"]);
        assert_eq!(
            tokenize(r#"SET "a b" 'c d'"#).unwrap(),
            vec!["SET", "a b", "c d"]
        );
        assert_eq!(
            tokenize(r#"GET "he said \"hi\"""#).unwrap(),
            vec!["GET", r#"he said "hi""#]
        );
        assert_eq!(
            tokenize(r#"X "line\nbreak""#).unwrap(),
            vec!["X", "line\nbreak"]
        );
        // Extra whitespace between tokens is collapsed.
        assert_eq!(tokenize("  GET   foo  ").unwrap(), vec!["GET", "foo"]);
    }

    #[test]
    fn tokenize_rejects_unterminated_quotes() {
        assert!(tokenize(r#"GET "unterminated"#).is_err());
        assert!(tokenize("GET 'unterminated").is_err());
    }

    #[test]
    fn extract_flags_reads_command_info_shape() {
        // COMMAND INFO get -> [[ "get", 2, ["readonly","fast"], 1, 1, 1 ]]
        let reply = Value::Array(vec![Value::Array(vec![
            Value::BulkString(b"get".to_vec()),
            Value::Int(2),
            Value::Array(vec![
                Value::SimpleString("readonly".into()),
                Value::SimpleString("fast".into()),
            ]),
        ])]);
        let flags = extract_flags(&reply);
        assert_eq!(flags, vec!["readonly", "fast"]);
        // Unknown command -> Nil entry -> no flags.
        assert!(extract_flags(&Value::Array(vec![Value::Nil])).is_empty());
        assert!(extract_flags(&Value::Nil).is_empty());
    }

    #[test]
    fn render_scalars() {
        assert_eq!(render_reply(&Value::Nil), "(nil)");
        assert_eq!(render_reply(&Value::Int(42)), "(integer) 42");
        assert_eq!(render_reply(&Value::Okay), "OK");
        assert_eq!(render_reply(&Value::BulkString(b"hello".to_vec())), "hello");
        assert_eq!(render_reply(&Value::SimpleString("PONG".into())), "PONG");
    }

    #[test]
    fn render_binary_bulk_is_base64_tagged() {
        let out = render_reply(&Value::BulkString(vec![0x00, 0xff]));
        assert_eq!(out, "base64:AP8=");
    }

    #[test]
    fn render_arrays_are_indexed() {
        let v = Value::Array(vec![
            Value::BulkString(b"a".to_vec()),
            Value::BulkString(b"b".to_vec()),
        ]);
        assert_eq!(render_reply(&v), "1) a\n2) b");
        assert_eq!(render_reply(&Value::Array(vec![])), "(empty)");
    }

    #[test]
    fn render_nested_arrays_indent() {
        let v = Value::Array(vec![
            Value::BulkString(b"top".to_vec()),
            Value::Array(vec![Value::Int(1), Value::Int(2)]),
        ]);
        assert_eq!(
            render_reply(&v),
            "1) top\n2)\n  1) (integer) 1\n  2) (integer) 2"
        );
    }
}
