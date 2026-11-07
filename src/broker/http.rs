//! Shared plumbing for the blocking HTTP management clients (ActiveMQ's Jolokia
//! in [`super::jolokia`], RabbitMQ's management API in
//! `super::rabbitmq::mgmt`): one agent configuration, HTTP Basic auth, and the
//! fallback wording for a non-2xx reply.
//!
//! The clients are blocking (`ureq`); callers drive them from a blocking context
//! (`tokio::task::spawn_blocking`) so they never stall the async runtime.

use std::time::Duration;

use base64::Engine as _;

/// How long to wait to connect to / read from a management API before giving
/// up. Management calls are best-effort enrichment, so they fail fast rather
/// than hanging the UI when the endpoint is unreachable.
pub(crate) const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
pub(crate) const READ_TIMEOUT: Duration = Duration::from_secs(5);

/// A `ureq` agent configured for a management API: the short timeouts above,
/// non-2xx replies kept as ordinary responses, and no proxy.
pub(crate) fn agent() -> ureq::Agent {
    ureq::Agent::config_builder()
        .timeout_connect(Some(CONNECT_TIMEOUT))
        .timeout_recv_response(Some(READ_TIMEOUT))
        .timeout_recv_body(Some(READ_TIMEOUT))
        // Non-2xx replies stay ordinary responses instead of becoming
        // `Error::StatusCode`, which carries the code but not the body — and the
        // body is what explains a rejection (see [`status_error`]).
        .http_status_as_error(false)
        // A management API is an internal endpoint reached directly; ureq would
        // otherwise route it through a `HTTP_PROXY` picked up from the
        // environment.
        .proxy(None)
        .build()
        .into()
}

/// The `Authorization` header value for HTTP Basic credentials.
pub(crate) fn basic_auth(username: &str, password: Option<&str>) -> String {
    let token = base64::engine::general_purpose::STANDARD
        .encode(format!("{username}:{}", password.unwrap_or("")));
    format!("Basic {token}")
}

/// Read a reply's status and body text, or explain why the request never got
/// one (an unreachable endpoint surfaces here as a transport error).
pub(crate) fn read_reply(
    sent: Result<ureq::http::Response<ureq::Body>, ureq::Error>,
) -> anyhow::Result<(u16, String)> {
    let mut resp = sent.map_err(|e| anyhow::anyhow!("could not reach the management API: {e}"))?;
    let status = resp.status().as_u16();
    let text = resp
        .body_mut()
        .read_to_string()
        .map_err(|e| anyhow::anyhow!("could not read the management API response: {e}"))?;
    Ok((status, text))
}

/// Turn a non-2xx reply into a user-facing message. A 401 means the credentials
/// are missing or wrong; anything else carries its body, if any.
pub(crate) fn status_error(code: u16, body: &str) -> anyhow::Error {
    if code == 401 {
        return anyhow::anyhow!(
            "the management API rejected the credentials (HTTP 401) — set \
             management_username/management_password"
        );
    }
    let body = body.trim();
    if body.is_empty() {
        anyhow::anyhow!("the management API returned HTTP {code}")
    } else {
        anyhow::anyhow!("the management API returned HTTP {code}: {body}")
    }
}

/// Loopback HTTP servers for driving the management clients over a real socket
/// — the only way to cover the request the HTTP client actually puts on the
/// wire.
#[cfg(test)]
pub(crate) mod stub {
    use std::io::{BufRead, BufReader, Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::thread::{self, JoinHandle};

    /// A one-shot HTTP server on loopback: answers the next request with
    /// `status`/`body`, then hands the raw request text back through its join
    /// handle.
    pub(crate) fn stub_console(status: u16, body: &str) -> (String, JoinHandle<String>) {
        let (base_url, server) = stub_server(vec![(status, body.to_string())]);
        let handle = thread::spawn(move || {
            server
                .join()
                .expect("stub server thread")
                .pop()
                .expect("one request")
        });
        (base_url, handle)
    }

    /// An HTTP server on loopback that answers one request per reply, in order,
    /// each on its own connection (every reply says `Connection: close`), then
    /// stops listening. The join handle yields the raw requests in the order
    /// they arrived. A client that sends fewer requests leaves the join waiting,
    /// and one that sends more finds the port closed.
    pub(crate) fn stub_server(replies: Vec<(u16, String)>) -> (String, JoinHandle<Vec<String>>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback");
        let base_url = format!("http://{}", listener.local_addr().expect("local addr"));
        let handle = thread::spawn(move || {
            replies
                .iter()
                .map(|(status, body)| {
                    let (sock, _) = listener.accept().expect("accept");
                    answer(sock, *status, body)
                })
                .collect()
        });
        (base_url, handle)
    }

    /// Read one request (head, then exactly as many body bytes as it declares),
    /// answer it, and return its raw text.
    fn answer(mut sock: TcpStream, status: u16, body: &str) -> String {
        let response = format!(
            "HTTP/1.1 {status} Stub\r\nContent-Type: application/json\r\n\
             Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        let mut reader = BufReader::new(sock.try_clone().expect("clone socket"));
        let mut head = String::new();
        loop {
            let mut line = String::new();
            if reader.read_line(&mut line).expect("read head") == 0 {
                break;
            }
            let blank = line == "\r\n";
            head.push_str(&line);
            if blank {
                break;
            }
        }
        let len = head
            .lines()
            .filter_map(|l| l.split_once(':'))
            .find(|(k, _)| k.eq_ignore_ascii_case("content-length"))
            .and_then(|(_, v)| v.trim().parse::<usize>().ok())
            .unwrap_or(0);
        let mut buf = vec![0u8; len];
        reader.read_exact(&mut buf).expect("read body");
        sock.write_all(response.as_bytes()).expect("write response");
        sock.flush().expect("flush response");
        format!("{head}{}", String::from_utf8_lossy(&buf))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_auth_encodes_the_credentials() {
        // base64("admin:admin")
        assert_eq!(basic_auth("admin", Some("admin")), "Basic YWRtaW46YWRtaW4=");
        // A missing password is sent as empty: base64("u:")
        assert_eq!(basic_auth("u", None), "Basic dTo=");
    }
}
