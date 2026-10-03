//! Loopback HTTP server for `asg serve` (#7).
//!
//! Minimal loopback HTTP/1.1 server built on std::net::TcpListener — no
//! heavy framework dependency (hyper/axum), keeping the supply chain lean.
//! Binds to 127.0.0.1 by default; random token authenticates each session.
//!
//! Security:
//! - Default: loopback only (127.0.0.1) + random token + Host/Origin check
//! - Explicit LAN mode: requires token + audit log
//! - All output goes through the cross-boundary redactor (ADR-0009)
//!
//! The server is the single backend; the Web UI is a protocol client over
//! the same Application ADT / Robot contract.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::mpsc::{SyncSender, TrySendError};
use std::sync::{Arc, Mutex, mpsc};
use std::time::Duration;

use agent_session_grep_adapters_sqlite::SqliteStore;

const READ_TIMEOUT: Duration = Duration::from_secs(15);
const WRITE_TIMEOUT: Duration = Duration::from_secs(15);
const WORKER_COUNT: usize = 4;
const QUEUE_CAPACITY: usize = 8;
const MAX_REQUEST_LINE_BYTES: usize = 8 * 1024;
const MAX_HEADER_BYTES: usize = 64 * 1024;
const MAX_HEADER_COUNT: usize = 100;
const MAX_BODY_BYTES: usize = 1024 * 1024;

/// 常量时间字节比较（无新依赖）：逐字节 XOR 累计，长度不等也照常遍历
/// 短者全程，避免 early-return 时序差异。
fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    let mut diff = 0u8;
    for (a, b) in left.iter().zip(right.iter()) {
        diff |= a ^ b;
    }
    left.len() == right.len() && diff == 0
}

/// A random bearer token generated for each `asg serve` session.
/// Clients must send it in the `Authorization: Bearer <token>` header.
pub struct ServeSession {
    token: String,
    address: String,
}

impl ServeSession {
    /// Bind a loopback listener on a random port and generate a session token.
    pub fn bind_loopback(port: u16) -> std::io::Result<Self> {
        let address = format!("127.0.0.1:{port}");
        // The listener is bound in run(); here we just prepare token/address.
        let token = generate_token();
        Ok(Self { token, address })
    }

    /// The bearer token clients must send.
    pub fn token(&self) -> &str {
        &self.token
    }

    /// The bound address.
    pub fn address(&self) -> &str {
        &self.address
    }
}

/// Run the loopback HTTP server until interrupted.
///
/// Network reads and writes run in a fixed-size worker pool. The Application
/// ADT stays on the listener thread because `SqliteStore` deliberately owns a
/// non-`Sync` SQLite connection; parsed requests are routed serially while a
/// slow or non-reading client can occupy at most one bounded worker slot.
pub fn run(
    session: &ServeSession,
    db: &str,
    offline: bool,
    store: &SqliteStore,
) -> Result<crate::protocol::Outcome, crate::CliError> {
    let listener = TcpListener::bind(session.address())
        .map_err(|e| crate::CliError::usage(format!("serve: bind failed: {e}")))?;
    let local_addr = listener
        .local_addr()
        .map_err(|e| crate::CliError::usage(format!("serve: local address unavailable: {e}")))?;
    // Token goes in the URL **fragment**, not the query string. A fragment is
    // never sent to the server, so it cannot land in a request log, an access
    // log, or a `Referer` header on any later navigation — whereas
    // `?token=<secret>` reaches every one of those. The page reads it from
    // `location.hash` and sends it as `Authorization: Bearer`, then strips it
    // from the visible URL so it does not persist in browser history.
    // Idea from cc-sessions-viewer's `web-server-mode.md` (no LICENSE file in
    // that repository — spec-level idea only, no code borrowed).
    eprintln!(
        "asg serve: open http://{local_addr}/#token={}",
        session.token()
    );
    eprintln!("asg serve: loopback-only; LAN mode is capability_not_supported");
    serve_listener(
        listener,
        session.token(),
        db,
        offline,
        store,
        ServerLimits::production(),
    )
    .map_err(|e| crate::CliError::usage(format!("serve: listener failed: {e}")))?;
    Ok(crate::protocol::Outcome::Success)
}

#[derive(Clone, Copy)]
struct ServerLimits {
    read_timeout: Duration,
    write_timeout: Duration,
    stop_after: Option<usize>,
}

impl ServerLimits {
    const fn production() -> Self {
        Self {
            read_timeout: READ_TIMEOUT,
            write_timeout: WRITE_TIMEOUT,
            stop_after: None,
        }
    }
}

enum WorkerEvent {
    Parsed {
        request: std::io::Result<HttpRequest>,
        reply: SyncSender<HttpResponse>,
    },
    Completed,
}

fn serve_listener(
    listener: TcpListener,
    token: &str,
    db: &str,
    offline: bool,
    store: &SqliteStore,
    limits: ServerLimits,
) -> std::io::Result<()> {
    listener.set_nonblocking(true)?;
    let (work_tx, work_rx) = mpsc::sync_channel::<TcpStream>(QUEUE_CAPACITY);
    let work_rx = Arc::new(Mutex::new(work_rx));
    let (event_tx, event_rx) = mpsc::channel::<WorkerEvent>();

    std::thread::scope(|scope| {
        for _ in 0..WORKER_COUNT {
            let work_rx = Arc::clone(&work_rx);
            let event_tx = event_tx.clone();
            scope.spawn(move || worker_loop(work_rx, event_tx, limits));
        }
        drop(event_tx);

        let mut accepted = 0usize;
        let mut completed = 0usize;
        loop {
            let accepting = limits.stop_after.is_none_or(|max| accepted < max);
            let in_flight = accepted.saturating_sub(completed);
            if accepting && in_flight < WORKER_COUNT + QUEUE_CAPACITY {
                match listener.accept() {
                    Ok((stream, _peer)) => match work_tx.try_send(stream) {
                        Ok(()) => accepted += 1,
                        Err(TrySendError::Full(_)) => {}
                        Err(TrySendError::Disconnected(_)) => {
                            return Err(std::io::Error::new(
                                std::io::ErrorKind::BrokenPipe,
                                "HTTP worker pool stopped",
                            ));
                        }
                    },
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
                    Err(error) => return Err(error),
                }
            }

            match event_rx.recv_timeout(Duration::from_millis(5)) {
                Ok(event) => handle_worker_event(event, token, db, offline, store, &mut completed),
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => break,
            }
            while let Ok(event) = event_rx.try_recv() {
                handle_worker_event(event, token, db, offline, store, &mut completed);
            }

            if limits.stop_after.is_some_and(|max| completed >= max) {
                break;
            }
        }
        drop(work_tx);
        Ok(())
    })
}

fn worker_loop(
    work_rx: Arc<Mutex<mpsc::Receiver<TcpStream>>>,
    event_tx: mpsc::Sender<WorkerEvent>,
    limits: ServerLimits,
) {
    loop {
        let stream = {
            let receiver = match work_rx.lock() {
                Ok(receiver) => receiver,
                Err(_) => return,
            };
            match receiver.recv() {
                Ok(stream) => stream,
                Err(_) => return,
            }
        };
        let mut stream = stream;
        // Windows 的 `accept` 会继承 listener 的非阻塞属性（`set_nonblocking(true)`
        // 在 serve_listener 里是为了让 accept 轮询不阻塞事件循环）。不显式还原为
        // 阻塞，SO_RCVTIMEO/SO_SNDTIMEO 就整体失效：请求字节晚到几百微秒即
        // WouldBlock → 立刻回 408（实测 140 次合法请求误判 2 次），而半关闭的
        // 客户端还会被随后的 RST 抹掉已经收到的响应（0 字节 + ConnectionAborted）。
        let _ = stream.set_nonblocking(false);
        let _ = stream.set_read_timeout(Some(limits.read_timeout));
        let _ = stream.set_write_timeout(Some(limits.write_timeout));
        let request = parse_request(&mut stream);
        let (reply_tx, reply_rx) = mpsc::sync_channel(1);
        if event_tx
            .send(WorkerEvent::Parsed {
                request,
                reply: reply_tx,
            })
            .is_err()
        {
            return;
        }
        let Ok(response) = reply_rx.recv() else {
            return;
        };
        let _ = stream.write_all(&response.to_bytes());
        let _ = stream.flush();
        if event_tx.send(WorkerEvent::Completed).is_err() {
            return;
        }
    }
}

fn handle_worker_event(
    event: WorkerEvent,
    token: &str,
    db: &str,
    offline: bool,
    store: &SqliteStore,
    completed: &mut usize,
) {
    match event {
        WorkerEvent::Parsed { request, reply } => {
            let response = match request {
                Ok(request) => route_request(&request, token, db, offline, store),
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock
                    ) =>
                {
                    fixed_error(408, "request_timeout", "request timed out")
                }
                Err(_) => fixed_error(400, "invalid_request", "malformed HTTP request"),
            };
            let _ = reply.send(response);
        }
        WorkerEvent::Completed => *completed += 1,
    }
}

/// A parsed HTTP request (minimal subset; the server accepts one request per
/// connection and always closes it after the response).
#[derive(Debug, Clone)]
pub struct HttpRequest {
    pub method: String,
    pub path: String,
    pub headers: Vec<(String, String)>,
    #[allow(dead_code)] // v1 首发的危险动作全部停在预览；body 留给未来经 CSRF/确认的 POST。
    pub body: Vec<u8>,
}

impl HttpRequest {
    /// Get a header value (case-insensitive lookup).
    pub fn header(&self, name: &str) -> Option<&str> {
        let mut matches = self
            .headers
            .iter()
            .filter(|(key, _)| key.eq_ignore_ascii_case(name));
        let value = matches.next().map(|(_, value)| value.as_str())?;
        if matches.next().is_some() {
            None
        } else {
            Some(value)
        }
    }

    /// Check the Authorization header against a token.
    ///
    /// 常量时间比较：即便 loopback-only 且 token 为 CSPRNG，也不给任何
    /// 时序侧信道（audit 建议的 one-line hardening）。
    pub fn check_token(&self, expected: &str) -> bool {
        if let Some(auth) = self.header("authorization")
            && let Some(token) = auth.strip_prefix("Bearer ")
        {
            return constant_time_eq(token.as_bytes(), expected.as_bytes());
        }
        false
    }

    /// Check the Host header is loopback (security: prevent DNS rebinding).
    pub fn check_host_loopback(&self) -> bool {
        loopback_host(self.header("host"))
    }

    /// A cross-origin browser fetch must never enter the API. Same-origin
    /// requests made by the embedded UI carry no Origin header; a present
    /// Origin must exactly match the loopback Host authority (Q27).
    pub fn check_origin_loopback(&self) -> bool {
        let mut origins = self
            .headers
            .iter()
            .filter(|(key, _)| key.eq_ignore_ascii_case("origin"))
            .map(|(_, value)| value.as_str());
        let Some(origin) = origins.next() else {
            return true;
        };
        // 重复 Origin 不是"没有 Origin"：`header` 对重复取值返回 None，沿用它会
        // 把两个 Origin 头当成同源放行（fail open）。在场即校验，歧义即拒绝。
        if origins.next().is_some() {
            return false;
        }
        let Some((scheme, authority)) = parse_http_origin(origin) else {
            return false;
        };
        let Some(host) = self.header("host") else {
            return false;
        };
        (scheme == "http" || scheme == "https")
            && loopback_host(Some(host))
            && authority.eq_ignore_ascii_case(host.trim())
    }

    pub fn check_csrf_token(&self, expected: &str) -> bool {
        self.header("x-csrf-token") == Some(expected)
    }

    /// Fail closed on reverse-proxy hints: a direct loopback client has no
    /// forwarding headers (agentsview hasForwardingHeader, idea-level port).
    pub fn check_direct_client(&self) -> bool {
        !self
            .headers
            .iter()
            .any(|(name, _)| is_forwarding_header(name))
    }

    /// Extract a query-string parameter from the request path (percent-
    /// decoding is minimal: '+' as space; other escapes passed through).
    pub fn query_param(&self, name: &str) -> Option<String> {
        let (_, query) = self.path.split_once('?')?;
        for pair in query.split('&') {
            let (raw_key, raw_value) = pair.split_once('=').unwrap_or((pair, ""));
            if percent_decode(raw_key).as_deref() == Some(name) {
                return percent_decode(raw_value);
            }
        }
        None
    }

    fn query_pairs(&self) -> Result<Vec<(String, String)>, HttpResponse> {
        let Some((_, query)) = self.path.split_once('?') else {
            return Ok(Vec::new());
        };
        query
            .split('&')
            .filter(|pair| !pair.is_empty())
            .map(|pair| {
                let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
                match (percent_decode(key), percent_decode(value)) {
                    (Some(key), Some(value)) => Ok((key, value)),
                    _ => Err(fixed_error(
                        400,
                        "invalid_request",
                        "invalid query parameter encoding",
                    )),
                }
            })
            .collect()
    }
}

fn percent_decode(value: &str) -> Option<String> {
    let mut output = Vec::with_capacity(value.len());
    let bytes = value.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'+' => output.push(b' '),
            b'%' if index + 2 < bytes.len() => {
                let high = (bytes[index + 1] as char).to_digit(16)?;
                let low = (bytes[index + 2] as char).to_digit(16)?;
                output.push((high * 16 + low) as u8);
                index += 2;
            }
            b'%' => return None,
            byte if byte.is_ascii_control() => return None,
            byte => output.push(byte),
        }
        index += 1;
    }
    String::from_utf8(output).ok()
}

fn loopback_host(host: Option<&str>) -> bool {
    let Some(host) = host else {
        return false;
    };
    let host = host.trim();
    let (name, port) = if let Some(rest) = host.strip_prefix('[') {
        let Some((name, tail)) = rest.split_once(']') else {
            return false;
        };
        let port = if tail.is_empty() {
            None
        } else if let Some(port) = tail.strip_prefix(':') {
            Some(port)
        } else {
            return false;
        };
        (name, port)
    } else if host.matches(':').count() > 1 {
        (host, None)
    } else if let Some((name, port)) = host.rsplit_once(':') {
        (name, Some(port))
    } else {
        (host, None)
    };
    if port.is_some_and(|port| port.parse::<u16>().is_err()) {
        return false;
    }
    name.eq_ignore_ascii_case("localhost") || name == "127.0.0.1" || name == "::1"
}

fn parse_http_origin(origin: &str) -> Option<(String, String)> {
    let (scheme, authority) = origin.split_once("://")?;
    if authority.is_empty()
        || authority.contains('/')
        || authority.contains('?')
        || authority.contains('#')
        || authority.contains('@')
    {
        None
    } else {
        Some((scheme.to_ascii_lowercase(), authority.to_string()))
    }
}

fn is_forwarding_header(name: &str) -> bool {
    name.eq_ignore_ascii_case("forwarded")
        || name.eq_ignore_ascii_case("x-forwarded-for")
        || name.eq_ignore_ascii_case("x-forwarded-host")
        || name.eq_ignore_ascii_case("x-real-ip")
}

/// A minimal HTTP response.
pub struct HttpResponse {
    pub status: u16,
    pub body: String,
    pub content_type: &'static str,
    pub headers: Vec<(&'static str, &'static str)>,
}

impl HttpResponse {
    pub fn json(status: u16, body: &str) -> Self {
        Self {
            status,
            body: body.to_string(),
            content_type: "application/json; charset=utf-8",
            headers: Vec::new(),
        }
    }

    pub fn text(status: u16, body: &str, content_type: &'static str) -> Self {
        Self {
            status,
            body: body.to_string(),
            content_type,
            headers: vec![("X-Content-Type-Options", "nosniff")],
        }
    }

    /// Serialize to HTTP/1.1 response bytes.
    pub fn to_bytes(&self) -> Vec<u8> {
        let status_text = match self.status {
            200 => "OK",
            400 => "Bad Request",
            401 => "Unauthorized",
            403 => "Forbidden",
            404 => "Not Found",
            408 => "Request Timeout",
            409 => "Conflict",
            429 => "Too Many Requests",
            500 => "Internal Server Error",
            501 => "Not Implemented",
            503 => "Service Unavailable",
            _ => "Unknown",
        };
        let mut header = format!(
            "HTTP/1.1 {self_status} {status_text}\r\nContent-Type: {ct}\r\nContent-Length: {len}\r\nConnection: close\r\nX-Content-Type-Options: nosniff\r\nReferrer-Policy: no-referrer\r\nCache-Control: no-store\r\n",
            self_status = self.status,
            ct = self.content_type,
            len = self.body.len()
        );
        for (name, value) in &self.headers {
            header.push_str(name);
            header.push_str(": ");
            header.push_str(value);
            header.push_str("\r\n");
        }
        header.push_str("\r\n");
        let mut bytes = header.into_bytes();
        bytes.extend_from_slice(self.body.as_bytes());
        bytes
    }
}

fn json_error(status: u16, code: &str, message: &str) -> HttpResponse {
    HttpResponse::json(
        status,
        &serde_json::json!({
            "error": {
                "code": code,
                "message": message,
            }
        })
        .to_string(),
    )
}

fn fixed_error(status: u16, code: &str, message: &str) -> HttpResponse {
    json_error(status, code, message)
}

/// Parse a minimal HTTP request from a TCP stream.
///
/// Header and body reads are explicitly bounded: the header section may not
/// exceed `MAX_HEADER_BYTES`, at most `MAX_HEADER_COUNT` lines are accepted,
/// and a request body may not exceed `MAX_BODY_BYTES`. Combined with the
/// socket read deadline, this keeps a slow client from holding a worker.
pub fn parse_request(stream: &mut TcpStream) -> std::io::Result<HttpRequest> {
    let mut reader = BufReader::new(stream);
    let mut request_line = String::new();
    let request_line_len =
        read_bounded_line(&mut reader, &mut request_line, MAX_REQUEST_LINE_BYTES)?;
    if request_line_len == 0 {
        return Err(invalid_data("empty request"));
    }

    let parts: Vec<&str> = request_line.split_whitespace().collect();
    if parts.len() != 3 || !parts[2].starts_with("HTTP/1.") {
        return Err(invalid_data("malformed request line"));
    }
    let method = parts[0].to_string();
    let path = parts[1].to_string();
    if !path.starts_with('/') {
        return Err(invalid_data("origin-form request target required"));
    }

    let mut headers = Vec::new();
    let mut header_bytes = 0usize;
    loop {
        let mut line = String::new();
        let n = read_bounded_line(&mut reader, &mut line, MAX_HEADER_BYTES - header_bytes)?;
        header_bytes += n;
        if n == 0 || line.trim().is_empty() {
            break;
        }
        // 计数放在"确认这一行是 header"之后：守卫在循环开头时，终止空行也要占
        // 一次迭代，于是恰好 MAX_HEADER_COUNT 条 header 的合法请求被判 400
        // （实际上限只有 99）。上限的含义是"接受这么多条 header"。
        if headers.len() >= MAX_HEADER_COUNT {
            return Err(invalid_data("too many request headers"));
        }
        let Some(idx) = line.find(':') else {
            return Err(invalid_data("malformed request header"));
        };
        let key = line[..idx].trim().to_string();
        let val = line[idx + 1..].trim().to_string();
        if key.is_empty()
            || !key
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        {
            return Err(invalid_data("malformed request header name"));
        }
        headers.push((key, val));
    }

    if headers
        .iter()
        .any(|(key, _)| key.eq_ignore_ascii_case("transfer-encoding"))
    {
        return Err(invalid_data("transfer encoding is not supported"));
    }
    let content_lengths = headers
        .iter()
        .filter(|(key, _)| key.eq_ignore_ascii_case("content-length"))
        .map(|(_, value)| value)
        .collect::<Vec<_>>();
    if content_lengths.len() > 1 {
        return Err(invalid_data("duplicate content length"));
    }

    // Read body if Content-Length is present. v1 routes do not consume a body,
    // but parse it (bounded) so the transport contract stays explicit.
    let mut body = Vec::new();
    if let Some(value) = content_lengths.first() {
        let cl = value
            .parse::<usize>()
            .map_err(|_| invalid_data("invalid content length"))?;
        if cl > MAX_BODY_BYTES {
            return Err(invalid_data("request body too large"));
        }
        body.resize(cl, 0);
        reader.read_exact(&mut body)?;
    }

    Ok(HttpRequest {
        method,
        path,
        headers,
        body,
    })
}

fn read_bounded_line(
    reader: &mut BufReader<&mut TcpStream>,
    line: &mut String,
    remaining: usize,
) -> std::io::Result<usize> {
    if remaining == 0 {
        return Err(invalid_data("request headers too large"));
    }
    let read = (&mut *reader).take(remaining as u64 + 1).read_line(line)?;
    if read > remaining {
        return Err(invalid_data("request headers too large"));
    }
    Ok(read)
}

fn invalid_data(message: &str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, message)
}

/// Generate a random 32-char hex token for session authentication.
///
/// Uses the platform CSPRNG via getrandom; the token guards loopback API
/// access, so it must not be predictable from the clock (LCG + timestamp
/// was not).
fn generate_token() -> String {
    let mut bytes = [0u8; 16];
    getrandom::fill(&mut bytes).expect("CSPRNG must be available on serve host");
    let mut hex = String::with_capacity(32);
    for byte in bytes {
        hex.push_str(&format!("{byte:02x}"));
    }
    hex
}

/// The embedded Web UI HTML (single-page app, no external dependencies).
const WEB_UI_HTML: &str = include_str!("web/index.html");

/// Strip a query string from a request path for route matching.
fn path_only(path: &str) -> &str {
    path.split_once('?').map(|(p, _)| p).unwrap_or(path)
}

/// Route an HTTP request to the appropriate response, backed by the
/// Application ADT over the same SqliteStore used by CLI/MCP/Robot.
///
/// All JSON responses pass through the cross-boundary redactor (ADR-0009):
/// Web 是跨边界输出,secret 模式一律脱敏(与 Robot/MCP/Handoff 同规则)。
pub fn route_request(
    req: &HttpRequest,
    token: &str,
    db: &str,
    offline: bool,
    store: &SqliteStore,
) -> HttpResponse {
    // The order deliberately reveals no token validity to a non-loopback Host
    // or Origin. Every rejection has a fixed, path-free, secret-free body.
    if !req.check_host_loopback() {
        return fixed_error(403, "forbidden_host", "loopback Host required");
    }
    if !req.check_origin_loopback() {
        return fixed_error(403, "forbidden_origin", "loopback Origin required");
    }
    if !req.check_direct_client() {
        return fixed_error(
            403,
            "forbidden_proxy",
            "direct loopback connection required",
        );
    }
    // The UI page itself is static HTML with no secrets and no data; it may be
    // fetched without a token so the fragment-based flow can boot at all — a
    // fragment is never sent to the server, so the page-load request carries
    // no credential by design (the query-string form keeps its historical
    // bootstrap path too). Every data route below still requires the bearer.
    let is_ui_page = req.method == "GET" && path_only(req.path.as_str()) == "/";
    let bootstrap_token =
        path_only(req.path.as_str()) == "/" && req.query_param("token").as_deref() == Some(token);
    if !is_ui_page && !req.check_token(token) && !bootstrap_token {
        return fixed_error(401, "unauthorized", "valid bearer token required");
    }

    // No mutation is in the first public Web surface. POST can therefore never
    // accidentally turn a preview into execution; clients receive an explicit
    // capability result instead of a misleading 404 or silent fallback.
    if req.method == "POST" {
        if req.header("origin").is_none() || !req.check_origin_loopback() {
            return fixed_error(403, "forbidden_origin", "same-origin POST required");
        }
        if !req.check_csrf_token(token) {
            return fixed_error(403, "forbidden_csrf", "CSRF token required");
        }
        audit_unsupported_action(path_only(req.path.as_str()));
        return fixed_error(
            501,
            "capability_not_supported",
            "HTTP mutation and execution are not supported; use preview endpoints",
        );
    }
    if req.method != "GET" {
        return fixed_error(400, "invalid_request", "GET requests only");
    }

    let mut args = match request_args(req) {
        Ok(args) => args,
        Err(response) => return response,
    };
    let path = path_only(req.path.as_str());
    if path == "/" {
        let mut response = HttpResponse::text(200, WEB_UI_HTML, "text/html; charset=utf-8");
        response.headers.push((
            "Content-Security-Policy",
            "default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; connect-src 'self'; img-src 'self' data:; base-uri 'none'; form-action 'none'; object-src 'none'; frame-ancestors 'none'",
        ));
        return response;
    }
    if path == "/health" {
        args = vec!["status".to_string()];
    }
    if path == "/api/providers" {
        // 与其它 Web 路由共用同一 envelope（command/outcome/data/page/warnings），
        // 客户端无需为单一路由特判响应形状（audit P1-6）。
        return redacted_json(
            200,
            serde_json::json!({
                "command": "providers",
                "outcome": "success",
                "data": crate::provider_matrix_data(),
                "page": { "next_cursor": null, "has_more": false },
                "warnings": [],
            }),
        );
    }
    if let Some(id) = path.strip_prefix("/api/show/") {
        if let Err(response) = check_argv_value("id", id) {
            return response;
        }
        args = vec!["show".to_string(), id.to_string()];
    } else if path == "/api/show" {
        let Some(id) = req.query_param("id").filter(|value| !value.is_empty()) else {
            return fixed_error(400, "invalid_request", "missing id parameter");
        };
        if let Err(response) = check_argv_value("id", &id) {
            return response;
        }
        args = vec!["show".to_string(), id];
    }
    if let Some(session_id) = path.strip_prefix("/api/resume/") {
        if let Err(response) = check_argv_value("session", session_id) {
            return response;
        }
        args = vec!["resume".to_string(), session_id.to_string()];
    } else if path == "/api/resume" {
        let Some(session_id) = req.query_param("session").filter(|value| !value.is_empty()) else {
            return fixed_error(400, "invalid_request", "missing session parameter");
        };
        if let Err(response) = check_argv_value("session", &session_id) {
            return response;
        }
        args = vec!["resume".to_string(), session_id];
    }
    if !matches!(
        path,
        "/health"
            | "/api/status"
            | "/api/search"
            | "/api/projection/search"
            | "/api/context"
            | "/api/handoff"
            | "/api/providers"
            | "/api/show"
            | "/api/resume"
    ) && !path.starts_with("/api/show/")
        && !path.starts_with("/api/resume/")
    {
        return fixed_error(404, "not_found", "HTTP route not found");
    }

    let result = match args.as_slice() {
        [command, session] if command == "resume" => crate::preview_resume(store, session)
            .map(|(outcome, data, page, warnings)| ("resume", outcome, data, page, warnings)),
        _ => crate::dispatch(
            store,
            db,
            &args,
            crate::protocol::OutputMode::Json,
            None,
            offline,
        ),
    };
    match result {
        Ok((command, outcome, mut data, page, warnings)) => {
            if command == "status"
                && let Some(data) = data.as_object_mut()
            {
                data.insert("web_capabilities".into(), serde_json::json!({
                    "search_parameters": WEB_SEARCH_PARAMETERS,
                    "repeated_parameters": ["provider"],
                    "provider_values": agent_session_grep_ports::capability::search_provider_filter_values(),
                    "retrieval_modes": ["lexical", "semantic", "hybrid"],
                    "limit_sets_max_items": true,
                    "mutation_and_execution": false,
                }));
            }
            let outcome = match outcome {
                crate::protocol::Outcome::Success => "success",
                crate::protocol::Outcome::Partial => "partial",
            };
            redacted_json(
                200,
                serde_json::json!({
                    "command": command,
                    "outcome": outcome,
                    "data": data,
                    "page": {
                        "next_cursor": page.next_cursor,
                        "has_more": page.has_more,
                    },
                    "warnings": warnings,
                }),
            )
        }
        Err(error) => protocol_error(error.0),
    }
}

const WEB_SEARCH_PARAMETERS: &[&str] = &[
    "q",
    "mode",
    "limit",
    "max_bytes",
    "cursor",
    "provider",
    "since",
    "until",
    "repo",
    "include_system",
    "group_by_session",
    "sidechain",
    "tool_kind",
    "tool_name",
];

fn request_args(req: &HttpRequest) -> Result<Vec<String>, HttpResponse> {
    let path = path_only(req.path.as_str());
    let value = |name: &str| -> Result<Option<String>, HttpResponse> {
        let Some(value) = req.query_param(name).filter(|value| !value.is_empty()) else {
            return Ok(None);
        };
        check_argv_value(name, &value)?;
        Ok(Some(value))
    };
    match path {
        "/" | "/health" | "/api/providers" => Ok(Vec::new()),
        "/api/status" => Ok(vec!["status".to_string()]),
        "/api/search" | "/api/projection/search" => {
            let parameters = req.query_pairs()?;
            let mut seen = std::collections::BTreeSet::new();
            for (name, parameter) in &parameters {
                if !WEB_SEARCH_PARAMETERS.contains(&name.as_str())
                    || (name != "provider" && !seen.insert(name.as_str()))
                    || parameter.trim().is_empty()
                {
                    return Err(fixed_error(
                        400,
                        "invalid_request",
                        "unknown, duplicate or empty search parameter",
                    ));
                }
                check_argv_value(name, parameter)?;
            }
            let Some(query) = value("q")? else {
                return Err(fixed_error(400, "invalid_request", "missing q parameter"));
            };
            let mut args = vec!["search".to_string(), query];
            append_value_flag(&mut args, "--mode", value("mode")?);
            append_value_flag(&mut args, "--max-items", value("limit")?);
            append_value_flag(&mut args, "--max-bytes", value("max_bytes")?);
            append_value_flag(&mut args, "--cursor", value("cursor")?);
            for (_, provider) in parameters.iter().filter(|(name, _)| name == "provider") {
                append_value_flag(&mut args, "--provider", Some(provider.clone()));
            }
            append_value_flag(&mut args, "--since", value("since")?);
            append_value_flag(&mut args, "--until", value("until")?);
            // repo（schema v16）：与 CLI `--repo` / MCP `repo` 同一维度，Web 面
            // 不得少一个过滤轴（五入口一致性）。空取值被 `value` 过滤掉 = 无过滤。
            append_value_flag(&mut args, "--repo", value("repo")?);
            append_value_flag(&mut args, "--tool-kind", value("tool_kind")?);
            append_value_flag(&mut args, "--tool-name", value("tool_name")?);
            match value("sidechain")?.as_deref() {
                None | Some("include") => {}
                Some("main_only") => args.push("--main-only".into()),
                Some("subagent_only") => args.push("--subagent-only".into()),
                Some(_) => {
                    return Err(fixed_error(
                        400,
                        "invalid_request",
                        "invalid sidechain parameter",
                    ));
                }
            }
            for (name, flag) in [
                ("include_system", "--include-system"),
                ("group_by_session", "--group-by-session"),
            ] {
                match value(name)?.as_deref() {
                    Some("true") => args.push(flag.into()),
                    None | Some("false") => {}
                    Some(_) => {
                        return Err(fixed_error(
                            400,
                            "invalid_request",
                            "search boolean must be true or false",
                        ));
                    }
                }
            }
            Ok(args)
        }
        "/api/context" => {
            let Some(session) = value("session")? else {
                return Err(fixed_error(
                    400,
                    "invalid_request",
                    "missing session parameter",
                ));
            };
            let mut args = vec!["context".to_string(), session];
            append_value_flag(&mut args, "--policy", value("policy")?);
            append_value_flag(&mut args, "--level", value("level")?);
            append_value_flag(&mut args, "--max-messages", value("max_messages")?);
            Ok(args)
        }
        "/api/handoff" => {
            let Some(query) = value("q")? else {
                return Err(fixed_error(400, "invalid_request", "missing q parameter"));
            };
            let mut args = vec!["handoff".to_string(), query];
            append_value_flag(&mut args, "--provider", value("provider")?);
            append_value_flag(&mut args, "--since", value("since")?);
            append_value_flag(&mut args, "--until", value("until")?);
            append_value_flag(&mut args, "--max-evidence", value("max_evidence")?);
            Ok(args)
        }
        _ => Ok(Vec::new()),
    }
}

/// Flag tokens this surface itself puts into the argv it hands `dispatch`.
/// [`crate::is_known_flag_name`] covers the prefix-position vocabulary but not
/// the per-subcommand flags marshalled here, so the two lists together are the
/// full set of tokens the parser can compare a value against.
const SERVE_ARGV_FLAGS: &[&str] = &[
    "--mode",
    "--max-items",
    "--max-bytes",
    "--tool-kind",
    "--tool-name",
    "--main-only",
    "--subagent-only",
    "--include-sidechain",
    "--cursor",
    "--provider",
    "--since",
    "--until",
    "--repo",
    "--include-system",
    "--group-by-session",
    "--policy",
    "--level",
    "--max-messages",
    "--max-evidence",
];

/// Every request value that reaches [`crate::dispatch`] travels as an argv
/// token, and the parser matches flag names by whole-token equality wherever
/// they sit. A value that *equals* a flag name therefore changed what actually
/// ran: `?q=--repo&repo=needle` searched `needle` filtered by repo `--repo` and
/// answered 200 for a query nobody asked, while `?q=--include-system` was eaten
/// as a boolean flag and answered a 400 claiming `q` was missing. Same doctrine
/// as the `--db <path>` flag-shaped-value guard (R8.1/R8.2): ambiguity is an
/// explicit usage error, never a silent reinterpretation.
///
/// The test is equality against the flag vocabulary, not a `-` prefix, because
/// equality is exactly what the parser does. `q` is a free-text search term and
/// coding transcripts are full of flag-*shaped* strings (`--no-verify`, `-Wall`,
/// `-D warnings`); rejecting those would drop real coverage without closing any
/// ambiguity, since the parser never compares against them.
fn check_argv_value(name: &str, value: &str) -> Result<(), HttpResponse> {
    if crate::is_known_flag_name(value) || SERVE_ARGV_FLAGS.contains(&value) {
        // 只回显参数名（调用点全是字面量），绝不回显取值——它可能是检索词。
        return Err(fixed_error(
            400,
            "invalid_request",
            &format!("parameter {name} must not be a CLI flag name"),
        ));
    }
    Ok(())
}

fn append_value_flag(args: &mut Vec<String>, flag: &str, value: Option<String>) {
    if let Some(value) = value {
        args.push(flag.to_string());
        args.push(value);
    }
}

fn audit_unsupported_action(path: &str) {
    let action = match path {
        "/api/resume" => "resume",
        "/api/handoff" => "handoff",
        "/api/provider/start" => "provider_start",
        _ => "unknown",
    };
    // Fixed vocabulary only: never log request headers, token, body, query,
    // filesystem paths, provider-native IDs, or transcript content.
    eprintln!(
        "asg serve audit: action={action} outcome=capability_not_supported secret_fields=omitted"
    );
}

fn protocol_error(error: crate::protocol::ProtocolError) -> HttpResponse {
    let status = match error.code {
        crate::protocol::CanonicalCode::InvalidRequest
        | crate::protocol::CanonicalCode::CursorInvalid
        | crate::protocol::CanonicalCode::CursorExpired => 400,
        crate::protocol::CanonicalCode::NotFound => 404,
        crate::protocol::CanonicalCode::GenerationMismatch
        | crate::protocol::CanonicalCode::SchemaIncompatible => 409,
        _ => 500,
    };
    let message = match error.code {
        crate::protocol::CanonicalCode::InvalidRequest => "invalid HTTP API request",
        crate::protocol::CanonicalCode::NotFound => "requested entity not found",
        crate::protocol::CanonicalCode::CursorInvalid => "cursor is invalid",
        crate::protocol::CanonicalCode::CursorExpired => "cursor is expired",
        crate::protocol::CanonicalCode::GenerationMismatch => "catalog generation changed",
        crate::protocol::CanonicalCode::SchemaIncompatible => "catalog schema is incompatible",
        crate::protocol::CanonicalCode::WriterBusy => "catalog writer is busy",
        crate::protocol::CanonicalCode::SourceChanged => "source changed during operation",
        crate::protocol::CanonicalCode::SourceIo => "source I/O failed",
        crate::protocol::CanonicalCode::SnapshotFailed => "source snapshot failed",
        crate::protocol::CanonicalCode::CatalogError => "catalog operation failed",
        crate::protocol::CanonicalCode::ProviderError => "provider operation failed",
        crate::protocol::CanonicalCode::CapabilityNotSupported => {
            "capability not supported in this mode"
        }
        crate::protocol::CanonicalCode::Internal => "internal operation failed",
    };
    redacted_json(
        status,
        serde_json::json!({
            "error": {
                "code": error.code.as_str(),
                "message": message,
                "retryable": error.code.retryable(),
                "details": error.details,
            }
        }),
    )
}

/// Serialize a JSON value through the cross-boundary redactor (ADR-0009).
fn redacted_json(status: u16, value: serde_json::Value) -> HttpResponse {
    let (redacted, _status) = crate::redaction::redact_value(value);
    HttpResponse::json(status, &redacted.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Shutdown, SocketAddr};
    use std::sync::{Arc, Barrier};

    const TEST_TOKEN: &str = "0123456789abcdef0123456789abcdef";

    fn request(method: &str, path: &str, headers: Vec<(String, String)>) -> HttpRequest {
        HttpRequest {
            method: method.to_string(),
            path: path.to_string(),
            headers,
            body: Vec::new(),
        }
    }

    fn authorized(method: &str, path: &str) -> HttpRequest {
        request(
            method,
            path,
            vec![
                ("Authorization".into(), format!("Bearer {TEST_TOKEN}")),
                ("Host".into(), "127.0.0.1:8080".into()),
            ],
        )
    }

    #[test]
    fn resume_get_is_stateless_and_preserves_cli_preview_gate() {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("session.jsonl");
        let db = dir.path().join("catalog.sqlite3");
        std::fs::write(
            &source,
            serde_json::json!({
                "type": "user", "uuid": "synthetic-message",
                "sessionId": "synthetic-web-session", "cwd": dir.path().to_str().unwrap(),
                "message": {"role": "user", "content": "synthetic preview"}
            })
            .to_string()
                + "\n",
        )
        .unwrap();
        let store = SqliteStore::open_in_memory().unwrap();
        crate::ingest_file(&store, source.to_str().unwrap()).unwrap();
        let sessions = agent_session_grep_ports::CatalogStore::list_sessions(&store, 10).unwrap();
        assert_eq!(sessions.len(), 1);
        let session = sessions[0].id.as_str();
        let path = format!("/api/resume?session={session}");
        let ack = agent_session_grep_application::resume::resume_preview_ack_path(dir.path());
        assert!(!ack.exists());
        let first = route_request(
            &authorized("GET", &path),
            TEST_TOKEN,
            db.to_str().unwrap(),
            true,
            &store,
        );
        assert_eq!(first.status, 200, "{}", first.body);
        assert!(!ack.exists(), "HTTP GET must not acknowledge a CLI preview");
        let second = route_request(
            &authorized("GET", &path),
            TEST_TOKEN,
            db.to_str().unwrap(),
            true,
            &store,
        );
        assert_eq!(first.body, second.body);
        let (_, _, data, _, _) = crate::dispatch(
            &store,
            db.to_str().unwrap(),
            &["resume".into(), session.into()],
            crate::protocol::OutputMode::Human,
            None,
            true,
        )
        .unwrap();
        assert_eq!(data["available"], true);
        assert_eq!(data["executed"], false);
        assert!(ack.exists(), "the CLI still owns its acknowledgement gate");
        let third = route_request(
            &authorized("GET", &path),
            TEST_TOKEN,
            db.to_str().unwrap(),
            true,
            &store,
        );
        assert_eq!(first.body, third.body);
        assert!(
            ack.exists(),
            "HTTP GET must not consume CLI acknowledgement"
        );
    }

    fn start_test_server(
        connection_count: usize,
        read_timeout: Duration,
    ) -> (SocketAddr, String, std::thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind test listener");
        let address = listener.local_addr().expect("test listener address");
        let token = TEST_TOKEN.to_string();
        let server_token = token.clone();
        let handle = std::thread::spawn(move || {
            let store = SqliteStore::open_in_memory().expect("in-memory store");
            serve_listener(
                listener,
                &server_token,
                "test.db",
                false,
                &store,
                ServerLimits {
                    read_timeout,
                    write_timeout: Duration::from_secs(2),
                    stop_after: Some(connection_count),
                },
            )
            .expect("test server");
        });
        (address, token, handle)
    }

    fn raw_http(address: SocketAddr, request: &str) -> String {
        let mut stream = TcpStream::connect(address).expect("connect test server");
        stream
            .set_read_timeout(Some(Duration::from_secs(3)))
            .expect("set client timeout");
        stream.write_all(request.as_bytes()).expect("write request");
        stream.shutdown(Shutdown::Write).expect("finish request");
        let mut response = String::new();
        stream.read_to_string(&mut response).expect("read response");
        response
    }

    fn get_request(address: SocketAddr, token: &str, path: &str) -> String {
        format!("GET {path} HTTP/1.1\r\nHost: {address}\r\nAuthorization: Bearer {token}\r\n\r\n")
    }

    #[test]
    fn generate_token_is_csprng_shaped_and_unique() {
        let first = generate_token();
        let second = generate_token();
        assert_eq!(first.len(), 32);
        assert!(first.chars().all(|c| c.is_ascii_hexdigit()));
        assert_ne!(first, second);
    }

    #[test]
    fn host_origin_token_and_proxy_guards_are_strict() {
        let valid = authorized("GET", "/api/status");
        assert!(valid.check_host_loopback());
        assert!(valid.check_origin_loopback());
        assert!(valid.check_direct_client());
        assert!(valid.check_token(TEST_TOKEN));

        let ipv6 = request(
            "GET",
            "/api/status",
            vec![("Host".into(), "[::1]:8080".into())],
        );
        assert!(ipv6.check_host_loopback());

        let bad_port = request(
            "GET",
            "/api/status",
            vec![("Host".into(), "localhost:not-a-port".into())],
        );
        assert!(!bad_port.check_host_loopback());

        let wrong_origin = request(
            "GET",
            "/api/status",
            vec![
                ("Host".into(), "127.0.0.1:8080".into()),
                ("Origin".into(), "http://127.0.0.1:8081".into()),
            ],
        );
        assert!(!wrong_origin.check_origin_loopback());

        let proxied = request(
            "GET",
            "/api/status",
            vec![("X-Forwarded-For".into(), "127.0.0.1".into())],
        );
        assert!(!proxied.check_direct_client());
    }

    /// `header` returns `None` for a duplicated value, and a *missing* Origin is
    /// deliberately treated as same-origin — so reusing it here let two Origin
    /// headers walk through the cross-origin gate: one bad Origin answered 403,
    /// the same bad Origin sent twice answered 200. Ambiguity must fail closed,
    /// the way a duplicated Host and Authorization already do.
    #[test]
    fn duplicate_origin_header_cannot_bypass_the_cross_origin_guard() {
        let store = SqliteStore::open_in_memory().expect("store");
        let mut duplicated = authorized("GET", "/api/status");
        duplicated
            .headers
            .push(("Origin".into(), "http://evil.test".into()));
        duplicated
            .headers
            .push(("Origin".into(), "http://evil.test".into()));
        assert!(!duplicated.check_origin_loopback());
        let response = route_request(&duplicated, TEST_TOKEN, "test.db", false, &store);
        assert_eq!(response.status, 403, "{}", response.body);
        assert!(
            response.body.contains("forbidden_origin"),
            "{}",
            response.body
        );
    }

    #[test]
    fn percent_decode_handles_unicode_and_rejects_bad_escape() {
        assert_eq!(percent_decode("hello+world"), Some("hello world".into()));
        assert_eq!(percent_decode("%E9%85%8D%E7%BD%AE"), Some("配置".into()));
        assert_eq!(percent_decode("%zz"), None);
    }

    #[test]
    fn routes_require_auth_and_preserve_bootstrap_only_for_the_page() {
        let store = SqliteStore::open_in_memory().expect("store");
        let no_token = request(
            "GET",
            "/api/status",
            vec![("Host".into(), "127.0.0.1:8080".into())],
        );
        assert_eq!(
            route_request(&no_token, TEST_TOKEN, "test.db", false, &store).status,
            401
        );

        let bad_host = request(
            "GET",
            "/api/status",
            vec![
                ("Authorization".into(), format!("Bearer {TEST_TOKEN}")),
                ("Host".into(), "example.test".into()),
            ],
        );
        assert_eq!(
            route_request(&bad_host, TEST_TOKEN, "test.db", false, &store).status,
            403
        );

        let bootstrap = request(
            "GET",
            &format!("/?token={TEST_TOKEN}"),
            vec![("Host".into(), "127.0.0.1:8080".into())],
        );
        let response = route_request(&bootstrap, TEST_TOKEN, "test.db", false, &store);
        assert_eq!(response.status, 200);
        assert!(response.body.contains("data-shell=\"asg-web\""));

        let api_query_token = request(
            "GET",
            &format!("/api/status?token={TEST_TOKEN}"),
            vec![("Host".into(), "127.0.0.1:8080".into())],
        );
        assert_eq!(
            route_request(&api_query_token, TEST_TOKEN, "test.db", false, &store).status,
            401
        );

        // The fragment-based flow loads the page with NO credential at all —
        // a fragment is never sent to the server, so `GET /` arrives bare.
        // The static page must still load (the UI reads the token from
        // `location.hash` and puts it in the Authorization header itself);
        // data routes must not.
        let bare_page = request("GET", "/", vec![("Host".into(), "127.0.0.1:8080".into())]);
        let response = route_request(&bare_page, TEST_TOKEN, "test.db", false, &store);
        assert_eq!(response.status, 200);
        assert!(response.body.contains("data-shell=\"asg-web\""));
    }

    #[test]
    fn core_preview_routes_use_the_shared_dispatch_contract() {
        let store = SqliteStore::open_in_memory().expect("store");
        for path in [
            "/api/status",
            "/api/providers",
            "/api/search?q=needle&mode=lexical&limit=20",
            "/api/search?q=needle&repo=github.com/synthetic-owner/synthetic-repo",
            "/api/handoff?q=needle",
        ] {
            let response = route_request(
                &authorized("GET", path),
                TEST_TOKEN,
                "test.db",
                false,
                &store,
            );
            assert_eq!(response.status, 200, "{path}: {}", response.body);
            let body: serde_json::Value = serde_json::from_str(&response.body).expect("JSON");
            assert!(body.get("data").is_some());
            assert_eq!(body.get("outcome"), Some(&serde_json::json!("success")));
        }

        let invalid = route_request(
            &authorized("GET", "/api/show?id=C%3A%2FUsers%2Falice%2Fsecret.jsonl"),
            TEST_TOKEN,
            "test.db",
            false,
            &store,
        );
        assert_eq!(invalid.status, 400);
        assert!(!invalid.body.contains("C:/Users"));
        assert!(!invalid.body.contains("secret.jsonl"));
    }

    /// Request values travel to `dispatch` as argv tokens, and the parser
    /// matches flag names by whole-token equality wherever they sit. Before the
    /// guard, `?q=--repo&repo=needle` really searched `needle` filtered by repo
    /// `--repo` and answered **200 for a query nobody asked**, while
    /// `?q=--include-system` was eaten as a boolean flag and answered a 400
    /// claiming the caller had omitted `q`. Both must be one explicit
    /// `invalid_request` naming the parameter.
    #[test]
    fn flag_shaped_request_values_are_rejected_instead_of_reparsed() {
        let store = SqliteStore::open_in_memory().expect("store");
        for path in [
            "/api/search?q=--repo&repo=needle",
            "/api/search?q=--include-system",
            "/api/search?q=needle&mode=-x",
            "/api/search?q=needle&repo=--repo",
            "/api/projection/search?q=--cursor&cursor=needle",
            "/api/handoff?q=--provider&provider=codex",
            "/api/context?session=--policy&policy=recent",
            "/api/show?id=--yes",
            "/api/show/--yes",
            "/api/resume?session=--yes",
            "/api/resume/--yes",
        ] {
            let response = route_request(
                &authorized("GET", path),
                TEST_TOKEN,
                "test.db",
                false,
                &store,
            );
            assert_eq!(response.status, 400, "{path}: {}", response.body);
            assert!(
                response.body.contains("invalid_request"),
                "{path}: {}",
                response.body
            );
        }

        // 取值本身绝不回显（可能是用户的检索词），只说参数名。
        let named = route_request(
            &authorized("GET", "/api/search?q=--repo&repo=needle"),
            TEST_TOKEN,
            "test.db",
            false,
            &store,
        );
        assert!(!named.body.contains("needle"), "{}", named.body);
        assert!(named.body.contains("parameter q"), "{}", named.body);
    }

    /// The guard tests equality against the flag vocabulary, not a `-` prefix:
    /// `q` is a free-text search term and coding transcripts are full of
    /// flag-shaped strings the parser never compares against. Rejecting those
    /// would make `--no-verify` or `-D warnings` unsearchable from the Web UI
    /// while closing no ambiguity at all.
    #[test]
    fn search_terms_that_merely_look_like_flags_stay_searchable() {
        let store = SqliteStore::open_in_memory().expect("store");
        for query in ["--no-verify", "-Wall", "-D%20warnings", "-1", "--repo%3Dx"] {
            let response = route_request(
                &authorized("GET", &format!("/api/search?q={query}")),
                TEST_TOKEN,
                "test.db",
                false,
                &store,
            );
            assert_eq!(response.status, 200, "q={query}: {}", response.body);
        }
    }

    /// Structural half of the guard: every flag name this surface marshals into
    /// argv must be one the guard refuses as a value. Adding a value-taking flag
    /// to `request_args` without registering it turns the next query that
    /// happens to equal that flag name back into a silently rewritten request,
    /// so the omission has to fail here instead.
    #[test]
    fn every_flag_serve_marshals_is_refused_as_a_value() {
        let populated = [
            "/api/search?q=needle&mode=lexical&limit=5&cursor=c&provider=codex\
             &since=1d&until=1h&repo=r&include_system=true&group_by_session=true",
            "/api/context?session=s&policy=recent&level=full&max_messages=5",
            "/api/handoff?q=needle&provider=codex&since=1d&until=1h&max_evidence=3",
        ];
        let mut seen = 0usize;
        for path in populated {
            let Ok(args) = request_args(&authorized("GET", path)) else {
                panic!("{path}: a fully populated request must be accepted");
            };
            for flag in args.iter().filter(|arg| arg.starts_with("--")) {
                assert!(
                    check_argv_value("q", flag).is_err(),
                    "{path}: serve emits {flag} but the guard accepts it as a value"
                );
                seen += 1;
            }
        }
        assert_eq!(
            seen, 16,
            "the populated routes emit a known number of flags; update this with them"
        );
    }

    #[test]
    fn search_routes_forward_the_repo_filter_like_the_cli_flag() {
        // 五入口一致性：Web 的检索路由必须携带与 CLI `--repo`/MCP `repo` 同一
        // 过滤轴。缺省 = 不过滤；空取值与 CLI/MCP 一样明确报错。
        for path in ["/api/search", "/api/projection/search"] {
            let Ok(with_repo) = request_args(&authorized(
                "GET",
                &format!("{path}?q=needle&repo=github.com/synthetic-owner/synthetic-repo"),
            )) else {
                panic!("{path}: repo 取值必须映射为 --repo 而不是请求错误");
            };
            assert!(
                with_repo.windows(2).any(|pair| pair[0] == "--repo"
                    && pair[1] == "github.com/synthetic-owner/synthetic-repo"),
                "{path}: {with_repo:?}"
            );
            let Ok(args) = request_args(&authorized("GET", &format!("{path}?q=needle"))) else {
                panic!("{path}: 缺省 repo 必须是合法请求");
            };
            assert!(!args.iter().any(|arg| arg == "--repo"), "{path}: {args:?}");
            assert!(request_args(&authorized("GET", &format!("{path}?q=needle&repo="))).is_err());
        }
    }

    #[test]
    fn search_forwards_budget_facets_and_every_provider_value() {
        let request = authorized(
            "GET",
            "/api/search?q=needle&max_bytes=4096&provider=claude&provider=grok-build&sidechain=subagent_only&tool_kind=command&tool_name=Bash",
        );
        let args = request_args(&request).unwrap_or_else(|response| panic!("{}", response.body));
        for expected in [
            ["--max-bytes", "4096"],
            ["--provider", "claude"],
            ["--provider", "grok-build"],
            ["--tool-kind", "command"],
            ["--tool-name", "Bash"],
        ] {
            assert!(
                args.windows(2)
                    .any(|pair| pair[0] == expected[0] && pair[1] == expected[1]),
                "{args:?}"
            );
        }
        assert!(args.iter().any(|arg| arg == "--subagent-only"));
        for query in [
            "q=x&max_bytes=",
            "q=x&mode=lexical&mode=semantic",
            "q=x&tool_name=%FF",
            "q=x&sidechain=other",
            "q=x&include_system=1",
            "q=x&unexpected=true",
        ] {
            assert!(
                request_args(&authorized("GET", &format!("/api/search?{query}"))).is_err(),
                "{query}"
            );
        }
    }

    #[test]
    fn web_search_applies_provider_union_and_reports_its_capabilities() {
        let dir = tempfile::tempdir().unwrap();
        let store = SqliteStore::open_in_memory().unwrap();
        let source = dir.path().join("grok.jsonl");
        std::fs::write(&source, r#"{"params":{"update":{"sessionUpdate":"user_message_chunk","content":"needle web"},"_meta":{"promptIndex":0}}}"#).unwrap();
        crate::ingest_file(&store, source.to_str().unwrap()).unwrap();
        crate::build_embeddings(&store).unwrap();
        for mode in ["lexical", "semantic", "hybrid"] {
            let response = route_request(
                &authorized(
                    "GET",
                    &format!(
                        "/api/search?q=needle&provider=claude&provider=grok-build&mode={mode}&max_bytes=4096&sidechain=main_only"
                    ),
                ),
                TEST_TOKEN,
                "test.db",
                true,
                &store,
            );
            assert_eq!(response.status, 200, "{}", response.body);
            let body: serde_json::Value = serde_json::from_str(&response.body).unwrap();
            assert_eq!(body["data"]["retrieval_mode"], mode);
            assert_eq!(body["data"]["hits"].as_array().unwrap().len(), 1, "{body}");
            assert_eq!(body["data"]["hits"][0]["text"], "needle web");
        }
        let status = route_request(
            &authorized("GET", "/api/status"),
            TEST_TOKEN,
            "test.db",
            true,
            &store,
        );
        let body: serde_json::Value = serde_json::from_str(&status.body).unwrap();
        assert_eq!(
            body["data"]["web_capabilities"]["search_parameters"],
            serde_json::json!(WEB_SEARCH_PARAMETERS)
        );
        assert_eq!(
            body["data"]["web_capabilities"]["mutation_and_execution"],
            false
        );
    }

    #[test]
    fn post_requires_origin_and_csrf_then_reports_unsupported() {
        let store = SqliteStore::open_in_memory().expect("store");
        let missing_origin = authorized("POST", "/api/resume");
        assert_eq!(
            route_request(&missing_origin, TEST_TOKEN, "test.db", false, &store).status,
            403
        );

        let mut valid_guard = authorized("POST", "/api/resume");
        valid_guard
            .headers
            .push(("Origin".into(), "http://127.0.0.1:8080".into()));
        assert_eq!(
            route_request(&valid_guard, TEST_TOKEN, "test.db", false, &store).status,
            403
        );
        valid_guard
            .headers
            .push(("X-CSRF-Token".into(), TEST_TOKEN.into()));
        let response = route_request(&valid_guard, TEST_TOKEN, "test.db", false, &store);
        assert_eq!(response.status, 501);
        assert!(response.body.contains("capability_not_supported"));
    }

    #[test]
    fn http_response_emits_security_and_length_headers() {
        let resp = HttpResponse::json(200, r#"{"ok":true}"#);
        let text = String::from_utf8(resp.to_bytes()).expect("HTTP bytes");
        assert!(text.starts_with("HTTP/1.1 200 OK"));
        assert!(text.contains("Content-Type: application/json; charset=utf-8"));
        assert!(text.contains("X-Content-Type-Options: nosniff"));
        assert!(text.contains("Referrer-Policy: no-referrer"));
        assert!(text.contains(r#"{"ok":true}"#));
    }

    #[test]
    fn integration_rejects_bad_token_host_and_origin() {
        let (address, token, server) = start_test_server(4, Duration::from_secs(2));
        let bad_token = raw_http(
            address,
            &format!(
                "GET /api/status HTTP/1.1\r\nHost: {address}\r\nAuthorization: Bearer wrong\r\n\r\n"
            ),
        );
        assert!(bad_token.starts_with("HTTP/1.1 401"));

        let bad_host = raw_http(
            address,
            &format!(
                "GET /api/status HTTP/1.1\r\nHost: evil.test\r\nAuthorization: Bearer {token}\r\n\r\n"
            ),
        );
        assert!(bad_host.starts_with("HTTP/1.1 403"));

        let bad_origin = raw_http(
            address,
            &format!(
                "GET /api/status HTTP/1.1\r\nHost: {address}\r\nOrigin: http://127.0.0.1:9\r\nAuthorization: Bearer {token}\r\n\r\n"
            ),
        );
        assert!(bad_origin.starts_with("HTTP/1.1 403"));

        let ok = raw_http(address, &get_request(address, &token, "/api/status"));
        assert!(ok.starts_with("HTTP/1.1 200"));
        server.join().expect("server join");
    }

    /// Windows' `accept` hands back a socket that inherited the listener's
    /// non-blocking flag (`set_nonblocking(true)` is what keeps the accept poll
    /// off the event loop). Without restoring blocking mode the read deadline is
    /// inert: a request whose bytes land a moment after `accept` makes
    /// `parse_request` return `WouldBlock`, which the server reports as 408 — 2
    /// of 140 valid requests were answered that way against the real catalog,
    /// and a half-closing client lost the response entirely to the RST that
    /// followed. Delay the request past any accept-time read to pin it.
    #[test]
    fn integration_delayed_request_is_served_not_timed_out() {
        let (address, token, server) = start_test_server(1, Duration::from_secs(5));
        let mut stream = TcpStream::connect(address).expect("connect test server");
        stream
            .set_read_timeout(Some(Duration::from_secs(10)))
            .expect("set client timeout");
        std::thread::sleep(Duration::from_millis(200));
        stream
            .write_all(get_request(address, &token, "/api/status").as_bytes())
            .expect("write request");
        stream.shutdown(Shutdown::Write).expect("finish request");
        let mut response = String::new();
        stream.read_to_string(&mut response).expect("read response");
        assert!(response.starts_with("HTTP/1.1 200"), "{response}");
        server.join().expect("server join");
    }

    /// The limit means "this many headers are accepted": counting at the top of
    /// the loop spent one iteration on the terminating blank line, so a
    /// well-formed request carrying exactly `MAX_HEADER_COUNT` headers was
    /// rejected 400 and the real ceiling was 99. Pin both sides of the boundary.
    #[test]
    fn integration_accepts_exactly_max_header_count_headers() {
        let (address, token, server) = start_test_server(2, Duration::from_secs(3));
        let required = format!("Host: {address}\r\nAuthorization: Bearer {token}\r\n");
        let pad = |count: usize| {
            (0..count)
                .map(|index| format!("X-Pad-{index}: v\r\n"))
                .collect::<String>()
        };
        let at_limit = raw_http(
            address,
            &format!(
                "GET /api/status HTTP/1.1\r\n{required}{}\r\n",
                pad(MAX_HEADER_COUNT - 2)
            ),
        );
        assert!(at_limit.starts_with("HTTP/1.1 200"), "{at_limit}");
        let over_limit = raw_http(
            address,
            &format!(
                "GET /api/status HTTP/1.1\r\n{required}{}\r\n",
                pad(MAX_HEADER_COUNT - 1)
            ),
        );
        assert!(over_limit.starts_with("HTTP/1.1 400"), "{over_limit}");
        server.join().expect("server join");
    }

    /// Slowloris isolation is a **structural** property, not a latency budget.
    /// The read timeout here is long enough that the parked connection provably
    /// cannot be reaped first, so a 200 can only mean the fast request was
    /// served alongside it; a server that serialised would stall past
    /// `raw_http`'s own client read timeout and fail loudly. An absolute
    /// wall-clock assertion instead measured how loaded the host was — it
    /// flaked in a full `--workspace` run while passing in isolation.
    #[test]
    fn integration_slowloris_does_not_block_a_fast_get() {
        let (address, token, server) = start_test_server(2, Duration::from_secs(30));
        let mut slow = TcpStream::connect(address).expect("connect slow client");
        slow.write_all(b"GET /api/status HTTP/1.1\r\nHost:")
            .expect("write partial request");

        let fast = raw_http(address, &get_request(address, &token, "/api/status"));
        assert!(fast.starts_with("HTTP/1.1 200"), "{fast}");

        // Release the parked connection so the second slot completes and
        // `stop_after` is reached without waiting out the 30s read timeout.
        drop(slow);
        server.join().expect("server join");
    }

    /// The reaping half of the same guard, with no timing assertion: a client
    /// that never finishes its headers is answered 408 once the server's own
    /// read timeout expires, whenever that happens to be.
    #[test]
    fn integration_stalled_headers_are_answered_408() {
        let (address, _token, server) = start_test_server(1, Duration::from_millis(300));
        let mut slow = TcpStream::connect(address).expect("connect slow client");
        slow.set_read_timeout(Some(Duration::from_secs(10)))
            .expect("slow read timeout");
        slow.write_all(b"GET /api/status HTTP/1.1\r\nHost:")
            .expect("write partial request");

        let mut slow_response = String::new();
        slow.read_to_string(&mut slow_response)
            .expect("read timeout response");
        assert!(slow_response.starts_with("HTTP/1.1 408"), "{slow_response}");
        server.join().expect("server join");
    }

    #[test]
    fn integration_concurrent_gets_complete_on_bounded_pool() {
        const CLIENTS: usize = 8;
        let (address, token, server) = start_test_server(CLIENTS, Duration::from_secs(2));
        let barrier = Arc::new(Barrier::new(CLIENTS));
        let clients = (0..CLIENTS)
            .map(|_| {
                let barrier = Arc::clone(&barrier);
                let token = token.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    raw_http(address, &get_request(address, &token, "/api/status"))
                })
            })
            .collect::<Vec<_>>();
        for client in clients {
            let response = client.join().expect("client join");
            assert!(response.starts_with("HTTP/1.1 200"), "{response}");
        }
        server.join().expect("server join");
    }

    #[test]
    fn integration_unsupported_post_echoes_no_secret_or_path() {
        let (address, token, server) = start_test_server(1, Duration::from_secs(2));
        let body =
            r#"{"token":"ghp_example_secret_value","path":"C:/Users/alice/transcript.jsonl"}"#;
        let request = format!(
            "POST /api/resume HTTP/1.1\r\nHost: {address}\r\nOrigin: http://{address}\r\nAuthorization: Bearer {token}\r\nX-CSRF-Token: {token}\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        );
        let response = raw_http(address, &request);
        assert!(response.starts_with("HTTP/1.1 501"));
        assert!(response.contains("capability_not_supported"));
        assert!(!response.contains("ghp_example"));
        assert!(!response.contains("C:/Users"));
        assert!(!response.contains(&token));
        server.join().expect("server join");
    }

    #[test]
    fn embedded_ui_is_offline_and_uses_safe_dom_projection() {
        assert!(!WEB_UI_HTML.contains("http://"));
        assert!(!WEB_UI_HTML.contains("https://"));
        assert!(!WEB_UI_HTML.contains("innerHTML"));
        for endpoint in [
            "/api/status",
            "/api/providers",
            "/api/search",
            "/api/show",
            "/api/context",
            "/api/handoff",
            "/api/resume",
        ] {
            assert!(WEB_UI_HTML.contains(endpoint), "missing {endpoint}");
        }
    }

    /// The UI hides regions with the `hidden` attribute, but `[hidden]` is only
    /// a UA-stylesheet `display: none`: any author `display` rule on the same
    /// element wins and the region stays visible. `.gate` sets `display: grid`
    /// and `.shell` sets `display: flex`, so without an author-level
    /// `[hidden] { display: none !important }` the token gate and the app shell
    /// render on top of each other and the gate's button looks dead. Pin both
    /// the rule and every element that depends on it.
    #[test]
    fn embedded_ui_enforces_the_hidden_attribute_over_author_display_rules() {
        assert!(
            WEB_UI_HTML.contains("[hidden] { display: none !important; }"),
            "author-level [hidden] override is required: `.gate`/`.shell` set display"
        );
        for id in [
            "gate",
            "shell",
            "contextPanel",
            "detailEmpty",
            "previewOutput",
        ] {
            assert!(
                WEB_UI_HTML.contains(&format!("id=\"{id}\"")),
                "missing element #{id} that the hidden-attribute contract covers"
            );
        }
    }

    /// Every `data-i18n` key the markup asks for must exist in **both**
    /// dictionaries, or the toggle silently blanks that control (an empty
    /// button is indistinguishable from a broken one). Both directions are
    /// checked: a key added to the markup without a translation, and a key
    /// present in one language only.
    #[test]
    fn embedded_ui_translates_every_referenced_i18n_key() {
        let keys: Vec<&str> = WEB_UI_HTML
            .match_indices("data-i18n=\"")
            .map(|(index, marker)| {
                let rest = &WEB_UI_HTML[index + marker.len()..];
                &rest[..rest.find('"').expect("unterminated data-i18n value")]
            })
            .collect();
        assert!(keys.len() >= 10, "expected the markup to use i18n keys");

        let zh_start = WEB_UI_HTML.find("  zh: {").expect("zh dictionary");
        let en_start = WEB_UI_HTML.find("  en: {").expect("en dictionary");
        assert!(zh_start < en_start, "dictionary order assumption changed");
        let zh = &WEB_UI_HTML[zh_start..en_start];
        let en_end = WEB_UI_HTML[en_start..]
            .find("\n};")
            .expect("dictionary terminator");
        let en = &WEB_UI_HTML[en_start..en_start + en_end];

        for key in keys {
            assert!(zh.contains(&format!("{key}:")), "zh missing i18n key {key}");
            assert!(en.contains(&format!("{key}:")), "en missing i18n key {key}");
        }
    }

    /// The source text between `anchor` and the first following `terminator`.
    /// Pins a behaviour to the handler that has to implement it: a bare
    /// whole-file `contains` passes when the required call sits anywhere at all,
    /// which is how a defect in one handler hides behind another's code.
    fn ui_slice(anchor: &str, terminator: &str) -> &'static str {
        let start = WEB_UI_HTML
            .find(anchor)
            .unwrap_or_else(|| panic!("web UI no longer contains `{anchor}`"));
        let rest = &WEB_UI_HTML[start..];
        let end = rest
            .find(terminator)
            .unwrap_or_else(|| panic!("`{anchor}` is not terminated by `{terminator}`"));
        &rest[..end + terminator.len()]
    }

    /// A URL that differs from the current one only in its fragment is a
    /// same-document navigation: the script does not re-run. So pasting the
    /// fresh `#token=…` line into the tab that is already open — exactly what
    /// the gate's rejection text instructs after serve rotates the token — left
    /// the gate sitting there unchanged, with the dead token still in storage.
    /// The fragment read has to be reachable again from `hashchange`.
    #[test]
    fn embedded_ui_reads_the_token_again_when_only_the_fragment_changes() {
        assert!(
            WEB_UI_HTML.contains("function readUrlToken()"),
            "the fragment read must be a function, not inline boot-only code"
        );
        let handler = ui_slice("window.addEventListener('hashchange'", "\n});");
        assert!(
            handler.contains("readUrlToken()"),
            "hashchange must re-read the fragment: {handler}"
        );
        assert!(
            handler.contains("acceptToken("),
            "a token found on hashchange must authenticate: {handler}"
        );
    }

    /// A preview belongs to the session it was opened from. `closeContext` hid
    /// it but `loadContext` did not, so opening session B while session A's
    /// resume preview was on screen left A's `--resume` command under B's
    /// header — a command that resumes the wrong session if copied.
    #[test]
    fn embedded_ui_scopes_the_preview_pane_to_the_open_session() {
        let load_context = ui_slice("async function loadContext(", "\n}");
        assert!(
            load_context.contains("previewOutput"),
            "opening a session must reset the preview pane: {load_context}"
        );
        assert!(
            load_context.contains("hidden = true"),
            "the preview pane must be hidden, not just emptied: {load_context}"
        );
    }

    /// `applyI18n()` only rewrites `[data-i18n]` markup and the select options.
    /// Everything else on screen came from `t()` at render time, so the toggle
    /// used to leave the status line, every hit card's session line, the session
    /// title and each `show` button in the previous language — an English UI
    /// reading "已连接 · 第 4 代 · 38 条记录". The toggle must re-run those
    /// renderers.
    #[test]
    fn embedded_ui_rerenders_dynamic_strings_when_the_language_changes() {
        let toggle = ui_slice("getElementById('langToggle').addEventListener", "\n});");
        for call in ["applyI18n()", "renderHits(", "loadContext(", "init()"] {
            assert!(
                toggle.contains(call),
                "language toggle must re-render via `{call}`: {toggle}"
            );
        }
    }

    /// The counter beside the "Hits" heading reported the last page's length,
    /// not what is on screen: paging 2 at a time through 18 hits left it reading
    /// "lexical · 2" under an 18-row list.
    #[test]
    fn embedded_ui_counts_every_loaded_page_in_the_hit_meta() {
        let meta = ui_slice("async function doSearch(", "\n}");
        assert!(
            meta.contains("lastHits.length"),
            "the hit meta must count all loaded pages: {meta}"
        );
    }

    /// Closing the context reset `activeHit` but left the `.active` class on the
    /// card, so the sidebar kept showing a selected hit with nothing open.
    #[test]
    fn embedded_ui_clears_the_hit_highlight_when_the_context_closes() {
        let close = ui_slice("function closeContext()", "\n}");
        assert!(
            close.contains("classList.remove('active')"),
            "closing the context must drop the hit highlight: {close}"
        );
    }

    /// `white-space: pre-wrap` breaks at soft opportunities only, and transcript
    /// text is full of runs with none (absolute paths, URLs, base64, hashes). A
    /// single 400-character run measured 3233px inside a 990px pane, giving the
    /// message list a horizontal scrollbar. Both text surfaces need a break rule.
    #[test]
    fn embedded_ui_wraps_unbreakable_runs_in_message_and_preview_text() {
        for anchor in [".msg-body {", "pre.preview {"] {
            let rule = ui_slice(anchor, "}");
            assert!(
                rule.contains("white-space: pre-wrap") && rule.contains("overflow-wrap:"),
                "`{anchor}` wraps pre-formatted text and needs overflow-wrap: {rule}"
            );
        }
    }

    /// `frame-ancestors` is defined to be ignored in a meta-delivered policy and
    /// the browser logs a CSP error for it on every page load. The directive is
    /// only real on the HTTP response, so the meta tag must not carry it while
    /// the served header still must.
    #[test]
    fn embedded_ui_meta_csp_omits_the_directive_only_a_header_can_carry() {
        let meta = ui_slice("<meta http-equiv=\"Content-Security-Policy\"", ">");
        assert!(
            !meta.contains("frame-ancestors"),
            "meta CSP must not declare frame-ancestors: {meta}"
        );

        let store = SqliteStore::open_in_memory().expect("store");
        let page = route_request(
            &authorized("GET", "/"),
            TEST_TOKEN,
            "test.db",
            false,
            &store,
        );
        let csp = page
            .headers
            .iter()
            .find(|(name, _)| *name == "Content-Security-Policy")
            .map(|(_, value)| *value)
            .expect("the UI page must send a CSP header");
        assert!(
            csp.contains("frame-ancestors 'none'"),
            "the header keeps the framing protection the meta tag cannot: {csp}"
        );
    }

    /// With no icon declared the browser probes `/favicon.ico` on its own. That
    /// path is not the UI page, so it needs a bearer token the probe never
    /// carries — a 401 in the console on every load. An inline `data:` icon
    /// stops the request; `img-src ... data:` already permits it.
    #[test]
    fn embedded_ui_declares_an_inline_icon_so_no_favicon_probe_is_made() {
        assert!(
            WEB_UI_HTML.contains("rel=\"icon\"") && WEB_UI_HTML.contains("href=\"data:"),
            "the page must declare an inline icon"
        );
    }

    /// A credential the server refused must not be replayed: keeping it made
    /// every later load paint the authenticated shell, wait out a `/health`
    /// round-trip, and only then fall back to the gate.
    #[test]
    fn embedded_ui_forgets_a_rejected_token() {
        let show_gate = ui_slice("function showGate(", "\n}");
        assert!(
            show_gate.contains("removeItem(TOKEN_KEY)"),
            "a rejected token must be dropped from storage: {show_gate}"
        );
    }

    /// `.ctl-row label` sets `flex: none` and beats `.ctl-row > *` on
    /// specificity, so the filter controls could not shrink to the 248px
    /// sidebar: they needed 308px and the page-size input was cut in half at the
    /// sidebar edge. The row must be allowed to wrap.
    #[test]
    fn embedded_ui_lets_the_filter_row_wrap_inside_the_sidebar() {
        let rule = ui_slice(".ctl-row {", "}");
        assert!(
            rule.contains("flex-wrap: wrap"),
            "unshrinkable filter labels overflow the sidebar without wrapping: {rule}"
        );
    }

    #[test]
    fn serve_session_generates_token_and_loopback_address() {
        let session = ServeSession::bind_loopback(0).expect("session");
        assert_eq!(session.token().len(), 32);
        assert_eq!(session.address(), "127.0.0.1:0");
    }
}
