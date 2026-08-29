//! Reading the admin port.
//!
//! One poll is three requests — `/admin/generations`, `/admin/routes`,
//! `/metrics` — issued concurrently and assembled into a [`Snapshot`]. They go
//! out together rather than in sequence because they are three views of the
//! same instant, and a serial poll would spread them across three round trips
//! and then difference the result as though it were one.
//!
//! # Everything here has a deadline
//!
//! An admin port that accepts a connection and then never answers is a
//! completely ordinary failure of an overloaded process, and it is the one that
//! would freeze a terminal UI hardest: the draw loop would sit inside an
//! `await` that has no reason to ever return. Every request below is wrapped in
//! a timeout, and a timeout is reported as a failed poll, which the display
//! already knows how to show.
//!
//! # And a size limit
//!
//! Response bodies are read through [`Limited`], because a route table is
//! bounded by what a cluster holds and a client that will buffer any number of
//! bytes handed to it is a client that can be made to exhaust memory by
//! pointing it at the wrong port.
//!
//! # The bearer token, on two requests out of five
//!
//! A daemon started with `--admin-token-file` refuses a mutating `/admin/`
//! request that carries no `Authorization: Bearer` header. That is `pin` and
//! `unpin` and nothing else — the three requests a poll makes are `GET`s, which
//! the admin listener never gates — so the header is attached by method rather
//! than to every request. Sending it on the polls would put the secret on the
//! wire once a second to answer a question that never asked for it.

use std::path::Path;
use std::time::Duration;

use bytes::Bytes;
use http::header::HeaderValue;
use http::{Method, Request, StatusCode, Uri};
use http_body_util::{BodyExt, Full, Limited};
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client;
use hyper_util::rt::{TokioExecutor, TokioTimer};
use serde::Serialize;

use crate::contract::{GenerationsResponse, RoutesResponse};
use crate::prom::{self, MetricsSnapshot};

/// The port `ingressd` serves admin traffic on, and ingress-nginx before it.
pub const DEFAULT_ADMIN_URL: &str = "http://127.0.0.1:10254";

/// How much of a response body this client will buffer.
///
/// Sixteen megabytes is far more than a route table that a human is reading in
/// a terminal, and far less than a number that lets a wrong URL exhaust memory.
const MAX_BODY_BYTES: usize = 16 * 1024 * 1024;

/// Why a poll did not produce a snapshot.
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    /// The URL could not be turned into something to connect to.
    #[error("`{url}` is not a usable admin URL: {reason}")]
    BadUrl {
        /// What was supplied.
        url: String,
        /// What was wrong with it.
        reason: String,
    },
    /// The connection failed, or the server hung up.
    #[error("cannot reach {path}: {source}")]
    Transport {
        /// The path being requested.
        path: &'static str,
        /// The underlying failure.
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    /// The server did not answer within the deadline.
    #[error("{path} did not answer within {}ms", timeout.as_millis())]
    Timeout {
        /// The path being requested.
        path: &'static str,
        /// The deadline that expired.
        timeout: Duration,
    },
    /// The server answered, with something other than success.
    #[error("{path} returned {status}")]
    Status {
        /// The path being requested.
        path: &'static str,
        /// What came back.
        status: StatusCode,
    },
    /// The body was not what the contract describes.
    #[error("{path} returned a body this client cannot read: {source}")]
    Body {
        /// The path being requested.
        path: &'static str,
        /// The parse failure.
        #[source]
        source: serde_json::Error,
    },
    /// The `--token-file` could not be turned into a header.
    #[error("cannot use the token file: {0}")]
    Token(String),
}

impl ClientError {
    /// A one-line form for a status bar, which has no room for a cause chain.
    pub fn brief(&self) -> String {
        match self {
            Self::BadUrl { reason, .. } => reason.clone(),
            Self::Transport { path, source } => format!("{path}: {source}"),
            Self::Timeout { path, timeout } => {
                format!("{path}: no answer in {}ms", timeout.as_millis())
            }
            Self::Status { path, status } => format!("{path}: HTTP {status}"),
            Self::Body { path, .. } => format!("{path}: unreadable body"),
            Self::Token(reason) => reason.clone(),
        }
    }
}

/// Everything one poll read, at one instant.
#[derive(Debug, Clone, Serialize)]
pub struct Snapshot {
    /// The URL it was read from.
    pub url: String,
    /// `GET /admin/generations`.
    pub generations: GenerationsResponse,
    /// `GET /admin/routes`.
    pub routes: RoutesResponse,
    /// `GET /metrics`, reduced to the series this client uses.
    pub metrics: MetricsSnapshot,
}

impl Snapshot {
    /// The generation traffic is pinned to, if any.
    ///
    /// The admin API is authoritative; `ramjet_pinned` is a fallback for a
    /// server that exports the metric but has not been asked for JSON yet, and
    /// exists so the red banner does not depend on which of the two endpoints
    /// answered first.
    pub fn pinned(&self) -> Option<u64> {
        self.generations.pinned.or(self.metrics.pinned)
    }

    /// The generation currently serving traffic.
    ///
    /// Taken from the admin API, falling back to the route listing and then to
    /// the metrics gauge — three sources for the same number, and the header
    /// should show it if any of them answered.
    pub fn serving(&self) -> u64 {
        if self.generations.serving != 0 {
            self.generations.serving
        } else if self.routes.generation != 0 {
            self.routes.generation
        } else {
            self.metrics.generation.unwrap_or(0)
        }
    }
}

/// A client for one ingressd admin port.
#[derive(Clone)]
pub struct AdminClient {
    base: String,
    timeout: Duration,
    http: Client<HttpConnector, Full<Bytes>>,
    /// Sent on mutating requests only. Pre-built so a keystroke that pins a
    /// generation cannot fail on a header that was invalid all along.
    bearer: Option<HeaderValue>,
}

impl std::fmt::Debug for AdminClient {
    /// Says whether there is a token, never what it is.
    ///
    /// Hand-written rather than derived because `HeaderValue`'s own `Debug`
    /// prints the value, and a client is the sort of thing that ends up in a
    /// panic message.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AdminClient")
            .field("base", &self.base)
            .field("timeout", &self.timeout)
            .field("bearer", &self.bearer.is_some())
            .finish_non_exhaustive()
    }
}

impl AdminClient {
    /// Builds a client for an admin endpoint.
    ///
    /// `base` is generous about what it accepts, because the thing a person
    /// types when they want to point this at a cluster port-forward is
    /// `localhost:10254`, and refusing that in favour of a full URL is a
    /// papercut with no upside.
    pub fn new(base: &str, timeout: Duration) -> Result<Self, ClientError> {
        let base = normalize_url(base)?;

        let mut connector = HttpConnector::new();
        // The admin port is loopback or a port-forward. A connect that has not
        // succeeded within the poll deadline is one the poll has no use for.
        connector.set_connect_timeout(Some(timeout));
        // Nagle's algorithm delays a small request behind an ACK, which on a
        // one-second poll of three small GETs is measurable and pointless.
        connector.set_nodelay(true);

        // HTTP/1.1 is what this builder does by default, and the admin port
        // offers nothing else; there is no `http2_only` to turn off because the
        // `http2` feature is not enabled for this crate in the first place.
        let mut builder = Client::builder(TokioExecutor::new());
        builder
            // Keep-alive across polls: at one poll a second this is the
            // difference between three connections a second and three
            // connections total.
            .pool_idle_timeout(Duration::from_secs(60))
            // Without a timer the pool has no way to notice an idle connection
            // has aged out, so the setting above would be inert rather than
            // wrong — the failure mode is a connection held open forever
            // against a server that has long since forgotten it.
            .pool_timer(TokioTimer::new());
        let http = builder.build(connector);

        Ok(Self {
            base,
            timeout,
            http,
            bearer: None,
        })
    }

    /// Sends the token in `path` on `pin` and `unpin`.
    ///
    /// Read once, here, rather than per request: the file is a mounted Secret or
    /// something a person put next to their port-forward script, and re-reading
    /// it on a keystroke would turn a missing file into a failure at the moment
    /// somebody is rolling a cluster back.
    ///
    /// # Errors
    ///
    /// The file could not be read, held nothing but whitespace, or held bytes no
    /// header can carry.
    pub fn with_token_file(mut self, path: &Path) -> Result<Self, ClientError> {
        let raw = std::fs::read_to_string(path)
            .map_err(|error| ClientError::Token(format!("{}: {error}", path.display())))?;
        let token = raw.trim();
        if token.is_empty() {
            return Err(ClientError::Token(format!(
                "{} is empty; there is no token in it to send",
                path.display()
            )));
        }
        let mut value = HeaderValue::from_str(&format!("Bearer {token}")).map_err(|_| {
            ClientError::Token(format!(
                "{} holds bytes an HTTP header cannot carry",
                path.display()
            ))
        })?;
        // Keeps the token out of any `Debug` rendering `http` does of a request
        // this header ends up on — which on the error paths below is a thing
        // that reaches a terminal.
        value.set_sensitive(true);
        self.bearer = Some(value);
        Ok(self)
    }

    /// The URL this client polls, normalized.
    pub fn url(&self) -> &str {
        &self.base
    }

    /// Reads one snapshot.
    ///
    /// The three requests run concurrently, so a slow `/metrics` does not delay
    /// the route table. If any of them fails the whole poll fails: a snapshot
    /// with two thirds of its numbers would be differenced against a complete
    /// one next time round and produce rates that are not wrong so much as
    /// meaningless.
    pub async fn snapshot(&self) -> Result<Snapshot, ClientError> {
        let (generations, routes, metrics) = tokio::try_join!(
            self.get_json::<GenerationsResponse>("/admin/generations"),
            self.get_json::<RoutesResponse>("/admin/routes"),
            self.get_metrics(),
        )?;

        Ok(Snapshot {
            url: self.base.clone(),
            generations,
            routes,
            metrics,
        })
    }

    /// Pins traffic to a generation — the emergency brake.
    pub async fn pin(&self, generation: u64) -> Result<(), ClientError> {
        let body = serde_json::json!({ "generation": generation }).to_string();
        self.send(
            Method::POST,
            "/admin/rollback",
            Full::new(Bytes::from(body)),
            Some("application/json"),
        )
        .await
        .map(|_| ())
    }

    /// Releases the pin, returning to the newest published generation.
    pub async fn unpin(&self) -> Result<(), ClientError> {
        self.send(Method::DELETE, "/admin/rollback", Full::default(), None)
            .await
            .map(|_| ())
    }

    /// Issues one request and reads the body, within the deadline.
    async fn send(
        &self,
        method: Method,
        path: &'static str,
        body: Full<Bytes>,
        content_type: Option<&'static str>,
    ) -> Result<Bytes, ClientError> {
        let uri: Uri = format!("{}{path}", self.base).parse().map_err(|e| {
            ClientError::BadUrl {
                url: format!("{}{path}", self.base),
                reason: format!("{e}"),
            }
        })?;

        // Attached by method, mirroring the rule the admin listener enforces:
        // everything that is not a read needs the token, and a read never does.
        let mut builder = Request::builder().method(&method).uri(uri);
        if method != Method::GET && method != Method::HEAD {
            if let Some(bearer) = &self.bearer {
                builder = builder.header(http::header::AUTHORIZATION, bearer.clone());
            }
        }
        if let Some(content_type) = content_type {
            builder = builder.header(http::header::CONTENT_TYPE, content_type);
        }
        let request = builder
            .header(http::header::USER_AGENT, "ramjet-top")
            .body(body)
            .map_err(|e| ClientError::Transport {
                path,
                source: Box::new(e),
            })?;

        let response = tokio::time::timeout(self.timeout, self.http.request(request))
            .await
            .map_err(|_| ClientError::Timeout {
                path,
                timeout: self.timeout,
            })?
            .map_err(|e| ClientError::Transport {
                path,
                source: Box::new(e),
            })?;

        let status = response.status();
        if !status.is_success() {
            return Err(ClientError::Status { path, status });
        }

        // The body has its own slice of the deadline rather than sharing the
        // one the headers already spent: a server that sends headers and then
        // stalls is the same hang, one layer down.
        let collected = tokio::time::timeout(
            self.timeout,
            Limited::new(response.into_body(), MAX_BODY_BYTES).collect(),
        )
        .await
        .map_err(|_| ClientError::Timeout {
            path,
            timeout: self.timeout,
        })?
        .map_err(|e| ClientError::Transport { path, source: e })?;

        Ok(collected.to_bytes())
    }

    /// Reads a JSON endpoint.
    async fn get_json<T: serde::de::DeserializeOwned>(
        &self,
        path: &'static str,
    ) -> Result<T, ClientError> {
        let bytes = self
            .send(Method::GET, path, Full::default(), None)
            .await?;
        serde_json::from_slice(&bytes).map_err(|source| ClientError::Body { path, source })
    }

    /// Reads the Prometheus endpoint.
    async fn get_metrics(&self) -> Result<MetricsSnapshot, ClientError> {
        let bytes = self
            .send(Method::GET, "/metrics", Full::default(), None)
            .await?;
        // Lossy rather than strict: an exposition page is ASCII, and a byte
        // that is not is a reason to lose that line, not the whole scrape.
        Ok(prom::parse(&String::from_utf8_lossy(&bytes)))
    }
}

/// Turns what a person types into something with a scheme and no trailing
/// slash.
///
/// A trailing slash matters more than it looks: paths are appended, so a base
/// of `http://host/` would produce `http://host//metrics`, which some servers
/// route and some do not.
pub fn normalize_url(input: &str) -> Result<String, ClientError> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Err(ClientError::BadUrl {
            url: input.to_string(),
            reason: "it is empty".to_string(),
        });
    }

    let with_scheme = if trimmed.contains("://") {
        trimmed.to_string()
    } else {
        format!("http://{trimmed}")
    };
    let with_scheme = with_scheme.trim_end_matches('/').to_string();

    let uri: Uri = with_scheme.parse().map_err(|e| ClientError::BadUrl {
        url: input.to_string(),
        reason: format!("{e}"),
    })?;

    match uri.scheme_str() {
        Some("http") => {}
        Some(other) => {
            return Err(ClientError::BadUrl {
                url: input.to_string(),
                // Deliberate: the admin port is plaintext, and pretending to
                // offer TLS here would mean linking one to fail at runtime.
                reason: format!("scheme `{other}` is not supported; the admin port is http"),
            })
        }
        None => {
            return Err(ClientError::BadUrl {
                url: input.to_string(),
                reason: "no scheme".to_string(),
            })
        }
    }

    if uri.authority().is_none() {
        return Err(ClientError::BadUrl {
            url: input.to_string(),
            reason: "no host".to_string(),
        });
    }

    Ok(with_scheme)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contract::{GenerationEntry, RouteEntry};

    #[test]
    fn a_bare_host_and_port_gains_a_scheme() {
        assert_eq!(
            normalize_url("127.0.0.1:10254").expect("usable"),
            "http://127.0.0.1:10254"
        );
        assert_eq!(
            normalize_url("localhost:10254").expect("usable"),
            "http://localhost:10254"
        );
    }

    #[test]
    fn a_full_url_is_left_alone_apart_from_a_trailing_slash() {
        assert_eq!(
            normalize_url("http://10.0.0.5:10254").expect("usable"),
            "http://10.0.0.5:10254"
        );
        assert_eq!(
            normalize_url("http://10.0.0.5:10254/").expect("usable"),
            "http://10.0.0.5:10254",
            "otherwise every path would be requested with a doubled slash"
        );
    }

    #[test]
    fn surrounding_whitespace_is_forgiven() {
        assert_eq!(
            normalize_url("  127.0.0.1:10254 \n").expect("usable"),
            "http://127.0.0.1:10254"
        );
    }

    #[test]
    fn https_is_refused_with_a_reason_rather_than_silently_downgraded() {
        let err = normalize_url("https://host:10254").expect_err("refused");
        assert!(err.to_string().contains("http"), "{err}");
        assert!(err.brief().contains("not supported"), "{}", err.brief());
    }

    #[test]
    fn unusable_urls_are_named() {
        assert!(normalize_url("").is_err());
        assert!(normalize_url("   ").is_err());
        assert!(normalize_url("http://").is_err());
    }

    #[test]
    fn the_default_url_is_the_conventional_admin_port() {
        assert_eq!(
            normalize_url(DEFAULT_ADMIN_URL).expect("usable"),
            DEFAULT_ADMIN_URL
        );
    }

    #[test]
    fn a_client_can_be_built_for_the_default() {
        let client = AdminClient::new(DEFAULT_ADMIN_URL, Duration::from_secs(2)).expect("built");
        assert_eq!(client.url(), DEFAULT_ADMIN_URL);
    }

    fn snapshot_with(pinned: Option<u64>, serving: u64) -> Snapshot {
        Snapshot {
            url: DEFAULT_ADMIN_URL.to_string(),
            generations: GenerationsResponse {
                pinned,
                serving,
                generations: vec![GenerationEntry {
                    generation: serving,
                    ..Default::default()
                }],
            },
            routes: RoutesResponse {
                generation: serving,
                routes: vec![RouteEntry::default()],
            },
            metrics: MetricsSnapshot::default(),
        }
    }

    #[test]
    fn the_admin_api_wins_over_the_metrics_gauge_for_the_pin() {
        let mut snapshot = snapshot_with(Some(7), 9);
        snapshot.metrics.pinned = Some(3);
        assert_eq!(snapshot.pinned(), Some(7));
    }

    #[test]
    fn the_metrics_gauge_is_a_fallback_for_the_pin() {
        let mut snapshot = snapshot_with(None, 9);
        snapshot.metrics.pinned = Some(3);
        assert_eq!(snapshot.pinned(), Some(3));

        snapshot.metrics.pinned = None;
        assert_eq!(snapshot.pinned(), None);
    }

    #[test]
    fn the_serving_generation_falls_back_through_all_three_sources() {
        assert_eq!(snapshot_with(None, 12).serving(), 12);

        let mut snapshot = snapshot_with(None, 0);
        snapshot.routes.generation = 5;
        assert_eq!(snapshot.serving(), 5, "the route listing knows");

        snapshot.routes.generation = 0;
        snapshot.metrics.generation = Some(4);
        assert_eq!(snapshot.serving(), 4, "the gauge knows");

        snapshot.metrics.generation = None;
        assert_eq!(snapshot.serving(), 0);
    }

    #[test]
    fn errors_render_briefly_enough_for_a_status_bar() {
        let timeout = ClientError::Timeout {
            path: "/admin/routes",
            timeout: Duration::from_millis(1500),
        };
        assert_eq!(timeout.brief(), "/admin/routes: no answer in 1500ms");

        let status = ClientError::Status {
            path: "/metrics",
            status: StatusCode::NOT_FOUND,
        };
        assert!(status.brief().starts_with("/metrics: HTTP 404"));

        let body = ClientError::Body {
            path: "/admin/routes",
            source: serde_json::from_str::<u8>("{").expect_err("invalid"),
        };
        assert_eq!(body.brief(), "/admin/routes: unreadable body");
        assert!(!body.brief().contains('\n'), "one line only");
    }
}
