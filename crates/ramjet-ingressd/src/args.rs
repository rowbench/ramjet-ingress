//! Command-line and environment parsing.
//!
//! Hand-rolled, and it is worth saying why: this binary has nine options, all
//! of them `--name value`, and `clap` would add roughly 200KB and a dozen
//! transitive crates to a data-plane image for the privilege of formatting the
//! help text. The parser below is about a hundred lines and every option it
//! accepts is visible in one place.
//!
//! Every flag has an environment variable twin, because that is how a container
//! is configured. The precedence is the usual one — an explicit flag beats the
//! environment, which beats the default — and it is worth being explicit that a
//! flag *always* wins, so a `kubectl edit` of the args cannot be silently
//! overridden by a ConfigMap somebody forgot about.

use std::ffi::OsString;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use ramjet_controller::ServiceRef;
use ramjet_proxy::{
    DEFAULT_ADMIN_PORT, DEFAULT_HTTP_PORT, DEFAULT_HTTPS_PORT, DEFAULT_MAX_BUF_SIZE,
    MIN_MAX_BUF_SIZE,
};

/// Why the command line could not be understood.
#[derive(Debug, thiserror::Error)]
pub enum ArgError {
    /// An option this binary does not have.
    #[error("unknown option `{0}` (try --help)")]
    Unknown(String),
    /// An option that needs a value did not get one.
    #[error("option `{0}` needs a value")]
    MissingValue(String),
    /// A positional argument, which this binary never takes.
    #[error("unexpected argument `{0}` (every option is a --flag)")]
    Unexpected(String),
    /// An argument that was not valid UTF-8.
    #[error("argument {0:?} is not valid UTF-8")]
    NotUtf8(OsString),
    /// A value that could not be parsed as the option's type.
    #[error("`{value}` is not a valid {kind} for `{option}`")]
    BadValue {
        /// The option being set.
        option: String,
        /// What was supplied.
        value: String,
        /// What was expected, e.g. "address".
        kind: &'static str,
    },
}

/// Which data plane serves traffic.
///
/// Two engines share every line of the routing and configuration path and
/// differ only in how they move bytes. The default is the one that has been
/// measured against nginx and carries TLS, HTTP/2 and upgrades; the other is an
/// experiment in whether a completion-based reactor gets under the syscall
/// floor `bench/PROFILE.md` identified.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Engine {
    /// hyper on tokio. Everything works.
    #[default]
    Hyper,
    /// The `ramjet` reactor: io_uring on Linux, kqueue elsewhere.
    ///
    /// HTTP/1.1 plaintext only, static routes only. See `ramjet_engine` for the
    /// full list of what it refuses.
    Uring,
}

impl Engine {
    /// The name this engine is selected by.
    pub fn as_str(self) -> &'static str {
        match self {
            Engine::Hyper => "hyper",
            Engine::Uring => "uring",
        }
    }
}

/// Everything the daemon was asked to do.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Args {
    /// Print usage and exit.
    pub help: bool,
    /// Print the version and exit.
    pub version: bool,
    /// Dev mode: serve the routes in this file instead of watching Kubernetes.
    ///
    /// Its presence is what selects the mode, because the two are mutually
    /// exclusive by nature: a file and an API server are two writers for one
    /// route table, and letting both write would make the winner a race.
    pub static_routes: Option<PathBuf>,
    /// `IngressClass` we answer to. Ingresses naming any other class are
    /// invisible to this replica, which is what makes running alongside
    /// ingress-nginx during a migration safe.
    pub ingress_class: String,
    /// Single namespace to watch, or `None` for the whole cluster.
    pub watch_namespace: Option<String>,
    /// Address written into managed Ingresses' `.status.loadBalancer`.
    pub publish_address: Option<String>,
    /// `namespace/name` of a Service whose own status supplies that address.
    pub publish_service: Option<String>,
    /// Backend for requests matching no rule, as `namespace/name:port`.
    pub default_backend: Option<ServiceRef>,
    /// `namespace/name` of the Secret answering a handshake whose SNI matches
    /// nothing.
    pub default_tls_secret: Option<String>,
    /// Whether to write Ingress status at all.
    pub update_status: bool,
    /// Compiled generations kept for `/admin/generations` and rollback.
    pub history_size: usize,
    /// URL the semantic configuration diff is POSTed to on every publish.
    pub audit_webhook: Option<String>,
    /// Plaintext listener, or `None` if disabled.
    pub http: Option<SocketAddr>,
    /// TLS listener, or `None` if disabled.
    pub https: Option<SocketAddr>,
    /// Admin listener, or `None` if disabled.
    pub admin: Option<SocketAddr>,
    /// Whether `--https` or `--no-https` was given explicitly.
    ///
    /// Without it, dev mode skips the TLS listener when the configuration
    /// declares no certificates — a listener that fails every handshake is not
    /// a useful default.
    pub https_explicit: bool,
    /// Bound on establishing an upstream connection.
    pub connect_timeout: Duration,
    /// Bound on receiving upstream response headers.
    pub response_timeout: Duration,
    /// How long in-flight requests get after a shutdown signal.
    pub shutdown_grace: Duration,
    /// Endpoints tried before giving up on a retryable failure.
    pub max_connect_attempts: usize,
    /// Idle upstream connections kept per endpoint.
    pub upstream_pool_idle: usize,
    /// Ceiling on one client connection's HTTP/1 read and write buffers.
    pub max_buf_size: usize,
    /// Serving runtimes to start, or `None` for one per available core.
    pub worker_threads: Option<usize>,
    /// Which data plane serves traffic.
    pub engine: Engine,
    /// Require a PROXY protocol header on the traffic listeners.
    ///
    /// Only safe behind a load balancer that always sends one: the header names
    /// the client, so a listener reachable from anywhere else is a listener
    /// whose clients can pick their own address.
    pub proxy_protocol: bool,
    /// How long a sender gets to deliver a complete PROXY header.
    pub proxy_protocol_timeout: Duration,
}

impl Default for Args {
    fn default() -> Self {
        let all = |port: u16| SocketAddr::from(([0, 0, 0, 0], port));
        Args {
            help: false,
            version: false,
            static_routes: None,
            ingress_class: "ramjet".to_owned(),
            watch_namespace: None,
            publish_address: None,
            publish_service: None,
            default_backend: None,
            default_tls_secret: None,
            update_status: true,
            history_size: ramjet_proxy::DEFAULT_HISTORY_SIZE,
            audit_webhook: None,
            http: Some(all(DEFAULT_HTTP_PORT)),
            https: Some(all(DEFAULT_HTTPS_PORT)),
            admin: Some(all(DEFAULT_ADMIN_PORT)),
            https_explicit: false,
            connect_timeout: Duration::from_secs(5),
            response_timeout: Duration::from_secs(60),
            shutdown_grace: Duration::from_secs(30),
            max_connect_attempts: 3,
            upstream_pool_idle: ramjet_proxy::DEFAULT_POOL_MAX_IDLE_PER_HOST,
            max_buf_size: DEFAULT_MAX_BUF_SIZE,
            worker_threads: None,
            engine: Engine::Hyper,
            proxy_protocol: false,
            // Long enough that a load balancer under load still fits, short
            // enough that holding the connection is not a cheap way to occupy
            // a task and a file descriptor. The header is the first thing a
            // sender writes, so a sender that has not finished it in five
            // seconds is not going to.
            proxy_protocol_timeout: Duration::from_secs(5),
        }
    }
}

/// What `--help` prints.
pub const USAGE: &str = "\
ramjet-ingressd — the ramjet-ingress data plane

USAGE:
    ramjet-ingressd [OPTIONS]

    With no --static-routes, the daemon watches the Kubernetes API and serves
    what the controller compiles. With one, it serves that file and never talks
    to Kubernetes at all.

KUBERNETES:
    --ingress-class <NAME>    IngressClass to answer to      [default: ramjet]
    --watch-namespace <NS>    Watch one namespace       [default: all of them]
    --default-backend <REF>   Backend for requests matching no rule, as
                              namespace/name:port
    --default-tls-secret <REF>
                              Secret (namespace/name) serving a handshake whose
                              SNI matches nothing
    --publish-address <ADDR>  Written into managed Ingresses' status
    --publish-service <REF>   Service (namespace/name) whose own status supplies
                              that address. Beats --publish-address.
    --no-status-update        Never write Ingress status.

    The client is configured the way every Kubernetes tool configures one: the
    in-cluster ServiceAccount if there is one, otherwise the current context of
    $KUBECONFIG or ~/.kube/config.

TIME TRAVEL AND THE AUDIT TRAIL:
    --history-size <N>        Compiled generations kept for /admin/generations
                              and rollback                      [default: 10]
    --audit-webhook <URL>     POST the semantic diff of every published
                              generation to URL

    Each kept generation holds its route table and its parsed certificates
    alive, which is roughly a hundred bytes per route per generation; the
    certificates are content-addressed and shared between generations that did
    not rotate them. Ten generations of a ten-thousand route cluster is a few
    megabytes.

    Every publish is written down three ways: a structured `tracing` event on
    the `audit` target, a Kubernetes Event on the IngressClass (reason
    ConfigApplied, ConfigPinned, or ConfigResumed), and — with --audit-webhook —
    one POST of the diff as JSON. The webhook is fire-and-forget: one attempt, a
    5s timeout, failures logged and never blocking a publish. It speaks http://
    only and refuses an https:// URL at startup rather than downgrading, because
    the control plane does not carry a TLS client for this; point it at a
    collector inside the cluster.

    Events need RBAC: `events.k8s.io` / `events` / create. The chart's
    ClusterRole has it. Without it the Events are skipped at debug level and
    everything else still works.

DEV MODE:
    --static-routes <FILE>    Serve the hosts, paths, backends, and certificates
                              described in FILE and never talk to Kubernetes.

LISTENERS:
    --http <ADDR>             Plaintext listener        [default: 0.0.0.0:8080]
    --https <ADDR>            TLS listener              [default: 0.0.0.0:8443]
    --admin <ADDR>            Metrics and probes        [default: 0.0.0.0:10254]
    --no-http, --no-https, --no-admin
                              Disable a listener.

    An address may be `host:port`, `:port`, or a bare port. In dev mode, without
    an explicit --https or --no-https, the TLS listener is skipped when the
    configuration declares no certificates. In Kubernetes mode it always binds:
    the certificates arrive over a watch, after the socket.

BEHIND A LOAD BALANCER:
    --proxy-protocol          Require a PROXY protocol header (v1 or v2) on the
                              --http and --https listeners, and take the client
                              address from it.
    --proxy-protocol-timeout <SECS>
                              Time a sender gets to deliver a complete header
                              before the connection is dropped   [default: 5]

    A cloud L4 load balancer — AWS NLB, DigitalOcean, Scaleway, GCP passthrough
    — forwards TCP without touching the payload, so without this every request
    is attributed to the balancer. Turn it on where the balancer is configured
    to send the header, and set the same option on both sides.

    SECURITY: the header *is* the client identity. Anything that can reach the
    listener can claim to be any address, and X-Forwarded-For, X-Real-IP and
    every application decision made from them follow. Enable it only on a
    listener nothing but the load balancer can reach. The header is required,
    not optional: a connection without a valid one is dropped, and the first
    such drop on each serving runtime is logged at warn — so a balancer that
    is not sending the header says so rather than looking like a network
    fault. The --admin listener never reads one, because Prometheus and the
    kubelet do not send one.

UPSTREAMS:
    --connect-timeout <SECS>      TCP connect bound          [default: 5]
    --response-timeout <SECS>     Response header bound      [default: 60]
    --max-connect-attempts <N>    Endpoints tried on a connect failure
                                                             [default: 3]
    --upstream-pool-idle <N>      Idle upstream connections kept per endpoint
                                                            [default: 128]

    --upstream-pool-idle is a ceiling, not a reservation: nothing is opened
    until a request needs it. Below the concurrent requests an endpoint
    receives, the surplus connections are closed as they go idle and reopened
    on the next request, which is a TCP handshake on the request path. Above
    it, the only cost is file descriptors.

SERVING:
    --engine <NAME>           Data plane: hyper or uring     [default: hyper]
    --worker-threads <N>      Serving runtimes, one per thread
                                              [default: one per available core]
    --max-buf-size <BYTES>    Ceiling on one client connection's HTTP/1 read and
                              write buffers, and so on the request head this
                              replica will accept    [default: 65536, min 8192]

    `uring` is experimental. It serves HTTP/1.1 plaintext on the ramjet reactor
    — io_uring on Linux, kqueue elsewhere — to find out whether batched
    submission gets under the syscall floor the hyper engine measured. It has no
    TLS, no HTTP/2, no protocol upgrades and no Kubernetes mode, and refuses
    each of those with a status and an explanation rather than silently doing
    something else. Everything about routing, load balancing, canaries, headers
    and /metrics is the same on both.

    Each runtime owns its connections, its upstream connection pool, and its
    timers, and a connection stays on the one it landed on. Setting this above
    the cores the process can actually use makes them compete; setting it to 1
    serves everything on one thread.

    --max-buf-size bounds the tail, not the common case. hyper allocates the
    first 8 KiB of each buffer whatever this is set to, and never shrinks one
    again while the connection lives — so a client that sends a 400 KiB header
    block would pin 400 KiB until it disconnects. 64 KiB accepts every request
    nginx's own 32 KiB limit would and bounds the worst case at a sixth of
    hyper's default. Requests over the ceiling are answered 431.

SHUTDOWN:
    --shutdown-grace <SECS>   In-flight requests get this long after SIGTERM
                                                             [default: 30]

OTHER:
    -h, --help                Print this help
    -V, --version             Print the version

Every option has an environment twin (RAMJET_STATIC_ROUTES, RAMJET_INGRESS_CLASS,
RAMJET_WATCH_NAMESPACE, RAMJET_DEFAULT_BACKEND, RAMJET_DEFAULT_TLS_SECRET,
RAMJET_PUBLISH_ADDRESS, RAMJET_PUBLISH_SERVICE, RAMJET_UPDATE_STATUS,
RAMJET_HISTORY_SIZE, RAMJET_AUDIT_WEBHOOK,
RAMJET_HTTP, RAMJET_HTTPS, RAMJET_ADMIN, RAMJET_CONNECT_TIMEOUT,
RAMJET_RESPONSE_TIMEOUT, RAMJET_MAX_CONNECT_ATTEMPTS, RAMJET_UPSTREAM_POOL_IDLE,
RAMJET_MAX_BUF_SIZE, RAMJET_WORKER_THREADS, RAMJET_SHUTDOWN_GRACE,
RAMJET_ENGINE, RAMJET_PROXY_PROTOCOL, RAMJET_PROXY_PROTOCOL_TIMEOUT).
A flag always beats the environment. RUST_LOG sets the log filter.

ADMIN ENDPOINTS:
    GET    /metrics             Prometheus text exposition
    GET    /healthz             Liveness: 200 whenever the process is answering
    GET    /readyz              Readiness: 200 once a route table has been
                                published. In Kubernetes mode that means a
                                compiled generation, not the controller's empty
                                seed.
    GET    /admin/generations   The generations this replica has applied, newest
                                first, each with what changed and whether it
                                went live
    GET    /admin/routes        Every route in the serving table, with its
                                request, error, and upstream-latency counters
    POST   /admin/rollback      {\"generation\": N} — republish N and hold
                                publication there. 404 if N is not in the
                                history, 409 if something is already pinned.
    DELETE /admin/rollback      Release the pin and publish the newest
                                generation. Idempotent.

    A rollback is an emergency brake, not desired state. The controller keeps
    watching and compiling while a pin is held — the generations it builds are
    recorded and marked as not published — and releasing the pin jumps straight
    to the newest one. The pin lives in memory and dies with the process:
    Kubernetes is the source of truth after a restart, so fix the objects and
    then release it.

    Per-route counters are served here and deliberately not exported as
    Prometheus series: ten thousand routes would mean ten thousand series on
    every scrape. /metrics gains only ramjet_pinned, which is 1 while a rollback
    is holding publication.

    The listener has no authentication and is not meant to be reachable from
    outside the cluster; the chart puts it behind a ClusterIP Service. Only POST
    and DELETE can change what is served, so nothing that follows links can roll
    a cluster back by accident.
";

impl Args {
    /// Parses the process arguments and the environment.
    pub fn from_env() -> Result<Args, ArgError> {
        Args::parse(std::env::args_os().skip(1), |name| std::env::var(name).ok())
    }

    /// Parses `arguments`, falling back to `env` for anything not given.
    pub fn parse<I, E>(arguments: I, env: E) -> Result<Args, ArgError>
    where
        I: IntoIterator,
        I::Item: Into<OsString>,
        E: Fn(&str) -> Option<String>,
    {
        let mut args = Args::from_environment(&env)?;
        let mut input = arguments.into_iter().map(Into::into);

        while let Some(raw) = input.next() {
            let argument = raw
                .clone()
                .into_string()
                .map_err(|_| ArgError::NotUtf8(raw))?;

            // `--flag=value` and `--flag value` are both accepted; splitting
            // here means every option below only has to handle one shape.
            let (name, inline) = match argument.split_once('=') {
                Some((name, value)) => (name.to_owned(), Some(value.to_owned())),
                None => (argument.clone(), None),
            };

            let mut value = || -> Result<String, ArgError> {
                match inline.clone() {
                    Some(value) => Ok(value),
                    None => input
                        .next()
                        .and_then(|next| next.into_string().ok())
                        .ok_or_else(|| ArgError::MissingValue(name.clone())),
                }
            };

            match name.as_str() {
                "-h" | "--help" => args.help = true,
                "-V" | "--version" => args.version = true,
                "--static-routes" => args.static_routes = Some(PathBuf::from(value()?)),
                "--ingress-class" => args.ingress_class = value()?,
                "--watch-namespace" => args.watch_namespace = Some(value()?),
                "--publish-address" => args.publish_address = Some(value()?),
                "--publish-service" => args.publish_service = Some(value()?),
                "--default-backend" => {
                    args.default_backend = Some(service_ref(&name, &value()?)?);
                }
                "--default-tls-secret" => args.default_tls_secret = Some(value()?),
                "--no-status-update" => args.update_status = false,
                "--history-size" => args.history_size = number(&name, &value()?)?.max(1),
                "--audit-webhook" => args.audit_webhook = Some(value()?),
                "--http" => args.http = Some(address(&name, &value()?)?),
                "--https" => {
                    args.https = Some(address(&name, &value()?)?);
                    args.https_explicit = true;
                }
                "--admin" => args.admin = Some(address(&name, &value()?)?),
                "--no-http" => args.http = None,
                "--no-https" => {
                    args.https = None;
                    args.https_explicit = true;
                }
                "--no-admin" => args.admin = None,
                "--connect-timeout" => args.connect_timeout = seconds(&name, &value()?)?,
                "--response-timeout" => args.response_timeout = seconds(&name, &value()?)?,
                "--shutdown-grace" => args.shutdown_grace = seconds(&name, &value()?)?,
                "--max-connect-attempts" => {
                    args.max_connect_attempts = number(&name, &value()?)?.max(1);
                }
                "--upstream-pool-idle" => {
                    args.upstream_pool_idle = number(&name, &value()?)?;
                }
                "--max-buf-size" => {
                    args.max_buf_size = number(&name, &value()?)?.max(MIN_MAX_BUF_SIZE);
                }
                "--worker-threads" => {
                    args.worker_threads = Some(number(&name, &value()?)?.max(1));
                }
                "--engine" => args.engine = engine(&name, &value()?)?,
                "--proxy-protocol" => args.proxy_protocol = true,
                "--proxy-protocol-timeout" => {
                    args.proxy_protocol_timeout = seconds(&name, &value()?)?;
                }
                other if other.starts_with('-') => {
                    return Err(ArgError::Unknown(other.to_owned()))
                }
                other => return Err(ArgError::Unexpected(other.to_owned())),
            }
        }

        Ok(args)
    }

    fn from_environment<E>(env: &E) -> Result<Args, ArgError>
    where
        E: Fn(&str) -> Option<String>,
    {
        let mut args = Args::default();
        if let Some(path) = env("RAMJET_STATIC_ROUTES") {
            args.static_routes = Some(PathBuf::from(path));
        }
        if let Some(value) = env("RAMJET_INGRESS_CLASS") {
            args.ingress_class = value;
        }
        if let Some(value) = env("RAMJET_WATCH_NAMESPACE") {
            args.watch_namespace = Some(value);
        }
        if let Some(value) = env("RAMJET_PUBLISH_ADDRESS") {
            args.publish_address = Some(value);
        }
        if let Some(value) = env("RAMJET_PUBLISH_SERVICE") {
            args.publish_service = Some(value);
        }
        if let Some(value) = env("RAMJET_DEFAULT_BACKEND") {
            args.default_backend = Some(service_ref("RAMJET_DEFAULT_BACKEND", &value)?);
        }
        if let Some(value) = env("RAMJET_DEFAULT_TLS_SECRET") {
            args.default_tls_secret = Some(value);
        }
        if let Some(value) = env("RAMJET_UPDATE_STATUS") {
            args.update_status = boolean("RAMJET_UPDATE_STATUS", &value)?;
        }
        if let Some(value) = env("RAMJET_HISTORY_SIZE") {
            args.history_size = number("RAMJET_HISTORY_SIZE", &value)?.max(1);
        }
        if let Some(value) = env("RAMJET_AUDIT_WEBHOOK") {
            args.audit_webhook = Some(value);
        }
        if let Some(value) = env("RAMJET_HTTP") {
            args.http = Some(address("RAMJET_HTTP", &value)?);
        }
        if let Some(value) = env("RAMJET_HTTPS") {
            args.https = Some(address("RAMJET_HTTPS", &value)?);
            args.https_explicit = true;
        }
        if let Some(value) = env("RAMJET_ADMIN") {
            args.admin = Some(address("RAMJET_ADMIN", &value)?);
        }
        if let Some(value) = env("RAMJET_CONNECT_TIMEOUT") {
            args.connect_timeout = seconds("RAMJET_CONNECT_TIMEOUT", &value)?;
        }
        if let Some(value) = env("RAMJET_RESPONSE_TIMEOUT") {
            args.response_timeout = seconds("RAMJET_RESPONSE_TIMEOUT", &value)?;
        }
        if let Some(value) = env("RAMJET_SHUTDOWN_GRACE") {
            args.shutdown_grace = seconds("RAMJET_SHUTDOWN_GRACE", &value)?;
        }
        if let Some(value) = env("RAMJET_MAX_CONNECT_ATTEMPTS") {
            args.max_connect_attempts = number("RAMJET_MAX_CONNECT_ATTEMPTS", &value)?.max(1);
        }
        if let Some(value) = env("RAMJET_UPSTREAM_POOL_IDLE") {
            args.upstream_pool_idle = number("RAMJET_UPSTREAM_POOL_IDLE", &value)?;
        }
        if let Some(value) = env("RAMJET_MAX_BUF_SIZE") {
            args.max_buf_size = number("RAMJET_MAX_BUF_SIZE", &value)?.max(MIN_MAX_BUF_SIZE);
        }
        if let Some(value) = env("RAMJET_ENGINE") {
            args.engine = engine("RAMJET_ENGINE", &value)?;
        }
        if let Some(value) = env("RAMJET_PROXY_PROTOCOL") {
            args.proxy_protocol = boolean("RAMJET_PROXY_PROTOCOL", &value)?;
        }
        if let Some(value) = env("RAMJET_PROXY_PROTOCOL_TIMEOUT") {
            args.proxy_protocol_timeout = seconds("RAMJET_PROXY_PROTOCOL_TIMEOUT", &value)?;
        }
        if let Some(value) = env("RAMJET_WORKER_THREADS") {
            args.worker_threads = Some(number("RAMJET_WORKER_THREADS", &value)?.max(1));
        }
        Ok(args)
    }
}

/// Parses `host:port`, `:port`, or a bare port.
///
/// The short forms exist because `--http :8080` is what everyone types, and
/// rejecting it in favour of `--http 0.0.0.0:8080` is a papercut with no
/// upside.
fn address(option: &str, value: &str) -> Result<SocketAddr, ArgError> {
    let bad = || ArgError::BadValue {
        option: option.to_owned(),
        value: value.to_owned(),
        kind: "address",
    };

    if let Ok(port) = value.parse::<u16>() {
        return Ok(SocketAddr::from(([0, 0, 0, 0], port)));
    }
    if let Some(port) = value.strip_prefix(':') {
        let port: u16 = port.parse().map_err(|_| bad())?;
        return Ok(SocketAddr::from(([0, 0, 0, 0], port)));
    }
    value.parse().map_err(|_| bad())
}

/// Parses `namespace/name:port`, the way an Ingress names a backend.
fn service_ref(option: &str, value: &str) -> Result<ServiceRef, ArgError> {
    value.parse().map_err(|_| ArgError::BadValue {
        option: option.to_owned(),
        value: value.to_owned(),
        kind: "service reference of the form namespace/name:port",
    })
}

/// Parses a boolean environment value.
///
/// Only exists for the environment twins: on the command line a boolean is a
/// flag, and `--no-status-update true` would be a worse way to say it.
fn boolean(option: &str, value: &str) -> Result<bool, ArgError> {
    match value.trim().to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" | "on" => Ok(true),
        "false" | "0" | "no" | "off" => Ok(false),
        _ => Err(ArgError::BadValue {
            option: option.to_owned(),
            value: value.to_owned(),
            kind: "boolean",
        }),
    }
}

fn seconds(option: &str, value: &str) -> Result<Duration, ArgError> {
    value
        .parse::<u64>()
        .map(Duration::from_secs)
        .map_err(|_| ArgError::BadValue {
            option: option.to_owned(),
            value: value.to_owned(),
            kind: "number of seconds",
        })
}

/// The engine named by a flag or an environment variable.
///
/// An unrecognised name is an error rather than a fallback to the default: an
/// operator who typed `--engine iouring` asked for something specific, and
/// silently serving on the other engine is the worst possible answer.
fn engine(option: &str, value: &str) -> Result<Engine, ArgError> {
    match value {
        "hyper" => Ok(Engine::Hyper),
        "uring" => Ok(Engine::Uring),
        _ => Err(ArgError::BadValue {
            option: option.to_owned(),
            value: value.to_owned(),
            kind: "engine (hyper or uring)",
        }),
    }
}

fn number(option: &str, value: &str) -> Result<usize, ArgError> {
    value.parse().map_err(|_| ArgError::BadValue {
        option: option.to_owned(),
        value: value.to_owned(),
        kind: "count",
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn no_env(_: &str) -> Option<String> {
        None
    }

    fn parse(arguments: &[&str]) -> Result<Args, ArgError> {
        Args::parse(arguments.iter().map(|a| (*a).to_owned()), no_env)
    }

    #[test]
    fn the_serving_knobs_take_a_flag_and_an_environment_twin() {
        let args = parse(&["--worker-threads", "3", "--upstream-pool-idle", "512"])
            .expect("valid");
        assert_eq!(args.worker_threads, Some(3));
        assert_eq!(args.upstream_pool_idle, 512);

        let env = |name: &str| match name {
            "RAMJET_WORKER_THREADS" => Some("2".to_owned()),
            "RAMJET_UPSTREAM_POOL_IDLE" => Some("64".to_owned()),
            _ => None,
        };
        let args = Args::parse(Vec::<String>::new(), env).expect("valid");
        assert_eq!(args.worker_threads, Some(2));
        assert_eq!(args.upstream_pool_idle, 64);

        // A flag beats the environment, so a `kubectl edit` of the args cannot
        // be silently overridden by a ConfigMap.
        let args = Args::parse(["--worker-threads=8".to_owned()], env).expect("valid");
        assert_eq!(args.worker_threads, Some(8));
    }

    #[test]
    fn zero_serving_runtimes_is_read_as_one() {
        // A data plane with nowhere to serve is not a configuration, it is a
        // hang, and `--worker-threads 0` is a typo rather than an intent.
        assert_eq!(parse(&["--worker-threads", "0"]).expect("valid").worker_threads, Some(1));
    }

    #[test]
    fn the_buffer_ceiling_takes_a_flag_and_an_environment_twin() {
        assert_eq!(
            parse(&[]).expect("valid").max_buf_size,
            DEFAULT_MAX_BUF_SIZE
        );
        assert_eq!(
            parse(&["--max-buf-size", "131072"]).expect("valid").max_buf_size,
            131_072
        );

        let env = |name: &str| match name {
            "RAMJET_MAX_BUF_SIZE" => Some("32768".to_owned()),
            _ => None,
        };
        let args = Args::parse(Vec::<String>::new(), env).expect("valid");
        assert_eq!(args.max_buf_size, 32_768);
    }

    #[test]
    fn a_buffer_ceiling_under_hypers_minimum_is_raised_to_it() {
        // hyper panics on anything smaller, and a data plane that aborts on a
        // number an operator typed is the worst of the available answers.
        assert_eq!(
            parse(&["--max-buf-size", "1024"]).expect("valid").max_buf_size,
            MIN_MAX_BUF_SIZE
        );
    }

    #[test]
    fn the_pool_default_is_the_one_the_proxy_documents() {
        assert_eq!(
            parse(&[]).expect("valid").upstream_pool_idle,
            ramjet_proxy::DEFAULT_POOL_MAX_IDLE_PER_HOST
        );
        assert_eq!(parse(&[]).expect("valid").worker_threads, None, "one per core");
    }

    #[test]
    fn defaults_match_the_documented_ports() {
        let args = parse(&[]).expect("no arguments is valid");
        assert_eq!(args.http.map(|a| a.port()), Some(8080));
        assert_eq!(args.https.map(|a| a.port()), Some(8443));
        assert_eq!(args.admin.map(|a| a.port()), Some(10254));
        assert!(!args.https_explicit);
    }

    #[test]
    fn both_flag_shapes_are_accepted() {
        let separate = parse(&["--http", "127.0.0.1:9000"]).expect("valid");
        let inline = parse(&["--http=127.0.0.1:9000"]).expect("valid");
        assert_eq!(separate.http, inline.http);
        assert_eq!(
            separate.http.map(|a| a.to_string()),
            Some("127.0.0.1:9000".to_owned())
        );
    }

    #[test]
    fn address_shorthands_bind_every_interface() {
        assert_eq!(
            parse(&["--http", ":9000"]).expect("valid").http,
            Some("0.0.0.0:9000".parse().expect("literal"))
        );
        assert_eq!(
            parse(&["--http", "9000"]).expect("valid").http,
            Some("0.0.0.0:9000".parse().expect("literal"))
        );
    }

    #[test]
    fn ipv6_addresses_survive_the_split_on_equals() {
        // `--https=[::1]:8443` contains no `=`, but the colon-heavy value is
        // the one most likely to be mangled by a naive parser.
        let args = parse(&["--https", "[::1]:8443"]).expect("valid");
        assert_eq!(args.https.map(|a| a.is_ipv6()), Some(true));
    }

    #[test]
    fn disabling_a_listener_is_explicit() {
        let args = parse(&["--no-http", "--no-admin"]).expect("valid");
        assert_eq!(args.http, None);
        assert_eq!(args.admin, None);
        assert!(args.https.is_some());

        let args = parse(&["--no-https"]).expect("valid");
        assert_eq!(args.https, None);
        assert!(
            args.https_explicit,
            "an explicit --no-https must suppress the certificate-based default"
        );
    }

    #[test]
    fn the_engine_defaults_to_hyper() {
        // The engine that has been measured against nginx and carries TLS,
        // HTTP/2 and upgrades is the one you get without asking.
        assert_eq!(parse(&[]).expect("valid").engine, Engine::Hyper);
    }

    #[test]
    fn the_engine_can_be_selected_either_way() {
        assert_eq!(
            parse(&["--engine", "uring"]).expect("valid").engine,
            Engine::Uring
        );
        let env = |name: &str| match name {
            "RAMJET_ENGINE" => Some("uring".to_owned()),
            _ => None,
        };
        let args = Args::parse(std::iter::empty::<OsString>(), env).expect("valid");
        assert_eq!(args.engine, Engine::Uring);
    }

    #[test]
    fn an_unknown_engine_is_an_error_not_a_fallback() {
        // Somebody who typed `--engine io_uring` asked for something specific.
        // Quietly serving on the other engine is the worst possible answer,
        // because it looks like it worked.
        let error = parse(&["--engine", "io_uring"]).expect_err("refused");
        assert!(
            matches!(&error, ArgError::BadValue { option, .. } if option == "--engine"),
            "{error:?}"
        );
        assert!(error.to_string().contains("hyper or uring"), "{error}");
    }

    #[test]
    fn a_flag_beats_the_environment() {
        let env = |name: &str| match name {
            "RAMJET_HTTP" => Some("0.0.0.0:1111".to_owned()),
            "RAMJET_ADMIN" => Some("0.0.0.0:2222".to_owned()),
            _ => None,
        };
        let args = Args::parse(["--http".to_owned(), "0.0.0.0:3333".to_owned()], env)
            .expect("valid");
        assert_eq!(args.http.map(|a| a.port()), Some(3333));
        assert_eq!(
            args.admin.map(|a| a.port()),
            Some(2222),
            "an option with no flag still takes its environment value"
        );
    }

    #[test]
    fn timeouts_are_read_as_seconds() {
        let args = parse(&["--connect-timeout", "2", "--shutdown-grace", "45"]).expect("valid");
        assert_eq!(args.connect_timeout, Duration::from_secs(2));
        assert_eq!(args.shutdown_grace, Duration::from_secs(45));
    }

    #[test]
    fn zero_attempts_is_clamped_to_one() {
        // Zero attempts would mean "never dispatch the request", which is not
        // a configuration anybody means.
        assert_eq!(
            parse(&["--max-connect-attempts", "0"])
                .expect("valid")
                .max_connect_attempts,
            1
        );
    }

    #[test]
    fn bad_input_is_named_precisely() {
        assert!(matches!(
            parse(&["--nope"]),
            Err(ArgError::Unknown(name)) if name == "--nope"
        ));
        assert!(matches!(parse(&["--http"]), Err(ArgError::MissingValue(_))));
        assert!(matches!(
            parse(&["--http", "not-an-address"]),
            Err(ArgError::BadValue { kind: "address", .. })
        ));
        assert!(matches!(
            parse(&["--connect-timeout", "soon"]),
            Err(ArgError::BadValue { .. })
        ));
        assert!(matches!(
            parse(&["routes.yaml"]),
            Err(ArgError::Unexpected(_))
        ));
    }

    #[test]
    fn help_and_version_short_forms_work() {
        assert!(parse(&["-h"]).expect("valid").help);
        assert!(parse(&["--help"]).expect("valid").help);
        assert!(parse(&["-V"]).expect("valid").version);
    }

    #[test]
    fn kubernetes_mode_is_the_default_and_has_the_documented_defaults() {
        let args = parse(&[]).expect("no arguments is valid");
        assert_eq!(args.static_routes, None, "no file means watch Kubernetes");
        assert_eq!(args.ingress_class, "ramjet");
        assert_eq!(args.watch_namespace, None, "None is every namespace");
        assert!(args.update_status, "status writeback is on by default");
    }

    #[test]
    fn the_kubernetes_flags_are_read() {
        let args = parse(&[
            "--ingress-class",
            "public",
            "--watch-namespace",
            "prod",
            "--publish-address",
            "203.0.113.10",
            "--publish-service",
            "ingress/ramjet-lb",
            "--default-backend",
            "kube-system/notfound:8080",
            "--default-tls-secret",
            "ingress/wildcard",
            "--no-status-update",
        ])
        .expect("valid");

        assert_eq!(args.ingress_class, "public");
        assert_eq!(args.watch_namespace.as_deref(), Some("prod"));
        assert_eq!(args.publish_address.as_deref(), Some("203.0.113.10"));
        assert_eq!(args.publish_service.as_deref(), Some("ingress/ramjet-lb"));
        assert_eq!(args.default_tls_secret.as_deref(), Some("ingress/wildcard"));
        assert!(!args.update_status);

        let backend = args.default_backend.expect("a parsed reference");
        assert_eq!(backend.backend_name(), "kube-system/notfound:8080");
    }

    #[test]
    fn a_malformed_default_backend_is_rejected_at_startup() {
        // Not at the first unmatched request, which is the other place a typo
        // here could plausibly show up.
        assert!(matches!(
            parse(&["--default-backend", "notfound"]),
            Err(ArgError::BadValue { ref option, .. }) if option == "--default-backend"
        ));
    }

    #[test]
    fn status_writeback_can_be_switched_off_from_the_environment() {
        let env = |name: &str| match name {
            "RAMJET_UPDATE_STATUS" => Some("false".to_owned()),
            "RAMJET_INGRESS_CLASS" => Some("public".to_owned()),
            _ => None,
        };
        let args = Args::parse(Vec::<String>::new(), env).expect("valid");
        assert!(!args.update_status);
        assert_eq!(args.ingress_class, "public");

        let bad = |name: &str| match name {
            "RAMJET_UPDATE_STATUS" => Some("sometimes".to_owned()),
            _ => None,
        };
        assert!(matches!(
            Args::parse(Vec::<String>::new(), bad),
            Err(ArgError::BadValue { kind: "boolean", .. })
        ));
    }

    #[test]
    fn the_usage_text_mentions_every_option_it_accepts() {
        for option in [
            "--static-routes",
            "--ingress-class",
            "--watch-namespace",
            "--publish-address",
            "--publish-service",
            "--default-backend",
            "--default-tls-secret",
            "--no-status-update",
            "--http",
            "--https",
            "--admin",
            "--no-http",
            "--no-https",
            "--no-admin",
            "--connect-timeout",
            "--response-timeout",
            "--shutdown-grace",
            "--max-connect-attempts",
            "--max-buf-size",
            "--engine",
            "--proxy-protocol",
            "--proxy-protocol-timeout",
            "--history-size",
            "--audit-webhook",
        ] {
            assert!(USAGE.contains(option), "{option} is undocumented");
        }
    }

    #[test]
    fn the_usage_text_documents_every_admin_endpoint() {
        for endpoint in [
            "/metrics",
            "/healthz",
            "/readyz",
            "/admin/generations",
            "/admin/routes",
            "/admin/rollback",
        ] {
            assert!(USAGE.contains(endpoint), "{endpoint} is undocumented");
        }
    }

    #[test]
    fn history_and_audit_take_a_flag_and_an_environment_twin() {
        let args = parse(&[
            "--history-size",
            "25",
            "--audit-webhook",
            "http://audit.svc:8080/hook",
        ])
        .expect("valid");
        assert_eq!(args.history_size, 25);
        assert_eq!(args.audit_webhook.as_deref(), Some("http://audit.svc:8080/hook"));

        let env = |name: &str| match name {
            "RAMJET_HISTORY_SIZE" => Some("3".to_owned()),
            "RAMJET_AUDIT_WEBHOOK" => Some("http://elsewhere:9000/".to_owned()),
            _ => None,
        };
        let args = Args::parse(Vec::<String>::new(), env).expect("valid");
        assert_eq!(args.history_size, 3);
        assert_eq!(args.audit_webhook.as_deref(), Some("http://elsewhere:9000/"));
    }

    #[test]
    fn a_history_of_zero_is_read_as_one() {
        // A ring that keeps nothing could never answer a rollback, and zero is
        // a typo rather than an intent.
        assert_eq!(parse(&["--history-size", "0"]).expect("valid").history_size, 1);
    }

    #[test]
    fn the_history_default_is_the_one_the_proxy_documents() {
        assert_eq!(
            parse(&[]).expect("valid").history_size,
            ramjet_proxy::DEFAULT_HISTORY_SIZE
        );
        assert_eq!(parse(&[]).expect("valid").audit_webhook, None);
    }

    #[test]
    fn the_proxy_protocol_is_off_unless_asked_for() {
        // On by default would mean a fresh deployment refuses every connection
        // that does not carry a header, which is every connection.
        let args = parse(&[]).expect("valid");
        assert!(!args.proxy_protocol);
        assert_eq!(args.proxy_protocol_timeout, Duration::from_secs(5));
    }

    #[test]
    fn the_proxy_protocol_takes_a_flag_and_an_environment_twin() {
        let args = parse(&["--proxy-protocol", "--proxy-protocol-timeout", "2"])
            .expect("valid");
        assert!(args.proxy_protocol);
        assert_eq!(args.proxy_protocol_timeout, Duration::from_secs(2));

        let env = |name: &str| match name {
            "RAMJET_PROXY_PROTOCOL" => Some("true".to_owned()),
            "RAMJET_PROXY_PROTOCOL_TIMEOUT" => Some("9".to_owned()),
            _ => None,
        };
        let args = Args::parse(Vec::<String>::new(), env).expect("valid");
        assert!(args.proxy_protocol);
        assert_eq!(args.proxy_protocol_timeout, Duration::from_secs(9));
    }

    #[test]
    fn a_bad_proxy_protocol_value_is_named_precisely() {
        assert!(matches!(
            parse(&["--proxy-protocol-timeout", "never"]),
            Err(ArgError::BadValue { ref option, .. }) if option == "--proxy-protocol-timeout"
        ));

        let bad = |name: &str| match name {
            "RAMJET_PROXY_PROTOCOL" => Some("perhaps".to_owned()),
            _ => None,
        };
        assert!(matches!(
            Args::parse(Vec::<String>::new(), bad),
            Err(ArgError::BadValue { kind: "boolean", .. })
        ));
    }
}
