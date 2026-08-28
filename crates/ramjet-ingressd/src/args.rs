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
    /// Two options that each parse but cannot both be honoured.
    ///
    /// Separate from [`BadValue`](ArgError::BadValue) because nothing about
    /// either option is wrong on its own: the pair is. Refusing at startup is
    /// the point — the alternative is a flag that was accepted and then
    /// quietly did nothing.
    #[error("{0}")]
    Conflict(String),
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
    /// HTTP/1.1 with TLS. What it refuses is HTTP/2, and so gRPC and HTTP/3;
    /// see `ramjet_engine` for the list.
    ///
    /// Falls back to [`Engine::Hyper`] on a host where the reactor cannot
    /// start, which in practice means `io_uring_setup` blocked by seccomp —
    /// Docker's default profile does exactly that.
    Uring,
    /// The same engine, and a refusal to start rather than a fallback.
    ///
    /// For a deployment that chose this engine on purpose and would rather
    /// crash-loop visibly than serve on the other one. A pod that silently
    /// fell back has none of the properties its operator selected it for and
    /// no obvious sign that anything happened; `kubectl get pods` showing
    /// `CrashLoopBackOff` is the louder failure, and sometimes that is what is
    /// wanted.
    UringStrict,
}

impl Engine {
    /// The name this engine is selected by.
    pub fn as_str(self) -> &'static str {
        match self {
            Engine::Hyper => "hyper",
            Engine::Uring => "uring",
            Engine::UringStrict => "uring-strict",
        }
    }

    /// Whether this selection runs the reactor engine.
    pub fn is_uring(self) -> bool {
        matches!(self, Engine::Uring | Engine::UringStrict)
    }

    /// Whether a reactor that will not start is fatal.
    pub fn is_strict(self) -> bool {
        self == Engine::UringStrict
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
    /// Largest request body copied to a mirror backend.
    pub mirror_max_body: usize,
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
    /// Serve HTTP/3 over QUIC on the TLS listener's port, in UDP.
    ///
    /// Experimental. It adds a UDP socket, one serving thread, and an
    /// `alt-svc` header on TLS responses; with it off none of those exist.
    pub http3: bool,
    /// Serve HTTP/2 on `--engine uring` by handing those connections to the
    /// hyper engine, rather than not offering HTTP/2 at all.
    ///
    /// On by default where the uring engine runs, and ignored otherwise. Off
    /// means the TLS listener advertises `http/1.1` alone and an HTTP/2 client
    /// negotiates HTTP/1.1 with it — which works, and is what every browser
    /// falls back to, but costs multiplexing. Turning it off also means the
    /// second engine's threads and upstream pools are never started.
    pub h2_dispatch: bool,
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
            mirror_max_body: ramjet_proxy::DEFAULT_MIRROR_MAX_BODY,
            worker_threads: None,
            engine: Engine::Hyper,
            proxy_protocol: false,
            // Long enough that a load balancer under load still fits, short
            // enough that holding the connection is not a cheap way to occupy
            // a task and a file descriptor. The header is the first thing a
            // sender writes, so a sender that has not finished it in five
            // seconds is not going to.
            proxy_protocol_timeout: Duration::from_secs(5),
            // Experimental, and a UDP port nobody asked for is a UDP port
            // nobody reviewed.
            http3: false,
            // On, because the alternative is an engine that quietly does not
            // speak HTTP/2 to clients that asked for it.
            h2_dispatch: true,
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

HTTP/3 (EXPERIMENTAL):
    --http3                   Also serve HTTP/3 over QUIC, on the --https port
                              in UDP, and advertise it with alt-svc.

    Off by default, and off costs nothing: no UDP socket is bound, no thread is
    started, and no header is added. On, the TLS listener's responses carry
    `alt-svc: h3=\":<port>\"; ma=86400`, which is how a client learns to retry
    over QUIC — so the same port number has to be reachable over UDP as well as
    TCP, all the way through whatever is in front of this process. An AWS NLB
    forwards UDP; most other cloud load balancers do not, or do only on a
    separate listener. See deploy/README.md.

    It shares the TLS listener's certificates, exactly: the same SNI resolution,
    the same store, the same rotation. What it does not share is the thread
    budget — the QUIC endpoint runs on one dedicated runtime rather than one per
    core, because sharding a UDP port by 4-tuple breaks connection migration —
    so it is not the path to put peak traffic on yet.

    No 0-RTT, no QUIC upstream (upstream stays HTTP/1.1), no protocol upgrades,
    and no PROXY protocol, which has no UDP form. Requires --https, and refuses
    --engine uring, which speaks no HTTP/2 and so no HTTP/3 either.

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
    --engine <NAME>           Data plane: hyper, uring or uring-strict
                                                             [default: hyper]
    --no-h2-dispatch          On --engine uring, stop offering HTTP/2 rather
                              than handing those connections to the hyper
                              engine
    --worker-threads <N>      Serving runtimes, one per thread
                                              [default: one per available core]
    --max-buf-size <BYTES>    Ceiling on one client connection's HTTP/1 read and
                              write buffers, and so on the request head this
                              replica will accept    [default: 65536, min 8192]

    `uring` serves HTTP/1.1 on the ramjet reactor — io_uring on Linux, kqueue
    elsewhere — to find out whether batched submission gets under the syscall
    floor the hyper engine measured. It terminates TLS, carries WebSocket and
    other upgrades, reads the PROXY protocol, and runs in Kubernetes mode, all
    against the same certificate store and route table the hyper engine reads.
    What it does not speak is HTTP/2 in any form, and so neither gRPC nor
    HTTP/3; it refuses those with a status and an explanation rather than
    silently doing something else. Everything about routing, load balancing,
    canaries, mirroring, headers and /metrics is the same on both, and a
    differential test drives the two with identical traffic to keep it that
    way.

    `uring` falls back to `hyper` on a host where the reactor will not start,
    logging the reason. In practice that means io_uring_setup blocked by
    seccomp, which Docker's default profile does. `uring-strict` refuses to
    start instead, for a deployment that would rather crash-loop visibly than
    serve on an engine it did not choose. On macOS and BSD the reactor uses
    kqueue and this never comes up.

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

TRAFFIC MIRRORING:
    --mirror-max-body <BYTES> Largest request body copied to a mirror backend
                                                         [default: 262144]

    A route whose Ingress carries `ramjet.dev/mirror-backend:
    <namespace>/<service>:<port>` gets a second, fire-and-forget copy of each
    sampled request sent to that backend, and the answer is thrown away.
    `ramjet.dev/mirror-percent` (0-100, default 100) samples it down, and
    `ramjet.dev/mirror-host` overrides the Host header on the copy — which a
    shadow deployment answering to a different name needs, and which stops a
    copy being routed back to production by whatever is in front of it. Copies
    carry `X-Mirrored-By: ramjet-ingress`.

    Mirroring cannot slow the request the client is waiting for. Nothing is
    awaited on the request path; each serving runtime has a bounded queue and
    drops on overflow; responses are drained and discarded; a mirror backend
    that is down, refusing, or wedged produces a number on /metrics and nothing
    else. The one real cost is the body: a request with one is read up to
    --mirror-max-body so both copies can have it, and a body over the cap is
    forwarded whole to the real backend with the mirror skipped. A request with
    no body -- every GET, HEAD, OPTIONS and DELETE -- is mirrored with no
    buffering at all and keeps its endpoint failover.

    Watch ramjet_mirrored_total, ramjet_mirror_dropped_total (queue full),
    ramjet_mirror_skipped_total (body over the cap), and
    ramjet_mirror_failures_total (the backend refused or did not answer).

CANARY AUTO-PROMOTION:
    No flags. It is annotation-driven, per canary Ingress, and off unless
    `ramjet.dev/auto-promote: \"true\"` is set on one:

      auto-promote-interval             [default: 60s]  observation window
      auto-promote-steps       [default: 5,10,25,50,100]  weights to walk
      auto-promote-max-5xx-percent      [default: 1]    canary error budget
      auto-promote-max-latency-factor   [default: 1.5]  canary mean vs stable
      auto-promote-min-requests         [default: 50]   per window, per side

    Every interval the daemon takes that window's request, 5xx and latency
    counters for the canary and stable sides of the route separately. If either
    side saw fewer than min-requests it holds -- no traffic is not failure.
    Otherwise it advances the canary Ingress's canary-weight to the next step,
    or, on a breach, pulls the weight to 0 and writes `auto-promote: \"false\"`
    plus `auto-promote-status: \"rolled-back: <reason>\"`. A rollback is one-way:
    re-arming is a human decision. Reaching the last step writes
    `auto-promote-status: promoted` and stops; swapping the production Ingress's
    backend stays a human edit, deliberately.

    Decisions are logged with their numbers on the `audit` target, written as
    Events on the IngressClass (CanaryStepped, CanaryPromoted, CanaryRolledBack)
    and POSTed to --audit-webhook. Everything is paused while a rollback pin is
    held. RBAC: `networking.k8s.io` / `ingresses` / `patch`, which the chart's
    ClusterRole has.

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
RAMJET_MAX_BUF_SIZE, RAMJET_MIRROR_MAX_BODY, RAMJET_WORKER_THREADS,
RAMJET_SHUTDOWN_GRACE,
RAMJET_ENGINE, RAMJET_PROXY_PROTOCOL, RAMJET_PROXY_PROTOCOL_TIMEOUT,
RAMJET_HTTP3).
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
                "--no-h2-dispatch" => args.h2_dispatch = false,
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
                "--mirror-max-body" => args.mirror_max_body = number(&name, &value()?)?,
                "--worker-threads" => {
                    args.worker_threads = Some(number(&name, &value()?)?.max(1));
                }
                "--engine" => args.engine = engine(&name, &value()?)?,
                "--http3" => args.http3 = true,
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

        // Combinations that each parse and cannot both be honoured. Checked
        // after everything is known, because a flag beats its environment twin
        // and the conflict is a property of the result rather than of the order
        // things arrived in.
        //
        // Skipped for --help and --version, which are requests for text and
        // should answer even when the rest of the command line is wrong.
        if !args.help && !args.version {
            args.check_conflicts()?;
        }
        Ok(args)
    }

    /// Options that are individually valid and jointly impossible.
    fn check_conflicts(&self) -> Result<(), ArgError> {
        if self.http3 && self.engine.is_uring() {
            // Refused rather than ignored. HTTP/3 is HTTP/2's semantics over
            // QUIC, and this engine speaks neither; honouring the flag would
            // mean binding nothing and advertising nothing, and the operator
            // would find out from a client that quietly stayed on TCP forever.
            return Err(ArgError::Conflict(
                "--http3 is not implemented on --engine uring, which speaks no \
                 HTTP/2 and so no HTTP/3; use --engine hyper"
                    .to_owned(),
            ));
        }
        if self.http3 && self.https.is_none() {
            // HTTP/3 is served on the TLS listener's port in UDP, and the
            // `alt-svc` header that advertises it names that port. With no TLS
            // listener there is no port to take, nothing to advertise it on,
            // and no client that would ever look.
            return Err(ArgError::Conflict(
                "--http3 serves on the --https port in UDP, and --no-https \
                 leaves it none; give --https a port back or drop --http3"
                    .to_owned(),
            ));
        }
        Ok(())
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
        if let Some(value) = env("RAMJET_MIRROR_MAX_BODY") {
            args.mirror_max_body = number("RAMJET_MIRROR_MAX_BODY", &value)?;
        }
        if let Some(value) = env("RAMJET_ENGINE") {
            args.engine = engine("RAMJET_ENGINE", &value)?;
        }
        if let Some(value) = env("RAMJET_HTTP3") {
            args.http3 = boolean("RAMJET_HTTP3", &value)?;
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
        "uring-strict" => Ok(Engine::UringStrict),
        _ => Err(ArgError::BadValue {
            option: option.to_owned(),
            value: value.to_owned(),
            kind: "engine (hyper, uring or uring-strict)",
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
        assert!(error.to_string().contains("hyper, uring or uring-strict"), "{error}");
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
            "--mirror-max-body",
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
    fn the_mirror_body_cap_takes_a_flag_and_an_environment_twin() {
        assert_eq!(
            parse(&[]).expect("valid").mirror_max_body,
            ramjet_proxy::DEFAULT_MIRROR_MAX_BODY
        );
        assert_eq!(
            parse(&["--mirror-max-body", "1024"])
                .expect("valid")
                .mirror_max_body,
            1024
        );

        let env = |name: &str| match name {
            "RAMJET_MIRROR_MAX_BODY" => Some("4096".to_owned()),
            _ => None,
        };
        let args = Args::parse(Vec::<String>::new(), env).expect("valid");
        assert_eq!(args.mirror_max_body, 4096);
    }

    #[test]
    fn a_mirror_body_cap_of_zero_is_a_legal_way_to_mirror_only_empty_bodies() {
        // Not clamped up, unlike the buffer ceiling: zero means "never buffer",
        // which still mirrors every GET and is a reasonable thing to ask for on
        // a route that carries large uploads.
        assert_eq!(
            parse(&["--mirror-max-body", "0"])
                .expect("valid")
                .mirror_max_body,
            0
        );
    }

    #[test]
    fn the_usage_text_documents_both_new_features() {
        for phrase in [
            "ramjet.dev/mirror-backend",
            "X-Mirrored-By",
            "ramjet_mirror_dropped_total",
            "auto-promote-steps",
            "auto-promote-max-latency-factor",
            "rolled-back",
        ] {
            assert!(USAGE.contains(phrase), "{phrase} is undocumented");
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
    fn http3_is_off_unless_asked_for() {
        // A UDP socket a deployment did not ask for is one nobody firewalled.
        assert!(!parse(&[]).expect("valid").http3);
    }

    #[test]
    fn http3_takes_a_flag_and_an_environment_twin() {
        assert!(parse(&["--http3"]).expect("valid").http3);

        let env = |name: &str| match name {
            "RAMJET_HTTP3" => Some("true".to_owned()),
            _ => None,
        };
        assert!(Args::parse(Vec::<String>::new(), env).expect("valid").http3);
    }

    #[test]
    fn http3_and_the_uring_engine_is_refused_at_startup() {
        // The uring engine has neither TLS nor QUIC. Accepting the flag and
        // doing nothing would leave an operator waiting for h3 traffic that
        // was never going to arrive.
        let error = parse(&["--http3", "--engine", "uring", "--static-routes", "r.yaml"])
            .expect_err("refused");
        let message = error.to_string();
        assert!(
            matches!(error, ArgError::Conflict(_)),
            "expected a conflict, got {message}"
        );
        assert!(message.contains("--engine hyper"), "{message} names no way out");
    }

    #[test]
    fn http3_without_a_tls_listener_is_refused_at_startup() {
        // h3 is served on the --https port in UDP and advertised by alt-svc
        // naming that port. With no TLS listener there is no port and no
        // response to advertise it on.
        let error = parse(&["--http3", "--no-https"]).expect_err("refused");
        assert!(matches!(error, ArgError::Conflict(_)), "{error}");
    }

    #[test]
    fn help_still_prints_over_a_refused_combination() {
        // `--help` is a request for text. Answering it with a usage error
        // about two other flags is the least useful moment to be strict.
        let args = parse(&["--http3", "--no-https", "--help"]).expect("help wins");
        assert!(args.help);
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
