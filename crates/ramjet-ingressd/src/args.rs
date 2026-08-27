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

use ramjet_proxy::{DEFAULT_ADMIN_PORT, DEFAULT_HTTP_PORT, DEFAULT_HTTPS_PORT};

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

/// Everything the daemon was asked to do.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Args {
    /// Print usage and exit.
    pub help: bool,
    /// Print the version and exit.
    pub version: bool,
    /// Dev mode: serve the routes in this file instead of watching Kubernetes.
    pub static_routes: Option<PathBuf>,
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
}

impl Default for Args {
    fn default() -> Self {
        let all = |port: u16| SocketAddr::from(([0, 0, 0, 0], port));
        Args {
            help: false,
            version: false,
            static_routes: None,
            http: Some(all(DEFAULT_HTTP_PORT)),
            https: Some(all(DEFAULT_HTTPS_PORT)),
            admin: Some(all(DEFAULT_ADMIN_PORT)),
            https_explicit: false,
            connect_timeout: Duration::from_secs(5),
            response_timeout: Duration::from_secs(60),
            shutdown_grace: Duration::from_secs(30),
            max_connect_attempts: 3,
        }
    }
}

/// What `--help` prints.
pub const USAGE: &str = "\
ramjet-ingressd — the ramjet-ingress data plane

USAGE:
    ramjet-ingressd [OPTIONS]

DEV MODE:
    --static-routes <FILE>    Serve the hosts, paths, backends, and certificates
                              described in FILE and never talk to Kubernetes.
                              Required until the controller phase lands.

LISTENERS:
    --http <ADDR>             Plaintext listener        [default: 0.0.0.0:8080]
    --https <ADDR>            TLS listener              [default: 0.0.0.0:8443]
    --admin <ADDR>            Metrics and probes        [default: 0.0.0.0:10254]
    --no-http, --no-https, --no-admin
                              Disable a listener.

    An address may be `host:port`, `:port`, or a bare port. Without an explicit
    --https or --no-https, the TLS listener is skipped when the configuration
    declares no certificates.

UPSTREAMS:
    --connect-timeout <SECS>      TCP connect bound          [default: 5]
    --response-timeout <SECS>     Response header bound      [default: 60]
    --max-connect-attempts <N>    Endpoints tried on a connect failure
                                                             [default: 3]

SHUTDOWN:
    --shutdown-grace <SECS>   In-flight requests get this long after SIGTERM
                                                             [default: 30]

OTHER:
    -h, --help                Print this help
    -V, --version             Print the version

Every option has an environment twin (RAMJET_STATIC_ROUTES, RAMJET_HTTP,
RAMJET_HTTPS, RAMJET_ADMIN, RAMJET_CONNECT_TIMEOUT, RAMJET_RESPONSE_TIMEOUT,
RAMJET_MAX_CONNECT_ATTEMPTS, RAMJET_SHUTDOWN_GRACE). A flag always beats the
environment.

ADMIN ENDPOINTS:
    /metrics    Prometheus text exposition
    /healthz    Liveness: 200 whenever the process is answering
    /readyz     Readiness: 200 once a route table has been published
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
    fn the_usage_text_mentions_every_option_it_accepts() {
        for option in [
            "--static-routes",
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
        ] {
            assert!(USAGE.contains(option), "{option} is undocumented");
        }
    }
}
