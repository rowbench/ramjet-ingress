//! Command-line parsing.
//!
//! Hand-rolled, for the reason `ramjet-ingressd` gives for the same choice: the
//! binary has six options and `clap` is a large dependency to format a help
//! text with. The parser is a hundred lines and every option it accepts is
//! visible in one place.
//!
//! Unlike `ramjet-ingressd`, there are no environment-variable twins. That
//! daemon has them because a container is configured by environment; this is a
//! program somebody runs by typing its name, and an invisible `RAMJET_TOP_URL`
//! left over in a shell profile pointing at the wrong cluster is a worse
//! failure than typing the URL again.
//!
//! `--token-file` is the one exception, and the exception proves the rule rather
//! than breaking it. A stale `RAMJET_TOP_URL` reads the wrong cluster and looks
//! like it worked; a stale `RAMJET_TOP_TOKEN_FILE` produces a 401 on the one
//! keystroke that uses it, which is a failure that announces itself. What it
//! buys is that the token path lives next to the `kubectl port-forward` in
//! whatever script set the port up, rather than being retyped per invocation.
//!
//! The URL may be given positionally, because `ramjet-top 10.0.0.5:10254` is
//! what a person's hands do.

use std::ffi::OsString;
use std::path::PathBuf;
use std::time::Duration;

use crate::client::DEFAULT_ADMIN_URL;

/// The default poll interval.
const DEFAULT_INTERVAL: Duration = Duration::from_secs(1);

/// The default per-request deadline.
///
/// Deliberately longer than the default interval: a poll that is slower than
/// the tick should be reported as a slow poll, not cancelled and reported as an
/// unreachable server.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(3);

/// The shortest interval this program will accept.
///
/// Below about a tenth of a second the three requests per poll stop being
/// negligible load on the admin port, and the numbers stop being readable
/// anyway.
const MIN_INTERVAL: Duration = Duration::from_millis(100);

/// Why the command line could not be understood.
#[derive(Debug, thiserror::Error)]
pub enum ArgError {
    /// An option this binary does not have.
    #[error("unknown option `{0}` (try --help)")]
    Unknown(String),
    /// An option that needs a value did not get one.
    #[error("option `{0}` needs a value")]
    MissingValue(String),
    /// More than one positional argument.
    #[error("unexpected argument `{0}`; the URL may only be given once")]
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
        /// What was expected.
        kind: &'static str,
    },
    /// A value that parsed but is out of range.
    #[error("{0}")]
    OutOfRange(String),
}

/// What the parser produced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Parsed {
    /// Run with these options.
    Run(Box<Options>),
    /// Print the help text and exit successfully.
    Help,
    /// Print the version and exit successfully.
    Version,
}

/// Everything the command line configures.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Options {
    /// The admin endpoint, not yet normalized.
    pub url: String,
    /// How often to poll.
    pub interval: Duration,
    /// The per-request deadline.
    pub timeout: Duration,
    /// Print one poll and exit, instead of drawing.
    pub once: bool,
    /// With `once`, print JSON instead of a table.
    pub json: bool,
    /// Disable pin and unpin.
    pub read_only: bool,
    /// File holding the bearer token `pin` and `unpin` must send.
    ///
    /// Only those two: everything this program polls is a `GET`, which the admin
    /// listener never gates. A token configured here and not needed costs one
    /// header on a keystroke nobody presses.
    pub token_file: Option<PathBuf>,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            url: DEFAULT_ADMIN_URL.to_string(),
            interval: DEFAULT_INTERVAL,
            timeout: DEFAULT_TIMEOUT,
            once: false,
            json: false,
            read_only: false,
            token_file: None,
        }
    }
}

/// The help text.
pub fn help() -> String {
    format!(
        "\
ramjet-top {version} — a live view of a running ramjet-ingress

USAGE:
    ramjet-top [URL] [OPTIONS]

    URL defaults to {DEFAULT_ADMIN_URL}. A bare host:port is accepted
    and given an http:// scheme.

OPTIONS:
        --url <URL>          the ingressd admin endpoint
    -i, --interval <TIME>    how often to poll [default: 1s]
        --timeout <TIME>     per-request deadline [default: 3s]
        --once               print one poll as text and exit, for scripts and CI
        --json               with --once, print the merged snapshot as JSON
                             (implies --once)
        --read-only          disable pin and unpin
        --token-file <PATH>  bearer token for pin and unpin, when the daemon was
                             started with --admin-token-file
                             [env: RAMJET_TOP_TOKEN_FILE]
    -h, --help               print this
    -V, --version            print the version

    TIME accepts `500ms`, `2s`, `1m`, or a bare number meaning seconds.

    Reading needs no token: everything this program polls is a GET, and the
    admin listener never gates those.

KEYS (interactive):
    q, Ctrl-C   quit                    Tab         routes / generations
    r e l h     sort; again reverses    /           filter, Esc clears
    j k, arrows move                    Enter       expand a generation diff
    Home End    ends of the list        g           poll now
    p           pin (guarded)           u           unpin (guarded)
",
        version = env!("CARGO_PKG_VERSION"),
    )
}

/// Parses a duration: `500ms`, `2s`, `1m`, or a bare number of seconds.
///
/// A bare number means seconds because `--interval 5` obviously means five
/// seconds, and reading it as five milliseconds would quietly hammer the admin
/// port two hundred times a second.
fn parse_duration(option: &str, value: &str) -> Result<Duration, ArgError> {
    let bad = || ArgError::BadValue {
        option: option.to_string(),
        value: value.to_string(),
        kind: "duration",
    };

    let trimmed = value.trim();
    let (number, multiplier) = if let Some(n) = trimmed.strip_suffix("ms") {
        (n, 1.0)
    } else if let Some(n) = trimmed.strip_suffix('s') {
        (n, 1_000.0)
    } else if let Some(n) = trimmed.strip_suffix('m') {
        (n, 60_000.0)
    } else {
        (trimmed, 1_000.0)
    };

    let parsed: f64 = number.trim().parse().map_err(|_| bad())?;
    if !parsed.is_finite() || parsed < 0.0 {
        return Err(bad());
    }
    let millis = parsed * multiplier;
    // `u64::MAX` milliseconds is 584 million years; anything past it is a typo
    // rather than an intention.
    if millis > u64::MAX as f64 {
        return Err(bad());
    }
    Ok(Duration::from_millis(millis as u64))
}

/// Fills in from the environment anything the command line left unset.
///
/// Kept out of [`parse`] so that the parser stays a pure function of its
/// arguments: a test that had to own the process environment to check an option
/// would be a test that cannot run alongside another one.
pub fn apply_env<E>(options: &mut Options, env: E)
where
    E: Fn(&str) -> Option<String>,
{
    if options.token_file.is_none() {
        if let Some(path) = env("RAMJET_TOP_TOKEN_FILE").filter(|p| !p.trim().is_empty()) {
            options.token_file = Some(PathBuf::from(path));
        }
    }
}

/// Parses an argument list that does *not* include the program name.
pub fn parse<I>(args: I) -> Result<Parsed, ArgError>
where
    I: IntoIterator<Item = OsString>,
{
    let mut options = Options::default();
    let mut url_from_positional = None;
    let mut url_from_flag = None;

    let mut iter = args.into_iter();
    while let Some(raw) = iter.next() {
        let arg = raw
            .clone()
            .into_string()
            .map_err(|_| ArgError::NotUtf8(raw))?;

        // One helper for every option that takes a value, so "the option was
        // last on the line" is one error rather than six chances to forget it.
        let mut value = |name: &str| -> Result<String, ArgError> {
            iter.next()
                .ok_or_else(|| ArgError::MissingValue(name.to_string()))?
                .into_string()
                .map_err(ArgError::NotUtf8)
        };

        match arg.as_str() {
            "-h" | "--help" => return Ok(Parsed::Help),
            "-V" | "--version" => return Ok(Parsed::Version),
            "--url" => url_from_flag = Some(value("--url")?),
            "-i" | "--interval" => {
                let raw = value("--interval")?;
                options.interval = parse_duration("--interval", &raw)?;
            }
            "--timeout" => {
                let raw = value("--timeout")?;
                options.timeout = parse_duration("--timeout", &raw)?;
            }
            "--once" => options.once = true,
            "--json" => {
                options.json = true;
                // `--json` on its own is unambiguous: there is no streaming
                // JSON mode, so it can only mean one snapshot. Requiring
                // `--once` alongside it would be a rule with nothing behind it.
                options.once = true;
            }
            "--read-only" => options.read_only = true,
            "--token-file" => options.token_file = Some(PathBuf::from(value("--token-file")?)),
            other if other.starts_with('-') && other != "-" => {
                return Err(ArgError::Unknown(other.to_string()))
            }
            positional => {
                if url_from_positional.is_some() {
                    return Err(ArgError::Unexpected(positional.to_string()));
                }
                url_from_positional = Some(positional.to_string());
            }
        }
    }

    // An explicit `--url` wins over a positional, so a shell alias that pins
    // one cannot be silently overridden by a stray argument.
    if let Some(url) = url_from_flag.or(url_from_positional) {
        options.url = url;
    }

    if options.interval < MIN_INTERVAL {
        return Err(ArgError::OutOfRange(format!(
            "--interval must be at least {}ms; polling faster is load on the \
             admin port and unreadable on screen",
            MIN_INTERVAL.as_millis()
        )));
    }
    if options.timeout.is_zero() {
        return Err(ArgError::OutOfRange(
            "--timeout must be greater than zero".to_string(),
        ));
    }

    Ok(Parsed::Run(Box::new(options)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_args(args: &[&str]) -> Result<Parsed, ArgError> {
        parse(args.iter().map(OsString::from))
    }

    fn options(args: &[&str]) -> Options {
        match parse_args(args).expect("valid arguments") {
            Parsed::Run(options) => *options,
            other => panic!("expected options, got {other:?}"),
        }
    }

    #[test]
    fn no_arguments_polls_the_conventional_admin_port_once_a_second() {
        let options = options(&[]);
        assert_eq!(options.url, DEFAULT_ADMIN_URL);
        assert_eq!(options.interval, Duration::from_secs(1));
        assert_eq!(options.timeout, Duration::from_secs(3));
        assert!(!options.once);
        assert!(!options.json);
        assert!(!options.read_only);
    }

    #[test]
    fn the_token_file_comes_from_a_flag_or_the_environment() {
        assert_eq!(options(&[]).token_file, None);
        assert_eq!(
            options(&["--token-file", "/etc/ramjet/token"]).token_file,
            Some(PathBuf::from("/etc/ramjet/token"))
        );

        let env = |name: &str| match name {
            "RAMJET_TOP_TOKEN_FILE" => Some("/from/env".to_owned()),
            _ => None,
        };
        let mut from_env = options(&[]);
        apply_env(&mut from_env, env);
        assert_eq!(from_env.token_file, Some(PathBuf::from("/from/env")));

        // The flag wins, and an empty variable is not a path.
        let mut from_flag = options(&["--token-file", "/from/flag"]);
        apply_env(&mut from_flag, env);
        assert_eq!(from_flag.token_file, Some(PathBuf::from("/from/flag")));

        let mut blank = options(&[]);
        apply_env(&mut blank, |_| Some("   ".to_owned()));
        assert_eq!(blank.token_file, None, "a blank variable is not a path");
    }

    #[test]
    fn the_help_text_says_reading_needs_no_token() {
        // The question somebody has after the daemon grew --admin-token-file is
        // whether this program still works without one. It does, for everything
        // but the two keys that change what is served.
        let help = help();
        assert!(help.contains("--token-file"), "{help}");
        assert!(help.contains("Reading needs no token"), "{help}");
    }

    #[test]
    fn the_url_can_be_positional() {
        assert_eq!(options(&["10.0.0.5:10254"]).url, "10.0.0.5:10254");
    }

    #[test]
    fn the_url_can_be_a_flag() {
        assert_eq!(options(&["--url", "10.0.0.5:10254"]).url, "10.0.0.5:10254");
    }

    #[test]
    fn an_explicit_url_flag_beats_a_positional() {
        let options = options(&["positional:1", "--url", "flag:2"]);
        assert_eq!(options.url, "flag:2");
    }

    #[test]
    fn a_second_positional_is_refused_rather_than_ignored() {
        let err = parse_args(&["one:1", "two:2"]).expect_err("refused");
        assert!(matches!(err, ArgError::Unexpected(_)), "{err}");
    }

    #[test]
    fn durations_accept_the_units_the_help_text_lists() {
        assert_eq!(
            options(&["--interval", "500ms"]).interval,
            Duration::from_millis(500)
        );
        assert_eq!(
            options(&["--interval", "2s"]).interval,
            Duration::from_secs(2)
        );
        assert_eq!(
            options(&["--interval", "1m"]).interval,
            Duration::from_secs(60)
        );
        assert_eq!(
            options(&["--interval", "5"]).interval,
            Duration::from_secs(5),
            "a bare number is seconds"
        );
        assert_eq!(
            options(&["--interval", "2.5s"]).interval,
            Duration::from_millis(2500)
        );
    }

    #[test]
    fn the_short_interval_flag_works() {
        assert_eq!(options(&["-i", "250ms"]).interval, Duration::from_millis(250));
    }

    #[test]
    fn an_unparseable_duration_names_the_option_and_the_value() {
        let err = parse_args(&["--interval", "soon"]).expect_err("refused");
        let message = err.to_string();
        assert!(message.contains("--interval"), "{message}");
        assert!(message.contains("soon"), "{message}");
        assert!(message.contains("duration"), "{message}");
    }

    #[test]
    fn an_interval_too_short_to_be_useful_is_refused_with_a_reason() {
        let err = parse_args(&["--interval", "1ms"]).expect_err("refused");
        assert!(matches!(err, ArgError::OutOfRange(_)), "{err}");
        assert!(err.to_string().contains("100ms"), "{err}");
    }

    #[test]
    fn a_zero_timeout_is_refused() {
        assert!(parse_args(&["--timeout", "0"]).is_err());
    }

    #[test]
    fn a_negative_duration_is_refused_rather_than_wrapping() {
        assert!(parse_args(&["--interval", "-5s"]).is_err());
    }

    #[test]
    fn json_implies_once_because_there_is_no_streaming_json_mode() {
        let options = options(&["--json"]);
        assert!(options.json);
        assert!(options.once, "otherwise --json alone would draw a TUI");
    }

    #[test]
    fn once_without_json_is_the_text_table() {
        let options = options(&["--once"]);
        assert!(options.once);
        assert!(!options.json);
    }

    #[test]
    fn read_only_is_a_flag() {
        assert!(options(&["--read-only"]).read_only);
    }

    #[test]
    fn flags_compose() {
        let options = options(&["--once", "--read-only", "-i", "2s", "host:1"]);
        assert!(options.once);
        assert!(options.read_only);
        assert_eq!(options.interval, Duration::from_secs(2));
        assert_eq!(options.url, "host:1");
    }

    #[test]
    fn help_and_version_short_circuit() {
        for flag in ["-h", "--help"] {
            assert_eq!(parse_args(&[flag]).expect("valid"), Parsed::Help);
        }
        for flag in ["-V", "--version"] {
            assert_eq!(parse_args(&[flag]).expect("valid"), Parsed::Version);
        }
        assert_eq!(
            parse_args(&["--interval", "1ms", "--help"]).expect("valid"),
            Parsed::Help,
            "help wins before validation, so a wrong flag can still be looked up"
        );
    }

    #[test]
    fn an_unknown_option_is_named() {
        let err = parse_args(&["--colour"]).expect_err("refused");
        assert!(matches!(err, ArgError::Unknown(_)), "{err}");
        assert!(err.to_string().contains("--colour"), "{err}");
        assert!(err.to_string().contains("--help"), "{err}");
    }

    #[test]
    fn an_option_missing_its_value_is_named() {
        for flag in ["--url", "--interval", "--timeout"] {
            let err = parse_args(&[flag]).expect_err("refused");
            assert!(matches!(err, ArgError::MissingValue(_)), "{flag}: {err}");
            assert!(err.to_string().contains(flag), "{err}");
        }
    }

    #[test]
    fn the_help_text_documents_every_option_the_parser_accepts() {
        let text = help();
        for flag in [
            "--url",
            "--interval",
            "--timeout",
            "--once",
            "--json",
            "--read-only",
            "--help",
            "--version",
        ] {
            assert!(text.contains(flag), "help does not mention {flag}");
        }
        assert!(text.contains(DEFAULT_ADMIN_URL), "help omits the default URL");
        assert!(text.contains(env!("CARGO_PKG_VERSION")));
    }

    #[test]
    fn the_help_text_documents_every_key_the_ui_binds() {
        let text = help();
        for key in ["q", "Tab", "Enter", "/", "p", "u", "g"] {
            assert!(text.contains(key), "help does not mention `{key}`");
        }
    }

    #[test]
    fn non_utf8_arguments_are_refused_rather_than_lossily_converted() {
        #[cfg(unix)]
        {
            use std::os::unix::ffi::OsStringExt;
            let bad = OsString::from_vec(vec![0x66, 0x80, 0x6f]);
            let err = parse(vec![bad]).expect_err("refused");
            assert!(matches!(err, ArgError::NotUtf8(_)), "{err}");
        }
    }
}
