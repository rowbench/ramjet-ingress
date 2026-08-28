//! The annotation vocabulary we understand.
//!
//! # Two prefixes, and the rule for choosing
//!
//! Anything ingress-nginx already spells gets the `nginx.ingress.kubernetes.io`
//! prefix, on purpose. Compatibility is the whole point: an existing cluster
//! should be able to swap controllers without rewriting every Ingress, so we
//! speak the annotations people already have — the canary family below is
//! transcribed from theirs, semantics included.
//!
//! Anything ingress-nginx has no equivalent for gets `ramjet.dev`. Traffic
//! mirroring and canary auto-promotion are both in that group: there is no
//! established spelling to be compatible with, and borrowing their prefix for a
//! key they do not define would be a claim about portability that is not true.
//! An operator reading `ramjet.dev/...` on an Ingress knows immediately that
//! moving back to ingress-nginx loses that behaviour.

use std::collections::BTreeMap;
use std::time::Duration;

/// Marks an Ingress as the canary half of a pair.
pub const ANNOTATION_CANARY: &str = "nginx.ingress.kubernetes.io/canary";
/// Share of traffic diverted to the canary, out of
/// [`ANNOTATION_CANARY_WEIGHT_TOTAL`].
pub const ANNOTATION_CANARY_WEIGHT: &str = "nginx.ingress.kubernetes.io/canary-weight";
/// Denominator for [`ANNOTATION_CANARY_WEIGHT`]; defaults to 100.
pub const ANNOTATION_CANARY_WEIGHT_TOTAL: &str =
    "nginx.ingress.kubernetes.io/canary-weight-total";
/// Request header that can force a canary decision.
pub const ANNOTATION_CANARY_BY_HEADER: &str = "nginx.ingress.kubernetes.io/canary-by-header";
/// Exact value [`ANNOTATION_CANARY_BY_HEADER`] must carry to divert.
pub const ANNOTATION_CANARY_BY_HEADER_VALUE: &str =
    "nginx.ingress.kubernetes.io/canary-by-header-value";
/// Regex [`ANNOTATION_CANARY_BY_HEADER`] must match to divert. Mutually
/// exclusive with [`ANNOTATION_CANARY_BY_HEADER_VALUE`].
pub const ANNOTATION_CANARY_BY_HEADER_PATTERN: &str =
    "nginx.ingress.kubernetes.io/canary-by-header-pattern";
/// Cookie that can force a canary decision.
pub const ANNOTATION_CANARY_BY_COOKIE: &str = "nginx.ingress.kubernetes.io/canary-by-cookie";

/// Protocol the data plane speaks to this Ingress's backends.
///
/// Transcribed from ingress-nginx, values included; see
/// [`BackendProtocolAnnotation`] for which of theirs are honoured.
pub const ANNOTATION_BACKEND_PROTOCOL: &str = "nginx.ingress.kubernetes.io/backend-protocol";

/// Backend a copy of each sampled request is sent to, as
/// `namespace/service:port`. Its presence is what turns mirroring on.
pub const ANNOTATION_MIRROR_BACKEND: &str = "ramjet.dev/mirror-backend";
/// Share of matching requests copied, `0`–`100`. Defaults to
/// [`DEFAULT_MIRROR_PERCENT`].
pub const ANNOTATION_MIRROR_PERCENT: &str = "ramjet.dev/mirror-percent";
/// `Host` header sent on the copy instead of the client's.
pub const ANNOTATION_MIRROR_HOST: &str = "ramjet.dev/mirror-host";

/// Opts a canary Ingress into automatic promotion.
pub const ANNOTATION_AUTO_PROMOTE: &str = "ramjet.dev/auto-promote";
/// How long each observation window is. Defaults to
/// [`DEFAULT_PROMOTE_INTERVAL`].
pub const ANNOTATION_AUTO_PROMOTE_INTERVAL: &str = "ramjet.dev/auto-promote-interval";
/// Comma-separated weights the canary is stepped through.
pub const ANNOTATION_AUTO_PROMOTE_STEPS: &str = "ramjet.dev/auto-promote-steps";
/// 5xx percentage that trips a rollback.
pub const ANNOTATION_AUTO_PROMOTE_MAX_5XX: &str = "ramjet.dev/auto-promote-max-5xx-percent";
/// Canary mean latency, as a multiple of stable's, that trips a rollback.
pub const ANNOTATION_AUTO_PROMOTE_MAX_LATENCY: &str =
    "ramjet.dev/auto-promote-max-latency-factor";
/// Requests each side needs in a window before it counts as evidence.
pub const ANNOTATION_AUTO_PROMOTE_MIN_REQUESTS: &str = "ramjet.dev/auto-promote-min-requests";
/// Written *by* the controller: what automatic promotion last did.
pub const ANNOTATION_AUTO_PROMOTE_STATUS: &str = "ramjet.dev/auto-promote-status";

/// Mirror everything, unless told otherwise.
///
/// The opposite default would make an operator who added `mirror-backend` and
/// saw no traffic conclude the feature does not work. Sampling exists to turn
/// mirroring *down* on a route that cannot afford the duplicate load.
pub const DEFAULT_MIRROR_PERCENT: u32 = 100;

/// One minute per observation window.
///
/// Long enough that a route with ordinary traffic clears the minimum-request
/// gate in one window, short enough that a five-step promotion finishes inside
/// a coffee break.
pub const DEFAULT_PROMOTE_INTERVAL: Duration = Duration::from_secs(60);

/// The weights a canary is stepped through, ending at 100.
pub const DEFAULT_PROMOTE_STEPS: &[u32] = &[5, 10, 25, 50, 100];

/// 5xx percentage over one window that trips a rollback.
pub const DEFAULT_PROMOTE_MAX_5XX_PERCENT: f64 = 1.0;

/// How much slower than stable the canary may be before it trips a rollback.
pub const DEFAULT_PROMOTE_MAX_LATENCY_FACTOR: f64 = 1.5;

/// Requests each side needs in a window before the window is evidence.
pub const DEFAULT_PROMOTE_MIN_REQUESTS: u64 = 50;

/// Pre-`IngressClass` way of claiming an Ingress. Still ubiquitous.
pub const ANNOTATION_LEGACY_CLASS: &str = "kubernetes.io/ingress.class";
/// Marks an `IngressClass` as the one that claims class-less Ingresses.
pub const ANNOTATION_IS_DEFAULT_CLASS: &str = "ingressclass.kubernetes.io/is-default-class";

/// The canary annotations on one Ingress, as written.
///
/// Field names mirror the annotation suffixes so the hand-off to
/// [`CanaryRules`](ramjet_router::CanaryRules) stays a transcription.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CanaryAnnotations {
    /// `canary: "true"`.
    pub enabled: bool,
    /// `canary-weight`; `0` when absent.
    pub weight: u32,
    /// `canary-weight-total`; `0` means the router's default of 100.
    pub weight_total: u32,
    /// `canary-by-header`.
    pub header: Option<String>,
    /// `canary-by-header-value`.
    pub header_value: Option<String>,
    /// `canary-by-header-pattern`.
    pub header_pattern: Option<String>,
    /// `canary-by-cookie`.
    pub cookie: Option<String>,
    /// Annotation keys whose values could not be parsed, for reporting. The
    /// annotation is ignored rather than fatal: a fat-fingered weight should
    /// not take the Ingress out of service.
    pub invalid: Vec<&'static str>,
}

impl CanaryAnnotations {
    /// Reads the canary annotations off an object's metadata.
    pub fn parse(annotations: &BTreeMap<String, String>) -> Self {
        let get = |key: &str| annotations.get(key).map(String::as_str);
        let mut invalid = Vec::new();

        let mut number = |key: &'static str| -> u32 {
            match get(key) {
                None => 0,
                Some(raw) => match raw.trim().parse::<u32>() {
                    Ok(v) => v,
                    Err(_) => {
                        invalid.push(key);
                        0
                    }
                },
            }
        };
        let weight = number(ANNOTATION_CANARY_WEIGHT);
        let weight_total = number(ANNOTATION_CANARY_WEIGHT_TOTAL);

        CanaryAnnotations {
            enabled: get(ANNOTATION_CANARY).is_some_and(is_true),
            weight,
            weight_total,
            header: get(ANNOTATION_CANARY_BY_HEADER).map(str::to_owned),
            header_value: get(ANNOTATION_CANARY_BY_HEADER_VALUE).map(str::to_owned),
            header_pattern: get(ANNOTATION_CANARY_BY_HEADER_PATTERN).map(str::to_owned),
            cookie: get(ANNOTATION_CANARY_BY_COOKIE).map(str::to_owned),
            invalid,
        }
    }

    /// Would this canary ever divert anything?
    ///
    /// A canary Ingress with `canary: "true"` and nothing else is inert in
    /// ingress-nginx too — weight 0, no header, no cookie — but it is worth
    /// saying so out loud rather than compiling a rule that can never fire.
    pub fn is_inert(&self) -> bool {
        self.weight == 0 && self.header.is_none() && self.cookie.is_none()
    }
}

/// The mirror annotations on one Ingress, as written.
///
/// Read from the *production* Ingress. A mirror is a property of the route, and
/// the canary Ingress is a second opinion about where a share of that route's
/// traffic goes — not a second route that could have its own shadow.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MirrorAnnotations {
    /// `mirror-backend`, unparsed. `None` disables mirroring entirely.
    pub backend: Option<String>,
    /// `mirror-percent`, defaulted to [`DEFAULT_MIRROR_PERCENT`].
    pub percent: u32,
    /// `mirror-host`.
    pub host: Option<String>,
    /// Annotation keys whose values could not be used, for reporting.
    pub invalid: Vec<&'static str>,
}

impl MirrorAnnotations {
    /// Reads the mirror annotations off an object's metadata.
    pub fn parse(annotations: &BTreeMap<String, String>) -> Self {
        let get = |key: &str| annotations.get(key).map(|v| v.trim()).filter(|v| !v.is_empty());
        let mut invalid = Vec::new();

        // Out of range rather than out of the annotation's grammar: `150` is a
        // number, so it parses, and it still means nothing. Both go in the same
        // list because the operator's next action is the same either way.
        let percent = match get(ANNOTATION_MIRROR_PERCENT) {
            None => DEFAULT_MIRROR_PERCENT,
            Some(raw) => match raw.parse::<u32>() {
                Ok(value) if value <= 100 => value,
                _ => {
                    invalid.push(ANNOTATION_MIRROR_PERCENT);
                    DEFAULT_MIRROR_PERCENT
                }
            },
        };

        MirrorAnnotations {
            backend: get(ANNOTATION_MIRROR_BACKEND).map(str::to_owned),
            percent,
            host: get(ANNOTATION_MIRROR_HOST).map(str::to_owned),
            invalid,
        }
    }

    /// Whether this Ingress asked for mirroring at all.
    pub fn enabled(&self) -> bool {
        self.backend.is_some()
    }
}

/// What `backend-protocol` on one Ingress resolved to.
///
/// ingress-nginx accepts six values. Two of them mean something this data plane
/// can do, and the other four name a capability it does not have yet:
///
/// | Value | Here |
/// |---|---|
/// | `HTTP` | HTTP/1.1 cleartext — the default |
/// | `GRPC` | h2c with prior knowledge |
/// | `GRPCS`, `HTTPS` | would need TLS to the upstream, which does not exist |
/// | `AUTO_HTTP` | would need per-endpoint scheme detection |
/// | `FCGI` | not an HTTP protocol at all |
///
/// The four unsupported values are **reported and then ignored**, leaving the
/// backend on HTTP/1.1. That combination is deliberate: silently treating
/// `GRPCS` as `HTTP` would send cleartext at a port expecting TLS with nothing
/// to explain the connection resets, and refusing to compile the Ingress would
/// hand one namespace owner a way to take out the table. So the request is
/// served the way an unannotated one would be, and the warning says exactly
/// which value was not honoured.
///
/// Values are matched **case-insensitively after trimming**, which is what
/// ingress-nginx does — it uppercases before matching — so `grpc` and ` GRPC `
/// both work on a cluster being migrated.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BackendProtocolAnnotation {
    /// What the data plane will actually speak.
    pub protocol: ramjet_router::BackendProtocol,
    /// The value as written, when it named a protocol we do not implement.
    ///
    /// `Some` is the reportable case: an operator asked for something specific
    /// and did not get it. A value we simply cannot parse lands here too, since
    /// the operator's next action — look at the annotation — is the same.
    pub unsupported: Option<String>,
}

impl BackendProtocolAnnotation {
    /// Reads `backend-protocol` off an object's metadata.
    pub fn parse(annotations: &BTreeMap<String, String>) -> Self {
        let Some(raw) = annotations
            .get(ANNOTATION_BACKEND_PROTOCOL)
            .map(|v| v.trim())
            .filter(|v| !v.is_empty())
        else {
            return Self::default();
        };

        // `eq_ignore_ascii_case` rather than allocating an uppercased copy: this
        // runs once per Ingress per rebuild, and every accepted spelling is
        // ASCII.
        if raw.eq_ignore_ascii_case("HTTP") {
            return Self::default();
        }
        if raw.eq_ignore_ascii_case("GRPC") {
            return BackendProtocolAnnotation {
                protocol: ramjet_router::BackendProtocol::H2c,
                unsupported: None,
            };
        }
        BackendProtocolAnnotation {
            protocol: ramjet_router::BackendProtocol::Http1,
            unsupported: Some(raw.to_owned()),
        }
    }
}

/// The automatic-promotion annotations on one canary Ingress, as written.
///
/// Every field has a default that is safe to run with, so opting in is one
/// annotation. The rest exist because "safe" is a property of a particular
/// service's error budget, and nobody else can know it.
#[derive(Debug, Clone, PartialEq)]
pub struct PromotionAnnotations {
    /// `auto-promote: "true"`.
    pub enabled: bool,
    /// How long one observation window is.
    pub interval: Duration,
    /// The weights to step through, ascending, ending at the last one.
    pub steps: Vec<u32>,
    /// 5xx percentage over one window that trips a rollback.
    pub max_5xx_percent: f64,
    /// Canary mean latency, as a multiple of stable's, that trips a rollback.
    pub max_latency_factor: f64,
    /// Requests each side needs in a window before it is evidence.
    pub min_requests: u64,
    /// `auto-promote-status`, as this controller last wrote it.
    ///
    /// Read back rather than kept in memory, because the flap guard has to
    /// survive a restart: a canary this controller rolled back an hour ago must
    /// still be refused after the pod is rescheduled.
    pub status: Option<String>,
    /// Annotation keys whose values could not be used, for reporting.
    pub invalid: Vec<&'static str>,
}

impl Default for PromotionAnnotations {
    fn default() -> Self {
        PromotionAnnotations {
            enabled: false,
            interval: DEFAULT_PROMOTE_INTERVAL,
            steps: DEFAULT_PROMOTE_STEPS.to_vec(),
            max_5xx_percent: DEFAULT_PROMOTE_MAX_5XX_PERCENT,
            max_latency_factor: DEFAULT_PROMOTE_MAX_LATENCY_FACTOR,
            min_requests: DEFAULT_PROMOTE_MIN_REQUESTS,
            status: None,
            invalid: Vec::new(),
        }
    }
}

impl PromotionAnnotations {
    /// Reads the promotion annotations off a canary Ingress's metadata.
    ///
    /// Every unparseable value falls back to its default and is reported. The
    /// alternative — refusing to promote because one threshold is misspelled —
    /// leaves a canary stuck at its starting weight with no explanation, which
    /// is a worse failure than promoting against a default somebody can see.
    pub fn parse(annotations: &BTreeMap<String, String>) -> Self {
        let get = |key: &str| annotations.get(key).map(|v| v.trim()).filter(|v| !v.is_empty());
        let mut parsed = PromotionAnnotations {
            enabled: get(ANNOTATION_AUTO_PROMOTE).is_some_and(is_true),
            status: get(ANNOTATION_AUTO_PROMOTE_STATUS).map(str::to_owned),
            ..PromotionAnnotations::default()
        };

        if let Some(raw) = get(ANNOTATION_AUTO_PROMOTE_INTERVAL) {
            match parse_duration(raw) {
                Some(interval) => parsed.interval = interval,
                None => parsed.invalid.push(ANNOTATION_AUTO_PROMOTE_INTERVAL),
            }
        }
        if let Some(raw) = get(ANNOTATION_AUTO_PROMOTE_STEPS) {
            match parse_steps(raw) {
                Some(steps) => parsed.steps = steps,
                None => parsed.invalid.push(ANNOTATION_AUTO_PROMOTE_STEPS),
            }
        }
        if let Some(raw) = get(ANNOTATION_AUTO_PROMOTE_MAX_5XX) {
            match raw.parse::<f64>() {
                Ok(value) if value.is_finite() && value >= 0.0 => parsed.max_5xx_percent = value,
                _ => parsed.invalid.push(ANNOTATION_AUTO_PROMOTE_MAX_5XX),
            }
        }
        if let Some(raw) = get(ANNOTATION_AUTO_PROMOTE_MAX_LATENCY) {
            match raw.parse::<f64>() {
                // Below 1.0 would demand the canary be *faster* than stable to
                // advance, which is not a health check, it is a benchmark.
                Ok(value) if value.is_finite() && value >= 1.0 => {
                    parsed.max_latency_factor = value;
                }
                _ => parsed.invalid.push(ANNOTATION_AUTO_PROMOTE_MAX_LATENCY),
            }
        }
        if let Some(raw) = get(ANNOTATION_AUTO_PROMOTE_MIN_REQUESTS) {
            match raw.parse::<u64>() {
                Ok(value) => parsed.min_requests = value,
                Err(_) => parsed.invalid.push(ANNOTATION_AUTO_PROMOTE_MIN_REQUESTS),
            }
        }
        parsed
    }

    /// Whether this canary has already been rolled back once.
    ///
    /// The flap guard. A rollback also sets `auto-promote: "false"`, so this is
    /// belt and braces — but the two annotations are written in one patch that
    /// can half-fail, and re-arming a canary that has already failed once is
    /// the single worst thing this loop could do.
    pub fn rolled_back(&self) -> bool {
        self.status
            .as_deref()
            .is_some_and(|status| status.starts_with(STATUS_ROLLED_BACK))
    }

    /// The next weight after `weight`, or `None` once the last step is reached.
    pub fn next_step(&self, weight: u32) -> Option<u32> {
        self.steps.iter().copied().find(|step| *step > weight)
    }

    /// The weight this canary is being promoted towards.
    pub fn final_step(&self) -> u32 {
        self.steps.last().copied().unwrap_or(100)
    }
}

/// Prefix of the `auto-promote-status` value written by a rollback.
pub const STATUS_ROLLED_BACK: &str = "rolled-back";

/// Value of `auto-promote-status` once a canary has reached its last step.
pub const STATUS_PROMOTED: &str = "promoted";

/// Parses `30s`, `5m`, `1h`, or a bare number of seconds.
///
/// Deliberately not a general duration grammar. An interval is a number and a
/// unit, and accepting `1h30m` would mean writing a parser whose failure modes
/// are more interesting than anything it enables.
fn parse_duration(raw: &str) -> Option<Duration> {
    let raw = raw.trim();
    let (value, multiplier) = match raw.as_bytes().last()? {
        b's' => (&raw[..raw.len() - 1], 1),
        b'm' => (&raw[..raw.len() - 1], 60),
        b'h' => (&raw[..raw.len() - 1], 3600),
        _ => (raw, 1),
    };
    let seconds: u64 = value.trim().parse().ok()?;
    // Zero would make the loop spin at whatever rate the executor allows, which
    // is a denial of service written as a configuration value.
    (seconds > 0).then(|| Duration::from_secs(seconds.saturating_mul(multiplier)))
}

/// Parses `5,10,25,50,100` into ascending, deduplicated weights.
///
/// Sorted here rather than trusted, so that `50,10,100` means what it obviously
/// means instead of promoting to 50 and then *demoting* to 10.
fn parse_steps(raw: &str) -> Option<Vec<u32>> {
    let mut steps: Vec<u32> = Vec::new();
    for field in raw.split(',') {
        let field = field.trim();
        if field.is_empty() {
            continue;
        }
        let step: u32 = field.parse().ok()?;
        if step == 0 || step > 100 {
            return None;
        }
        steps.push(step);
    }
    if steps.is_empty() {
        return None;
    }
    steps.sort_unstable();
    steps.dedup();
    Some(steps)
}

/// Kubernetes annotation values are strings, and people write all of these.
fn is_true(value: &str) -> bool {
    value.trim().eq_ignore_ascii_case("true")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ann(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect()
    }

    #[test]
    fn absent_annotations_produce_a_disabled_canary() {
        let c = CanaryAnnotations::parse(&ann(&[]));
        assert!(!c.enabled);
        assert_eq!(c, CanaryAnnotations::default());
    }

    #[test]
    fn canary_flag_is_case_insensitive_and_trimmed() {
        for raw in ["true", "True", "TRUE", " true "] {
            assert!(
                CanaryAnnotations::parse(&ann(&[(ANNOTATION_CANARY, raw)])).enabled,
                "`{raw}` should enable the canary"
            );
        }
        for raw in ["false", "1", "yes", ""] {
            assert!(
                !CanaryAnnotations::parse(&ann(&[(ANNOTATION_CANARY, raw)])).enabled,
                "`{raw}` should not enable the canary"
            );
        }
    }

    #[test]
    fn every_canary_annotation_is_read() {
        let c = CanaryAnnotations::parse(&ann(&[
            (ANNOTATION_CANARY, "true"),
            (ANNOTATION_CANARY_WEIGHT, "30"),
            (ANNOTATION_CANARY_WEIGHT_TOTAL, "1000"),
            (ANNOTATION_CANARY_BY_HEADER, "x-canary"),
            (ANNOTATION_CANARY_BY_HEADER_VALUE, "beta"),
            (ANNOTATION_CANARY_BY_HEADER_PATTERN, "beta.*"),
            (ANNOTATION_CANARY_BY_COOKIE, "canary-cookie"),
        ]));
        assert!(c.enabled);
        assert_eq!(c.weight, 30);
        assert_eq!(c.weight_total, 1000);
        assert_eq!(c.header.as_deref(), Some("x-canary"));
        assert_eq!(c.header_value.as_deref(), Some("beta"));
        assert_eq!(c.header_pattern.as_deref(), Some("beta.*"));
        assert_eq!(c.cookie.as_deref(), Some("canary-cookie"));
        assert!(c.invalid.is_empty());
    }

    #[test]
    fn unparseable_weight_is_reported_not_fatal() {
        let c = CanaryAnnotations::parse(&ann(&[
            (ANNOTATION_CANARY, "true"),
            (ANNOTATION_CANARY_WEIGHT, "thirty"),
        ]));
        assert!(c.enabled, "a bad weight must not disable the Ingress");
        assert_eq!(c.weight, 0);
        assert_eq!(c.invalid, vec![ANNOTATION_CANARY_WEIGHT]);
    }

    #[test]
    fn negative_weight_is_rejected_rather_than_wrapped() {
        let c = CanaryAnnotations::parse(&ann(&[(ANNOTATION_CANARY_WEIGHT, "-5")]));
        assert_eq!(c.weight, 0);
        assert_eq!(c.invalid, vec![ANNOTATION_CANARY_WEIGHT]);
    }

    // -----------------------------------------------------------------------
    // Mirroring
    // -----------------------------------------------------------------------

    #[test]
    fn no_mirror_backend_means_no_mirroring() {
        let m = MirrorAnnotations::parse(&ann(&[]));
        assert!(!m.enabled());
        assert_eq!(m.backend, None);
    }

    #[test]
    fn a_backend_alone_mirrors_everything() {
        // The opposite default would make an operator who added one annotation
        // and saw no traffic conclude the feature does not work.
        let m = MirrorAnnotations::parse(&ann(&[(
            ANNOTATION_MIRROR_BACKEND,
            "shadow/api:80",
        )]));
        assert!(m.enabled());
        assert_eq!(m.backend.as_deref(), Some("shadow/api:80"));
        assert_eq!(m.percent, DEFAULT_MIRROR_PERCENT);
        assert_eq!(m.host, None);
        assert!(m.invalid.is_empty());
    }

    #[test]
    fn every_mirror_annotation_is_read() {
        let m = MirrorAnnotations::parse(&ann(&[
            (ANNOTATION_MIRROR_BACKEND, "shadow/api:80"),
            (ANNOTATION_MIRROR_PERCENT, "10"),
            (ANNOTATION_MIRROR_HOST, "shadow.internal"),
        ]));
        assert_eq!(m.percent, 10);
        assert_eq!(m.host.as_deref(), Some("shadow.internal"));
        assert!(m.invalid.is_empty());
    }

    #[test]
    fn a_mirror_percent_of_zero_is_kept_not_defaulted() {
        // Turning a mirror off without deleting the annotation that says where
        // it points is the whole reason the knob is separate from the backend.
        let m = MirrorAnnotations::parse(&ann(&[
            (ANNOTATION_MIRROR_BACKEND, "shadow/api:80"),
            (ANNOTATION_MIRROR_PERCENT, "0"),
        ]));
        assert_eq!(m.percent, 0);
        assert!(m.invalid.is_empty());
    }

    #[test]
    fn an_out_of_range_or_unparseable_percent_is_reported_not_fatal() {
        for raw in ["101", "-5", "lots", "50%"] {
            let m = MirrorAnnotations::parse(&ann(&[
                (ANNOTATION_MIRROR_BACKEND, "shadow/api:80"),
                (ANNOTATION_MIRROR_PERCENT, raw),
            ]));
            assert!(m.enabled(), "`{raw}` must not disable the mirror");
            assert_eq!(m.percent, DEFAULT_MIRROR_PERCENT);
            assert_eq!(m.invalid, vec![ANNOTATION_MIRROR_PERCENT], "for `{raw}`");
        }
    }

    #[test]
    fn a_blank_annotation_reads_as_absent() {
        // `mirror-backend: ""` is somebody trying to turn it off, and an empty
        // backend name would otherwise reach the router and be rejected there.
        let m = MirrorAnnotations::parse(&ann(&[
            (ANNOTATION_MIRROR_BACKEND, "   "),
            (ANNOTATION_MIRROR_HOST, ""),
        ]));
        assert!(!m.enabled());
        assert_eq!(m.host, None);
    }

    // -----------------------------------------------------------------------
    // Backend protocol
    // -----------------------------------------------------------------------

    #[test]
    fn no_backend_protocol_annotation_means_http1() {
        let p = BackendProtocolAnnotation::parse(&ann(&[]));
        assert_eq!(p.protocol, ramjet_router::BackendProtocol::Http1);
        assert_eq!(p.unsupported, None);
    }

    #[test]
    fn grpc_selects_h2c_however_it_is_spelled() {
        // ingress-nginx uppercases and trims before matching, so a cluster being
        // migrated may carry any of these and all of them have to mean the same
        // thing here.
        for spelling in ["GRPC", "grpc", "Grpc", "  GRPC  ", "\tgRPC\n"] {
            let p = BackendProtocolAnnotation::parse(&ann(&[(
                ANNOTATION_BACKEND_PROTOCOL,
                spelling,
            )]));
            assert_eq!(
                p.protocol,
                ramjet_router::BackendProtocol::H2c,
                "{spelling:?}"
            );
            assert_eq!(p.unsupported, None, "{spelling:?}");
        }
    }

    #[test]
    fn http_is_accepted_explicitly_and_is_not_reported() {
        // Writing the default out is not a mistake, and warning about it would
        // put noise in the log of every cluster that spells its intent.
        for spelling in ["HTTP", "http", " Http "] {
            let p = BackendProtocolAnnotation::parse(&ann(&[(
                ANNOTATION_BACKEND_PROTOCOL,
                spelling,
            )]));
            assert_eq!(p.protocol, ramjet_router::BackendProtocol::Http1);
            assert_eq!(p.unsupported, None, "{spelling:?}");
        }
    }

    #[test]
    fn the_protocols_ingress_nginx_has_and_we_do_not_are_named_back() {
        // Each of these means something specific to somebody migrating, and each
        // is refused by name rather than quietly becoming HTTP.
        for value in ["GRPCS", "HTTPS", "AUTO_HTTP", "FCGI"] {
            let p =
                BackendProtocolAnnotation::parse(&ann(&[(ANNOTATION_BACKEND_PROTOCOL, value)]));
            assert_eq!(
                p.protocol,
                ramjet_router::BackendProtocol::Http1,
                "{value} must not change the protocol"
            );
            assert_eq!(
                p.unsupported.as_deref(),
                Some(value),
                "{value} must be reported back verbatim"
            );
        }
    }

    #[test]
    fn an_unrecognised_value_is_reported_rather_than_guessed_at() {
        let p = BackendProtocolAnnotation::parse(&ann(&[(ANNOTATION_BACKEND_PROTOCOL, "h2c")]));
        assert_eq!(p.protocol, ramjet_router::BackendProtocol::Http1);
        // `h2c` is what *we* call it internally, and it is still not one of the
        // six values ingress-nginx defines. Accepting our own spelling would
        // make an Ingress that works here and nowhere else.
        assert_eq!(p.unsupported.as_deref(), Some("h2c"));
    }

    #[test]
    fn a_blank_backend_protocol_reads_as_absent() {
        let p = BackendProtocolAnnotation::parse(&ann(&[(ANNOTATION_BACKEND_PROTOCOL, "   ")]));
        assert_eq!(p.protocol, ramjet_router::BackendProtocol::Http1);
        assert_eq!(p.unsupported, None);
    }

    // -----------------------------------------------------------------------
    // Automatic promotion
    // -----------------------------------------------------------------------

    #[test]
    fn promotion_is_off_and_fully_defaulted_without_annotations() {
        let p = PromotionAnnotations::parse(&ann(&[]));
        assert!(!p.enabled);
        assert_eq!(p.interval, DEFAULT_PROMOTE_INTERVAL);
        assert_eq!(p.steps, DEFAULT_PROMOTE_STEPS);
        assert_eq!(p.max_5xx_percent, DEFAULT_PROMOTE_MAX_5XX_PERCENT);
        assert_eq!(p.max_latency_factor, DEFAULT_PROMOTE_MAX_LATENCY_FACTOR);
        assert_eq!(p.min_requests, DEFAULT_PROMOTE_MIN_REQUESTS);
        assert!(!p.rolled_back());
    }

    #[test]
    fn opting_in_is_one_annotation() {
        let p = PromotionAnnotations::parse(&ann(&[(ANNOTATION_AUTO_PROMOTE, "true")]));
        assert!(p.enabled);
        assert_eq!(p.steps, DEFAULT_PROMOTE_STEPS);
    }

    #[test]
    fn every_promotion_annotation_is_read() {
        let p = PromotionAnnotations::parse(&ann(&[
            (ANNOTATION_AUTO_PROMOTE, "true"),
            (ANNOTATION_AUTO_PROMOTE_INTERVAL, "5m"),
            (ANNOTATION_AUTO_PROMOTE_STEPS, "20,40,60,80,100"),
            (ANNOTATION_AUTO_PROMOTE_MAX_5XX, "0.5"),
            (ANNOTATION_AUTO_PROMOTE_MAX_LATENCY, "2"),
            (ANNOTATION_AUTO_PROMOTE_MIN_REQUESTS, "500"),
        ]));
        assert!(p.enabled);
        assert_eq!(p.interval, Duration::from_secs(300));
        assert_eq!(p.steps, vec![20, 40, 60, 80, 100]);
        assert_eq!(p.max_5xx_percent, 0.5);
        assert_eq!(p.max_latency_factor, 2.0);
        assert_eq!(p.min_requests, 500);
        assert!(p.invalid.is_empty());
    }

    #[test]
    fn intervals_accept_seconds_minutes_and_hours() {
        assert_eq!(parse_duration("45"), Some(Duration::from_secs(45)));
        assert_eq!(parse_duration("45s"), Some(Duration::from_secs(45)));
        assert_eq!(parse_duration("2m"), Some(Duration::from_secs(120)));
        assert_eq!(parse_duration("1h"), Some(Duration::from_secs(3600)));
        assert_eq!(parse_duration(" 30s "), Some(Duration::from_secs(30)));
    }

    #[test]
    fn a_zero_interval_is_refused() {
        // It would spin the loop at whatever rate the executor allows, which is
        // a denial of service written as a configuration value.
        assert_eq!(parse_duration("0"), None);
        assert_eq!(parse_duration("0s"), None);
        assert_eq!(parse_duration("1h30m"), None);
        assert_eq!(parse_duration("soon"), None);
        assert_eq!(parse_duration(""), None);
    }

    #[test]
    fn steps_are_sorted_and_deduplicated() {
        // `50,10,100` obviously means step up through those three; taking it in
        // written order would promote to 50 and then demote to 10.
        assert_eq!(parse_steps("50,10,100"), Some(vec![10, 50, 100]));
        assert_eq!(parse_steps("10,10,50"), Some(vec![10, 50]));
        assert_eq!(parse_steps(" 5 , 10 "), Some(vec![5, 10]));
        assert_eq!(parse_steps("100"), Some(vec![100]));
    }

    #[test]
    fn a_step_outside_a_weight_is_refused() {
        assert_eq!(parse_steps("0,50"), None);
        assert_eq!(parse_steps("50,150"), None);
        assert_eq!(parse_steps("half"), None);
        assert_eq!(parse_steps(""), None);
        assert_eq!(parse_steps(",,"), None);
    }

    #[test]
    fn a_bad_threshold_falls_back_and_is_reported() {
        // A canary stuck at its starting weight because one threshold is
        // misspelled, with nothing said about why, is worse than promoting
        // against a default that shows up in a warning.
        let p = PromotionAnnotations::parse(&ann(&[
            (ANNOTATION_AUTO_PROMOTE, "true"),
            (ANNOTATION_AUTO_PROMOTE_INTERVAL, "never"),
            (ANNOTATION_AUTO_PROMOTE_STEPS, "1,2,999"),
            (ANNOTATION_AUTO_PROMOTE_MAX_5XX, "-1"),
            (ANNOTATION_AUTO_PROMOTE_MAX_LATENCY, "0.5"),
            (ANNOTATION_AUTO_PROMOTE_MIN_REQUESTS, "many"),
        ]));
        assert!(p.enabled);
        assert_eq!(p.interval, DEFAULT_PROMOTE_INTERVAL);
        assert_eq!(p.steps, DEFAULT_PROMOTE_STEPS);
        assert_eq!(p.max_5xx_percent, DEFAULT_PROMOTE_MAX_5XX_PERCENT);
        assert_eq!(p.max_latency_factor, DEFAULT_PROMOTE_MAX_LATENCY_FACTOR);
        assert_eq!(p.min_requests, DEFAULT_PROMOTE_MIN_REQUESTS);
        assert_eq!(p.invalid.len(), 5);
    }

    #[test]
    fn a_latency_factor_below_one_is_refused() {
        // It would demand the canary be faster than stable in order to
        // advance, which is a benchmark and not a health check.
        let p = PromotionAnnotations::parse(&ann(&[(ANNOTATION_AUTO_PROMOTE_MAX_LATENCY, "0.9")]));
        assert_eq!(p.max_latency_factor, DEFAULT_PROMOTE_MAX_LATENCY_FACTOR);
        assert_eq!(p.invalid, vec![ANNOTATION_AUTO_PROMOTE_MAX_LATENCY]);

        let exact = PromotionAnnotations::parse(&ann(&[(ANNOTATION_AUTO_PROMOTE_MAX_LATENCY, "1")]));
        assert_eq!(exact.max_latency_factor, 1.0, "parity is a legal demand");
        assert!(exact.invalid.is_empty());
    }

    #[test]
    fn the_flap_guard_reads_the_status_this_controller_wrote() {
        // It has to survive a restart: a canary rolled back an hour ago must
        // still be refused after the pod is rescheduled.
        let rolled = PromotionAnnotations::parse(&ann(&[
            (ANNOTATION_AUTO_PROMOTE, "true"),
            (ANNOTATION_AUTO_PROMOTE_STATUS, "rolled-back: 5xx 4.2% over 1%"),
        ]));
        assert!(rolled.rolled_back());

        let promoted = PromotionAnnotations::parse(&ann(&[(
            ANNOTATION_AUTO_PROMOTE_STATUS,
            STATUS_PROMOTED,
        )]));
        assert!(!promoted.rolled_back());
    }

    #[test]
    fn steps_advance_past_the_current_weight_and_then_stop() {
        let p = PromotionAnnotations::parse(&ann(&[(
            ANNOTATION_AUTO_PROMOTE_STEPS,
            "5,10,25,50,100",
        )]));
        assert_eq!(p.next_step(0), Some(5));
        assert_eq!(p.next_step(5), Some(10));
        assert_eq!(p.next_step(7), Some(10), "an off-step weight takes the next one up");
        assert_eq!(p.next_step(50), Some(100));
        assert_eq!(p.next_step(100), None, "the last step is the end of the road");
        assert_eq!(p.final_step(), 100);
    }

    #[test]
    fn inertness_notices_a_canary_that_can_never_fire() {
        let mut c = CanaryAnnotations {
            enabled: true,
            ..Default::default()
        };
        assert!(c.is_inert());
        c.weight = 1;
        assert!(!c.is_inert());

        let c = CanaryAnnotations {
            enabled: true,
            header: Some("x-canary".to_owned()),
            ..Default::default()
        };
        assert!(!c.is_inert(), "a header-only canary still fires");
    }
}
