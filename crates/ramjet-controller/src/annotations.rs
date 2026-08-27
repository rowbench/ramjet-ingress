//! The ingress-nginx annotation vocabulary we understand.
//!
//! The prefix is `nginx.ingress.kubernetes.io` on purpose, not
//! `ramjet.dev`. Compatibility is the whole point: an existing cluster should
//! be able to swap controllers without rewriting every Ingress, so we speak the
//! annotations people already have.

use std::collections::BTreeMap;

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
