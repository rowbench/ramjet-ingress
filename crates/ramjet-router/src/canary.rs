//! Canary routing, matching ingress-nginx annotation semantics.
//!
//! In ingress-nginx a canary is a second Ingress carrying the same host and
//! path as the production one plus `nginx.ingress.kubernetes.io/canary: "true"`.
//! The controller merges the pair; here they arrive already merged, as a
//! [`CanarySpec`] hanging off the production [`PathRule`](crate::PathRule).
//!
//! # Precedence
//!
//! Header beats cookie beats weight. The subtlety is what "beats" means: only
//! the literal values `always` and `never` are *decisive*. A header that is
//! present but says something else is ignored, and evaluation continues to the
//! next rule. That is why [`CanarySpec::decide`] falls through instead of
//! returning early on a mismatch — getting this wrong makes every request with
//! an unrelated header value bypass the weight split.
//!
//! # Sans-io
//!
//! This crate has no opinion about how headers are stored. Rather than take a
//! map or a trait object, [`decide`](CanarySpec::decide) is handed the two
//! values it might need. The caller asks [`header_name`](CanarySpec::header_name)
//! and [`cookie_name`](CanarySpec::cookie_name) which ones to look up, pulls
//! them out of whatever representation it already has, and passes borrowed
//! `&str`s. Nothing is allocated and no header collection type leaks in here.

use regex::Regex;

use crate::backend::BackendId;

/// How a canary header's value is tested.
enum HeaderMatch {
    /// No value configured: only `always` and `never` mean anything.
    AlwaysNever,
    /// `canary-by-header-value`: exact string equality.
    Value(Box<str>),
    /// `canary-by-header-pattern`: a regex, anchored to the whole value by the
    /// builder so a partial match cannot divert traffic.
    Pattern(Box<Regex>),
}

struct HeaderRule {
    name: Box<str>,
    test: HeaderMatch,
}

/// The three header annotations, as the builder collected them.
///
/// `value` and `pattern` are mutually exclusive; the builder rejects the
/// combination, and [`CanarySpec::new`] prefers `pattern` if both survive.
pub(crate) struct HeaderSpec {
    /// `canary-by-header`.
    pub name: Box<str>,
    /// `canary-by-header-value`.
    pub value: Option<Box<str>>,
    /// `canary-by-header-pattern`, already anchored by the builder.
    pub pattern: Option<Box<Regex>>,
}

/// A canary attached to a route.
pub struct CanarySpec {
    backend: BackendId,
    header: Option<HeaderRule>,
    cookie: Option<Box<str>>,
    weight: u32,
    weight_total: u32,
}

impl CanarySpec {
    pub(crate) fn new(
        backend: BackendId,
        header: Option<HeaderSpec>,
        cookie: Option<Box<str>>,
        weight: u32,
        weight_total: u32,
    ) -> Self {
        let header = header.map(|spec| {
            let test = match (spec.value, spec.pattern) {
                (_, Some(re)) => HeaderMatch::Pattern(re),
                (Some(v), None) => HeaderMatch::Value(v),
                (None, None) => HeaderMatch::AlwaysNever,
            };
            HeaderRule {
                name: spec.name,
                test,
            }
        });
        CanarySpec {
            backend,
            header,
            cookie,
            weight,
            weight_total,
        }
    }

    /// The backend canaried traffic goes to.
    pub fn backend(&self) -> BackendId {
        self.backend
    }

    /// The request header to inspect, if the canary is header-driven.
    pub fn header_name(&self) -> Option<&str> {
        self.header.as_ref().map(|h| &*h.name)
    }

    /// The cookie to inspect, if the canary is cookie-driven.
    pub fn cookie_name(&self) -> Option<&str> {
        self.cookie.as_deref()
    }

    /// The share of unmatched traffic sent to the canary, out of
    /// [`weight_total`](Self::weight_total).
    pub fn weight(&self) -> u32 {
        self.weight
    }

    /// The denominator for [`weight`](Self::weight); ingress-nginx defaults it
    /// to 100 but allows `canary-weight-total` to override it.
    pub fn weight_total(&self) -> u32 {
        self.weight_total
    }

    /// The weight expressed as a percentage, for display and metrics.
    pub fn weight_percent(&self) -> u32 {
        self.weight
            .saturating_mul(100)
            .checked_div(self.weight_total)
            .unwrap_or(0)
    }

    /// Decides whether this request goes to the canary backend.
    ///
    /// `header_value` and `cookie_value` are the values of
    /// [`header_name`](Self::header_name) and [`cookie_name`](Self::cookie_name),
    /// or `None` if absent. `roll` is a caller-supplied random number in
    /// `0..weight_total`, used only if no rule was decisive.
    pub fn decide(&self, header_value: Option<&str>, cookie_value: Option<&str>, roll: u32) -> bool {
        if let (Some(rule), Some(value)) = (&self.header, header_value) {
            match &rule.test {
                HeaderMatch::AlwaysNever => match value {
                    "always" => return true,
                    "never" => return false,
                    // Any other value: not decisive, keep going.
                    _ => {}
                },
                HeaderMatch::Value(want) => {
                    if value == &**want {
                        return true;
                    }
                }
                HeaderMatch::Pattern(re) => {
                    if re.is_match(value) {
                        return true;
                    }
                }
            }
        }

        if self.cookie.is_some() {
            match cookie_value {
                Some("always") => return true,
                Some("never") => return false,
                _ => {}
            }
        }

        self.weight > 0 && roll < self.weight
    }
}

impl std::fmt::Debug for CanarySpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CanarySpec")
            .field("backend", &self.backend)
            .field("header", &self.header_name())
            .field("cookie", &self.cookie_name())
            .field("weight", &self.weight)
            .field("weight_total", &self.weight_total)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(
        header: Option<(&str, Option<&str>, Option<&str>)>,
        cookie: Option<&str>,
        weight: u32,
    ) -> CanarySpec {
        let header = header.map(|(n, v, p)| HeaderSpec {
            name: n.into(),
            value: v.map(Into::into),
            pattern: p.map(|p| {
                Box::new(Regex::new(&format!("^(?:{p})$")).expect("test pattern compiles"))
            }),
        });
        CanarySpec::new(BackendId(1), header, cookie.map(Into::into), weight, 100)
    }

    #[test]
    fn header_always_beats_a_zero_weight() {
        let c = spec(Some(("x-canary", None, None)), None, 0);
        assert!(c.decide(Some("always"), None, 99));
    }

    /// The headline rule: a header match wins even when the weight says no.
    #[test]
    fn header_beats_weight() {
        let c = spec(Some(("x-canary", Some("beta"), None)), None, 0);
        assert!(
            c.decide(Some("beta"), None, 99),
            "a matching header must divert regardless of weight"
        );

        let c = spec(Some(("x-canary", None, None)), None, 100);
        assert!(
            !c.decide(Some("never"), None, 0),
            "`never` must win against a 100% weight"
        );
    }

    #[test]
    fn unmatched_header_value_falls_through_to_weight() {
        // This is the case that is easy to get wrong: a header that is present
        // but says something unrelated must not suppress the weight split.
        let c = spec(Some(("x-canary", Some("beta"), None)), None, 100);
        assert!(c.decide(Some("something-else"), None, 0));

        let c = spec(Some(("x-canary", Some("beta"), None)), None, 0);
        assert!(!c.decide(Some("something-else"), None, 0));
    }

    #[test]
    fn absent_header_falls_through_to_weight() {
        let c = spec(Some(("x-canary", None, None)), None, 100);
        assert!(c.decide(None, None, 0));
        let c = spec(Some(("x-canary", None, None)), None, 0);
        assert!(!c.decide(None, None, 0));
    }

    #[test]
    fn header_beats_cookie() {
        let c = spec(Some(("x-canary", None, None)), Some("canary"), 0);
        assert!(c.decide(Some("always"), Some("never"), 99));
        assert!(!c.decide(Some("never"), Some("always"), 0));
    }

    #[test]
    fn cookie_beats_weight() {
        let c = spec(None, Some("canary"), 0);
        assert!(c.decide(None, Some("always"), 99));
        let c = spec(None, Some("canary"), 100);
        assert!(!c.decide(None, Some("never"), 0));
    }

    #[test]
    fn unrecognised_cookie_falls_through() {
        let c = spec(None, Some("canary"), 100);
        assert!(c.decide(None, Some("maybe"), 0));
        let c = spec(None, Some("canary"), 0);
        assert!(!c.decide(None, Some("maybe"), 0));
    }

    #[test]
    fn weight_splits_the_roll() {
        let c = spec(None, None, 30);
        assert!(c.decide(None, None, 0));
        assert!(c.decide(None, None, 29));
        assert!(!c.decide(None, None, 30));
        assert!(!c.decide(None, None, 99));
    }

    #[test]
    fn zero_weight_never_diverts() {
        let c = spec(None, None, 0);
        for roll in 0..100 {
            assert!(!c.decide(None, None, roll));
        }
    }

    #[test]
    fn pattern_is_anchored() {
        let c = spec(Some(("x-canary", None, Some("beta.*"))), None, 0);
        assert!(c.decide(Some("beta-1"), None, 99));
        assert!(
            !c.decide(Some("not-beta"), None, 99),
            "an unanchored pattern would divert this"
        );
    }

    #[test]
    fn pattern_overrides_value_when_both_are_set() {
        // The builder rejects this combination; the type still has to pick one.
        let c = spec(Some(("x-canary", Some("exact"), Some("beta.*"))), None, 0);
        assert!(c.decide(Some("beta-2"), None, 99));
    }

    #[test]
    fn weight_percent_uses_the_total() {
        let c = CanarySpec::new(BackendId(1), None, None, 5, 10);
        assert_eq!(c.weight_percent(), 50);
        let c = CanarySpec::new(BackendId(1), None, None, 5, 0);
        assert_eq!(c.weight_percent(), 0);
    }

    #[test]
    fn header_matching_is_case_sensitive() {
        // ingress-nginx compares the value literally.
        let c = spec(Some(("x-canary", None, None)), None, 0);
        assert!(!c.decide(Some("Always"), None, 99));
    }
}
