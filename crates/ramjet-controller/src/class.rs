//! Deciding which Ingresses are ours.
//!
//! Getting this wrong in either direction is a production incident: claim too
//! much and you fight another controller for the same hostnames, claim too
//! little and traffic silently 404s. So the rules below follow ingress-nginx
//! exactly, including the parts that look like historical accidents.

use std::collections::HashSet;
use std::sync::Arc;

use k8s_openapi::api::networking::v1::{Ingress, IngressClass};
use kube::ResourceExt;

use crate::annotations::{ANNOTATION_IS_DEFAULT_CLASS, ANNOTATION_LEGACY_CLASS};
use crate::config::CONTROLLER_NAME;

/// Precomputed answer to "is this Ingress ours?".
///
/// Built once per rebuild from the `IngressClass` objects in the snapshot, so
/// the per-Ingress check is two hash lookups rather than a scan.
#[derive(Debug, Clone)]
pub struct ClassFilter {
    /// Names of `IngressClass` objects whose `spec.controller` is ours.
    ours: HashSet<String>,
    /// Names of every `IngressClass` in the cluster, ours or not. Lets us tell
    /// "belongs to another controller" (silence) from "names a class that does
    /// not exist" (worth a warning).
    known: HashSet<String>,
    /// Whether one of *our* classes carries `is-default-class: "true"`.
    default_is_ours: bool,
    /// Value the legacy annotation must carry.
    legacy_value: String,
}

impl ClassFilter {
    /// Builds the filter from the cluster's `IngressClass` objects.
    pub fn new(classes: &[Arc<IngressClass>], legacy_value: &str) -> Self {
        let mut ours = HashSet::new();
        let mut known = HashSet::new();
        let mut default_is_ours = false;

        for class in classes {
            let name = class.name_any();
            known.insert(name.clone());

            let is_ours = class
                .spec
                .as_ref()
                .and_then(|s| s.controller.as_deref())
                .is_some_and(|c| c == CONTROLLER_NAME);
            if !is_ours {
                continue;
            }
            default_is_ours |= class
                .annotations()
                .get(ANNOTATION_IS_DEFAULT_CLASS)
                .is_some_and(|v| v.trim().eq_ignore_ascii_case("true"));
            ours.insert(name);
        }

        ClassFilter {
            ours,
            known,
            default_is_ours,
            legacy_value: legacy_value.to_owned(),
        }
    }

    /// Do we manage this Ingress?
    pub fn manages(&self, ingress: &Ingress) -> bool {
        matches!(self.classify(ingress), Claim::Ours(_))
    }

    /// Why we do or do not manage this Ingress.
    pub(crate) fn classify(&self, ingress: &Ingress) -> Claim {
        // The legacy annotation is decisive when present, even if
        // `spec.ingressClassName` says something else. This is not the order
        // the Ingress API documents, but it is what ingress-nginx does, and an
        // object that sets both is asking a compatibility question, not a
        // spec-compliance one.
        if let Some(annotated) = ingress.annotations().get(ANNOTATION_LEGACY_CLASS) {
            return if annotated.trim() == self.legacy_value {
                Claim::Ours(ClaimVia::LegacyAnnotation)
            } else {
                Claim::OtherController
            };
        }

        match ingress.spec.as_ref().and_then(|s| s.ingress_class_name.as_deref()) {
            Some(name) if self.ours.contains(name) => Claim::Ours(ClaimVia::ClassName),
            Some(name) if self.known.contains(name) => Claim::OtherController,
            // A dangling `ingressClassName` is a typo or a missing manifest.
            // Either way the Ingress serves nothing and nobody is told why,
            // which is the failure mode worth a log line.
            Some(_) => Claim::UnknownClass,
            None if self.default_is_ours => Claim::Ours(ClaimVia::DefaultClass),
            None => Claim::NoClass,
        }
    }
}

/// Outcome of the class check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Claim {
    /// Ours, by the given route.
    Ours(ClaimVia),
    /// Explicitly another controller's.
    OtherController,
    /// Names an `IngressClass` that does not exist.
    UnknownClass,
    /// Sets no class, and no default class is ours.
    NoClass,
}

/// Which rule claimed the Ingress. Reported in the rebuild span so an operator
/// can tell a default-class claim from an explicit one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ClaimVia {
    /// `spec.ingressClassName` named one of our classes.
    ClassName,
    /// `kubernetes.io/ingress.class` matched.
    LegacyAnnotation,
    /// No class set, and our class is the cluster default.
    DefaultClass,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::translate::test_support::{in_class, ingress, ingress_class, legacy_class};

    fn filter(classes: Vec<IngressClass>) -> ClassFilter {
        let classes: Vec<Arc<IngressClass>> = classes.into_iter().map(Arc::new).collect();
        ClassFilter::new(&classes, "ramjet")
    }

    #[test]
    fn class_name_pointing_at_our_controller_is_ours() {
        let f = filter(vec![ingress_class("ramjet", CONTROLLER_NAME, false)]);
        let ing = in_class(ingress("default", "web", &[]), "ramjet");
        assert_eq!(f.classify(&ing), Claim::Ours(ClaimVia::ClassName));
    }

    #[test]
    fn class_name_pointing_at_another_controller_is_not_ours() {
        let f = filter(vec![
            ingress_class("nginx", "k8s.io/ingress-nginx", false),
            ingress_class("ramjet", CONTROLLER_NAME, false),
        ]);
        let ing = in_class(ingress("default", "web", &[]), "nginx");
        assert_eq!(f.classify(&ing), Claim::OtherController);
        assert!(!f.manages(&ing));
    }

    #[test]
    fn dangling_class_name_is_distinguished_from_someone_elses() {
        let f = filter(vec![ingress_class("ramjet", CONTROLLER_NAME, false)]);
        let ing = in_class(ingress("default", "web", &[]), "typo");
        assert_eq!(f.classify(&ing), Claim::UnknownClass);
    }

    #[test]
    fn classless_ingress_is_ignored_when_we_are_not_the_default() {
        let f = filter(vec![ingress_class("ramjet", CONTROLLER_NAME, false)]);
        assert_eq!(f.classify(&ingress("default", "web", &[])), Claim::NoClass);
    }

    #[test]
    fn classless_ingress_is_claimed_when_our_class_is_the_default() {
        let f = filter(vec![ingress_class("ramjet", CONTROLLER_NAME, true)]);
        assert_eq!(
            f.classify(&ingress("default", "web", &[])),
            Claim::Ours(ClaimVia::DefaultClass)
        );
    }

    #[test]
    fn another_controllers_default_class_does_not_claim_for_us() {
        let f = filter(vec![
            ingress_class("nginx", "k8s.io/ingress-nginx", true),
            ingress_class("ramjet", CONTROLLER_NAME, false),
        ]);
        assert_eq!(f.classify(&ingress("default", "web", &[])), Claim::NoClass);
    }

    #[test]
    fn legacy_annotation_claims_the_ingress() {
        let f = filter(vec![]);
        let ing = legacy_class(ingress("default", "web", &[]), "ramjet");
        assert_eq!(f.classify(&ing), Claim::Ours(ClaimVia::LegacyAnnotation));
    }

    #[test]
    fn legacy_annotation_wins_over_class_name() {
        let f = filter(vec![ingress_class("ramjet", CONTROLLER_NAME, false)]);

        // Annotation says someone else, class name says us: annotation wins.
        let ing = legacy_class(in_class(ingress("default", "web", &[]), "ramjet"), "nginx");
        assert_eq!(f.classify(&ing), Claim::OtherController);

        // And the other way round.
        let ing = legacy_class(in_class(ingress("default", "web", &[]), "nginx"), "ramjet");
        assert_eq!(f.classify(&ing), Claim::Ours(ClaimVia::LegacyAnnotation));
    }

    #[test]
    fn legacy_annotation_value_is_configurable() {
        let classes: Vec<Arc<IngressClass>> = Vec::new();
        let f = ClassFilter::new(&classes, "edge");
        let ing = legacy_class(ingress("default", "web", &[]), "edge");
        assert!(f.manages(&ing));
    }

    #[test]
    fn an_ingress_class_without_a_controller_field_claims_nothing() {
        let mut class = ingress_class("ramjet", CONTROLLER_NAME, true);
        class.spec = None;
        let f = filter(vec![class]);
        assert_eq!(f.classify(&ingress("default", "web", &[])), Claim::NoClass);
        let ing = in_class(ingress("default", "web", &[]), "ramjet");
        assert_eq!(f.classify(&ing), Claim::OtherController);
    }
}
