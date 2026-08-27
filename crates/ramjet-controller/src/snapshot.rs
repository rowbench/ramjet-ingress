//! A point-in-time view of every watched object.
//!
//! This is the seam that keeps [`translate`](crate::translate) pure. The watch
//! loop reads the reflector stores into a `ClusterSnapshot` and hands it over;
//! tests construct one by hand. Neither path can tell the difference, which is
//! why the interesting rules in this crate are testable without a cluster.
//!
//! Objects are held behind `Arc` because that is what
//! [`Store::state`](kube::runtime::reflector::Store::state) already hands back;
//! taking a snapshot is a pointer copy per object, not a deep clone of the
//! cluster.

use std::sync::Arc;

use k8s_openapi::api::core::v1::{Secret, Service};
use k8s_openapi::api::discovery::v1::EndpointSlice;
use k8s_openapi::api::networking::v1::{Ingress, IngressClass};

/// Everything the translator is allowed to look at.
#[derive(Debug, Clone, Default)]
pub struct ClusterSnapshot {
    /// Every Ingress in scope, managed or not. Class filtering happens in
    /// [`translate`](crate::translate), not here, so a test can assert on what
    /// filtering rejects.
    pub ingresses: Vec<Arc<Ingress>>,
    /// Every IngressClass in the cluster; cluster-scoped, so never filtered by
    /// namespace.
    pub ingress_classes: Vec<Arc<IngressClass>>,
    /// Services, for port resolution.
    pub services: Vec<Arc<Service>>,
    /// EndpointSlices, for address resolution.
    pub endpoint_slices: Vec<Arc<EndpointSlice>>,
    /// TLS Secrets.
    pub secrets: Vec<Arc<Secret>>,
}

impl ClusterSnapshot {
    /// An empty snapshot.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds an Ingress. Chainable, for building fixtures.
    #[must_use]
    pub fn with_ingress(mut self, ingress: Ingress) -> Self {
        self.ingresses.push(Arc::new(ingress));
        self
    }

    /// Adds an IngressClass.
    #[must_use]
    pub fn with_ingress_class(mut self, class: IngressClass) -> Self {
        self.ingress_classes.push(Arc::new(class));
        self
    }

    /// Adds a Service.
    #[must_use]
    pub fn with_service(mut self, service: Service) -> Self {
        self.services.push(Arc::new(service));
        self
    }

    /// Adds an EndpointSlice.
    #[must_use]
    pub fn with_endpoint_slice(mut self, slice: EndpointSlice) -> Self {
        self.endpoint_slices.push(Arc::new(slice));
        self
    }

    /// Adds a Secret.
    #[must_use]
    pub fn with_secret(mut self, secret: Secret) -> Self {
        self.secrets.push(Arc::new(secret));
        self
    }

    /// Object counts, for the rebuild span.
    pub(crate) fn counts(&self) -> SnapshotCounts {
        SnapshotCounts {
            ingresses: self.ingresses.len(),
            ingress_classes: self.ingress_classes.len(),
            services: self.services.len(),
            endpoint_slices: self.endpoint_slices.len(),
            secrets: self.secrets.len(),
        }
    }
}

/// How much the translator was handed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SnapshotCounts {
    pub(crate) ingresses: usize,
    pub(crate) ingress_classes: usize,
    pub(crate) services: usize,
    pub(crate) endpoint_slices: usize,
    pub(crate) secrets: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::translate::test_support::ingress;

    #[test]
    fn builders_accumulate() {
        let snap = ClusterSnapshot::new()
            .with_ingress(ingress("default", "web", &[]))
            .with_ingress(ingress("default", "api", &[]));
        assert_eq!(snap.counts().ingresses, 2);
        assert_eq!(snap.counts().services, 0);
    }
}
