//! Kubernetes Ingress path semantics.
//!
//! The `Prefix` rule is the one everybody gets wrong. It is **not** a string
//! prefix: it matches whole path elements, so `/foo` matches `/foo` and
//! `/foo/bar` but must not match `/foobar`. The upstream spec's own table is
//! reproduced in the tests at the bottom of this file, case for case.

use crate::backend::BackendId;
use crate::canary::CanarySpec;
use crate::mirror::MirrorSpec;
use regex::Regex;

/// How a rule's path is compared against the request path.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PathType {
    /// Byte-for-byte equality. `/foo` does not match `/foo/`.
    Exact,
    /// Element-wise path-segment prefix, per the Ingress spec.
    Prefix,
    /// Controller-defined. We follow ingress-nginx and treat the path as a
    /// regular expression, anchored at the start of the request path.
    ImplementationSpecific,
}

impl PathType {
    /// Precedence class, low value wins. Exact beats Prefix beats regex,
    /// which is the order ingress-nginx resolves locations in.
    pub(crate) fn rank(self) -> u8 {
        match self {
            PathType::Exact => 0,
            PathType::Prefix => 1,
            PathType::ImplementationSpecific => 2,
        }
    }

    /// The spelling the Ingress API uses, which is also what the admin API
    /// reports.
    pub fn as_str(self) -> &'static str {
        match self {
            PathType::Exact => "Exact",
            PathType::Prefix => "Prefix",
            PathType::ImplementationSpecific => "ImplementationSpecific",
        }
    }
}

/// Normalizes a `Prefix` path into the number of leading bytes that must match.
///
/// Trailing slashes carry no meaning for a prefix rule (`/aaa/bbb/` and
/// `/aaa/bbb` select the same subtree), and the root prefix `/` matches
/// everything. Folding both into a single length lets [`prefix_matches`] run
/// without a special case: `/` becomes a match length of zero, so the
/// "next byte is a separator" test lands on the request path's own leading
/// slash and always succeeds.
pub(crate) fn prefix_match_len(path: &str) -> usize {
    let trimmed = path.trim_end_matches('/');
    if trimmed.is_empty() {
        0
    } else {
        trimmed.len()
    }
}

/// Element-wise prefix test.
///
/// `match_len` comes from [`prefix_match_len`] and is always `<=
/// rule_path.len()`, so the rule-side slice cannot be out of range.
#[inline]
pub(crate) fn prefix_matches(rule_path: &str, match_len: usize, path: &str) -> bool {
    let p = path.as_bytes();
    let r = rule_path.as_bytes();
    match (p.get(..match_len), r.get(..match_len)) {
        (Some(head), Some(rule)) if head == rule => {
            // Either the paths ended together, or the request continues into a
            // new segment. `/foobar` against `/foo` stops here: the byte after
            // the prefix is `b`, not `/`.
            p.len() == match_len || p.get(match_len) == Some(&b'/')
        }
        _ => false,
    }
}

/// The optional behaviour a rule can carry, behind one pointer.
///
/// A canary and a mirror are both rare and both boxed for the same reason: the
/// rules of a host are scanned linearly, so [`PathRule`]'s size decides how
/// many of them arrive in a cache line together. What makes this *one* box
/// rather than two is that the rarity compounds — giving every rule in a
/// ten-thousand route table a second null pointer, to describe the handful that
/// have a mirror, is the wrong side of that trade. A rule with neither pays
/// nothing; a rule with either pays one allocation it was going to make anyway.
pub(crate) struct RuleExtras {
    pub canary: Option<CanarySpec>,
    pub mirror: Option<MirrorSpec>,
}

impl RuleExtras {
    /// `None` when neither half is present, so a plain rule keeps a null
    /// pointer rather than an allocation holding two `None`s.
    pub(crate) fn build(
        canary: Option<CanarySpec>,
        mirror: Option<MirrorSpec>,
    ) -> Option<Box<Self>> {
        if canary.is_none() && mirror.is_none() {
            return None;
        }
        Some(Box::new(RuleExtras { canary, mirror }))
    }
}

/// One path rule inside a [`VirtualHost`](crate::VirtualHost).
///
/// Rules are stored pre-sorted into their final precedence order, so matching
/// is a linear scan that returns the first hit. Nothing here is mutated after
/// the table is built.
pub struct PathRule {
    path: Box<str>,
    /// Precomputed for `Prefix`; unused by the other kinds.
    match_len: u32,
    path_type: PathType,
    /// `Some` exactly when `path_type` is `ImplementationSpecific`. Boxed to
    /// keep the common Exact/Prefix rule small.
    regex: Option<Box<Regex>>,
    backend: BackendId,
    /// Index into this table's [`RouteStats`](crate::RouteStats). Assigned
    /// after the precedence sort, because that is the order the rules end up
    /// in; counters survive a rebuild by identity, not by index.
    stats_index: u32,
    /// The canary and the mirror, if either is configured. See [`RuleExtras`].
    extras: Option<Box<RuleExtras>>,
}

impl PathRule {
    pub(crate) fn new(
        path: Box<str>,
        path_type: PathType,
        regex: Option<Box<Regex>>,
        backend: BackendId,
        extras: Option<Box<RuleExtras>>,
    ) -> Self {
        let match_len = match path_type {
            PathType::Prefix => prefix_match_len(&path) as u32,
            _ => 0,
        };
        PathRule {
            path,
            match_len,
            path_type,
            regex,
            backend,
            stats_index: 0,
            extras,
        }
    }

    /// Assigns the rule's place in the table's counter slab.
    pub(crate) fn set_stats_index(&mut self, index: u32) {
        self.stats_index = index;
    }

    /// Does this rule match `path`?
    #[inline]
    pub(crate) fn matches(&self, path: &str) -> bool {
        match self.path_type {
            PathType::Exact => path == &*self.path,
            PathType::Prefix => prefix_matches(&self.path, self.match_len as usize, path),
            PathType::ImplementationSpecific => {
                self.regex.as_ref().is_some_and(|re| re.is_match(path))
            }
        }
    }

    /// Sort key giving the precedence order: Exact first, then Prefix from
    /// longest to shortest, then regex. Applied with a stable sort so regex
    /// rules keep the order the controller supplied them in.
    pub(crate) fn sort_key(&self) -> (u8, std::cmp::Reverse<u32>) {
        (self.path_type.rank(), std::cmp::Reverse(self.match_len))
    }

    /// The rule's path as configured.
    pub fn path(&self) -> &str {
        &self.path
    }

    /// How this rule compares paths.
    pub fn path_type(&self) -> PathType {
        self.path_type
    }

    /// The backend this rule routes to when no canary diverts the request.
    pub fn backend(&self) -> BackendId {
        self.backend
    }

    /// This rule's index into the table's
    /// [`RouteStats`](crate::RouteStats).
    pub fn stats_index(&self) -> u32 {
        self.stats_index
    }

    /// The canary attached to this route, if any.
    pub fn canary(&self) -> Option<&CanarySpec> {
        self.extras.as_ref()?.canary.as_ref()
    }

    /// The mirror attached to this route, if any.
    pub fn mirror(&self) -> Option<&MirrorSpec> {
        self.extras.as_ref()?.mirror.as_ref()
    }
}

impl std::fmt::Debug for PathRule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PathRule")
            .field("path", &self.path)
            .field("path_type", &self.path_type)
            .field("backend", &self.backend)
            .field("canary", &self.canary().is_some())
            .field("mirror", &self.mirror().is_some())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pfx(rule: &str, path: &str) -> bool {
        prefix_matches(rule, prefix_match_len(rule), path)
    }

    /// The example table from the Kubernetes Ingress documentation, verbatim.
    #[test]
    fn spec_table() {
        // Prefix "/" matches everything.
        assert!(pfx("/", "/"));
        assert!(pfx("/", "/anything"));
        assert!(pfx("/", "/deeply/nested/path"));

        // Prefix "/foo" matches "/foo" and "/foo/".
        assert!(pfx("/foo", "/foo"));
        assert!(pfx("/foo", "/foo/"));

        // Prefix "/foo/" matches "/foo" and "/foo/" -- trailing slash ignored.
        assert!(pfx("/foo/", "/foo"));
        assert!(pfx("/foo/", "/foo/"));

        // "/aaa/bb" does NOT match "/aaa/bbb".
        assert!(!pfx("/aaa/bb", "/aaa/bbb"));
        assert!(pfx("/aaa/bbb", "/aaa/bbb"));

        // Trailing slash on either side is ignored / matched.
        assert!(pfx("/aaa/bbb/", "/aaa/bbb"));
        assert!(pfx("/aaa/bbb", "/aaa/bbb/"));

        // Subpaths match.
        assert!(pfx("/aaa/bbb", "/aaa/bbb/ccc"));

        // String prefixes that are not element prefixes do NOT match.
        assert!(!pfx("/aaa/bbb", "/aaa/bbbxyz"));
    }

    /// The trap this whole module exists for.
    #[test]
    fn foo_does_not_match_foobar() {
        assert!(!pfx("/foo", "/foobar"));
        assert!(!pfx("/foo", "/foo-bar"));
        assert!(!pfx("/foo", "/foobar/baz"));
        // ...but the segment boundary version does.
        assert!(pfx("/foo", "/foo/bar"));
    }

    #[test]
    fn shorter_request_never_matches_longer_prefix() {
        assert!(!pfx("/foo/bar", "/foo"));
        assert!(!pfx("/foo", "/fo"));
        assert!(!pfx("/foo", ""));
    }

    #[test]
    fn repeated_trailing_slashes_collapse() {
        assert_eq!(prefix_match_len("/foo///"), 4);
        assert_eq!(prefix_match_len("/"), 0);
        assert_eq!(prefix_match_len("///"), 0);
        assert!(pfx("/foo///", "/foo/bar"));
    }

    #[test]
    fn root_prefix_has_zero_match_len() {
        // The zero length is load-bearing: it makes the separator check read
        // the request's own leading slash instead of needing a branch.
        assert_eq!(prefix_match_len("/"), 0);
        assert!(pfx("/", "/x"));
    }

    #[test]
    fn precedence_rank_order() {
        assert!(PathType::Exact.rank() < PathType::Prefix.rank());
        assert!(PathType::Prefix.rank() < PathType::ImplementationSpecific.rank());
    }
}
