//! Fire-and-forget request mirroring.
//!
//! A route carrying `ramjet.dev/mirror-backend` sends a second copy of each
//! sampled request to a shadow backend and throws the answer away. The point is
//! to put production traffic through a rewrite, a new version, or a new cluster
//! before any of them are responsible for a user's response.
//!
//! # The invariant, and everything it costs
//!
//! **A mirror must never make the primary request slower or more likely to
//! fail.** That is not a goal, it is the property that decides whether this
//! feature can be turned on in front of real traffic at all, and every design
//! choice below is downstream of it:
//!
//! - **Nothing is awaited.** The request path hands the copy to a queue and
//!   returns. It never waits for a connection, a response, or a timeout.
//! - **The queue is bounded and drops.** One [`mpsc::channel`] per serving
//!   runtime, [`MIRROR_QUEUE_DEPTH`] deep, entered with `try_send`. A mirror
//!   backend that cannot keep up fills the queue and the overflow is counted
//!   and discarded — the alternative, an unbounded queue, converts a slow
//!   shadow into unbounded memory growth on the pod serving production.
//! - **Responses are drained and discarded.** Draining rather than dropping so
//!   the upstream connection goes back to the pool instead of being closed;
//!   discarded because there is nothing here that could act on it.
//! - **Failures are counted, never propagated.** A mirror backend that is down,
//!   refusing, or absent produces a number on `/metrics` and nothing else.
//! - **No in-flight accounting.** A mirrored request does not take the
//!   `LeastConn` guard its primary does. The guard borrows out of the route
//!   table and cannot cross the queue, and letting shadow traffic move
//!   production's load-balancing decisions would be its own kind of leak.
//!
//! # The body is the hard part
//!
//! Everything above is cheap because a request head is small and already in
//! memory. A body is neither. Replaying one means having kept it, and this
//! crate's entire position on buffering request bodies is that it does not do
//! it — see [`body`](crate::body).
//!
//! So the cap is real and small. A request whose body is *known* to be empty —
//! every `GET`, `HEAD`, `OPTIONS` and `DELETE`, which is the overwhelming
//! majority of ingress traffic — is mirrored with no buffering at all and keeps
//! its endpoint failover. A request with a body is read up to
//! `--mirror-max-body`; if it fits, both copies get it, and if it does not, the
//! bytes already read become a prefix on the primary's body, the rest keeps
//! streaming, and the mirror is skipped and counted. The primary is never held
//! waiting for more than the cap, and never fails because of the attempt.

use std::time::Duration;

use bytes::{Bytes, BytesMut};
use http::header::{HeaderName, HeaderValue};
use http::request::Parts;
use http::Request;
use http_body_util::BodyExt;
use tokio::sync::mpsc;
use tracing::debug;

use crate::body::ProxyBody;
use crate::metrics::Metrics;
use crate::upstream::Upstream;

/// Marks a request as a copy, so a shadow backend can tell one from the real
/// thing — in its own logs, and in any write it was about to make.
pub const MIRRORED_BY: HeaderName = HeaderName::from_static("x-mirrored-by");

/// The value of [`MIRRORED_BY`].
pub const MIRRORED_BY_VALUE: HeaderValue = HeaderValue::from_static("ramjet-ingress");

/// Largest request body copied to a mirror, by default.
///
/// 256 KiB is above essentially every API request and JSON payload and below
/// every upload. The cost of the cap being too low is a skipped mirror; the
/// cost of it being too high is memory on the pod serving production, held for
/// as long as the slowest client takes to finish its upload.
pub const DEFAULT_MIRROR_MAX_BODY: usize = 256 * 1024;

/// Copies one serving runtime will hold before it starts dropping them.
///
/// Per-runtime rather than per-process, so the bound scales with the cores
/// doing the work and no two runtimes contend on the same queue. Deep enough to
/// absorb a burst, shallow enough that a mirror backend which has stopped
/// answering costs a bounded amount of memory rather than a growing one.
pub const MIRROR_QUEUE_DEPTH: usize = 256;

/// How long one mirrored exchange gets, in total.
///
/// Fixed rather than configurable, and much shorter than the primary's
/// `--response-timeout`: nobody is waiting for this answer, so the only thing
/// the deadline protects is the queue behind it. A mirror slow enough to matter
/// here is one whose copies should be dropped anyway.
const MIRROR_TIMEOUT: Duration = Duration::from_secs(5);

/// One copy, fully formed and ready to send.
///
/// The URI is already absolute and already points at a chosen endpoint: the
/// route table was loaded on the request path and cannot be reached from the
/// worker, so every decision that needs it is made before the job is queued.
struct MirrorJob {
    parts: Parts,
    body: Bytes,
}

/// The queue in front of one serving runtime's mirror worker.
#[derive(Debug, Clone)]
pub struct Mirror {
    jobs: mpsc::Sender<MirrorJob>,
    max_body: usize,
}

impl Mirror {
    /// Starts the worker for one serving runtime.
    ///
    /// Must be called from inside that runtime: the worker is a task, and the
    /// whole point is that it runs on the same threads the traffic does rather
    /// than on a shared pool everything else would then queue behind.
    pub fn spawn(upstream: Upstream, metrics: std::sync::Arc<Metrics>) -> Self {
        let (jobs, mut rx) = mpsc::channel::<MirrorJob>(MIRROR_QUEUE_DEPTH);

        tokio::spawn(async move {
            // Serial, and deliberately: the queue is the bound, so a worker
            // that spawned a task per job would replace a bound that drops
            // with one that does not. `MIRROR_TIMEOUT` is what keeps one slow
            // mirror from holding the line for long.
            while let Some(job) = rx.recv().await {
                let request = Request::from_parts(job.parts, ProxyBody::once(job.body));
                match tokio::time::timeout(MIRROR_TIMEOUT, exchange(&upstream, request)).await {
                    Ok(Ok(())) => metrics.record_mirrored(),
                    Ok(Err(error)) => {
                        metrics.record_mirror_failure();
                        debug!(%error, "mirror backend did not accept a copy");
                    }
                    Err(_) => {
                        metrics.record_mirror_failure();
                        debug!(
                            timeout_secs = MIRROR_TIMEOUT.as_secs(),
                            "mirror backend did not answer in time"
                        );
                    }
                }
            }
        });

        Mirror {
            jobs,
            max_body: DEFAULT_MIRROR_MAX_BODY,
        }
    }

    /// Sets the largest body this runtime will buffer in order to mirror it.
    pub fn with_max_body(mut self, max_body: usize) -> Self {
        self.max_body = max_body;
        self
    }

    /// The body cap in force.
    pub fn max_body(&self) -> usize {
        self.max_body
    }

    /// Queues one copy, or counts it as dropped.
    ///
    /// Never blocks and never fails: `try_send` on a full queue is the drop,
    /// and a closed queue means the runtime is shutting down, which is not a
    /// condition the request path should learn about.
    pub fn enqueue(&self, metrics: &Metrics, parts: Parts, body: Bytes) {
        if self.jobs.try_send(MirrorJob { parts, body }).is_err() {
            metrics.record_mirror_dropped();
        }
    }
}

/// Sends one copy and drains whatever comes back.
async fn exchange(
    upstream: &Upstream,
    request: Request<ProxyBody>,
) -> Result<(), crate::upstream::UpstreamError> {
    let response = upstream.send(request).await?;
    // Read to the end rather than dropping the body: an unread `Incoming` makes
    // hyper close the connection instead of returning it to the pool, which
    // would put a TCP handshake on every single mirrored request.
    let mut body = response.into_body();
    while let Some(frame) = body.frame().await {
        if frame.is_err() {
            break;
        }
    }
    Ok(())
}

/// What reading a request body for mirroring produced.
pub enum Buffered {
    /// The whole body, within the cap. Both copies get these bytes.
    Complete(Bytes),
    /// The body was larger than the cap, or carried something that is not data.
    ///
    /// The bytes already read, and the rest of the stream. The primary gets
    /// both halves — see [`ProxyBody::prefixed`] — and the mirror is skipped.
    TooLarge(Bytes, ProxyBody),
}

/// Reads `body` up to `cap` bytes.
///
/// Stops at the first frame that is not data — a trailer block — rather than
/// dropping it: trailers on a request are rare, and silently removing part of
/// the request the client sent in order to make a *copy* of it would be exactly
/// the wrong trade. The whole thing goes down the streaming path instead.
pub async fn buffer(mut body: ProxyBody, cap: usize) -> Buffered {
    let mut buffered = BytesMut::new();
    loop {
        let Some(frame) = body.frame().await else {
            return Buffered::Complete(buffered.freeze());
        };
        // A body that errors mid-read is a request the primary is going to fail
        // anyway. Handing back what was read plus the stream that produced the
        // error keeps that failure exactly where it was.
        let Ok(frame) = frame else {
            return Buffered::TooLarge(buffered.freeze(), body);
        };
        let Ok(data) = frame.into_data() else {
            return Buffered::TooLarge(buffered.freeze(), body);
        };
        buffered.extend_from_slice(&data);
        if buffered.len() > cap {
            return Buffered::TooLarge(buffered.freeze(), body);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_marker_header_names_this_controller() {
        // A shadow backend has to be able to tell a copy from the real thing
        // before it decides whether to charge somebody's card.
        assert_eq!(MIRRORED_BY.as_str(), "x-mirrored-by");
        assert_eq!(MIRRORED_BY_VALUE.to_str().ok(), Some("ramjet-ingress"));
    }

    #[test]
    fn the_defaults_bound_memory_rather_than_coverage() {
        assert_eq!(DEFAULT_MIRROR_MAX_BODY, 256 * 1024);
        assert_eq!(MIRROR_QUEUE_DEPTH, 256);
        assert!(
            MIRROR_TIMEOUT < Duration::from_secs(60),
            "a mirror must not get the primary's response budget"
        );
    }

    /// The drop, without a runtime to send into: a queue whose worker is gone
    /// must count and continue rather than fail the caller.
    #[tokio::test]
    async fn a_closed_queue_counts_a_drop_and_returns() {
        let (jobs, rx) = mpsc::channel::<MirrorJob>(1);
        drop(rx);
        let mirror = Mirror {
            jobs,
            max_body: DEFAULT_MIRROR_MAX_BODY,
        };
        let metrics = Metrics::new();

        let (parts, _) = Request::new(()).into_parts();
        mirror.enqueue(&metrics, parts, Bytes::new());
        assert_eq!(metrics.mirror_dropped(), 1);
    }

    #[tokio::test]
    async fn a_full_queue_drops_rather_than_waiting() {
        // The invariant the bound exists for: the request path must return at
        // the same speed whether or not the mirror is keeping up.
        let (jobs, _rx) = mpsc::channel::<MirrorJob>(1);
        let mirror = Mirror {
            jobs,
            max_body: DEFAULT_MIRROR_MAX_BODY,
        };
        let metrics = Metrics::new();

        for _ in 0..4 {
            let (parts, _) = Request::new(()).into_parts();
            mirror.enqueue(&metrics, parts, Bytes::new());
        }
        assert_eq!(metrics.mirrored(), 0, "nothing was sent, only queued");
        assert_eq!(
            metrics.mirror_dropped(),
            3,
            "one fits in the queue and the rest are dropped"
        );
    }

    #[test]
    fn the_body_cap_is_settable_per_runtime() {
        let (jobs, _rx) = mpsc::channel::<MirrorJob>(1);
        let mirror = Mirror {
            jobs,
            max_body: DEFAULT_MIRROR_MAX_BODY,
        }
        .with_max_body(4096);
        assert_eq!(mirror.max_body(), 4096);
    }
}
