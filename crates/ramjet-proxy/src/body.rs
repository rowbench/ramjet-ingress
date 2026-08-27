//! The one body type that flows in both directions.
//!
//! A proxy handles exactly three kinds of body: a stream borrowed from the
//! other side of the connection, a small constant this crate generated itself
//! (an error page), and nothing at all. [`ProxyBody`] is those three variants
//! and no more.
//!
//! The alternative — `BoxBody<Bytes, E>` — is what most hyper proxies reach
//! for, and it costs a heap allocation and a vtable dispatch per body frame for
//! a set of cases that is closed and three elements long. An enum with a
//! projected `Incoming` costs neither.
//!
//! # Streaming
//!
//! Nothing here buffers. `poll_frame` on the `Stream` variant is a straight
//! delegation to `Incoming`, so a response passes through frame by frame at
//! whatever rate the slower of the two connections can take it, with hyper's
//! flow control applying backpressure across the proxy. A 4GB download moves
//! through a few tens of kilobytes of buffer, and the process memory of an
//! ingress replica does not depend on what any client happens to be
//! downloading. This is the behaviour ingress-nginx gets by *disabling*
//! `proxy_buffering`; here there is no other mode to be in.

use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use http_body::{Body, Frame, SizeHint};
use hyper::body::Incoming;

pin_project_lite::pin_project! {
    #[project = InnerProj]
    enum Inner {
        Empty,
        Once { data: Option<Bytes> },
        Stream { #[pin] body: Incoming },
    }
}

pin_project_lite::pin_project! {
    /// A request or response body moving through the proxy.
    pub struct ProxyBody {
        #[pin]
        inner: Inner,
    }
}

impl ProxyBody {
    /// A body with no frames at all.
    pub fn empty() -> Self {
        ProxyBody {
            inner: Inner::Empty,
        }
    }

    /// A body consisting of one already-known chunk.
    ///
    /// Every caller in this crate passes a `Bytes::from_static`, so an error
    /// response allocates nothing for its body.
    pub fn once(data: Bytes) -> Self {
        if data.is_empty() {
            return Self::empty();
        }
        ProxyBody {
            inner: Inner::Once { data: Some(data) },
        }
    }

    /// A body streamed from the peer on the other side of the proxy.
    pub fn stream(body: Incoming) -> Self {
        ProxyBody {
            inner: Inner::Stream { body },
        }
    }

    /// Whether this body is known, without reading it, to carry no data.
    ///
    /// This is the question the retry logic asks: a request body that is known
    /// to be empty can be reconstructed for a second endpoint, and one that is
    /// streaming cannot be without buffering it. See
    /// [`forward`](crate::forward).
    pub fn is_known_empty(&self) -> bool {
        match &self.inner {
            Inner::Empty => true,
            Inner::Once { data } => data.as_ref().is_none_or(Bytes::is_empty),
            Inner::Stream { body } => body.size_hint().exact() == Some(0),
        }
    }
}

impl std::fmt::Debug for ProxyBody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let kind = match &self.inner {
            Inner::Empty => "Empty",
            Inner::Once { .. } => "Once",
            Inner::Stream { .. } => "Stream",
        };
        f.debug_tuple("ProxyBody").field(&kind).finish()
    }
}

impl Body for ProxyBody {
    type Data = Bytes;
    /// `hyper::Error` because the `Stream` variant produces them and the other
    /// two produce nothing. There is no case where this crate needs to invent
    /// one.
    type Error = hyper::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        match self.project().inner.project() {
            InnerProj::Empty => Poll::Ready(None),
            InnerProj::Once { data } => Poll::Ready(data.take().map(|d| Ok(Frame::data(d)))),
            InnerProj::Stream { body } => body.poll_frame(cx),
        }
    }

    fn is_end_stream(&self) -> bool {
        match &self.inner {
            Inner::Empty => true,
            Inner::Once { data } => data.is_none(),
            Inner::Stream { body } => body.is_end_stream(),
        }
    }

    fn size_hint(&self) -> SizeHint {
        match &self.inner {
            Inner::Empty => SizeHint::with_exact(0),
            Inner::Once { data } => {
                SizeHint::with_exact(data.as_ref().map_or(0, |d| d.len() as u64))
            }
            Inner::Stream { body } => body.size_hint(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::BodyExt;

    #[tokio::test]
    async fn empty_yields_no_frames() {
        let body = ProxyBody::empty();
        assert!(body.is_end_stream());
        assert_eq!(body.size_hint().exact(), Some(0));
        assert!(body.is_known_empty());
        let collected = body.collect().await.expect("collects").to_bytes();
        assert!(collected.is_empty());
    }

    #[tokio::test]
    async fn once_yields_exactly_one_frame() {
        let body = ProxyBody::once(Bytes::from_static(b"hello"));
        assert!(!body.is_end_stream());
        assert_eq!(body.size_hint().exact(), Some(5));
        assert!(!body.is_known_empty());
        let collected = body.collect().await.expect("collects").to_bytes();
        assert_eq!(&collected[..], b"hello");
    }

    #[test]
    fn an_empty_chunk_collapses_to_empty() {
        // Otherwise a zero-length error body would advertise a frame that
        // never carries anything, and hyper would emit a stray chunk for it.
        assert!(ProxyBody::once(Bytes::new()).is_end_stream());
    }
}
