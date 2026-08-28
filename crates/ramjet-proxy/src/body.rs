//! The one body type that flows in both directions.
//!
//! A proxy handles a small and closed set of bodies: a stream borrowed from the
//! other side of the connection, a small constant this crate generated itself
//! (an error page), and nothing at all. [`ProxyBody`] is those cases and no
//! more.
//!
//! The alternative — `BoxBody<Bytes, E>` — is what most hyper proxies reach
//! for, and it costs a heap allocation and a vtable dispatch per body frame for
//! a set of cases that is closed and short. An enum with a projected `Incoming`
//! costs neither.
//!
//! # Two kinds of borrowed stream
//!
//! A borrowed stream is nearly always hyper's `Incoming`, and that variant is
//! held inline and projected. An HTTP/3 request body is the exception: it comes
//! off a QUIC stream, which hyper knows nothing about, so it is a variant of its
//! own — and a boxed one, deliberately. An enum is as large as its largest
//! variant, and charging every HTTP/1.1 and HTTP/2 body on the hot path for the
//! size of a QUIC request stream, to serve an experimental listener that is off
//! by default, is the wrong way round. Boxing costs one allocation per HTTP/3
//! request that actually carries a body, and nothing at all otherwise.
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
        Http3 { data: Option<Bytes>, body: Box<crate::http3::RequestBody> },
    }
}

/// Why a body stopped before it was complete.
///
/// Two variants because there are two ways a body can arrive: over a hyper
/// connection, and — for a request only — over a QUIC stream. `hyper::Error`
/// cannot be constructed outside hyper, so an HTTP/3 stream failure has nowhere
/// to go without this type.
///
/// It matters that these are *reported* rather than turned into a clean end of
/// stream. A request body that stops early has to fail the upstream exchange;
/// silently ending it would hand the origin a truncated request that it has no
/// way to tell from a complete one.
#[derive(Debug)]
pub enum BodyError {
    /// The hyper connection carrying the body failed.
    Http(hyper::Error),
    /// The HTTP/3 request stream carrying the body failed.
    Http3(h3::error::StreamError),
}

impl std::fmt::Display for BodyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BodyError::Http(error) => write!(f, "body stream failed: {error}"),
            BodyError::Http3(error) => write!(f, "HTTP/3 request stream failed: {error}"),
        }
    }
}

impl std::error::Error for BodyError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            BodyError::Http(error) => Some(error),
            BodyError::Http3(error) => Some(error),
        }
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

    /// A request body arriving as HTTP/3 DATA frames on a QUIC stream.
    ///
    /// Boxed; see the module docs for why the HTTP/1.1 and HTTP/2 paths should
    /// not pay for the size of this one.
    pub(crate) fn http3(body: crate::http3::RequestBody) -> Self {
        ProxyBody {
            inner: Inner::Http3 {
                data: None,
                body: Box::new(body),
            },
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
            Inner::Http3 { data: Some(_), .. } => false,
            Inner::Http3 { data: None, body } => body.is_known_empty(),
        }
    }
}

impl std::fmt::Debug for ProxyBody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let kind = match &self.inner {
            Inner::Empty => "Empty",
            Inner::Once { .. } => "Once",
            Inner::Stream { .. } => "Stream",
            Inner::Http3 { .. } => "Http3",
        };
        f.debug_tuple("ProxyBody").field(&kind).finish()
    }
}

impl Body for ProxyBody {
    type Data = Bytes;
    /// The two ways a borrowed stream can fail; see [`BodyError`]. The
    /// constant and empty variants produce nothing, so there is no case where
    /// this crate has to invent an error.
    type Error = BodyError;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        match self.project().inner.project() {
            InnerProj::Empty => Poll::Ready(None),
            InnerProj::Once { data } => Poll::Ready(data.take().map(|d| Ok(Frame::data(d)))),
            InnerProj::Stream { body } => hyper_frame(body.poll_frame(cx)),
            InnerProj::Http3 { data, body } => match data.take() {
                Some(prefix) => Poll::Ready(Some(Ok(Frame::data(prefix)))),
                None => body.poll_frame(cx),
            },
        }
    }

    fn is_end_stream(&self) -> bool {
        match &self.inner {
            Inner::Empty => true,
            Inner::Once { data } => data.is_none(),
            Inner::Stream { body } => body.is_end_stream(),
            Inner::Http3 { data, body } => data.is_none() && body.is_end_stream(),
        }
    }

    fn size_hint(&self) -> SizeHint {
        match &self.inner {
            Inner::Empty => SizeHint::with_exact(0),
            Inner::Once { data } => {
                SizeHint::with_exact(data.as_ref().map_or(0, |d| d.len() as u64))
            }
            Inner::Stream { body } => body.size_hint(),
            Inner::Http3 { data, body } => {
                // The held chunk has already been taken off `body`'s hint, so
                // it has to be added back: hyper decides the upstream framing
                // from this, and a hint that undercounted by it would describe
                // a shorter request than the one being sent.
                let held = data.as_ref().map_or(0, |d| d.len() as u64);
                let rest = body.size_hint();
                let mut hint = SizeHint::new();
                hint.set_lower(rest.lower().saturating_add(held));
                if let Some(upper) = rest.upper() {
                    hint.set_upper(upper.saturating_add(held));
                }
                hint
            }
        }
    }
}

/// Retags a hyper body's frame with this crate's error type.
fn hyper_frame(
    poll: Poll<Option<Result<Frame<Bytes>, hyper::Error>>>,
) -> Poll<Option<Result<Frame<Bytes>, BodyError>>> {
    poll.map(|frame| frame.map(|result| result.map_err(BodyError::Http)))
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
