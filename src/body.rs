//! What an overlay does with a **streamed body** (ledger #2263): every overlay forwards
//! [`Endpoint::invoke_body`], so a body passes through it as the endpoint below answered
//! it, and never collected into a buffer behind the caller's back (core's default
//! `invoke_body` wraps `invoke`, which would).
//!
//! A buffer ([`Body::Bytes`]) and a body that can be read again ([`Body::Replay`]) are
//! complete when they are handed over: the work that made them is done, and a replay may
//! be stored and read long after. A governor treats either as it treats a bytes answer. A
//! one-shot body ([`Body::Once`]) is still in flight when it is handed over, so a governor
//! that cares how the work ENDS (a breaker's count, a bulkhead's slot) hears it at the
//! body's end: [`on_end`].

use std::pin::Pin;
use std::task::{Context, Poll};

use futures::Stream;
use ikigai_core::{Body, ByteStream, Bytes, Endpoint, Error, Invocation, Representation};

/// The two faces an endpoint answers on, so a governor that does the same thing before
/// either (a rate check, a retry loop, a failover walk) is written once.
#[async_trait::async_trait]
pub(crate) trait Face: Sized + Send {
    /// Invoke `endpoint` on this face.
    async fn call(endpoint: &dyn Endpoint, inv: &Invocation<'_>) -> Result<Self, Error>;
}

#[async_trait::async_trait]
impl Face for Representation {
    async fn call(endpoint: &dyn Endpoint, inv: &Invocation<'_>) -> Result<Self, Error> {
        endpoint.invoke(inv).await
    }
}

#[async_trait::async_trait]
impl Face for Representation<Body> {
    async fn call(endpoint: &dyn Endpoint, inv: &Invocation<'_>) -> Result<Self, Error> {
        endpoint.invoke_body(inv).await
    }
}

/// Call `end` when `representation`'s body ends: with `None` when it is read to its end,
/// with the error it ends with when it fails part-way. A buffer or a replay ends now, as
/// a success. A one-shot body dropped before its end never calls `end`, and drops it (so
/// what `end` holds, a bulkhead's slot, is released with the body): neither a success nor
/// a failure of the work, which the reader gave up on.
pub(crate) fn on_end<E>(representation: Representation<Body>, end: E) -> Representation<Body>
where
    E: FnOnce(Option<&Error>) + Send + 'static,
{
    representation.map_body(|body| match body {
        Body::Once(stream) => Body::once(Ended {
            inner: Some(stream),
            end: Some(end),
        }),
        complete => {
            end(None);
            complete
        }
    })
}

/// A one-shot body that reports its end: see [`on_end`].
struct Ended<E> {
    /// `None` once the body has ended.
    inner: Option<ByteStream>,
    end: Option<E>,
}

// `E` is only ever moved out, never pinned.
impl<E> Unpin for Ended<E> {}

impl<E: FnOnce(Option<&Error>)> Stream for Ended<E> {
    type Item = Result<Bytes, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let Some(inner) = self.inner.as_mut() else {
            return Poll::Ready(None);
        };
        let polled = inner.as_mut().poll_next(cx);
        let ended = match &polled {
            Poll::Ready(None) => Some(None),
            // An `Err` item ends a body as a failure: nothing after it is read.
            Poll::Ready(Some(Err(error))) => Some(Some(error)),
            _ => None,
        };
        if let Some(outcome) = ended {
            if let Some(end) = self.end.take() {
                end(outcome);
            }
            self.inner = None;
        }
        polled
    }
}
