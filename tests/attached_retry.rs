//! Reproduction for ledger #2471. Core 0.2.3 and 0.2.4 expose no non-consuming
//! attachment replayability query, so the desired policy remains blocked.
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use futures::executor::block_on;
use ikigai_core::{
    ArgSpec, Body, BodyIssue, ByteStream, Bytes, Capability, ContentId, Description, Endpoint,
    EndpointSpace, Error, Exact, Invocation, Iri, Kernel, Replay, ReprType, Representation,
    Request, Space, Spill, Verb,
};
use ikigai_throttle::{Failover, Retry};

struct ReadThenFail(Arc<AtomicU32>);

#[async_trait::async_trait]
impl Endpoint for ReadThenFail {
    async fn invoke(&self, inv: &Invocation<'_>) -> Result<Representation, Error> {
        let attempt = self.0.fetch_add(1, Ordering::SeqCst);
        if inv.request.args.contains_key("content") {
            let body = inv.content_body("content").await?;
            assert_eq!(inv.collect(body).await?, Bytes::from_static(b"input"));
        }
        if attempt == 0 {
            Err(Error::Unavailable("original transient failure".into()))
        } else {
            Ok(Representation::new(
                ReprType::new("text/plain"),
                b"ok".to_vec(),
            ))
        }
    }

    fn describe(&self) -> Description {
        Description::new("read then fail")
            .verb(Verb::Source)
            .input(ArgSpec::new("content").reads_body())
    }
}

fn fixture(failover: bool) -> (Kernel, Arc<AtomicU32>) {
    let calls = Arc::new(AtomicU32::new(0));
    let leaf = || {
        Arc::new(EndpointSpace::new().bind(Exact::new("urn:read"), ReadThenFail(calls.clone())))
            as Arc<dyn Space>
    };
    let overlay: Arc<dyn Space> = if failover {
        Arc::new(Failover::new(vec![leaf(), leaf()]))
    } else {
        Arc::new(Retry::new(leaf(), 3))
    };
    (Kernel::new(overlay), calls)
}

fn issue(kernel: &Kernel, body: Option<Body>) -> Result<Representation, Error> {
    let request = Request::new(Verb::Source, Iri::parse("urn:read").unwrap());
    let options = match body {
        Some(body) => BodyIssue::new().attach("content", body),
        None => BodyIssue::new(),
    };
    block_on(kernel.issue_with(request, &Capability::root(), options))
}

fn once() -> Body {
    Body::once(futures::stream::iter([Ok(Bytes::from_static(b"input"))]))
}

#[test]
fn reproduces_one_shot_attachment_replacing_transient_failure_with_conflict() {
    for failover in [false, true] {
        let (kernel, calls) = fixture(failover);
        let error = issue(&kernel, Some(once())).unwrap_err();
        // This pins the CURRENT DEFECT, not the desired policy. Before a fix, the
        // desired assertion `error == Unavailable("original transient failure")`
        // fails here: the second reader replaces it with Conflict.
        assert!(
            matches!(&error, Error::Conflict(detail)
                if detail.contains("one-shot content") && detail.contains("already read once")),
            "failover={failover}: {error:?}"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }
}

#[test]
fn no_attachment_still_retries() {
    for failover in [false, true] {
        let (kernel, calls) = fixture(failover);
        assert_eq!(issue(&kernel, None).unwrap().bytes, b"ok");
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }
}

// A small in-memory spill facility; production hosts can install a file-backed one.
struct Held(Bytes);

impl Replay for Held {
    fn open(&self) -> ByteStream {
        Box::pin(futures::stream::iter([Ok(self.0.clone())]))
    }

    fn len_hint(&self) -> Option<u64> {
        Some(self.0.len() as u64)
    }

    fn validator(&self) -> Option<ContentId> {
        Some(ContentId::of(&self.0))
    }
}

struct InMemorySpill;

#[async_trait::async_trait]
impl Spill for InMemorySpill {
    async fn spill(&self, mut body: ByteStream, max_bytes: u64) -> Result<Arc<dyn Replay>, Error> {
        use futures::StreamExt;
        let mut held = Vec::new();
        while let Some(chunk) = body.next().await {
            let chunk = chunk?;
            if (held.len() + chunk.len()) as u64 > max_bytes {
                return Err(Error::OverBudget {
                    detail: "spill limit exceeded".into(),
                });
            }
            held.extend_from_slice(&chunk);
        }
        Ok(Arc::new(Held(held.into())))
    }
}

#[test]
fn spilled_attachment_reopens_and_succeeds_on_second_attempt() {
    for failover in [false, true] {
        let (kernel, calls) = fixture(failover);
        let kernel = kernel.with_spill(Arc::new(InMemorySpill));
        let spill = Request::new(Verb::Source, Iri::parse("urn:stream:spill").unwrap());
        let spilled = block_on(kernel.issue_body_with(
            spill,
            &Capability::root(),
            BodyIssue::new().attach("content", once()),
        ))
        .unwrap();
        assert!(matches!(spilled.bytes, Body::Replay(_)));
        assert_eq!(issue(&kernel, Some(spilled.bytes)).unwrap().bytes, b"ok");
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }
}
