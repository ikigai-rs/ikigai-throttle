//! Attached input bodies must reopen before Retry or Failover re-issues (#2471).
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::executor::block_on;
use ikigai_core::{
    ArgSpec, Body, BodyIssue, ByteStream, Bytes, Capability, ContentId, Description, Endpoint,
    EndpointSpace, Error, Exact, Invocation, Iri, Kernel, Replay, ReprType, Representation,
    Request, RetryBudget, Space, Spill, TraceEvent, Tracer, Verb,
};
use ikigai_throttle::{Failover, Retry, Timer};

struct ReadThenFail {
    calls: Arc<AtomicU32>,
    pool: Arc<Mutex<Option<RetryBudget>>>,
    retry_after: bool,
}

struct NoWait;

#[async_trait::async_trait]
impl Timer for NoWait {
    async fn sleep(&self, _: Duration) {
        panic!("a one-shot attachment must skip the wait");
    }
}

#[derive(Default)]
struct Notes(Mutex<Vec<(String, String)>>);

impl Tracer for Notes {
    fn record(&self, event: TraceEvent) {
        self.0.lock().unwrap().extend(event.notes);
    }
}

#[async_trait::async_trait]
impl Endpoint for ReadThenFail {
    async fn invoke(&self, inv: &Invocation<'_>) -> Result<Representation, Error> {
        let attempt = self.calls.fetch_add(1, Ordering::SeqCst);
        *self.pool.lock().unwrap() = inv.retry_budget().cloned();
        if inv.request.args.contains_key("content") {
            let body = inv.content_body("content").await?;
            assert_eq!(inv.collect(body).await?, Bytes::from_static(b"input"));
        }
        if attempt == 0 {
            if self.retry_after {
                Err(Error::RetryAfter {
                    delay: Some(Duration::from_millis(1)),
                    detail: "original retry-after failure".into(),
                })
            } else {
                Err(Error::Unavailable("original transient failure".into()))
            }
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
    let (kernel, calls, _) = observed_fixture(failover, false);
    (kernel, calls)
}

fn observed_fixture(
    failover: bool,
    retry_after: bool,
) -> (Kernel, Arc<AtomicU32>, Arc<Mutex<Option<RetryBudget>>>) {
    let calls = Arc::new(AtomicU32::new(0));
    let pool = Arc::new(Mutex::new(None));
    let leaf = || {
        Arc::new(EndpointSpace::new().bind(
            Exact::new("urn:read"),
            ReadThenFail {
                calls: calls.clone(),
                pool: pool.clone(),
                retry_after,
            },
        )) as Arc<dyn Space>
    };
    let overlay: Arc<dyn Space> = if failover {
        Arc::new(Failover::new(vec![leaf(), leaf()]))
    } else {
        Arc::new(
            Retry::new(leaf(), 3)
                .with_timer(Arc::new(NoWait))
                .with_max_wait(Duration::from_secs(1)),
        )
    };
    (Kernel::new(overlay).with_retry_budget(2, 2), calls, pool)
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
fn one_shot_attachment_preserves_original_failure_without_reissue() {
    for failover in [false, true] {
        for retry_after in [false, true] {
            for body_face in [false, true] {
                let (kernel, calls, pool) = observed_fixture(failover, retry_after);
                let notes = Arc::new(Notes::default());
                kernel.set_tracer(notes.clone());
                let request = Request::new(Verb::Source, Iri::parse("urn:read").unwrap());
                let options = BodyIssue::new().attach("content", once());
                let cap = Capability::root();
                let error = if body_face {
                    block_on(kernel.issue_body_with(request, &cap, options)).unwrap_err()
                } else {
                    block_on(kernel.issue_with(request, &cap, options)).unwrap_err()
                };
                if retry_after {
                    assert!(
                        matches!(&error, Error::RetryAfter { delay: Some(delay), detail }
                        if *delay == Duration::from_millis(1) && detail == "original retry-after failure"),
                        "{error:?}"
                    );
                } else {
                    assert!(
                        matches!(&error, Error::Unavailable(detail)
                        if detail == "original transient failure"),
                        "{error:?}"
                    );
                }
                assert_eq!(calls.load(Ordering::SeqCst), 1);
                assert_eq!(pool.lock().unwrap().as_ref().unwrap().remaining(), 2);
                let notes = notes.0.lock().unwrap();
                assert!(
                    notes.iter().any(|(key, value)| key == "retry_skipped"
                        && value.contains("one-shot attachment")
                        && value.contains("retry skipped")
                        && value.contains("spill first (urn:stream:spill)")),
                    "{notes:?}"
                );
            }
        }
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
