use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use futures::executor::block_on;
use ikigai_core::{
    ActionSpec, ArgRef, ArgSpec, Capability, Description, Endpoint, EndpointSpace, Error, Exact,
    Invocation, Iri, Kernel, ReprType, Representation, Request, Verb,
};
use ikigai_throttle::Failover;

struct Target {
    calls: Arc<AtomicUsize>,
    action: ActionSpec,
    error: Option<Error>,
}

#[async_trait::async_trait]
impl Endpoint for Target {
    async fn invoke(&self, _: &Invocation<'_>) -> Result<Representation, Error> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        match &self.error {
            Some(error) => Err(error.clone()),
            None => Ok(Representation::new(
                ReprType::new("text/plain"),
                b"ok".to_vec(),
            )),
        }
    }
    fn describe(&self) -> Description {
        Description::new("target").action(self.action.clone())
    }
}

#[test]
fn keyed_sink_fails_over_when_the_call_carries_the_key() {
    let action = ActionSpec::new(Verb::Sink)
        .input(ArgSpec::new("key").optional())
        .resend_safe_with_key("key");
    let primary_calls = Arc::new(AtomicUsize::new(0));
    let backup_calls = Arc::new(AtomicUsize::new(0));
    let primary = EndpointSpace::new().bind(
        Exact::new("urn:dep"),
        Target {
            calls: primary_calls.clone(),
            action: action.clone(),
            error: Some(Error::Unavailable("down".into())),
        },
    );
    let backup = EndpointSpace::new().bind(
        Exact::new("urn:dep"),
        Target {
            calls: backup_calls.clone(),
            action,
            error: None,
        },
    );
    let kernel = Kernel::new(Arc::new(Failover::new(vec![
        Arc::new(primary),
        Arc::new(backup),
    ])));
    let request = Request::new(Verb::Sink, Iri::parse("urn:dep").unwrap())
        .with_arg("key", ArgRef::Inline(b"order-7".to_vec()));
    assert!(block_on(kernel.issue(request, &Capability::root())).is_ok());
    assert_eq!(primary_calls.load(Ordering::SeqCst), 1);
    assert_eq!(backup_calls.load(Ordering::SeqCst), 1);
}

#[test]
fn failover_obeys_sink_contract_key_and_failure_class_on_both_faces() {
    for body in [false, true] {
        for (action, key, error, advances) in [
            (
                ActionSpec::new(Verb::Sink).resend_safe(),
                None,
                Error::Unavailable("down".into()),
                true,
            ),
            (
                ActionSpec::new(Verb::Sink)
                    .input(ArgSpec::new("key").optional())
                    .resend_safe_with_key("key"),
                None,
                Error::Unavailable("down".into()),
                false,
            ),
            (
                ActionSpec::new(Verb::Sink)
                    .input(ArgSpec::new("key").optional())
                    .resend_safe_with_key("key"),
                Some(" "),
                Error::Unavailable("down".into()),
                false,
            ),
            (
                ActionSpec::new(Verb::Sink),
                Some("order-7"),
                Error::Unavailable("down".into()),
                false,
            ),
            (
                ActionSpec::new(Verb::Sink).resend_safe(),
                None,
                Error::Endpoint("bad".into()),
                false,
            ),
            (
                ActionSpec::new(Verb::Sink).resend_safe(),
                None,
                Error::RetryAfter {
                    delay: None,
                    detail: "limited".into(),
                },
                true,
            ),
        ] {
            let primary_calls = Arc::new(AtomicUsize::new(0));
            let backup_calls = Arc::new(AtomicUsize::new(0));
            let primary = EndpointSpace::new().bind(
                Exact::new("urn:dep"),
                Target {
                    calls: primary_calls.clone(),
                    action: action.clone(),
                    error: Some(error.clone()),
                },
            );
            let backup = EndpointSpace::new().bind(
                Exact::new("urn:dep"),
                Target {
                    calls: backup_calls.clone(),
                    action,
                    error: None,
                },
            );
            let kernel = Kernel::new(Arc::new(Failover::new(vec![
                Arc::new(primary),
                Arc::new(backup),
            ])));
            let mut request = Request::new(Verb::Sink, Iri::parse("urn:dep").unwrap());
            if let Some(key) = key {
                request = request.with_arg("key", ArgRef::Inline(key.as_bytes().to_vec()));
            }
            let result = if body {
                block_on(kernel.issue_body(request, &Capability::root())).map(|_| ())
            } else {
                block_on(kernel.issue(request, &Capability::root())).map(|_| ())
            };
            assert_eq!(
                result.is_ok(),
                advances,
                "body={body}, key={key:?}, error={error:?}"
            );
            assert_eq!(primary_calls.load(Ordering::SeqCst), 1);
            assert_eq!(backup_calls.load(Ordering::SeqCst), usize::from(advances));
        }
    }
}

#[test]
fn failover_checks_the_failed_candidates_binding() {
    use ikigai_core::UriTemplate;
    let action = ActionSpec::new(Verb::Sink)
        .input(ArgSpec::new("key").binding())
        .resend_safe_with_key("key");
    let primary_calls = Arc::new(AtomicUsize::new(0));
    let backup_calls = Arc::new(AtomicUsize::new(0));
    let primary = EndpointSpace::new().bind(
        UriTemplate::parse("urn:orders/{key}").unwrap(),
        Target {
            calls: primary_calls.clone(),
            action: action.clone(),
            error: Some(Error::Unavailable("down".into())),
        },
    );
    let backup = EndpointSpace::new().bind(
        UriTemplate::parse("urn:orders/{other}").unwrap(),
        Target {
            calls: backup_calls.clone(),
            action,
            error: None,
        },
    );
    let kernel = Kernel::new(Arc::new(Failover::new(vec![
        Arc::new(primary),
        Arc::new(backup),
    ])));
    let request = Request::new(Verb::Sink, Iri::parse("urn:orders/order-7").unwrap());
    assert!(block_on(kernel.issue(request, &Capability::root())).is_ok());
    assert_eq!(primary_calls.load(Ordering::SeqCst), 1);
    assert_eq!(backup_calls.load(Ordering::SeqCst), 1);
}
