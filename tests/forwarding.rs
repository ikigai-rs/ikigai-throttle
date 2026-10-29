//! Forwarding metadata must survive every reliability overlay (ledger #2537).
use std::{sync::Arc, time::Duration};

use futures::{executor::block_on, StreamExt};
use ikigai_core::{
    Alias, AliasTable, ArgRef, ArgSpec, AsyncFnEndpoint, Body, BodyIssue, Bytes, Capability,
    Description, EndpointSpace, Exact, Fallback, FixedArgument, FixedClock, FnEndpoint, Iri,
    Kernel, ReprType, Representation, Request, Resolution, Scope, Space, TopologyRule, Verb,
};
use ikigai_throttle::{CircuitBreaker, Failover, Rate, RateLimit, Retry, Throttle, Timeout};

const VIEW: &str = "urn:test:view";
const NAMED: &str = "urn:test:named";
const TARGET: &str = "urn:test:target";
const REFERENT: &str = "urn:test:referent";

fn fixture() -> impl Space {
    let target = AsyncFnEndpoint::new("target", |inv| {
        Box::pin(async move {
            let uri = inv.inline_str("uri")?.to_owned();
            let body = inv.content_body("content").await?;
            assert!(body.is_one_shot(), "the view collected the attached body");
            let bytes = inv.collect(body).await?;
            Ok(Representation::new(
                ReprType::new("text/plain"),
                [uri.as_bytes(), b":", &bytes].concat(),
            ))
        })
    })
    .with_description(
        Description::new("target")
            .title("Target contract")
            .verb(Verb::Source)
            .input(ArgSpec::new("uri").class("xsd:anyURI"))
            .input(ArgSpec::new("content").reads_body())
            .output("text/plain"),
    );
    let doors = EndpointSpace::new().bind(Exact::new(TARGET), target).bind(
        Exact::new(REFERENT),
        FnEndpoint::new("referent", |_| {
            panic!("an anyURI argument must not dereference its IRI")
        }),
    );
    let names = AliasTable::new()
        .exact_at_root(VIEW, TARGET)
        .with_rule(
            TopologyRule::named_request(NAMED, TARGET)
                .verb(Verb::Source)
                .fixing(FixedArgument::value("uri", REFERENT)),
        )
        .unwrap();
    let views = Alias::new(Arc::new(names), Arc::new(EndpointSpace::new()));
    Fallback::new(vec![Arc::new(doors), Arc::new(views)])
}

fn overlays() -> Vec<(&'static str, Arc<dyn Space>)> {
    let clock = || Arc::new(FixedClock::at(1_000));
    vec![
        ("bare", Arc::new(fixture())),
        (
            "RateLimit",
            Arc::new(
                RateLimit::new(fixture())
                    .limit("urn:", Rate::new(100, Duration::from_secs(1)))
                    .with_clock(clock()),
            ),
        ),
        ("Retry", Arc::new(Retry::new(fixture(), 3))),
        (
            "CircuitBreaker",
            Arc::new(CircuitBreaker::new(fixture(), 3, Duration::from_secs(1)).with_clock(clock())),
        ),
        (
            "Failover",
            Arc::new(Failover::new(vec![
                Arc::new(fixture()),
                Arc::new(fixture()),
            ])),
        ),
        (
            "Throttle",
            Arc::new(Throttle::new(fixture()).limit("urn:", 2)),
        ),
        (
            "Timeout",
            Arc::new(Timeout::new(fixture(), Duration::from_secs(30))),
        ),
        (
            "stack",
            Arc::new(Timeout::new(
                Throttle::new(Retry::new(
                    CircuitBreaker::new(
                        RateLimit::new(Failover::new(vec![Arc::new(fixture())]))
                            .limit("urn:", Rate::new(100, Duration::from_secs(1)))
                            .with_clock(clock()),
                        3,
                        Duration::from_secs(1),
                    )
                    .with_clock(clock()),
                    3,
                ))
                .limit("urn:", 2),
                Duration::from_secs(30),
            )),
        ),
    ]
}

#[test]
fn every_overlay_projects_the_target_contract() {
    let bare = Kernel::new(Arc::new(fixture()));
    for (name, space) in overlays() {
        let kernel = Kernel::new(space);
        for target in [VIEW, NAMED] {
            let iri = Iri::parse(target).unwrap();
            assert_eq!(
                kernel.describe(&iri).unwrap(),
                bare.describe(&iri).unwrap(),
                "{name}: {target} must expose the target's inputs, outputs and fixed arguments"
            );
        }
    }
}

#[test]
fn every_overlay_leaves_uri_references_and_attached_bodies_for_the_target() {
    for (name, space) in overlays() {
        let kernel = Kernel::new(space);
        for target in [VIEW, NAMED] {
            // Exercise both invocation faces through the overlay.
            for streamed in [false, true] {
                let mut request = Request::new(Verb::Source, Iri::parse(target).unwrap());
                if target == VIEW {
                    request =
                        request.with_arg("uri", ArgRef::Reference(Iri::parse(REFERENT).unwrap()));
                }
                let body = Body::once(futures::stream::iter([Ok(Bytes::from_static(b"body"))]));
                let issue = BodyIssue::new().attach("content", body);
                let root = Capability::root();
                let bytes = if streamed {
                    let answer = block_on(kernel.issue_body_with(request, &root, issue))
                        .unwrap_or_else(|e| panic!("{name}: {target}: {e}"));
                    block_on(async {
                        let mut stream = answer.bytes.into_stream();
                        let mut bytes = Vec::new();
                        while let Some(chunk) = stream.next().await {
                            bytes.extend_from_slice(&chunk.unwrap());
                        }
                        bytes
                    })
                } else {
                    block_on(kernel.issue_with(request, &root, issue))
                        .unwrap_or_else(|e| panic!("{name}: {target}: {e}"))
                        .bytes
                };
                assert_eq!(
                    bytes,
                    format!("{REFERENT}:body").as_bytes(),
                    "{name}: {target}"
                );
            }
        }
    }
}

#[test]
fn an_over_budget_rate_limit_preserves_forwarding_metadata() {
    let bare = fixture();
    let limited = Arc::new(
        RateLimit::new(fixture())
            .limit("urn:", Rate::new(0, Duration::from_secs(1)))
            .with_clock(Arc::new(FixedClock::at(1_000))),
    );
    for target in [VIEW, NAMED] {
        let request = Request::new(Verb::Source, Iri::parse(target).unwrap());
        let Resolution::Hit(original) = bare.resolve(&request, &Scope::empty()) else {
            panic!("the bare view must resolve");
        };
        let Resolution::Hit(refusal) = limited.resolve(&request, &Scope::empty()) else {
            panic!("the refusal must resolve");
        };
        assert!(original.endpoint.forwards().is_some());
        assert_eq!(refusal.endpoint.forwards(), original.endpoint.forwards());
        let kernel = Kernel::new(limited.clone());
        let error = block_on(kernel.issue(request, &Capability::root())).unwrap_err();
        assert!(matches!(error, ikigai_core::Error::RetryAfter { .. }));
    }
}
