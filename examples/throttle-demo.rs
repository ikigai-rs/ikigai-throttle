//! Watch a runaway loop hit the wall.
//!
//!   cargo run --example throttle-demo
use ikigai_core::{
    Capability, EndpointSpace, Exact, FnEndpoint, Iri, Kernel, ReprType, Representation, Request,
    Verb,
};
use ikigai_throttle::{Rate, Throttle};
use std::{sync::Arc, time::Duration};

fn main() {
    let inner = EndpointSpace::new().bind(
        Exact::new("urn:system:exec"),
        FnEndpoint::new("exec", |_inv| {
            Ok(Representation::new(
                ReprType::new("text/plain"),
                b"(ran)".to_vec(),
            ))
        }),
    );
    // A budget a well-behaved caller stays under, and a runaway blows through.
    let space =
        Throttle::new(inner).limit("urn:system:exec", Rate::new(3, Duration::from_secs(10)));
    let kernel = Kernel::new(Arc::new(space));

    println!("budget: 3 exec calls / 10s\n");
    for i in 1..=5 {
        let req = Request::new(Verb::Source, Iri::parse("urn:system:exec").unwrap());
        match futures::executor::block_on(kernel.issue(req, &Capability::root())) {
            Ok(r) => println!("  call {i}: {}", String::from_utf8_lossy(&r.bytes)),
            Err(e) => println!("  call {i}: BLOCKED — {e}"),
        }
    }
}
