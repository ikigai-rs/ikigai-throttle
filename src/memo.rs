//! One wrapper per inner endpoint, so a governed resource keeps ONE identity.
//!
//! The kernel memoizes each resolved endpoint's capability floor by the address of
//! its `Arc` (core 0.1.74, `FloorMemo`): a resource bound once is described once.
//! Every governor in this crate decorates the endpoint it resolved, and it used to
//! build that decoration as a fresh `Arc` on every resolution, so a governed
//! resource never presented the same address twice. Each request missed the
//! kernel's memo, ran `describe()` again, and inserted an entry whose key died at
//! once (ledger #534). The same shape would defeat any later per-endpoint memo,
//! such as a path cache.
//!
//! [`Wrappers`] is the fix: a small table on the governor from "what this wrapper
//! was built from" to the wrapper, so the same inner endpoint resolves to the same
//! wrapper `Arc` every time.
//!
//! **Why a hit is the same endpoint.** The key holds the inner endpoint's address,
//! and the wrapper stored under it holds that endpoint STRONGLY (it has to: it
//! invokes it). So while an entry lives, its inner allocation cannot be freed, and
//! no other endpoint can be allocated at that address: a hit is the same
//! allocation by construction, the same reservation the kernel's memo makes with a
//! `Weak`. A REBIND (a space that drops one endpoint and binds another) therefore
//! always presents a new address and gets a new wrapper.
//!
//! **Why it does not leak.** Holding the inner strongly is also what could keep a
//! replaced endpoint alive, and a space that allocates a fresh endpoint per
//! resolution (a remote's forwarding endpoint is built that way) never hits at all.
//! So once the table reaches its sweep mark, an entry is dropped when nothing
//! outside it holds the wrapper (no resolution in flight) and some endpoint it
//! encloses is held only by this table's wrappers (no space binds it any more).
//! The mark doubles with what survives, so sweeping is amortized O(1) per insert,
//! and the table never exceeds [`BOUND`]: if a sweep leaves it three quarters full
//! of live entries, it is cleared, which costs only a fresh wrapper per resource on
//! its next resolution. One case the sweep cannot see, stated so nobody hunts for
//! it: a governor stacked on a governor holds the inner one's wrapper, so a stale
//! entry in the inner table stays "held" until the outer one is swept; the bound
//! is what limits that.

use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Arc, RwLock};

use ikigai_core::Endpoint;

/// The most entries one governor's table holds.
pub(crate) const BOUND: usize = 1024;

/// The first sweep mark; later marks follow what a sweep left.
const FIRST_SWEEP: usize = 64;

/// The thin address of an endpoint's allocation: the identity a binding keeps.
pub(crate) fn address(endpoint: &Arc<dyn Endpoint>) -> usize {
    Arc::as_ptr(endpoint) as *const () as usize
}

/// A digest of a name, so a wrapper keyed by its target is looked up without
/// allocating the target on every resolution. Never trusted alone: the wrapper
/// carries the name itself and `fits` compares it, so two names that collide only
/// replace each other's entry, never share one.
pub(crate) fn digest(name: &str) -> u64 {
    use std::hash::Hasher;
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    hasher.write(name.as_bytes());
    hasher.finish()
}

/// A wrapper that can say which endpoints it holds, so the sweep can tell whether
/// anything outside the table still holds them.
pub(crate) trait Encloses: Endpoint {
    /// Every endpoint this wrapper holds a strong reference to.
    fn enclosed(&self) -> Vec<&Arc<dyn Endpoint>>;
}

/// One governor's table of wrappers, keyed by what each was built from.
pub(crate) struct Wrappers<K, W> {
    table: RwLock<Table<K, W>>,
}

struct Table<K, W> {
    entries: HashMap<K, Arc<W>>,
    sweep_at: usize,
}

impl<K: Hash + Eq, W: Encloses> Wrappers<K, W> {
    pub(crate) fn new() -> Self {
        Wrappers {
            table: RwLock::new(Table {
                entries: HashMap::new(),
                sweep_at: FIRST_SWEEP,
            }),
        }
    }

    /// The wrapper stored under `key` when `fits` accepts it, else a new one from
    /// `wrap`, stored. `fits` is for a wrapper whose key cannot carry everything it
    /// was built from (a failover's per-candidate captures); for every other
    /// governor the key is the whole story and `fits` accepts anything.
    pub(crate) fn get_or_wrap(
        &self,
        key: K,
        fits: impl Fn(&W) -> bool,
        wrap: impl FnOnce() -> W,
    ) -> Arc<W> {
        {
            let table = self.table.read().expect("wrapper memo lock");
            if let Some(wrapper) = table.entries.get(&key).filter(|w| fits(w)) {
                return Arc::clone(wrapper);
            }
        }
        let fresh = Arc::new(wrap());
        let mut table = self.table.write().expect("wrapper memo lock");
        // A concurrent resolution of the same resource may have stored one between
        // the two locks: share it, so both callers see one identity.
        if let Some(wrapper) = table.entries.get(&key).filter(|w| fits(w)) {
            return Arc::clone(wrapper);
        }
        if table.entries.len() >= table.sweep_at {
            table.sweep();
        }
        table.entries.insert(key, Arc::clone(&fresh));
        fresh
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.table.read().expect("wrapper memo lock").entries.len()
    }
}

impl<K: Hash + Eq, W: Encloses> Table<K, W> {
    /// Drop every entry nobody outside the table can reach again; see the module
    /// doc for the rule, the mark and the bound.
    fn sweep(&mut self) {
        // How many of this table's wrappers hold each endpoint: an endpoint whose
        // strong count is no more than that is held by nothing else.
        let mut holders: HashMap<usize, usize> = HashMap::new();
        for wrapper in self.entries.values() {
            for endpoint in wrapper.enclosed() {
                *holders.entry(address(endpoint)).or_default() += 1;
            }
        }
        self.entries.retain(|_, wrapper| {
            let in_flight = Arc::strong_count(wrapper) > 1;
            let unbound = wrapper
                .enclosed()
                .into_iter()
                .any(|endpoint| Arc::strong_count(endpoint) <= holders[&address(endpoint)]);
            in_flight || !unbound
        });
        if self.entries.len() >= BOUND / 4 * 3 {
            self.entries.clear();
        }
        self.sweep_at = (self.entries.len() * 2).clamp(FIRST_SWEEP, BOUND);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ikigai_core::{Error, Invocation, Representation};

    struct Leaf;
    #[async_trait::async_trait]
    impl Endpoint for Leaf {
        async fn invoke(&self, _inv: &Invocation<'_>) -> Result<Representation, Error> {
            Err(Error::Unavailable("unused".into()))
        }
    }

    struct Wrapper(Arc<dyn Endpoint>);
    #[async_trait::async_trait]
    impl Endpoint for Wrapper {
        async fn invoke(&self, inv: &Invocation<'_>) -> Result<Representation, Error> {
            self.0.invoke(inv).await
        }
    }
    impl Encloses for Wrapper {
        fn enclosed(&self) -> Vec<&Arc<dyn Endpoint>> {
            vec![&self.0]
        }
    }

    fn wrap(memo: &Wrappers<usize, Wrapper>, inner: Arc<dyn Endpoint>) -> Arc<Wrapper> {
        memo.get_or_wrap(address(&inner), |_| true, || Wrapper(inner))
    }

    /// Every inner endpoint still bound (held outside the table), none of them ever
    /// hitting twice: the sweep finds nothing to drop, and the bound still holds.
    #[test]
    fn the_table_never_exceeds_its_bound_even_when_every_entry_is_live() {
        let memo = Wrappers::new();
        let mut bound: Vec<Arc<dyn Endpoint>> = Vec::new();
        for _ in 0..(BOUND * 5) {
            let inner: Arc<dyn Endpoint> = Arc::new(Leaf);
            bound.push(Arc::clone(&inner));
            drop(wrap(&memo, inner));
            assert!(memo.len() <= BOUND, "{} entries", memo.len());
        }
    }

    /// A wrapper whose inner endpoint nothing else holds is swept at the next mark,
    /// while one whose inner is still bound survives it and keeps its identity.
    #[test]
    fn a_sweep_drops_unbound_entries_and_keeps_bound_ones() {
        let memo = Wrappers::new();
        let kept: Arc<dyn Endpoint> = Arc::new(Leaf);
        let first = wrap(&memo, Arc::clone(&kept));
        drop(first);
        for _ in 0..FIRST_SWEEP {
            drop(wrap(&memo, Arc::new(Leaf)));
        }
        // The insert that crossed the mark swept the unbound ones before landing.
        assert!(memo.len() < FIRST_SWEEP, "{} entries", memo.len());
        let again = wrap(&memo, Arc::clone(&kept));
        let and_again = wrap(&memo, Arc::clone(&kept));
        assert!(Arc::ptr_eq(&again, &and_again));
    }
}
