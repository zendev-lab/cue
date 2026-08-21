//! Test-only `MockProvider` for exercising `ProviderRegistry` and
//! scheduler integration.
//!
//! The mock is parameterised at construction time:
//! * `MockBehaviour::AlwaysGrant` — always returns a fresh `Grant` whose
//!   `env` is the configured map.
//! * `MockBehaviour::AlwaysReject` — always returns the configured `Reject`.
//! * `MockBehaviour::Scripted` — returns the next `Result` from a vector
//!   (then keeps returning the last).
//!
//! All call sites are observable via atomic counters so tests can assert
//! reserve / release was actually invoked the expected number of times.
//!
//! Compiled into the daemon library so integration tests in `tests/` can
//! reach `crate::resource::mock::MockProvider`. Public to make that
//! cross-crate visibility possible.

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
};

use cue_core::resource::{
    Grant, ProviderId, Reject, Reservation, ReservationId, ResourceQuantity, ResourceUnit, Snapshot,
};

use super::provider::{Provider, ReserveRequest};

/// What the mock returns from each `reserve()` call.
#[derive(Debug, Clone)]
pub enum MockBehaviour {
    /// Always succeed; the `env` and `info` maps are copied into every grant.
    AlwaysGrant,
    /// Always reject with the configured rejection.
    AlwaysReject(Reject),
    /// Return the next entry from this list; once exhausted, repeat the last.
    Scripted(Vec<Result<(), Reject>>),
}

#[derive(Debug)]
pub struct MockProvider {
    id: ProviderId,
    keys: Vec<String>,
    behaviour: Mutex<MockBehaviour>,
    /// Optional env to attach to a grant.
    env: Mutex<std::collections::BTreeMap<String, String>>,
    /// Counters for assertions.
    reserve_calls: AtomicU64,
    release_calls: AtomicU64,
    /// Auto-incrementing reservation id seed.
    seq: AtomicU64,
    /// Last seen reserve request (cloned). For tests.
    last_request: Mutex<Option<ReserveRequest>>,
    /// Snapshot returned by `probe()`. Default empty.
    snapshot_units: Mutex<Vec<ResourceUnit>>,
}

impl MockProvider {
    /// Construct an always-granting provider.
    pub fn new(id: &str, keys: &[&str]) -> Self {
        Self::with_behaviour(id, keys, MockBehaviour::AlwaysGrant)
    }

    pub fn with_behaviour(id: &str, keys: &[&str], behaviour: MockBehaviour) -> Self {
        Self {
            id: ProviderId::new(id),
            keys: keys.iter().map(|s| (*s).to_owned()).collect(),
            behaviour: Mutex::new(behaviour),
            env: Mutex::new(Default::default()),
            reserve_calls: AtomicU64::new(0),
            release_calls: AtomicU64::new(0),
            seq: AtomicU64::new(0),
            last_request: Mutex::new(None),
            snapshot_units: Mutex::new(Vec::new()),
        }
    }

    /// Set the env returned in every grant from now on.
    pub fn set_env(&self, env: std::collections::BTreeMap<String, String>) {
        *self.env.lock().unwrap() = env;
    }

    /// Replace the configured behaviour at runtime (used by the
    /// release-then-retry test case).
    pub fn set_behaviour(&self, b: MockBehaviour) {
        *self.behaviour.lock().unwrap() = b;
    }

    /// Replace the snapshot units returned by `probe()`.
    pub fn set_snapshot_units(&self, units: Vec<ResourceUnit>) {
        *self.snapshot_units.lock().unwrap() = units;
    }

    pub fn reserve_calls(&self) -> u64 {
        self.reserve_calls.load(Ordering::SeqCst)
    }

    pub fn release_calls(&self) -> u64 {
        self.release_calls.load(Ordering::SeqCst)
    }

    pub fn last_request(&self) -> Option<ReserveRequest> {
        self.last_request.lock().unwrap().clone()
    }

    fn next_id(&self) -> ReservationId {
        let n = self.seq.fetch_add(1, Ordering::SeqCst);
        ReservationId::new(format!("{}-{}", self.id.as_str(), n))
    }
}

impl Provider for MockProvider {
    fn id(&self) -> ProviderId {
        self.id.clone()
    }

    fn keys(&self) -> Vec<String> {
        self.keys.clone()
    }

    fn probe(&self) -> Snapshot {
        Snapshot::new(self.id.clone(), self.snapshot_units.lock().unwrap().clone())
    }

    fn reserve(&self, req: &ReserveRequest) -> Result<Grant, Reject> {
        self.reserve_calls.fetch_add(1, Ordering::SeqCst);
        *self.last_request.lock().unwrap() = Some(req.clone());

        let outcome: Result<(), Reject> = {
            let mut behaviour = self.behaviour.lock().unwrap();
            match &mut *behaviour {
                MockBehaviour::AlwaysGrant => Ok(()),
                MockBehaviour::AlwaysReject(r) => Err(r.clone()),
                MockBehaviour::Scripted(steps) => {
                    if steps.is_empty() {
                        Ok(())
                    } else if steps.len() == 1 {
                        steps[0].clone()
                    } else {
                        steps.remove(0)
                    }
                }
            }
        };

        match outcome {
            Ok(()) => {
                let env = self.env.lock().unwrap().clone();
                let info: std::collections::BTreeMap<String, ResourceQuantity> =
                    req.need.iter().map(|(k, v)| (k.to_owned(), v)).collect();
                Ok(Reservation {
                    id: self.next_id(),
                    step_id: req.step_id,
                    provider_id: self.id.clone(),
                    env,
                    info,
                    acquired_at: std::time::SystemTime::now(),
                })
            }
            Err(r) => Err(r),
        }
    }

    fn release(&self, _grant_id: &ReservationId) {
        self.release_calls.fetch_add(1, Ordering::SeqCst);
    }
}

/// Convenience: typed `Arc` for tests that store the provider once and
/// inject it into both the registry and assertions.
pub fn mock_provider(id: &str, keys: &[&str]) -> Arc<MockProvider> {
    Arc::new(MockProvider::new(id, keys))
}
