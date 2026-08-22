//! Provider registry: routing and atomic multi-provider reservation.
//!
//! The registry owns the set of configured `Provider` instances and acts
//! as the daemon's single entry point for admission control. It does
//! three things:
//!
//! 1. **Key routing.** Each provider self-reports the keys it owns. At
//!    construction we build a map `key → ProviderId`; a duplicate key
//!    across providers is a hard configuration error.
//! 2. **Atomic reserve.** A job's `Need` map is dispatched provider by
//!    provider in deterministic ProviderId order. If any provider
//!    rejects, every prior reservation is rolled back (each provider
//!    `release`d) before the rejection is returned. This avoids partial
//!    holds across providers.
//! 3. **Per-job release.** Reservations are tracked by `JobId` so the
//!    scheduler can issue one `release(job_id)` on terminal job
//!    transitions, regardless of how many providers were involved.
//!
//! The registry is *not* an actor; it lives behind an `Arc` and uses
//! `std::sync::Mutex` internally. The bookkeeping is small enough
//! (one entry per active job) that there's no value in promoting it
//! to its own task. Per-provider serialisation is the provider's
//! responsibility (e.g. NVML provider holds its own `Mutex`).

use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex},
};

use anyhow::{Result, anyhow};
use cue_core::{
    JobId,
    resource::{Grant, Need, ProviderId, Snapshot},
};

use super::provider::{Provider, RejectGroup, ReserveRequest};

/// Tracked reservation: the granted reservation plus the provider that
/// must release it.
#[derive(Debug, Clone)]
struct TrackedGrant {
    provider_id: ProviderId,
    grant: Grant,
}

/// Provider registry — owns providers and their reservations.
pub struct ProviderRegistry {
    /// Stable order: entries kept sorted by ProviderId for deterministic
    /// reserve order and snapshot output.
    providers: Vec<Arc<dyn Provider>>,
    /// Lookup by id (Arc cloned from `providers`).
    by_id: BTreeMap<ProviderId, Arc<dyn Provider>>,
    /// Reverse routing table: `need.X` → provider id.
    key_to_provider: HashMap<String, ProviderId>,
    /// Per-job reservation bookkeeping (cleared by `release`).
    state: Mutex<RegistryState>,
}

#[derive(Default)]
struct RegistryState {
    reservations: HashMap<JobId, Vec<TrackedGrant>>,
}

impl fmt::Debug for ProviderRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProviderRegistry")
            .field(
                "providers",
                &self.providers.iter().map(|p| p.id()).collect::<Vec<_>>(),
            )
            .field("keys", &self.key_to_provider)
            .finish()
    }
}

use std::fmt;

impl ProviderRegistry {
    /// Build a registry from a slice of providers, validating that no two
    /// providers claim the same `need.<key>`.
    pub fn from_providers(providers: Vec<Arc<dyn Provider>>) -> Result<Self> {
        let mut sorted = providers;
        sorted.sort_by_key(|p| p.id());

        // Detect duplicate provider ids first — would otherwise mask key
        // collision errors.
        for pair in sorted.windows(2) {
            if pair[0].id() == pair[1].id() {
                return Err(anyhow!(
                    "duplicate resource provider id {:?}",
                    pair[0].id().as_str()
                ));
            }
        }

        let mut key_to_provider: HashMap<String, ProviderId> = HashMap::new();
        for p in &sorted {
            for key in p.keys() {
                if let Some(existing) = key_to_provider.get(&key) {
                    return Err(anyhow!(
                        "resource key {:?} claimed by both {:?} and {:?}",
                        key,
                        existing.as_str(),
                        p.id().as_str()
                    ));
                }
                key_to_provider.insert(key, p.id());
            }
        }

        let by_id: BTreeMap<_, _> = sorted.iter().map(|p| (p.id(), Arc::clone(p))).collect();

        Ok(Self {
            providers: sorted,
            by_id,
            key_to_provider,
            state: Mutex::new(RegistryState::default()),
        })
    }

    /// Empty registry — useful as a placeholder before any provider is
    /// configured. `try_reserve` on an empty registry rejects any non-empty
    /// `Need` with an `unknown_resource_key` reason.
    pub fn empty() -> Self {
        Self {
            providers: Vec::new(),
            by_id: BTreeMap::new(),
            key_to_provider: HashMap::new(),
            state: Mutex::new(RegistryState::default()),
        }
    }

    /// Number of registered providers.
    pub fn len(&self) -> usize {
        self.providers.len()
    }

    pub fn is_empty(&self) -> bool {
        self.providers.is_empty()
    }

    /// All known providers' ids, sorted.
    pub fn provider_ids(&self) -> Vec<ProviderId> {
        self.providers.iter().map(|p| p.id()).collect()
    }

    /// Key → provider routing table (read-only, snapshot for `:providers`).
    pub fn key_routes(&self) -> Vec<(String, ProviderId)> {
        let mut v: Vec<_> = self
            .key_to_provider
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        v.sort_by(|a, b| a.0.cmp(&b.0));
        v
    }

    /// Snapshot every provider's current capacity. Never errors.
    pub fn snapshot(&self) -> Vec<(ProviderId, Snapshot)> {
        self.providers.iter().map(|p| (p.id(), p.probe())).collect()
    }

    /// Active reservation count per provider, sorted by provider id.
    pub fn active_reservation_counts(&self) -> Vec<(ProviderId, usize)> {
        let state = self.state.lock().expect("resource registry mutex poisoned");
        self.providers
            .iter()
            .map(|provider| {
                let id = provider.id();
                let count = state
                    .reservations
                    .values()
                    .flatten()
                    .filter(|grant| grant.provider_id == id)
                    .count();
                (id, count)
            })
            .collect()
    }

    /// Lookup the provider that owns `key`, if any. Public so the scheduler
    /// can pre-validate `Need` before queuing a job.
    pub fn provider_for_key(&self, key: &str) -> Option<ProviderId> {
        self.key_to_provider.get(key).cloned()
    }

    /// Atomically reserve `need` across all providers that own a subset of
    /// its keys. On any rejection, prior grants are released and the first
    /// rejection is returned wrapped with its provider id.
    ///
    /// Returns `Ok(Vec<Grant>)` on full success; the caller is expected to
    /// merge `grant.env` into the spawning scope and pass the grants to
    /// `release(job_id)` when the job terminates.
    pub fn try_reserve(&self, job_id: JobId, need: &Need) -> Result<Vec<Grant>, RejectGroup> {
        if need.is_empty() {
            return Ok(Vec::new());
        }

        // Check unknown keys first — fail fast without contacting any
        // provider. Surfaces as a single Reject under a synthetic provider
        // id `"core"`.
        let unknown: Vec<&str> = need
            .keys()
            .filter(|k| !self.key_to_provider.contains_key(*k))
            .collect();
        if !unknown.is_empty() {
            let key = unknown[0].to_owned();
            return Err(RejectGroup {
                provider_id: ProviderId::new("core"),
                reject: cue_core::resource::Reject::new(format!(
                    "unknown resource key: {}",
                    unknown.join(", ")
                ))
                .with_demand(key.clone(), need.get(&key).unwrap()),
            });
        }

        // Group keys by provider, in deterministic provider id order.
        let mut by_provider: BTreeMap<ProviderId, Vec<String>> = BTreeMap::new();
        for k in need.keys() {
            if let Some(pid) = self.key_to_provider.get(k) {
                by_provider
                    .entry(pid.clone())
                    .or_default()
                    .push(k.to_owned());
            }
        }

        let mut acquired: Vec<TrackedGrant> = Vec::new();

        for (pid, keys) in by_provider {
            let key_refs: Vec<&str> = keys.iter().map(String::as_str).collect();
            let sub_need = need.select(&key_refs);
            let provider = self
                .by_id
                .get(&pid)
                .expect("by_id should contain every routed provider id");

            let req = ReserveRequest::new(job_id, sub_need);
            match provider.reserve(&req) {
                Ok(grant) => {
                    acquired.push(TrackedGrant {
                        provider_id: pid.clone(),
                        grant,
                    });
                }
                Err(rej) => {
                    // Roll back already-acquired grants in reverse order.
                    for tg in acquired.iter().rev() {
                        if let Some(p) = self.by_id.get(&tg.provider_id) {
                            p.release(&tg.grant.id);
                        }
                    }
                    return Err(RejectGroup {
                        provider_id: pid,
                        reject: rej,
                    });
                }
            }
        }

        // Track for later `release(job_id)`.
        {
            let mut state = self.state.lock().expect("registry state lock poisoned");
            state
                .reservations
                .entry(job_id)
                .or_default()
                .extend(acquired.clone());
        }

        Ok(acquired.into_iter().map(|tg| tg.grant).collect())
    }

    /// Release every reservation currently held by `job_id`. Idempotent:
    /// calling on an unknown job id is a no-op.
    ///
    /// Returns the number of reservations released (useful in tests).
    pub fn release(&self, job_id: JobId) -> usize {
        let to_release: Vec<TrackedGrant> = {
            let mut state = self.state.lock().expect("registry state lock poisoned");
            state.reservations.remove(&job_id).unwrap_or_default()
        };
        let n = to_release.len();
        for tg in to_release {
            if let Some(p) = self.by_id.get(&tg.provider_id) {
                p.release(&tg.grant.id);
            }
        }
        n
    }

    /// Total active reservations (all jobs). Diagnostics only.
    pub fn active_reservation_count(&self) -> usize {
        let state = self.state.lock().expect("registry state lock poisoned");
        state.reservations.values().map(|v| v.len()).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resource::mock::{MockBehaviour, MockProvider};
    use cue_core::resource::{Reject, ReservationId, ResourceQuantity};

    fn count(n: u64) -> ResourceQuantity {
        ResourceQuantity::Count(n)
    }

    #[test]
    fn key_collision_aborts_construction() {
        let p1 = Arc::new(MockProvider::new("a", &["gpu"]));
        let p2 = Arc::new(MockProvider::new("b", &["gpu"]));
        let err =
            ProviderRegistry::from_providers(vec![p1, p2]).expect_err("collision should fail");
        let s = format!("{err}");
        assert!(s.contains("\"gpu\""), "msg should mention key: {s}");
        assert!(
            s.contains("\"a\"") && s.contains("\"b\""),
            "msg should mention both providers: {s}"
        );
    }

    #[test]
    fn duplicate_provider_id_aborts_construction() {
        let p1 = Arc::new(MockProvider::new("gpu", &["gpu"]));
        let p2 = Arc::new(MockProvider::new("gpu", &["tpu"]));
        let err = ProviderRegistry::from_providers(vec![p1, p2]).expect_err("dup id should fail");
        assert!(format!("{err}").contains("duplicate resource provider id"));
    }

    #[test]
    fn unknown_key_is_rejected_synthetically() {
        let p1 = Arc::new(MockProvider::new("gpu", &["gpu", "gpu_mem"]));
        let reg = ProviderRegistry::from_providers(vec![p1]).unwrap();

        let need = Need::from_pairs([("tpu_mem", count(1))]);
        let err = reg.try_reserve(JobId(1), &need).unwrap_err();
        assert_eq!(err.provider_id, ProviderId::new("core"));
        assert!(err.reject.reason.contains("unknown resource key"));
        assert!(err.reject.reason.contains("tpu_mem"));
    }

    #[test]
    fn empty_need_succeeds_without_calling_providers() {
        let p1 = Arc::new(MockProvider::new("gpu", &["gpu"]));
        let reg = ProviderRegistry::from_providers(vec![p1.clone()]).unwrap();

        let grants = reg.try_reserve(JobId(1), &Need::new()).unwrap();
        assert!(grants.is_empty());
        assert_eq!(p1.reserve_calls(), 0);
    }

    #[test]
    fn routes_subneeds_to_owning_providers() {
        let gpu = Arc::new(MockProvider::new("gpu", &["gpu", "gpu_mem"]));
        let tpu = Arc::new(MockProvider::new("tpu", &["tpu"]));
        let reg = ProviderRegistry::from_providers(vec![gpu.clone(), tpu.clone()]).unwrap();

        let need = Need::from_pairs([
            ("gpu", count(1)),
            ("gpu_mem", ResourceQuantity::Bytes(24 * 1024 * 1024 * 1024)),
            ("tpu", count(2)),
        ]);
        let grants = reg.try_reserve(JobId(1), &need).unwrap();
        assert_eq!(grants.len(), 2);

        // Each provider got only its own keys.
        let gpu_calls = gpu.last_request().expect("gpu reserve call");
        assert_eq!(
            gpu_calls.need.keys().collect::<Vec<_>>(),
            vec!["gpu", "gpu_mem"]
        );
        let tpu_calls = tpu.last_request().expect("tpu reserve call");
        assert_eq!(tpu_calls.need.keys().collect::<Vec<_>>(), vec!["tpu"]);
    }

    #[test]
    fn rejection_rolls_back_prior_grants() {
        let gpu = Arc::new(MockProvider::new("gpu", &["gpu"]));
        let tpu = Arc::new(MockProvider::with_behaviour(
            "tpu",
            &["tpu"],
            MockBehaviour::AlwaysReject(Reject::new("tpu unavailable")),
        ));
        let reg = ProviderRegistry::from_providers(vec![gpu.clone(), tpu.clone()]).unwrap();

        let need = Need::from_pairs([("gpu", count(1)), ("tpu", count(1))]);
        let err = reg.try_reserve(JobId(1), &need).unwrap_err();
        assert_eq!(err.provider_id, ProviderId::new("tpu"));
        assert_eq!(err.reject.reason, "tpu unavailable");

        // gpu provider must have observed exactly one reserve and one
        // release (the rollback).
        assert_eq!(gpu.reserve_calls(), 1);
        assert_eq!(gpu.release_calls(), 1);
        assert_eq!(reg.active_reservation_count(), 0);
    }

    #[test]
    fn release_clears_per_job_bookkeeping() {
        let gpu = Arc::new(MockProvider::new("gpu", &["gpu"]));
        let tpu = Arc::new(MockProvider::new("tpu", &["tpu"]));
        let reg = ProviderRegistry::from_providers(vec![gpu.clone(), tpu.clone()]).unwrap();

        let need = Need::from_pairs([("gpu", count(1)), ("tpu", count(1))]);
        let grants = reg.try_reserve(JobId(7), &need).unwrap();
        assert_eq!(grants.len(), 2);
        assert_eq!(reg.active_reservation_count(), 2);

        let n = reg.release(JobId(7));
        assert_eq!(n, 2);
        assert_eq!(gpu.release_calls(), 1);
        assert_eq!(tpu.release_calls(), 1);
        assert_eq!(reg.active_reservation_count(), 0);

        // Idempotent: releasing again is a no-op.
        assert_eq!(reg.release(JobId(7)), 0);
        assert_eq!(gpu.release_calls(), 1);
    }

    #[test]
    fn snapshot_returns_one_entry_per_provider() {
        let gpu = Arc::new(MockProvider::new("gpu", &["gpu"]));
        let tpu = Arc::new(MockProvider::new("tpu", &["tpu"]));
        let reg = ProviderRegistry::from_providers(vec![gpu, tpu]).unwrap();

        let snaps = reg.snapshot();
        assert_eq!(snaps.len(), 2);
        // Sorted by provider id (gpu < tpu lexicographically).
        assert_eq!(snaps[0].0, ProviderId::new("gpu"));
        assert_eq!(snaps[1].0, ProviderId::new("tpu"));
    }

    #[test]
    fn release_unknown_id_is_silent() {
        let gpu = Arc::new(MockProvider::new("gpu", &["gpu"]));
        let reg = ProviderRegistry::from_providers(vec![gpu.clone()]).unwrap();

        // Provider never saw this id, but release shouldn't panic.
        gpu.release(&ReservationId::new("nonexistent"));
        assert_eq!(gpu.release_calls(), 1);
        assert_eq!(reg.release(JobId(99)), 0);
    }
}
