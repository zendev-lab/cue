//! Resource scheduling: provider abstraction.
//!
//! A `Provider` is the daemon's plug-in point for one class of resources
//! (e.g. NVIDIA GPUs via NVML, or a TPU pool exposed by an external CLI
//! script). Providers are completely opaque to the rest of the daemon: the
//! scheduler never reasons about specific resource keys, it just routes the
//! job's `Need` map to whichever provider claims each key.
//!
//! ## Contract
//! * `id()` — unique provider identifier (e.g. `"gpu"`). Used for stable
//!   ordering and diagnostics.
//! * `keys()` — the `need.<key>` names this provider owns. The registry
//!   asserts that every key is claimed by **at most one** provider; a
//!   collision aborts daemon startup rather than silently shadowing.
//! * `probe()` — fresh capacity snapshot for `:resources`. Implementations
//!   may cache internally (e.g. NVML provider has its own TTL); the
//!   registry calls through without enforcing a global TTL.
//! * `reserve(req)` — acquire the bytes/count described by `req.need`.
//!   Implementations are expected to serialise themselves with their own
//!   internal `Mutex` so that concurrent admission attempts can't race over
//!   the same physical unit.
//! * `release(grant_id)` — release a previously granted reservation by id.
//!   Best-effort: no error is propagated, since terminal step paths must
//!   always succeed.
//!
//! All methods are synchronous. The daemon's scheduler runs admission off
//! the actor thread when needed; providers internally must not block for
//! more than a small bounded interval (TTL-cached probes, short
//! `Command::output` calls for CLI providers, etc.).

use std::fmt;

use cue_core::{
    StepId,
    resource::{Grant, Need, ProviderId, Reject, ReservationId, Snapshot},
};

/// A single reservation request routed to one provider.
///
/// `need` only contains keys this provider owns (the registry pre-slices
/// the global `Need` via `Need::select`).
#[derive(Debug, Clone)]
pub struct ReserveRequest {
    pub step_id: StepId,
    pub need: Need,
}

impl ReserveRequest {
    pub fn new(step_id: StepId, need: Need) -> Self {
        Self { step_id, need }
    }
}

/// Rejection annotated with the provider that produced it.
#[derive(Debug, Clone)]
pub struct RejectGroup {
    pub provider_id: ProviderId,
    pub reject: Reject,
}

impl fmt::Display for RejectGroup {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.provider_id, self.reject.reason)
    }
}

/// The plug-in interface implemented by every resource backend.
///
/// `Send + Sync` because the registry holds providers behind `Arc<dyn
/// Provider>` and may be shared across actor handles.
pub trait Provider: Send + Sync {
    /// Stable provider identifier; routed via the registry.
    fn id(&self) -> ProviderId;

    /// Keys this provider owns. Returned as owned `Vec` so providers are
    /// free to compute the list dynamically (e.g. depending on whether a
    /// driver is detected at construction time).
    fn keys(&self) -> Vec<String>;

    /// Fresh capacity snapshot. Must be cheap (TTL-cached when the
    /// underlying probe is expensive).
    fn probe(&self) -> Snapshot;

    /// Acquire the resources described by `req.need`. Implementations
    /// **must** serialise concurrent reserve calls internally so two
    /// admissions can't co-allocate the same unit.
    fn reserve(&self, req: &ReserveRequest) -> Result<Grant, Reject>;

    /// Release a previously granted reservation. Idempotent — calling on an
    /// unknown id is a no-op.
    fn release(&self, grant_id: &ReservationId);
}

#[cfg(test)]
mod tests {
    use super::*;
    use cue_core::resource::ResourceQuantity;

    #[test]
    fn reserve_request_is_constructible() {
        let need = Need::from_pairs([("gpu", ResourceQuantity::Count(1))]);
        let req = ReserveRequest::new(
            StepId {
                execution: cue_core::ExecutionId(1),
                index: 1,
            },
            need.clone(),
        );
        assert_eq!(
            req.step_id,
            StepId {
                execution: cue_core::ExecutionId(1),
                index: 1
            }
        );
        assert_eq!(req.need, need);
    }

    #[test]
    fn reject_group_display_includes_provider() {
        let rg = RejectGroup {
            provider_id: ProviderId::new("gpu"),
            reject: Reject::new("not enough memory"),
        };
        assert_eq!(rg.to_string(), "gpu: not enough memory");
    }
}
