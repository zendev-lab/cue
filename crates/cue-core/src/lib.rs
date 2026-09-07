//! cue-core — shared types for the Cue ecosystem.
//!
//! This crate defines the core domain types and pure scheduling primitives used
//! by both the daemon (cued) and clients (cue-tui, cue-cli). It contains no
//! daemon runtime or I/O logic.

pub mod command;
pub mod cron;
pub mod event_channel;
pub mod execution;
pub mod id;
pub mod ipc;
pub mod launch;
pub mod pipeline;
pub mod process_status;
pub mod resource;
pub mod scope;
pub mod spawn_adapter;
pub mod tui_debug;
/// Target execution contract under construction for the IPC v4 hard cut.
///
/// The current daemon continues to use the v3 root modules until every caller
/// has migrated. This namespace is removed when vNext becomes the sole public
/// contract; it is not a second daemon protocol.
pub mod vnext;

// Re-export commonly used types at crate root.
pub use event_channel::EventChannel;
pub use id::{EventId, ExecutionId, ScheduleId, ScopeHash, StepId};
pub use launch::{SandboxMode, SandboxSettings, SandboxUpper};
pub use resource::{
    Grant, Need, ParseQuantityError, ParseQuantityReason, ProviderId, Reject, Reservation,
    ReservationId, ResourceQuantity, ResourceUnit, Snapshot,
};
pub use spawn_adapter::{SecretToken, SpawnAdapterHandle};
