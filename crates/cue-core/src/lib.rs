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
pub mod job;
pub mod pipeline;
pub mod process_status;
pub mod resource;
pub mod scope;
pub mod spawn_adapter;
pub mod tui_debug;

// Re-export commonly used types at crate root.
pub use event_channel::EventChannel;
pub use id::{ExecutionId, ScheduleId, ScopeHash, StepId};
pub use job::{LaunchOptions, SandboxMode, SandboxSettings, SandboxUpper};
pub use resource::{
    Grant, Need, ParseQuantityError, ParseQuantityReason, ProviderId, Reject, Reservation,
    ReservationId, ResourceQuantity, ResourceUnit, Snapshot,
};
pub use spawn_adapter::{SecretToken, SpawnAdapterHandle};
