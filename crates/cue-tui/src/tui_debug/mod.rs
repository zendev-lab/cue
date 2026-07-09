//! External debug control surface for cue-tui (zellij-style harness support).

mod buffer;
mod client;
mod keys;
mod server;
mod snapshot;

pub(crate) use client::{DebugCliCommand, run_debug_command};
pub(crate) use server::{DebugControl, record_frame_snapshot, spawn_debug_server};
pub(crate) use snapshot::{shared_debug_snapshots, update_state_snapshot};
