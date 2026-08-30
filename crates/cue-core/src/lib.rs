//! Closed execution semantics shared by every Cue component.

pub mod id;
mod kernel;

pub use id::{EventId, ExecutionId, ScopeHash, StepId};
pub use kernel::*;
