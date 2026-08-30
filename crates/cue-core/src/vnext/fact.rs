use serde::{Deserialize, Serialize};

use crate::{EventId, ExecutionId, ScopeHash, StepId};

use super::{ExecutionSnapshot, ExecutionState, StepState};

/// Durable output channel. PTY output is one terminal byte stream; captured
/// execution keeps stdout and stderr distinct.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutputStream {
    Stdout,
    Stderr,
    Terminal,
}

/// One committed execution fact. Live connection and PTY attachment events
/// deliberately do not belong to this algebra.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "payload",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum Fact {
    ExecutionCreated {
        id: ExecutionId,
        scope: ScopeHash,
    },
    StepStateChanged {
        id: StepId,
        previous: StepState,
        next: StepState,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        input_scope: Option<ScopeHash>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        output_scope: Option<ScopeHash>,
    },
    ExecutionStateChanged {
        id: ExecutionId,
        previous: ExecutionState,
        next: ExecutionState,
    },
    OutputAppended {
        step: StepId,
        stream: OutputStream,
        start_offset: u64,
        end_offset: u64,
    },
    ExecutionFinished {
        id: ExecutionId,
        state: ExecutionState,
    },
}

impl Fact {
    pub const fn execution_id(&self) -> ExecutionId {
        match self {
            Self::ExecutionCreated { id, .. }
            | Self::ExecutionStateChanged { id, .. }
            | Self::ExecutionFinished { id, .. } => *id,
            Self::StepStateChanged { id, .. } | Self::OutputAppended { step: id, .. } => {
                id.execution
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FactEvent {
    pub id: EventId,
    pub occurred_at_ms: i64,
    pub fact: Fact,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FactDraft {
    pub occurred_at_ms: i64,
    pub fact: Fact,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionProjection {
    pub snapshot: ExecutionSnapshot,
    pub state: ExecutionState,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn step_facts_derive_their_execution_identity() {
        let id = StepId {
            execution: ExecutionId(7),
            index: 2,
        };
        let fact = Fact::OutputAppended {
            step: id,
            stream: OutputStream::Stdout,
            start_offset: 0,
            end_offset: 3,
        };
        assert_eq!(fact.execution_id(), ExecutionId(7));
    }
}
