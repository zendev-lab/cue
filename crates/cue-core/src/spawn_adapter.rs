//! Generic local process-launch adapter protocol.
//!
//! The protocol deliberately knows nothing about policy engines or approval
//! systems. A client supplies an ephemeral handle; cued asks the local adapter
//! to transform or reject each final argv and reports the resulting process
//! settlement.

use std::{fmt, path::PathBuf};

use serde::{Deserialize, Serialize};

use crate::{ExecutionId, StepId};

/// Maximum diagnostic tail carried to a spawn adapter.
pub const MAX_SPAWN_DIAGNOSTIC_BYTES: usize = 16 * 1024;

/// Secret bearer token whose debug representation never exposes its value.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SecretToken(String);

impl SecretToken {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn expose(&self) -> &str {
        &self.0
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl fmt::Debug for SecretToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SecretToken([REDACTED])")
    }
}

/// Ephemeral local handle attached to one execution submission.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SpawnAdapterHandle {
    pub endpoint: PathBuf,
    pub token: SecretToken,
}

impl fmt::Debug for SpawnAdapterHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SpawnAdapterHandle")
            .field("endpoint", &self.endpoint)
            .field("token", &self.token)
            .finish()
    }
}

/// One request per local adapter connection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum SpawnAdapterRequest {
    Prepare {
        token: SecretToken,
        execution_id: ExecutionId,
        step_id: StepId,
        /// Zero-based process segment index within the pipeline step.
        segment_index: u32,
        argv: Vec<String>,
        cwd: PathBuf,
    },
    Settle {
        token: SecretToken,
        execution_id: ExecutionId,
        step_id: StepId,
        segment_index: u32,
        result: SpawnResult,
        /// Lossy UTF-8 stderr or PTY tail, bounded by cued.
        diagnostic_tail: String,
        diagnostic_truncated: bool,
    },
}

/// Response to one prepare or settle request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum SpawnAdapterResponse {
    Prepared { argv: Vec<String> },
    Rejected { message: String },
    Settled,
    InfrastructureFailure { message: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum SpawnResult {
    Exited { code: i32 },
    Signaled { signal: i32 },
    SpawnError { message: String },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_redacts_tokens_recursively() {
        let handle = SpawnAdapterHandle {
            endpoint: PathBuf::from("/run/user/1/cue/adapters/a.sock"),
            token: SecretToken::new("secret-token"),
        };
        let request = SpawnAdapterRequest::Prepare {
            token: handle.token.clone(),
            execution_id: ExecutionId(3),
            step_id: StepId {
                execution: ExecutionId(3),
                index: 1,
            },
            segment_index: 0,
            argv: vec!["true".into()],
            cwd: PathBuf::from("/tmp"),
        };

        let debug = format!("{handle:?} {request:?}");
        assert!(debug.contains("[REDACTED]"));
        assert!(!debug.contains("secret-token"));
    }

    #[test]
    fn strict_protocol_rejects_unknown_fields() {
        let request = r#"{
            "type":"prepare",
            "token":"t",
            "execution_id":1,
            "step_id":{"execution":1,"index":1},
            "segment_index":0,
            "argv":["true"],
            "cwd":"/tmp",
            "policy":"dsh"
        }"#;

        assert!(serde_json::from_str::<SpawnAdapterRequest>(request).is_err());
    }
}
