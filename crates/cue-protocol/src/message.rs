use cue_core::vnext::{
    CancelMode, ExecutionSnapshot, ExecutionSpec, ExecutionState, FactEvent, OutputStream, Scope,
};
use cue_core::{ExecutionId, ScopeHash, StepId};
use serde::{Deserialize, Serialize};

use crate::{AttachmentId, ClientId, EventId, OperationId, RequestId};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum Message {
    Query {
        request_id: RequestId,
        query: Query,
    },
    Command {
        request_id: RequestId,
        operation_id: OperationId,
        command: Command,
    },
    Response {
        request_id: RequestId,
        payload: ResponsePayload,
    },
    Event {
        payload: EventPayload,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Hello {
    pub protocol_version: u32,
    pub client_id: ClientId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "payload",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum Query {
    Hello(Hello),
    Ping,
    GetScope {
        hash: ScopeHash,
    },
    GetExecution {
        id: ExecutionId,
    },
    ListExecutions {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        before: Option<ExecutionId>,
        limit: u16,
    },
    WaitExecution {
        id: ExecutionId,
    },
    TailOutput {
        step: StepId,
        stream: OutputStream,
        max_bytes: u32,
    },
    ReadOutput {
        step: StepId,
        stdout: OutputRange,
        stderr: OutputRange,
        terminal: OutputRange,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutputRange {
    pub offset: u64,
    pub max_bytes: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "payload",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum Command {
    PutScope {
        scope: Box<Scope>,
    },
    SubmitExecution {
        spec: Box<ExecutionSpec>,
    },
    CancelExecution {
        id: ExecutionId,
        mode: CancelMode,
    },
    WatchExecution {
        id: ExecutionId,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        after_event: Option<EventId>,
    },
    UnwatchExecution {
        id: ExecutionId,
    },
    AttachPty {
        step: StepId,
        replay_bytes: u32,
    },
    DetachPty {
        attachment: AttachmentId,
    },
    ClaimPtyControl {
        attachment: AttachmentId,
    },
    ReleasePtyControl {
        attachment: AttachmentId,
    },
    PtyInput {
        attachment: AttachmentId,
        #[serde(with = "base64_bytes")]
        data: Vec<u8>,
    },
    PtyResize {
        attachment: AttachmentId,
        cols: u16,
        rows: u16,
    },
    Restart,
    Shutdown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "status",
    content = "payload",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum ResponsePayload {
    Ok(ResultPayload),
    Error(ProtocolError),
}

impl ResponsePayload {
    pub const fn ack() -> Self {
        Self::Ok(ResultPayload::Ack)
    }

    pub fn error(code: ProtocolErrorCode, message: impl Into<String>) -> Self {
        Self::Error(ProtocolError {
            code,
            message: message.into(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "payload",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum ResultPayload {
    Ack,
    Hello {
        protocol_version: u32,
        server_version: String,
        instance_id: String,
        capabilities: Vec<Capability>,
    },
    Scope {
        hash: ScopeHash,
        scope: Box<Scope>,
    },
    ScopeStored {
        hash: ScopeHash,
        durable: bool,
    },
    Execution {
        execution: Box<ExecutionView>,
    },
    Executions {
        executions: Vec<ExecutionView>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        next_before: Option<ExecutionId>,
    },
    ExecutionSubmitted {
        execution: Box<ExecutionView>,
    },
    Output {
        chunks: Vec<OutputChunk>,
    },
    Watching {
        execution: Box<ExecutionView>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        latest_event: Option<EventId>,
    },
    PtyAttached {
        attachment: AttachmentId,
        step: StepId,
        role: PtyRole,
        control_available: bool,
        #[serde(with = "base64_bytes")]
        snapshot: Vec<u8>,
        snapshot_truncated: bool,
        next_offset: u64,
    },
    RestartAccepted {
        restart_id: String,
        target_instance_id: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionView {
    pub snapshot: ExecutionSnapshot,
    pub state: ExecutionState,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProtocolError {
    pub code: ProtocolErrorCode,
    pub message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProtocolErrorCode {
    UpgradeRequired,
    InvalidRequest,
    InvalidScope,
    NotFound,
    Conflict,
    InvalidState,
    PermissionDenied,
    NotSupported,
    Draining,
    Internal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Capability {
    OperationIdempotency,
    EventReplay,
    SharedPty,
    GracefulRestart,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "payload",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum EventPayload {
    Fact(FactEvent),
    PtyOutput {
        attachment: AttachmentId,
        offset: u64,
        #[serde(with = "base64_bytes")]
        data: Vec<u8>,
    },
    PtyRoleChanged {
        attachment: AttachmentId,
        role: PtyRole,
        control_available: bool,
    },
    PtyDetached {
        attachment: AttachmentId,
        reason: String,
    },
    ServerDraining {
        reason: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutputChunk {
    pub step: StepId,
    pub stream: OutputStream,
    pub offset: u64,
    #[serde(with = "base64_bytes")]
    pub data: Vec<u8>,
    pub eof: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PtyRole {
    Controller,
    Observer,
}

mod base64_bytes {
    use base64::Engine as _;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S>(data: &[u8], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        base64::engine::general_purpose::STANDARD
            .encode(data)
            .serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let encoded = String::deserialize(deserializer)?;
        base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use cue_core::vnext::{
        AbsolutePath, Argv, Execution, ExecutionPlan, Fact, FileModeMask, IoMode, Pipeline, Process,
    };

    use super::*;

    fn scope() -> Scope {
        Scope::new(
            AbsolutePath::new("/workspace").unwrap(),
            BTreeMap::new(),
            FileModeMask::new(0o022).unwrap(),
        )
    }

    fn execution_view() -> ExecutionView {
        let scope = scope();
        let process = Process::new(Argv::new("true", Vec::new()).unwrap());
        let spec = ExecutionSpec::new(
            scope.compute_hash(),
            ExecutionPlan::run(Pipeline::simple(process), IoMode::Captured),
        )
        .unwrap();
        let execution = Execution::new(ExecutionId(7), spec);
        ExecutionView {
            state: execution.state(),
            snapshot: execution.snapshot(),
            created_at_ms: 10,
            updated_at_ms: 10,
        }
    }

    #[test]
    fn commands_require_operation_identity_while_queries_do_not_have_one() {
        let command = Message::Command {
            request_id: RequestId::new(1).unwrap(),
            operation_id: OperationId::new("tool-call:submit").unwrap(),
            command: Command::SubmitExecution {
                spec: Box::new(execution_view().snapshot.spec.clone()),
            },
        };
        let command_json = serde_json::to_value(&command).unwrap();
        assert_eq!(command_json["type"], "command");
        assert_eq!(command_json["operation_id"], "tool-call:submit");

        let query = Message::Query {
            request_id: RequestId::new(2).unwrap(),
            query: Query::GetExecution { id: ExecutionId(7) },
        };
        let query_json = serde_json::to_value(&query).unwrap();
        assert_eq!(query_json["type"], "query");
        assert!(query_json.get("operation_id").is_none());
    }

    #[test]
    fn protocol_surface_contains_no_session_schedule_or_resource_request() {
        let view = execution_view();
        let messages = [
            Message::Command {
                request_id: RequestId::new(1).unwrap(),
                operation_id: OperationId::new("submit").unwrap(),
                command: Command::SubmitExecution {
                    spec: Box::new(view.snapshot.spec.clone()),
                },
            },
            Message::Response {
                request_id: RequestId::new(1).unwrap(),
                payload: ResponsePayload::Ok(ResultPayload::ExecutionSubmitted {
                    execution: Box::new(view),
                }),
            },
        ];
        let json = serde_json::to_string(&messages).unwrap().to_lowercase();
        for retired in ["session", "schedule", "resource", "retry_policy"] {
            assert!(
                !json.contains(retired),
                "unexpected retired owner {retired}"
            );
        }
    }

    #[test]
    fn strict_messages_reject_unknown_and_v3_ambient_fields() {
        let unknown = r#"{
            "type":"query",
            "request_id":1,
            "query":{"kind":"ping"},
            "session_id":"SS-1"
        }"#;
        assert!(serde_json::from_str::<Message>(unknown).is_err());

        let v3_handshake = r#"{
            "type":"query",
            "request_id":1,
            "query":{"kind":"hello","payload":{
                "protocol_version":4,
                "client_id":"client",
                "cwd":"/tmp",
                "env":{}
            }}
        }"#;
        assert!(serde_json::from_str::<Message>(v3_handshake).is_err());
    }

    #[test]
    fn binary_payloads_are_base64_strings_not_integer_arrays() {
        let message = Message::Command {
            request_id: RequestId::new(3).unwrap(),
            operation_id: OperationId::new("input:3").unwrap(),
            command: Command::PtyInput {
                attachment: AttachmentId::new(9).unwrap(),
                data: vec![0, 0xff, b'a'],
            },
        };
        let json = serde_json::to_value(&message).unwrap();
        assert_eq!(json["command"]["payload"]["data"], "AP9h");
        assert_eq!(serde_json::from_value::<Message>(json).unwrap(), message);
    }

    #[test]
    fn fact_execution_identity_is_explicit_and_derived_from_steps() {
        let execution = ExecutionId(4);
        let step = StepId {
            execution,
            index: 2,
        };
        let fact = Fact::OutputAppended {
            step,
            stream: OutputStream::Stderr,
            start_offset: 5,
            end_offset: 8,
        };
        assert_eq!(fact.execution_id(), execution);
    }
    #[test]
    fn sensitivity_roundtrips_through_strict_scope_commands() {
        use cue_core::vnext::{EnvKey, EnvValue, Sensitivity};
        for classification in [Sensitivity::Normal, Sensitivity::Sensitive] {
            let scope = Scope::new(
                scope().cwd().clone(),
                BTreeMap::from([(
                    EnvKey::new("VALUE").unwrap(),
                    EnvValue::classified("payload", classification).unwrap(),
                )]),
                scope().umask(),
            );
            let message = Message::Command {
                request_id: RequestId::new(1).unwrap(),
                operation_id: OperationId::new("scope-classification").unwrap(),
                command: Command::PutScope {
                    scope: Box::new(scope),
                },
            };
            let frame = crate::encode_message(&message).unwrap();
            assert_eq!(crate::decode_message(&frame).unwrap(), message);
        }
    }
}
