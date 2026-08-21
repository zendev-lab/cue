//! IPC protocol types for cued ↔ client communication.
//!
//! Transport: Unix domain socket with length-prefixed JSON framing.
//! See `docs/design/ipc-protocol.md` for the full specification.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::cron::{CronSchedule, CronStatus};
use crate::event_channel::EventChannel;
use crate::execution::{CancelMode, ExecutionSpec, ExecutionState, StepState};
use crate::id::{ExecutionId, ScheduleId, ScopeHash, StepId};
use crate::resource::ResourceUnit;
use crate::scope::EnvDelta;

/// IPC protocol version required by sessionized clients.
pub const IPC_PROTOCOL_VERSION: u32 = 3;
/// Capability advertised by daemons that reject session-dependent requests before `Handshake`.
pub const IPC_CAPABILITY_SESSION_HANDSHAKE_REQUIRED: &str = "session-handshake-required";
/// Typed, quiescent cancellation for executions.
pub const IPC_CAPABILITY_CANCEL_EXECUTION: &str = "cancel-execution";
/// Cross-connection replay and conflict detection for side-effecting requests.
pub const IPC_CAPABILITY_OPERATION_IDEMPOTENCY: &str = "operation-idempotency";
/// Drain-first daemon restart with a fenced single successor.
pub const IPC_CAPABILITY_GRACEFUL_RESTART: &str = "graceful-restart";
/// Durable named process sessions that multiple human and agent clients can attach to.
pub const IPC_CAPABILITY_NAMED_SESSIONS: &str = "named-sessions";
/// Safe, reversible archive/restore lifecycle for durable named sessions.
pub const IPC_CAPABILITY_SESSION_ARCHIVE: &str = "session-archive";
/// Unified typed execution submission and observation contract.
pub const IPC_CAPABILITY_EXECUTION_V3: &str = "execution-v3";
const IPC_CAPABILITIES: &[&str] = &[
    IPC_CAPABILITY_EXECUTION_V3,
    IPC_CAPABILITY_SESSION_HANDSHAKE_REQUIRED,
    IPC_CAPABILITY_CANCEL_EXECUTION,
    IPC_CAPABILITY_OPERATION_IDEMPOTENCY,
    IPC_CAPABILITY_GRACEFUL_RESTART,
    IPC_CAPABILITY_NAMED_SESSIONS,
    IPC_CAPABILITY_SESSION_ARCHIVE,
];

pub fn current_protocol_capabilities() -> Vec<String> {
    IPC_CAPABILITIES
        .iter()
        .map(|capability| (*capability).to_string())
        .collect()
}

// ── Message Envelope ──

/// Top-level message, length-prefixed JSON over Unix socket.
///
/// The envelope schema is fixed. Unknown envelope fields are rejected instead
/// of being silently ignored.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum Message {
    Request {
        id: u32,
        /// Stable logical operation key used to deduplicate side effects across
        /// transport reconnects. It is optional for backward compatibility.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        operation_id: Option<String>,
        payload: RequestPayload,
    },
    Response {
        id: u32,
        payload: ResponsePayload,
    },
    Event {
        payload: EventPayload,
    },
}

// ── Requests (Client → cued) ──

/// Daemon input boundary. Unknown request fields are rejected so typed clients
/// cannot accidentally depend on parameters the daemon silently ignores.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum RequestPayload {
    // Connection management
    Handshake {
        /// Exact wire version. v3 is a hard cut; older clients receive an
        /// explicit upgrade error before any session or execution side effect.
        #[serde(default)]
        protocol_version: u32,
        session_id: String,
        cwd: String,
        env: BTreeMap<String, String>,
        /// Explicitly replace an existing session cursor with this handshake snapshot.
        /// Defaults to false so ordinary reconnects keep the existing session scope.
        #[serde(default)]
        refresh: bool,
    },
    /// Create a durable named session from the calling client's current scope
    /// and attach that client to it.
    CreateSession {
        name: String,
    },
    /// List active durable named sessions known to this daemon.
    /// Archived sessions are omitted; use `ListArchivedSessions` or
    /// `ListAllSessions` when cleanup state must be inspected explicitly.
    ListSessions {},
    /// List only archived durable named sessions.
    ListArchivedSessions {},
    /// List active and archived durable named sessions.
    ListAllSessions {},
    /// Hide an idle durable named session from the default list without
    /// deleting its identity, scope cursor, or terminal history.
    ArchiveSession {
        selector: String,
    },
    /// Make a previously archived durable named session attachable again.
    RestoreSession {
        selector: String,
    },
    /// Attach the calling client to an existing durable named session.
    ///
    /// `refresh` is required when a sensitive, process-local scope could not
    /// survive a daemon restart. It deliberately replaces the named session's
    /// missing cursor with the calling client's current scope.
    AttachSession {
        selector: String,
        #[serde(default)]
        refresh: bool,
    },
    /// Inspect the current named session or an explicitly selected one.
    SessionInfo {
        selector: Option<String>,
    },
    Subscribe {
        channels: Vec<String>,
    },
    Unsubscribe {
        channels: Vec<String>,
    },

    // Unified execution runtime.
    SubmitExecution {
        spec: Box<ExecutionSpec>,
    },
    GetExecution {
        id: ExecutionId,
    },
    ListExecutions {
        limit: Option<usize>,
    },
    WaitExecution {
        id: ExecutionId,
    },
    ReadExecutionOutput {
        id: ExecutionId,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        step_id: Option<StepId>,
        stdout_bytes: Option<usize>,
        stderr_bytes: Option<usize>,
    },
    ApplyScopeDelta {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        base: Option<ScopeHash>,
        delta: EnvDelta,
    },
    GetScope {
        hash: ScopeHash,
    },
    CreateSchedule {
        schedule: CronSchedule,
        execution: Box<ExecutionSpec>,
    },
    ListSchedules {
        limit: Option<usize>,
    },
    PauseSchedule {
        id: ScheduleId,
    },
    ResumeSchedule {
        id: ScheduleId,
    },
    RemoveSchedule {
        id: ScheduleId,
    },
    StepAttach {
        id: StepId,
    },
    StepWatch {
        id: StepId,
    },

    /// Acquire the free controller lease for the currently observed PTY job.
    StepClaimControl {},
    /// Release the controller lease while remaining an observer.
    StepReleaseControl {},
    StepDetach {},
    StepInput {
        #[serde(with = "serde_bytes_base64")]
        data: Vec<u8>,
    },
    StepResize {
        cols: u16,
        rows: u16,
    },

    // Typed scope/configuration queries.
    ListScopes {
        limit: Option<usize>,
    },
    /// Inspect provider routing, current capacity, and active reservations.
    ListResources {},
    CancelExecution {
        id: ExecutionId,
        mode: CancelMode,
    },
    ShowEnv {
        tail_bytes: Option<usize>,
    },
    ShowConfig {
        tail_bytes: Option<usize>,
    },

    // System
    Ping {},
    /// Stop new execution admission, let already accepted work finish, then
    /// hand ownership to one successor daemon.
    Restart {},
    Shutdown {},
}

impl RequestPayload {
    pub fn subscribe(channels: &[EventChannel]) -> Self {
        Self::Subscribe {
            channels: event_channel_names(channels),
        }
    }

    pub fn unsubscribe(channels: &[EventChannel]) -> Self {
        Self::Unsubscribe {
            channels: event_channel_names(channels),
        }
    }
}

fn event_channel_names(channels: &[EventChannel]) -> Vec<String> {
    channels.iter().map(ToString::to_string).collect()
}

// ── Responses (cued → Client) ──

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ResponsePayload {
    Ok(OkPayload),
    Err { code: String, message: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OkPayload {
    Ack {},
    ExecutionCreated {
        execution: Box<ExecutionInfo>,
    },
    ExecutionInfo(Box<ExecutionInfo>),
    ExecutionList(Vec<ExecutionInfo>),
    ExecutionOutput {
        id: ExecutionId,
        steps: Vec<StepOutput>,
    },
    ScheduleCreated {
        schedule: Box<ScheduleInfo>,
    },
    ScheduleList(Vec<ScheduleInfo>),
    ScopeCreated {
        hash: String,
        summary: String,
    },

    ScopeInfo(ScopeInfo),
    ScopeList(Vec<ScopeInfo>),
    ScopeListPage {
        scopes: Vec<ScopeInfo>,
        page: PageInfo,
    },
    ResourceList(Vec<ResourceProviderInfo>),
    SessionInfo(Box<SessionInfo>),
    SessionList(Vec<SessionInfo>),
    TextOutput {
        text: String,
        truncated: bool,
        #[serde(default)]
        encoding: OutputEncoding,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        base64: Option<String>,
    },

    FgAttached(Box<ForegroundAttachmentInfo>),
    FgRoleChanged {
        id: StepId,
        /// Identifies the exact foreground attachment this transition belongs to.
        #[serde(default)]
        attachment_id: u64,
        role: ForegroundRole,
        control_available: bool,
    },
    Pong {
        /// Daemon `cued` build version reported by the running daemon.
        version: String,
        /// Stable UUID for this daemon process. Changes after every restart.
        /// Empty when talking to a daemon that predates instance IDs.
        #[serde(default)]
        instance_id: String,
        /// Restart generation token. A planned successor must match the target
        /// generation preallocated in the restart intent.
        #[serde(default)]
        generation_id: String,
        /// True only after startup restoration, exact restart fencing, and
        /// scheduler execution activation have all completed. Missing means
        /// true for compatibility with daemons predating startup fencing.
        #[serde(default = "default_pong_ready")]
        ready: bool,
        /// IPC protocol version implemented by the daemon.
        protocol_version: u32,
        /// Feature flags implemented by the daemon for explicit client gating.
        capabilities: Vec<String>,
    },
    RestartAccepted {
        /// Stable across repeated restart requests handled by this generation.
        restart_id: String,
        /// The daemon generation that accepted and owns the drain.
        daemon_instance_id: String,
        /// Exact generation token the successor must advertise in Pong.
        target_generation: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceProviderInfo {
    pub id: String,
    pub keys: Vec<String>,
    pub active_reservations: usize,
    pub captured_at_ms: u64,
    pub units: Vec<ResourceUnit>,
}

fn default_pong_ready() -> bool {
    true
}

// ── Events (cued → Client, pushed) ──

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EventPayload {
    ExecutionCreated {
        execution: Box<ExecutionInfo>,
    },
    ExecutionStateChanged {
        id: ExecutionId,
        old_state: ExecutionState,
        new_state: ExecutionState,
    },
    StepStateChanged {
        id: StepId,
        old_state: StepState,
        new_state: StepState,
    },
    ExecutionFinished {
        execution: Box<ExecutionInfo>,
    },
    OutputChunk {
        id: StepId,
        stream: Stream,
        #[serde(with = "serde_bytes_base64")]
        data: Vec<u8>,
    },

    // :fg (sent only to fg-attached client)
    FgOutput {
        id: StepId,
        attachment_id: u64,
        #[serde(with = "serde_bytes_base64")]
        data: Vec<u8>,
    },
    FgControlChanged {
        id: StepId,
        attachment_id: u64,
        control_available: bool,
    },
    FgExited {
        id: StepId,
        attachment_id: u64,
        reason: String,
    },

    // System channel
    ShuttingDown {
        reason: String,
    },
}

/// Output stream type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Stream {
    Stdout,
    Stderr,
}

/// A client's effective role in a shared foreground attachment.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ForegroundRole {
    /// Owns the exclusive input and resize lease.
    #[default]
    Controller,
    /// Receives output and exit events but cannot write or resize.
    Observer,
}

/// Atomic foreground registration result: a byte snapshot followed by live events.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForegroundAttachmentInfo {
    pub id: StepId,
    /// Monotonic, non-zero identifier for this exact job/client attachment.
    #[serde(default)]
    pub attachment_id: u64,
    /// Defaults to the historical exclusive attachment role when decoding an
    /// old `{ "FgAttached": { "id": ... } }` response.
    #[serde(default)]
    pub role: ForegroundRole,
    #[serde(default)]
    pub control_available: bool,
    #[serde(default, with = "serde_bytes_base64")]
    pub snapshot: Vec<u8>,
    #[serde(default)]
    pub snapshot_truncated: bool,
}

// ── Info structs (shared by Response and queries) ──

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PageInfo {
    pub total: usize,
    pub shown: usize,
    pub limit: Option<usize>,
    pub truncated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamText {
    /// Backward-compatible display text. For binary output this is an explicit
    /// lossy UTF-8 view; `base64` is the authoritative byte representation.
    pub data: String,
    pub truncated: bool,
    #[serde(default)]
    pub encoding: OutputEncoding,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base64: Option<String>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OutputEncoding {
    #[default]
    Utf8,
    Base64,
}

/// Reconnect-safe projection of one unified execution.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionInfo {
    pub id: ExecutionId,
    pub state: ExecutionState,
    pub steps: Vec<ExecutionStepInfo>,
    /// Original replayable contract with ephemeral launch leases removed.
    pub spec: ExecutionSpec,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionStepInfo {
    pub id: StepId,
    pub state: StepState,
    pub pipeline: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StepOutput {
    pub id: StepId,
    pub stdout: StreamText,
    pub stderr: StreamText,
    pub stderr_pty_merged: bool,
}

/// Durable trigger template. The execution contract never contains an
/// ephemeral spawn adapter.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScheduleInfo {
    pub id: ScheduleId,
    pub schedule: CronSchedule,
    pub execution: ExecutionSpec,
    pub status: CronStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_trigger_at_ms: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScopeInfo {
    pub hash: String,
    pub parent: Option<String>,
    pub cwd: String,
    pub env_count: usize,
}

/// Whether a named session cursor can survive a daemon restart.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionScopeState {
    /// The scope is available now and has a durable SQLite record.
    ReadyDurable,
    /// The scope is available to this daemon process but intentionally stays
    /// in memory because it contains credential-like environment names.
    ReadyVolatile,
    /// The durable identity survived a restart, but its volatile scope did
    /// not. An explicit refreshed attach is required before execution.
    NeedsRefresh,
}

/// Public metadata for a durable named process session.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionInfo {
    pub id: String,
    pub name: String,
    pub scope_state: SessionScopeState,
    pub scope_hash: Option<String>,
    pub connected_clients: usize,
    pub restart_safe: bool,
    pub current: bool,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
    /// Present when the session is hidden from the default active-session list.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub archived_at_ms: Option<i64>,
}

// ── Error codes ──

/// Standard IPC error codes.
pub mod error_code {
    pub const PROTOCOL_UPGRADE_REQUIRED: &str = "PROTOCOL_UPGRADE_REQUIRED";
    pub const NOT_FOUND: &str = "NOT_FOUND";
    pub const INVALID_REQUEST: &str = "INVALID_REQUEST";
    pub const INVALID_STATE: &str = "INVALID_STATE";
    pub const INVALID_SCOPE: &str = "INVALID_SCOPE";
    pub const INVALID_SYNTAX: &str = "INVALID_SYNTAX";
    pub const ALREADY_EXISTS: &str = "ALREADY_EXISTS";
    pub const NOT_SUPPORTED: &str = "NOT_SUPPORTED";
    pub const PERMISSION_DENIED: &str = "PERMISSION_DENIED";
    pub const BLOCKED: &str = "BLOCKED";
    pub const WARNED: &str = "WARNED";
    pub const INTERNAL: &str = "INTERNAL";
    pub const DAEMON_DRAINING: &str = "DAEMON_DRAINING";
}

impl ResponsePayload {
    /// Convenience: create an Ok(Ack) response.
    pub fn ack() -> Self {
        Self::Ok(OkPayload::Ack {})
    }

    /// Convenience: create an error response.
    pub fn err(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Err {
            code: code.into(),
            message: message.into(),
        }
    }
}

// ── Framing helpers ──

/// Maximum message body size (16 MiB).
pub const MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024;

/// Encode a message to length-prefixed JSON bytes.
pub fn encode_message(msg: &Message) -> Result<Vec<u8>, serde_json::Error> {
    let json = serde_json::to_vec(msg)?;
    let len = json.len() as u32;
    let mut buf = Vec::with_capacity(4 + json.len());
    buf.extend_from_slice(&len.to_be_bytes());
    buf.extend_from_slice(&json);
    Ok(buf)
}

/// Serde helper for Vec<u8> ↔ base64 string (for binary data in JSON).
mod serde_bytes_base64 {
    use base64::Engine as _;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S: Serializer>(data: &Vec<u8>, serializer: S) -> Result<S::Ok, S::Error> {
        base64::engine::general_purpose::STANDARD
            .encode(data)
            .serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<u8>, D::Error> {
        let text = String::deserialize(deserializer)?;
        base64::engine::general_purpose::STANDARD
            .decode(text)
            .map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn step(execution: u64) -> StepId {
        StepId {
            execution: ExecutionId(execution),
            index: 1,
        }
    }

    #[test]
    fn roundtrip_typed_execution_request() {
        let msg = Message::Request {
            id: 1,
            operation_id: Some("tool-call-1:execute".into()),
            payload: RequestPayload::SubmitExecution {
                spec: Box::new(ExecutionSpec {
                    plan: crate::execution::ExecutionPlan::pipeline(
                        crate::pipeline::Pipeline::simple(vec!["true".into()]),
                    ),
                    start_scope: None,
                    launch_context: Default::default(),
                    source: None,
                    retry_of: None,
                }),
            },
        };
        let encoded = encode_message(&msg).unwrap();
        // First 4 bytes = length
        let len = u32::from_be_bytes(encoded[..4].try_into().unwrap()) as usize;
        assert_eq!(len, encoded.len() - 4);
        // Deserialize body
        let decoded: Message = serde_json::from_slice(&encoded[4..]).unwrap();
        if let Message::Request {
            id,
            operation_id,
            payload: RequestPayload::SubmitExecution { spec },
        } = decoded
        {
            assert_eq!(id, 1);
            assert_eq!(operation_id.as_deref(), Some("tool-call-1:execute"));
            assert_eq!(spec.plan.node_count(), 1);
        } else {
            panic!("wrong variant");
        }
    }

    #[test]
    fn request_message_rejects_unknown_envelope_fields() {
        let json = r#"{"type":"request","id":1,"payload":{"Ping":{}},"trace_id":"abc"}"#;

        let error = serde_json::from_str::<Message>(json)
            .expect_err("unknown top-level message fields must not be ignored");

        assert!(
            error.to_string().contains("unknown field `trace_id`"),
            "wrong error: {error}"
        );
    }

    #[test]
    fn subscription_request_constructors_use_event_channel_wire_names() {
        let subscribe = RequestPayload::subscribe(&[
            EventChannel::Executions,
            EventChannel::Scopes,
            EventChannel::System,
        ]);
        match subscribe {
            RequestPayload::Subscribe { channels } => {
                assert_eq!(channels, vec!["executions", "scopes", "system"]);
            }
            _ => panic!("wrong variant"),
        }

        let unsubscribe =
            RequestPayload::unsubscribe(&[EventChannel::Scopes, EventChannel::System]);
        match unsubscribe {
            RequestPayload::Unsubscribe { channels } => {
                assert_eq!(channels, vec!["scopes", "system"]);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn roundtrip_error_response() {
        let msg = Message::Response {
            id: 1,
            payload: ResponsePayload::err("INVALID_SYNTAX", "unexpected token"),
        };
        let json = serde_json::to_string(&msg).unwrap();
        let decoded: Message = serde_json::from_str(&json).unwrap();
        if let Message::Response {
            payload: ResponsePayload::Err { code, message },
            ..
        } = decoded
        {
            assert_eq!(code, "INVALID_SYNTAX");
            assert_eq!(message, "unexpected token");
        } else {
            panic!("wrong variant");
        }
    }

    #[test]
    fn response_payload_helpers() {
        assert!(matches!(
            ResponsePayload::ack(),
            ResponsePayload::Ok(OkPayload::Ack {})
        ));
    }

    #[test]
    fn typed_output_query_roundtrips() {
        let msg = Message::Request {
            id: 7,
            operation_id: None,
            payload: RequestPayload::ReadExecutionOutput {
                id: ExecutionId(1),
                step_id: Some(StepId {
                    execution: ExecutionId(1),
                    index: 2,
                }),
                stdout_bytes: Some(20),
                stderr_bytes: Some(4096),
            },
        };
        let json = serde_json::to_string(&msg).unwrap();
        let decoded: Message = serde_json::from_str(&json).unwrap();
        match decoded {
            Message::Request {
                payload:
                    RequestPayload::ReadExecutionOutput {
                        id,
                        step_id,
                        stdout_bytes,
                        stderr_bytes,
                    },
                ..
            } => {
                assert_eq!(id, ExecutionId(1));
                assert_eq!(step_id.expect("step").index, 2);
                assert_eq!(stdout_bytes, Some(20));
                assert_eq!(stderr_bytes, Some(4096));
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn named_session_requests_and_info_roundtrip() {
        let requests = [
            RequestPayload::CreateSession {
                name: "shared-dev".into(),
            },
            RequestPayload::ListSessions {},
            RequestPayload::ListArchivedSessions {},
            RequestPayload::ListAllSessions {},
            RequestPayload::ArchiveSession {
                selector: "shared-dev".into(),
            },
            RequestPayload::RestoreSession {
                selector: "SS-1".into(),
            },
            RequestPayload::AttachSession {
                selector: "shared-dev".into(),
                refresh: true,
            },
            RequestPayload::SessionInfo { selector: None },
        ];
        for payload in requests {
            let json = serde_json::to_string(&payload).expect("serialize session request");
            serde_json::from_str::<RequestPayload>(&json).expect("deserialize session request");
        }

        let info = SessionInfo {
            id: "SS-1".into(),
            name: "shared-dev".into(),
            scope_state: SessionScopeState::ReadyVolatile,
            scope_hash: Some("abc".into()),
            connected_clients: 2,
            restart_safe: false,
            current: true,
            created_at_ms: 1,
            updated_at_ms: 2,
            archived_at_ms: Some(3),
        };
        let payload = OkPayload::SessionInfo(Box::new(info.clone()));
        let decoded: OkPayload =
            serde_json::from_str(&serde_json::to_string(&payload).expect("serialize session info"))
                .expect("deserialize session info");
        assert!(matches!(decoded, OkPayload::SessionInfo(actual) if actual.as_ref() == &info));
        assert!(
            current_protocol_capabilities()
                .iter()
                .any(|capability| capability == IPC_CAPABILITY_NAMED_SESSIONS)
        );
        assert!(
            current_protocol_capabilities()
                .iter()
                .any(|capability| capability == IPC_CAPABILITY_SESSION_ARCHIVE)
        );

        let legacy_json = r#"{"id":"SS-1","name":"shared-dev","scope_state":"ready_durable","scope_hash":null,"connected_clients":0,"restart_safe":true,"current":false,"created_at_ms":1,"updated_at_ms":2}"#;
        let legacy: SessionInfo = serde_json::from_str(legacy_json).expect("legacy session info");
        assert_eq!(legacy.archived_at_ms, None);
    }

    #[test]
    fn scope_created_payload_has_no_label_field() {
        let payload = ResponsePayload::Ok(OkPayload::ScopeCreated {
            hash: "S@abc12345".into(),
            summary: "S@abc12345\ncwd: /old -> /tmp".into(),
        });
        let json = serde_json::to_string(&payload).unwrap();
        assert!(!json.contains("label"));

        let decoded: ResponsePayload = serde_json::from_str(&json).unwrap();
        match decoded {
            ResponsePayload::Ok(OkPayload::ScopeCreated { hash, summary }) => {
                assert_eq!(hash, "S@abc12345");
                assert!(summary.contains("cwd: /old -> /tmp"));
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn binary_payloads_serialize_as_base64_strings() {
        let msg = Message::Event {
            payload: EventPayload::FgOutput {
                id: step(7),
                attachment_id: 11,
                data: vec![0, 1, 2, 0xfe, 0xff],
            },
        };
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("\"AAEC/v8=\""));
    }

    #[test]
    fn foreground_attachment_snapshot_serializes_as_base64() {
        let payload =
            ResponsePayload::Ok(OkPayload::FgAttached(Box::new(ForegroundAttachmentInfo {
                id: step(7),
                attachment_id: 23,
                role: ForegroundRole::Observer,
                control_available: true,
                snapshot: vec![0, 1, 2, 0xfe, 0xff],
                snapshot_truncated: true,
            })));

        let json = serde_json::to_string(&payload).unwrap();
        assert!(json.contains("\"AAEC/v8=\""));
        let decoded: ResponsePayload = serde_json::from_str(&json).unwrap();
        assert!(matches!(
            decoded,
            ResponsePayload::Ok(OkPayload::FgAttached(info))
                if info.attachment_id == 23
                    && info.role == ForegroundRole::Observer
                    && info.control_available
                    && info.snapshot == vec![0, 1, 2, 0xfe, 0xff]
                    && info.snapshot_truncated
        ));
    }

    #[test]
    fn shared_foreground_requests_and_control_event_roundtrip() {
        for payload in [
            RequestPayload::StepWatch {
                id: StepId {
                    execution: ExecutionId(7),
                    index: 1,
                },
            },
            RequestPayload::StepClaimControl {},
            RequestPayload::StepReleaseControl {},
        ] {
            let json = serde_json::to_string(&payload).unwrap();
            serde_json::from_str::<RequestPayload>(&json).unwrap();
        }

        let message = Message::Event {
            payload: EventPayload::FgControlChanged {
                id: step(7),
                attachment_id: 29,
                control_available: true,
            },
        };
        let json = serde_json::to_string(&message).unwrap();
        assert!(matches!(
            serde_json::from_str::<Message>(&json).unwrap(),
            Message::Event {
                payload: EventPayload::FgControlChanged {
                    id,
                    attachment_id: 29,
                    control_available: true,
                },
            } if id == step(7)
        ));

        let role_response = ResponsePayload::Ok(OkPayload::FgRoleChanged {
            id: step(7),
            attachment_id: 29,
            role: ForegroundRole::Controller,
            control_available: false,
        });
        let json = serde_json::to_string(&role_response).unwrap();
        assert!(matches!(
            serde_json::from_str::<ResponsePayload>(&json).unwrap(),
            ResponsePayload::Ok(OkPayload::FgRoleChanged {
                id,
                attachment_id: 29,
                role: ForegroundRole::Controller,
                control_available: false,
            }) if id == step(7)
        ));
        assert!(
            current_protocol_capabilities()
                .iter()
                .any(|capability| capability == IPC_CAPABILITY_EXECUTION_V3)
        );
    }

    #[test]
    fn binary_payloads_reject_array_encoding() {
        let json = r#"{"type":"event","payload":{"FgOutput":{"data":[65,66,67]}}}"#;
        let error = serde_json::from_str::<Message>(json)
            .expect_err("binary payloads must use base64 string encoding");

        assert!(
            error.to_string().contains("invalid type"),
            "wrong error: {error}"
        );
    }

    #[test]
    fn pong_requires_version_field() {
        let json = r#"{"Ok":{"Pong":{}}}"#;
        let error = serde_json::from_str::<ResponsePayload>(json)
            .expect_err("Pong must carry a daemon version");

        assert!(
            error.to_string().contains("missing field `version`"),
            "wrong error: {error}"
        );
    }

    #[test]
    fn pong_requires_protocol_version_field() {
        let json = r#"{"Ok":{"Pong":{"version":"0.1.0","capabilities":[]}}}"#;
        let error = serde_json::from_str::<ResponsePayload>(json)
            .expect_err("Pong must carry a protocol version");

        assert!(
            error
                .to_string()
                .contains("missing field `protocol_version`"),
            "wrong error: {error}"
        );
    }

    #[test]
    fn pong_requires_capabilities_field() {
        let json = r#"{"Ok":{"Pong":{"version":"0.1.0","instance_id":"00000000-0000-4000-8000-000000000000","protocol_version":2}}}"#;
        let error = serde_json::from_str::<ResponsePayload>(json)
            .expect_err("Pong must carry protocol capabilities");

        assert!(
            error.to_string().contains("missing field `capabilities`"),
            "wrong error: {error}"
        );
    }

    #[test]
    fn pong_decodes_legacy_payload_without_instance_id() {
        let json = r#"{"type":"response","id":7,"payload":{"Ok":{"Pong":{"version":"0.1.0","protocol_version":3,"capabilities":["session-handshake-required"]}}}}"#;
        let decoded: Message = serde_json::from_str(json).unwrap();

        match decoded {
            Message::Response {
                id: 7,
                payload:
                    ResponsePayload::Ok(OkPayload::Pong {
                        version,
                        instance_id,
                        generation_id,
                        ready,
                        protocol_version,
                        capabilities,
                    }),
            } => {
                assert_eq!(version, "0.1.0");
                assert_eq!(instance_id, "");
                assert_eq!(generation_id, "");
                assert!(ready);
                assert_eq!(protocol_version, IPC_PROTOCOL_VERSION);
                assert_eq!(
                    capabilities,
                    vec![IPC_CAPABILITY_SESSION_HANDSHAKE_REQUIRED.to_string()]
                );
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn pong_decodes_versioned_payload() {
        let json = r#"{"Ok":{"Pong":{"version":"0.1.0","instance_id":"00000000-0000-4000-8000-000000000000","generation_id":"generation-1","protocol_version":3,"capabilities":["execution-v3","session-handshake-required","cancel-execution","operation-idempotency","graceful-restart","named-sessions","session-archive"]}}}"#;
        let decoded: ResponsePayload = serde_json::from_str(json).unwrap();
        match decoded {
            ResponsePayload::Ok(OkPayload::Pong {
                version,
                instance_id,
                generation_id,
                ready,
                protocol_version,
                capabilities,
            }) => {
                assert_eq!(version, "0.1.0");
                assert_eq!(instance_id, "00000000-0000-4000-8000-000000000000");
                assert_eq!(generation_id, "generation-1");
                assert!(ready);
                assert_eq!(protocol_version, IPC_PROTOCOL_VERSION);
                assert_eq!(capabilities, current_protocol_capabilities());
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn pong_serializes_reported_version() {
        let payload = ResponsePayload::Ok(OkPayload::Pong {
            version: "0.1.0".into(),
            instance_id: "00000000-0000-4000-8000-000000000000".into(),
            generation_id: "generation-1".into(),
            ready: true,
            protocol_version: IPC_PROTOCOL_VERSION,
            capabilities: current_protocol_capabilities(),
        });
        let json = serde_json::to_string(&payload).unwrap();
        assert_eq!(
            json,
            r#"{"Ok":{"Pong":{"version":"0.1.0","instance_id":"00000000-0000-4000-8000-000000000000","generation_id":"generation-1","ready":true,"protocol_version":3,"capabilities":["execution-v3","session-handshake-required","cancel-execution","operation-idempotency","graceful-restart","named-sessions","session-archive"]}}}"#
        );
    }

    #[test]
    fn cancel_execution_roundtrips_as_typed_request() {
        let message = Message::Request {
            id: 42,
            operation_id: None,
            payload: RequestPayload::CancelExecution {
                id: ExecutionId(7),
                mode: CancelMode::Force,
            },
        };
        let json = serde_json::to_string(&message).unwrap();
        assert_eq!(
            json,
            r#"{"type":"request","id":42,"payload":{"CancelExecution":{"id":7,"mode":"force"}}}"#
        );
        assert!(matches!(
            serde_json::from_str::<Message>(&json).unwrap(),
            Message::Request {
                id: 42,
                payload: RequestPayload::CancelExecution { id, mode: CancelMode::Force },
                ..
            } if id == ExecutionId(7)
        ));
    }
}
