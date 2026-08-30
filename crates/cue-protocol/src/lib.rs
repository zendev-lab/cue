//! Strict, versioned wire contract for Cue vNext.
//!
//! The protocol depends on Core facts, never the reverse. Commands always
//! carry an operation identity; queries cannot accidentally trigger a durable
//! side effect.

mod framing;
mod id;
mod message;

pub use framing::{FrameError, MAX_MESSAGE_SIZE, decode_message, encode_message};
pub use id::{AttachmentId, ClientId, EventId, IdError, OperationId, RequestId};
pub use message::{
    Capability, Command, EventPayload, ExecutionView, Fact, FactEvent, Hello, Message, OutputChunk,
    OutputRange, OutputStream, ProtocolError, ProtocolErrorCode, PtyRole, Query, ResponsePayload,
    ResultPayload,
};

pub const PROTOCOL_VERSION: u32 = 4;
