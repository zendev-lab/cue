//! Wire protocol for the cue-tui debug control socket.
//!
//! Transport: newline-delimited JSON over a Unix domain socket.
//! See `docs/design/tui-debug-control.md` for the full specification.

use serde::{Deserialize, Serialize};

/// Top-level debug control request from an external client.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TuiDebugRequest {
    pub id: u32,
    #[serde(flatten)]
    pub body: TuiDebugRequestBody,
}

/// Command body carried by [`TuiDebugRequest`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "command", rename_all = "kebab-case", deny_unknown_fields)]
pub enum TuiDebugRequestBody {
    Capture {
        #[serde(default)]
        styled: bool,
    },
    SendKeys {
        keys: Vec<String>,
    },
    WriteChars {
        text: String,
    },
    State,
    Subscribe {
        #[serde(default)]
        styled: bool,
    },
}

/// Successful one-shot response to a debug request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TuiDebugResponse {
    pub id: u32,
    #[serde(flatten)]
    pub body: TuiDebugResponseBody,
}

/// Response payload variants.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum TuiDebugResponseBody {
    Ok { ok: TuiDebugOkPayload },
    Err { err: TuiDebugError },
}

/// Pushed frame event for `subscribe` clients.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TuiDebugFrameEvent {
    pub event: String,
    pub text: String,
    pub width: u16,
    pub height: u16,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub styled: Option<String>,
}

impl TuiDebugFrameEvent {
    pub fn frame(text: String, width: u16, height: u16, styled: Option<String>) -> Self {
        Self {
            event: "frame".into(),
            text,
            width,
            height,
            styled,
        }
    }
}

/// Successful command result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TuiDebugOkPayload {
    Ack {},
    Capture(TuiDebugCapture),
    State(TuiDebugState),
}

/// Rendered frame text returned by `capture`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TuiDebugCapture {
    pub text: String,
    pub width: u16,
    pub height: u16,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub styled: Option<String>,
}

/// JSON summary of cue-tui app state returned by `state`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TuiDebugState {
    pub mode: String,
    pub focus: String,
    pub input: String,
    pub sidebar_visible: bool,
    pub connected: bool,
    pub job_count: usize,
    pub cron_count: usize,
    pub active_display_tab: Option<usize>,
    pub display_tab_labels: Vec<String>,
    pub fg_active: bool,
    pub should_quit: bool,
    pub terminal_width: u16,
    pub terminal_height: u16,
}

/// Structured error returned for malformed or unsupported requests.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TuiDebugError {
    pub code: String,
    pub message: String,
}

impl TuiDebugError {
    pub fn invalid_request(message: impl Into<String>) -> Self {
        Self {
            code: error_code::INVALID_REQUEST.into(),
            message: message.into(),
        }
    }

    pub fn unavailable(message: impl Into<String>) -> Self {
        Self {
            code: error_code::UNAVAILABLE.into(),
            message: message.into(),
        }
    }
}

/// Standard debug-control error codes.
pub mod error_code {
    pub const INVALID_REQUEST: &str = "INVALID_REQUEST";
    pub const UNAVAILABLE: &str = "UNAVAILABLE";
    pub const INTERNAL: &str = "INTERNAL";
}

impl TuiDebugResponse {
    pub fn ok(id: u32, payload: TuiDebugOkPayload) -> Self {
        Self {
            id,
            body: TuiDebugResponseBody::Ok { ok: payload },
        }
    }

    pub fn err(id: u32, error: TuiDebugError) -> Self {
        Self {
            id,
            body: TuiDebugResponseBody::Err { err: error },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capture_request_roundtrips() {
        let json = r#"{"id":1,"command":"capture","styled":true}"#;
        let req: TuiDebugRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.id, 1);
        assert_eq!(req.body, TuiDebugRequestBody::Capture { styled: true });
    }

    #[test]
    fn send_keys_request_roundtrips() {
        let json = r#"{"id":2,"command":"send-keys","keys":["enter","ctrl+c"]}"#;
        let req: TuiDebugRequest = serde_json::from_str(json).unwrap();
        assert_eq!(
            req.body,
            TuiDebugRequestBody::SendKeys {
                keys: vec!["enter".into(), "ctrl+c".into()],
            }
        );
    }

    #[test]
    fn response_ok_roundtrips() {
        let resp = TuiDebugResponse::ok(
            3,
            TuiDebugOkPayload::Capture(TuiDebugCapture {
                text: "hello".into(),
                width: 80,
                height: 24,
                styled: None,
            }),
        );
        let json = serde_json::to_string(&resp).unwrap();
        let decoded: TuiDebugResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, resp);
    }
}
