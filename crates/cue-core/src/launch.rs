use std::path::PathBuf;

use serde::{Deserialize, Serialize};

/// Exit code used when a process has no OS-provided exit status.
pub const EXIT_CODE_UNAVAILABLE: i32 = -1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SandboxSettings {
    pub mode: SandboxMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upper: Option<SandboxUpper>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SandboxMode {
    Overlay,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SandboxUpper {
    Directory(PathBuf),
    Tmpfs,
}
