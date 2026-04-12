use serde::{Deserialize, Serialize};

/// Agent lifecycle state.
///
/// ```text
/// ┌───────┐       ┌─────────────┐       ┌────┐
/// │Running│ ←───→ │WaitingInput │ ────→ │Done│
/// └───────┘       └─────────────┘       └────┘
///     │                                 ┌──────┐
///     └───────────────────────────────→ │Failed│
///                                       └──────┘
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AgentStatus {
    Running,
    WaitingInput,
    Done,
    Failed,
}

/// How the agent process is managed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AgentKind {
    /// Subprocess with optional pty (`:fg` available).
    Cli { command: String, has_pty: bool },
    /// HTTP/API-based agent (no pty, `:fg` returns error).
    Api { model: String },
}

/// Role in the Planner/Executor hierarchy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AgentRole {
    /// Singleton, orchestrates executors.
    Planner,
    /// Multiple instances, runs concrete tasks.
    Executor,
}

impl AgentStatus {
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Done | Self::Failed)
    }

    pub fn label(&self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::WaitingInput => "waiting",
            Self::Done => "done",
            Self::Failed => "failed",
        }
    }
}

impl AgentKind {
    /// Whether `:fg` is supported for this agent kind.
    pub fn supports_fg(&self) -> bool {
        matches!(self, Self::Cli { has_pty: true, .. })
    }
}
