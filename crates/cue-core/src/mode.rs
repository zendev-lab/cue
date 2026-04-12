use std::fmt;

use serde::{Deserialize, Serialize};

/// TUI input mode — determines default command for bare input.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
pub enum Mode {
    /// ⚡ Bare input → `:run`
    #[default]
    Job,
    /// 🤖 Bare input → `:ask`
    Agent,
    /// ⏰ Bare input → `:cron`
    Cron,
}

impl Mode {
    /// Cycle to next mode (Shift+Tab).
    pub fn next(self) -> Self {
        match self {
            Self::Job => Self::Agent,
            Self::Agent => Self::Cron,
            Self::Cron => Self::Job,
        }
    }

    /// Status bar indicator.
    pub fn indicator(self) -> &'static str {
        match self {
            Self::Job => "⚡ JOB",
            Self::Agent => "🤖 AGENT",
            Self::Cron => "⏰ CRON",
        }
    }

    /// Default command name injected for bare input.
    pub fn default_command(self) -> &'static str {
        match self {
            Self::Job => "run",
            Self::Agent => "ask",
            Self::Cron => "cron",
        }
    }
}

impl fmt::Display for Mode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Job => "Job",
            Self::Agent => "Agent",
            Self::Cron => "Cron",
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mode_cycle() {
        assert_eq!(Mode::Job.next(), Mode::Agent);
        assert_eq!(Mode::Agent.next(), Mode::Cron);
        assert_eq!(Mode::Cron.next(), Mode::Job);
    }

    #[test]
    fn mode_default_is_job() {
        assert_eq!(Mode::default(), Mode::Job);
    }
}
