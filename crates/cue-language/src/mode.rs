use std::fmt;

/// Frontend input mode — determines the language construct for bare input.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum Mode {
    /// Primary execution mode: bare input compiles as `:run`.
    #[default]
    Job,
    /// Schedule mode: bare input compiles as `:cron`.
    Cron,
}

impl Mode {
    pub fn next(self) -> Self {
        match self {
            Self::Job => Self::Cron,
            Self::Cron => Self::Job,
        }
    }

    pub fn indicator(self) -> &'static str {
        match self {
            Self::Job => "⚡ JOB",
            Self::Cron => "⏰ CRON",
        }
    }

    pub fn default_command(self) -> &'static str {
        match self {
            Self::Job => "run",
            Self::Cron => "cron",
        }
    }
}

impl fmt::Display for Mode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Job => "Job",
            Self::Cron => "Cron",
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mode_cycle() {
        assert_eq!(Mode::Job.next(), Mode::Cron);
        assert_eq!(Mode::Cron.next(), Mode::Job);
    }

    #[test]
    fn mode_default_is_job() {
        assert_eq!(Mode::default(), Mode::Job);
    }
}
