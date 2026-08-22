use std::{error::Error, fmt, str::FromStr};

use crate::JobId;

/// Event subscription channels exposed by the IPC protocol.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum EventChannel {
    Executions,
    Jobs,
    Crons,
    Scopes,
    System,
    Output(JobId),
}

impl EventChannel {
    pub const EXECUTIONS: &'static str = "executions";
    pub const JOBS: &'static str = "jobs";
    pub const CRONS: &'static str = "crons";
    pub const SCOPES: &'static str = "scopes";
    pub const SYSTEM: &'static str = "system";
    pub const OUTPUT_PREFIX: &'static str = "output:";
    pub const EXPECTED: &'static str =
        "`executions`, `jobs`, `crons`, `scopes`, `system`, or `output:<job-id>`";

    pub fn parse_list(channels: &[String]) -> Result<Vec<Self>, ParseEventChannelError> {
        channels
            .iter()
            .map(|channel| channel.parse::<Self>())
            .collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseEventChannelError {
    input: String,
}

impl ParseEventChannelError {
    fn new(input: &str) -> Self {
        Self {
            input: input.to_owned(),
        }
    }

    pub fn input(&self) -> &str {
        &self.input
    }
}

impl fmt::Display for EventChannel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Executions => f.write_str(Self::EXECUTIONS),
            Self::Jobs => f.write_str(Self::JOBS),
            Self::Crons => f.write_str(Self::CRONS),
            Self::Scopes => f.write_str(Self::SCOPES),
            Self::System => f.write_str(Self::SYSTEM),
            Self::Output(job_id) => write!(f, "{}{job_id}", Self::OUTPUT_PREFIX),
        }
    }
}

impl fmt::Display for ParseEventChannelError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid event channel {}", self.input)
    }
}

impl Error for ParseEventChannelError {}

impl From<EventChannel> for String {
    fn from(channel: EventChannel) -> Self {
        channel.to_string()
    }
}

impl FromStr for EventChannel {
    type Err = ParseEventChannelError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        match input {
            Self::EXECUTIONS => Ok(Self::Executions),
            Self::JOBS => Ok(Self::Jobs),
            Self::CRONS => Ok(Self::Crons),
            Self::SCOPES => Ok(Self::Scopes),
            Self::SYSTEM => Ok(Self::System),
            _ => {
                let Some(id) = input.strip_prefix(Self::OUTPUT_PREFIX) else {
                    return Err(ParseEventChannelError::new(input));
                };
                id.parse()
                    .map(Self::Output)
                    .map_err(|_| ParseEventChannelError::new(input))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn displays_wire_names() {
        assert_eq!(EventChannel::Jobs.to_string(), "jobs");
        assert_eq!(EventChannel::Crons.to_string(), "crons");
        assert_eq!(EventChannel::Scopes.to_string(), "scopes");
        assert_eq!(EventChannel::System.to_string(), "system");
        assert_eq!(EventChannel::Output(JobId(7)).to_string(), "output:J7");
    }

    #[test]
    fn parses_known_wire_names() {
        assert_eq!("jobs".parse::<EventChannel>(), Ok(EventChannel::Jobs));
        assert_eq!("crons".parse::<EventChannel>(), Ok(EventChannel::Crons));
        assert_eq!("scopes".parse::<EventChannel>(), Ok(EventChannel::Scopes));
        assert_eq!("system".parse::<EventChannel>(), Ok(EventChannel::System));
        assert_eq!(
            "output:J7".parse::<EventChannel>(),
            Ok(EventChannel::Output(JobId(7)))
        );
    }

    #[test]
    fn rejects_unknown_or_malformed_wire_names() {
        assert!("".parse::<EventChannel>().is_err());
        assert!("job".parse::<EventChannel>().is_err());
        assert!("output:".parse::<EventChannel>().is_err());
        assert!("output:C1".parse::<EventChannel>().is_err());
        assert!("output:J+1".parse::<EventChannel>().is_err());
    }

    #[test]
    fn parses_wire_name_lists_and_reports_the_bad_channel() {
        let channels = vec!["jobs".into(), "output:J7".into()];

        assert_eq!(
            EventChannel::parse_list(&channels),
            Ok(vec![EventChannel::Jobs, EventChannel::Output(JobId(7))])
        );

        let error = EventChannel::parse_list(&["jobs".into(), "output:C1".into()])
            .expect_err("invalid channel should fail the whole list");
        assert_eq!(error.input(), "output:C1");
    }
}
