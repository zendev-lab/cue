use std::{error::Error, fmt, str::FromStr};

/// Event subscription channels exposed by the IPC protocol.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum EventChannel {
    Executions,
    Scopes,
    System,
}

impl EventChannel {
    pub const EXECUTIONS: &'static str = "executions";
    pub const SCOPES: &'static str = "scopes";
    pub const SYSTEM: &'static str = "system";
    pub const EXPECTED: &'static str = "`executions`, `scopes`, or `system`";

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
            Self::Scopes => f.write_str(Self::SCOPES),
            Self::System => f.write_str(Self::SYSTEM),
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
            Self::SCOPES => Ok(Self::Scopes),
            Self::SYSTEM => Ok(Self::System),
            _ => Err(ParseEventChannelError::new(input)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn displays_wire_names() {
        assert_eq!(EventChannel::Executions.to_string(), "executions");
        assert_eq!(EventChannel::Scopes.to_string(), "scopes");
        assert_eq!(EventChannel::System.to_string(), "system");
    }

    #[test]
    fn parses_known_wire_names() {
        assert_eq!(
            "executions".parse::<EventChannel>(),
            Ok(EventChannel::Executions)
        );
        assert_eq!("scopes".parse::<EventChannel>(), Ok(EventChannel::Scopes));
        assert_eq!("system".parse::<EventChannel>(), Ok(EventChannel::System));
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
        let channels = vec!["executions".into(), "scopes".into()];

        assert_eq!(
            EventChannel::parse_list(&channels),
            Ok(vec![EventChannel::Executions, EventChannel::Scopes])
        );

        let error = EventChannel::parse_list(&["executions".into(), "jobs".into()])
            .expect_err("invalid channel should fail the whole list");
        assert_eq!(error.input(), "jobs");
    }
}
