use std::{error::Error, fmt, str::FromStr};

use serde::{Deserialize, Serialize};

/// Job sequence number, displayed as J1, J2, ...
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct JobId(pub u32);

/// Cron sequence number, displayed as C1, C2, ...
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CronId(pub u32);

/// Chain sequence number, displayed as CH1, CH2, ...
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ChainId(pub u32);

/// Script submission sequence number, displayed as R1, R2, ...
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ScriptId(pub u32);

/// Unified execution sequence number, displayed as E1, E2, ...
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ExecutionId(pub u64);

/// Stable process-step identity within one execution, displayed as E1/S1.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct StepId {
    pub execution: ExecutionId,
    pub index: u32,
}

/// Content-addressed scope hash (blake3), displayed as S@a3f1...
#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ScopeHash(pub [u8; 32]);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseIdError {
    kind: &'static str,
    input: String,
}

impl ParseIdError {
    fn new(kind: &'static str, input: &str) -> Self {
        Self {
            kind,
            input: input.to_owned(),
        }
    }
}

/// Unified entity reference for commands like :fg, :kill, :out
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EntityRef {
    Job(JobId),
    Cron(CronId),
    Scope(ScopeHash),
}

// --- Display impls ---

impl fmt::Display for JobId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "J{}", self.0)
    }
}

impl fmt::Display for CronId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "C{}", self.0)
    }
}

impl fmt::Display for ChainId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CH{}", self.0)
    }
}

impl fmt::Display for ScriptId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "R{}", self.0)
    }
}

impl fmt::Display for ExecutionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "E{}", self.0)
    }
}

impl fmt::Display for StepId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/S{}", self.execution, self.index)
    }
}

impl fmt::Display for ScopeHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Show first 4 bytes (8 hex chars) as short form
        let hex: String = self.0[..4].iter().map(|b| format!("{b:02x}")).collect();
        write!(f, "S@{hex}")
    }
}

impl fmt::Debug for ScopeHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ScopeHash({self})")
    }
}

impl fmt::Display for EntityRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Job(id) => write!(f, "{id}"),
            Self::Cron(id) => write!(f, "{id}"),
            Self::Scope(hash) => write!(f, "{hash}"),
        }
    }
}

impl fmt::Display for ParseIdError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid {} id {}", self.kind, self.input)
    }
}

impl Error for ParseIdError {}

impl FromStr for JobId {
    type Err = ParseIdError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        parse_prefixed_id(input, "J", "job").map(Self)
    }
}

impl FromStr for CronId {
    type Err = ParseIdError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        parse_prefixed_id(input, "C", "cron").map(Self)
    }
}

impl FromStr for ChainId {
    type Err = ParseIdError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        parse_prefixed_id(input, "CH", "chain").map(Self)
    }
}

impl FromStr for ScriptId {
    type Err = ParseIdError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        parse_prefixed_id(input, "R", "script").map(Self)
    }
}

impl FromStr for ExecutionId {
    type Err = ParseIdError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        parse_prefixed_u64(input, "E", "execution").map(Self)
    }
}

impl FromStr for StepId {
    type Err = ParseIdError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let (execution, step) = input
            .split_once("/S")
            .ok_or_else(|| ParseIdError::new("step", input))?;
        let execution = execution
            .parse::<ExecutionId>()
            .map_err(|_| ParseIdError::new("step", input))?;
        let index = step
            .parse::<u32>()
            .ok()
            .filter(|index| *index > 0)
            .ok_or_else(|| ParseIdError::new("step", input))?;
        Ok(Self { execution, index })
    }
}

fn parse_prefixed_id(input: &str, prefix: &str, kind: &'static str) -> Result<u32, ParseIdError> {
    let digits = input
        .strip_prefix(prefix)
        .ok_or_else(|| ParseIdError::new(kind, input))?;
    if digits.is_empty() || !digits.chars().all(|ch| ch.is_ascii_digit()) {
        return Err(ParseIdError::new(kind, input));
    }
    digits.parse().map_err(|_| ParseIdError::new(kind, input))
}

fn parse_prefixed_u64(input: &str, prefix: &str, kind: &'static str) -> Result<u64, ParseIdError> {
    let digits = input
        .strip_prefix(prefix)
        .ok_or_else(|| ParseIdError::new(kind, input))?;
    if digits.is_empty() || !digits.chars().all(|ch| ch.is_ascii_digit()) {
        return Err(ParseIdError::new(kind, input));
    }
    digits.parse().map_err(|_| ParseIdError::new(kind, input))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_ids() {
        assert_eq!(JobId(1).to_string(), "J1");
        assert_eq!(CronId(3).to_string(), "C3");
        assert_eq!(ChainId(7).to_string(), "CH7");
        assert_eq!(ScriptId(9).to_string(), "R9");
        assert_eq!(ExecutionId(11).to_string(), "E11");
        assert_eq!(
            StepId {
                execution: ExecutionId(11),
                index: 3,
            }
            .to_string(),
            "E11/S3"
        );
    }

    #[test]
    fn parse_ids() {
        assert_eq!("J1".parse::<JobId>(), Ok(JobId(1)));
        assert_eq!("C3".parse::<CronId>(), Ok(CronId(3)));
        assert_eq!("CH7".parse::<ChainId>(), Ok(ChainId(7)));
        assert_eq!("R9".parse::<ScriptId>(), Ok(ScriptId(9)));
        assert_eq!("E11".parse::<ExecutionId>(), Ok(ExecutionId(11)));
        assert_eq!(
            "E11/S3".parse::<StepId>(),
            Ok(StepId {
                execution: ExecutionId(11),
                index: 3,
            })
        );
    }

    #[test]
    fn parse_ids_reject_wrong_prefixes_and_non_digits() {
        assert!("C1".parse::<JobId>().is_err());
        assert!("C+1".parse::<CronId>().is_err());
        assert!("CH".parse::<ChainId>().is_err());
        assert!("Rabc".parse::<ScriptId>().is_err());
        assert!("E-1".parse::<ExecutionId>().is_err());
        assert!("E1/S0".parse::<StepId>().is_err());
    }

    #[test]
    fn display_scope_hash() {
        let mut h = [0u8; 32];
        h[0] = 0xa3;
        h[1] = 0xf1;
        h[2] = 0x00;
        h[3] = 0xff;
        let s = ScopeHash(h);
        assert_eq!(s.to_string(), "S@a3f100ff");
    }

    #[test]
    fn entity_ref_display() {
        assert_eq!(EntityRef::Job(JobId(5)).to_string(), "J5");
    }
}
