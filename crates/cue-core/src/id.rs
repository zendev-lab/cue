use std::{error::Error, fmt, num::NonZeroU64, str::FromStr};

use serde::{Deserialize, Deserializer, Serialize};

/// Unified execution sequence number, displayed as E1, E2, ...
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub struct ExecutionId(pub u64);

/// Durable trigger sequence number, displayed as T1, T2, ...
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub struct ScheduleId(pub u64);

/// Stable process-step identity within one execution, displayed as E1/S1.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub struct StepId {
    pub execution: ExecutionId,
    pub index: u32,
}

/// Monotonic durable fact cursor, scoped by the backing event store.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct EventId(NonZeroU64);

impl EventId {
    pub fn new(value: u64) -> Result<Self, ParseIdError> {
        NonZeroU64::new(value)
            .map(Self)
            .ok_or_else(|| ParseIdError::new("event", "0"))
    }

    pub const fn get(self) -> u64 {
        self.0.get()
    }
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

// --- Display impls ---

impl fmt::Display for ExecutionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "E{}", self.0)
    }
}

impl fmt::Display for ScheduleId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "T{}", self.0)
    }
}

impl fmt::Display for StepId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/S{}", self.execution, self.index)
    }
}

impl fmt::Display for EventId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "V{}", self.get())
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

impl fmt::Display for ParseIdError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid {} id {}", self.kind, self.input)
    }
}

impl Error for ParseIdError {}

impl FromStr for ExecutionId {
    type Err = ParseIdError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        parse_prefixed_u64(input, "E", "execution")
            .and_then(|value| nonzero_id(value, "execution", input))
            .map(Self)
    }
}

impl FromStr for ScheduleId {
    type Err = ParseIdError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        parse_prefixed_u64(input, "T", "schedule")
            .and_then(|value| nonzero_id(value, "schedule", input))
            .map(Self)
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

fn parse_prefixed_u64(input: &str, prefix: &str, kind: &'static str) -> Result<u64, ParseIdError> {
    let digits = input
        .strip_prefix(prefix)
        .ok_or_else(|| ParseIdError::new(kind, input))?;
    if digits.is_empty() || !digits.chars().all(|ch| ch.is_ascii_digit()) {
        return Err(ParseIdError::new(kind, input));
    }
    digits.parse().map_err(|_| ParseIdError::new(kind, input))
}

fn nonzero_id(value: u64, kind: &'static str, input: &str) -> Result<u64, ParseIdError> {
    (value > 0)
        .then_some(value)
        .ok_or_else(|| ParseIdError::new(kind, input))
}

impl<'de> Deserialize<'de> for ExecutionId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = u64::deserialize(deserializer)?;
        if value == 0 {
            return Err(serde::de::Error::custom("execution id must be non-zero"));
        }
        Ok(Self(value))
    }
}

impl<'de> Deserialize<'de> for ScheduleId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = u64::deserialize(deserializer)?;
        if value == 0 {
            return Err(serde::de::Error::custom("schedule id must be non-zero"));
        }
        Ok(Self(value))
    }
}

impl<'de> Deserialize<'de> for StepId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct WireStepId {
            execution: ExecutionId,
            index: u32,
        }

        let wire = WireStepId::deserialize(deserializer)?;
        if wire.index == 0 {
            return Err(serde::de::Error::custom("step index must be non-zero"));
        }
        Ok(Self {
            execution: wire.execution,
            index: wire.index,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_ids() {
        assert_eq!(ExecutionId(11).to_string(), "E11");
        assert_eq!(ScheduleId(5).to_string(), "T5");
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
        assert_eq!("E11".parse::<ExecutionId>(), Ok(ExecutionId(11)));
        assert_eq!("T5".parse::<ScheduleId>(), Ok(ScheduleId(5)));
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
        assert!("E-1".parse::<ExecutionId>().is_err());
        assert!("E0".parse::<ExecutionId>().is_err());
        assert!("T0".parse::<ScheduleId>().is_err());
        assert!("E1/S0".parse::<StepId>().is_err());
    }

    #[test]
    fn json_rejects_zero_and_unknown_identity_fields() {
        assert!(serde_json::from_str::<ExecutionId>("0").is_err());
        assert!(serde_json::from_str::<ScheduleId>("0").is_err());
        assert!(serde_json::from_str::<EventId>("0").is_err());
        assert!(serde_json::from_str::<StepId>(r#"{"execution":1,"index":0}"#).is_err());
        assert!(
            serde_json::from_str::<StepId>(r#"{"execution":1,"index":1,"extra":true}"#).is_err()
        );
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
}
