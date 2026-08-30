use std::fmt;
use std::num::NonZeroU64;

use serde::{Deserialize, Deserializer, Serialize};
use thiserror::Error;

macro_rules! numeric_id {
    ($name:ident, $label:literal) => {
        #[derive(
            Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
        )]
        #[serde(transparent)]
        pub struct $name(NonZeroU64);

        impl $name {
            pub fn new(value: u64) -> Result<Self, IdError> {
                NonZeroU64::new(value)
                    .map(Self)
                    .ok_or(IdError::Zero($label))
            }

            pub const fn get(self) -> u64 {
                self.0.get()
            }
        }
    };
}

numeric_id!(RequestId, "request");
numeric_id!(AttachmentId, "attachment");

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct ClientId(String);

impl ClientId {
    pub fn new(value: impl Into<String>) -> Result<Self, IdError> {
        let value = value.into();
        validate_text_id("client", &value, 128)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'de> Deserialize<'de> for ClientId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::new(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

impl fmt::Display for ClientId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct OperationId(String);

impl OperationId {
    pub fn new(value: impl Into<String>) -> Result<Self, IdError> {
        let value = value.into();
        validate_text_id("operation", &value, 256)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'de> Deserialize<'de> for OperationId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::new(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

impl fmt::Display for OperationId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

fn validate_text_id(kind: &'static str, value: &str, maximum: usize) -> Result<(), IdError> {
    if value.is_empty() {
        return Err(IdError::Empty(kind));
    }
    if value.len() > maximum {
        return Err(IdError::TooLong {
            kind,
            maximum,
            actual: value.len(),
        });
    }
    if value.trim() != value {
        return Err(IdError::Padded(kind));
    }
    if value.chars().any(char::is_control) {
        return Err(IdError::ControlCharacter(kind));
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum IdError {
    #[error("{0} id must be non-zero")]
    Zero(&'static str),
    #[error("{0} id must not be empty")]
    Empty(&'static str),
    #[error("{kind} id is {actual} bytes; maximum is {maximum}")]
    TooLong {
        kind: &'static str,
        maximum: usize,
        actual: usize,
    },
    #[error("{0} id must not have surrounding whitespace")]
    Padded(&'static str),
    #[error("{0} id contains a forbidden control character")]
    ControlCharacter(&'static str),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn numeric_ids_reject_zero_at_construction_and_json_boundaries() {
        assert!(RequestId::new(0).is_err());
        assert!(serde_json::from_str::<RequestId>("0").is_err());
        assert_eq!(RequestId::new(7).unwrap().get(), 7);
    }

    #[test]
    fn textual_ids_are_exact_bounded_and_control_free() {
        assert!(ClientId::new("").is_err());
        assert!(ClientId::new(" padded").is_err());
        assert!(OperationId::new("line\nbreak").is_err());
        assert!(OperationId::new("tab\tbreak").is_err());
        assert!(OperationId::new("x".repeat(257)).is_err());
        let id = OperationId::new("tool-call:execute:1").unwrap();
        assert_eq!(id.as_str(), "tool-call:execute:1");
        assert!(serde_json::from_str::<OperationId>(r#"" bad""#).is_err());
    }
}
