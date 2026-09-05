use std::collections::BTreeMap;
use std::fmt;

use serde::{Deserialize, Deserializer, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct EnvKey(String);

impl EnvKey {
    pub fn new(value: impl Into<String>) -> Result<Self, EnvError> {
        let value = value.into();
        if !valid_env_key(&value) {
            return Err(EnvError::InvalidKey(value));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for EnvKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for EnvKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Sensitivity {
    Normal,
    Sensitive,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct EnvValue {
    value: String,
    sensitivity: Sensitivity,
}

impl EnvValue {
    pub fn new(value: impl Into<String>) -> Result<Self, EnvError> {
        Self::classified(value, Sensitivity::Normal)
    }

    pub fn classified(
        value: impl Into<String>,
        sensitivity: Sensitivity,
    ) -> Result<Self, EnvError> {
        let value = value.into();
        if value.contains('\0') {
            return Err(EnvError::ValueContainsNul);
        }
        Ok(Self { value, sensitivity })
    }

    pub fn as_str(&self) -> &str {
        &self.value
    }

    pub const fn sensitivity(&self) -> Sensitivity {
        self.sensitivity
    }
}

impl<'de> Deserialize<'de> for EnvValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            value: String,
            sensitivity: Sensitivity,
        }
        let wire = Wire::deserialize(deserializer)?;
        Self::classified(wire.value, wire.sensitivity).map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum EnvEdit {
    Set(EnvValue),
    Unset,
}

impl EnvEdit {
    pub fn set(value: impl Into<String>) -> Result<Self, EnvError> {
        Ok(Self::Set(EnvValue::new(value)?))
    }
}

pub type Env = BTreeMap<EnvKey, EnvValue>;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct EnvPatch(BTreeMap<EnvKey, EnvEdit>);

impl EnvPatch {
    pub fn new(edits: BTreeMap<EnvKey, EnvEdit>) -> Self {
        Self(edits)
    }

    pub fn empty() -> Self {
        Self::default()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn get(&self, key: &EnvKey) -> Option<&EnvEdit> {
        self.0.get(key)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&EnvKey, &EnvEdit)> {
        self.0.iter()
    }

    pub fn apply(&self, environment: &Env) -> Env {
        let mut result = environment.clone();
        for (key, edit) in &self.0 {
            match edit {
                EnvEdit::Set(value) => {
                    result.insert(key.clone(), value.clone());
                }
                EnvEdit::Unset => {
                    result.remove(key);
                }
            }
        }
        result
    }
}

impl IntoIterator for EnvPatch {
    type Item = (EnvKey, EnvEdit);
    type IntoIter = std::collections::btree_map::IntoIter<EnvKey, EnvEdit>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum EnvError {
    #[error("invalid environment variable name {0:?}")]
    InvalidKey(String),
    #[error("environment value contains NUL")]
    ValueContainsNul,
}

fn valid_env_key(value: &str) -> bool {
    !value.is_empty() && !value.contains(['=', '\0'])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(value: &str) -> EnvKey {
        EnvKey::new(value).unwrap()
    }

    fn value(value: &str) -> EnvValue {
        EnvValue::new(value).unwrap()
    }

    #[test]
    fn validates_keys_and_values_at_deserialization_boundary() {
        assert!(EnvKey::new("A_1").is_ok());
        assert!(EnvKey::new("A-B").is_ok());
        assert!(EnvKey::new("1A").is_ok());
        assert!(serde_json::from_str::<EnvKey>(r#""A=B""#).is_err());
        assert!(serde_json::from_str::<EnvKey>(r#""A\u0000B""#).is_err());
        assert!(EnvValue::new("a\0b").is_err());
    }

    #[test]
    fn sensitivity_is_explicit_and_survives_serialization() {
        for sensitivity in [Sensitivity::Normal, Sensitivity::Sensitive] {
            let value = EnvValue::classified("", sensitivity).unwrap();
            let wire = serde_json::to_value(&value).unwrap();
            assert_eq!(serde_json::from_value::<EnvValue>(wire).unwrap(), value);
        }
        assert!(serde_json::from_str::<EnvValue>(r#"{"value":"secret"}"#).is_err());
        assert!(
            serde_json::from_str::<EnvValue>(r#"{"value":"a\u0000b","sensitivity":"sensitive"}"#)
                .is_err()
        );
    }

    #[test]
    fn patch_can_set_and_unset_distinct_keys_without_conflict_state() {
        let base = BTreeMap::from([(key("KEEP"), value("yes")), (key("REMOVE"), value("old"))]);
        let patch = EnvPatch::new(BTreeMap::from([
            (key("ADD"), EnvEdit::set("").unwrap()),
            (key("REMOVE"), EnvEdit::Unset),
        ]));

        let applied = patch.apply(&base);
        assert_eq!(applied.get(&key("KEEP")).unwrap().as_str(), "yes");
        assert_eq!(applied.get(&key("ADD")).unwrap().as_str(), "");
        assert!(!applied.contains_key(&key("REMOVE")));
    }
}
