use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;

use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub enum Quantity {
    Count(u64),
    Bytes(u64),
}

impl Quantity {
    pub fn value(self) -> u64 {
        match self {
            Self::Count(v) | Self::Bytes(v) => v,
        }
    }
    pub fn same_kind(self, other: Self) -> bool {
        matches!(
            (self, other),
            (Self::Count(_), Self::Count(_)) | (Self::Bytes(_), Self::Bytes(_))
        )
    }
}
impl fmt::Display for Quantity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Count(v) => write!(f, "{v}"),
            Self::Bytes(v) => write!(f, "{v}B"),
        }
    }
}
impl From<Quantity> for String {
    fn from(v: Quantity) -> Self {
        v.to_string()
    }
}
impl TryFrom<String> for Quantity {
    type Error = anyhow::Error;
    fn try_from(v: String) -> Result<Self> {
        v.parse()
    }
}
impl FromStr for Quantity {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self> {
        let n = s.bytes().take_while(u8::is_ascii_digit).count();
        if n == 0 {
            bail!("quantity must be a positive integer with an optional byte unit")
        }
        let value: u64 = s[..n].parse()?;
        if value == 0 {
            bail!("quantity must be positive")
        }
        let suffix = &s[n..];
        let multiplier = match suffix {
            "" => return Ok(Self::Count(value)),
            "B" => 1,
            "KB" => 1_000,
            "MB" => 1_000_000,
            "GB" => 1_000_000_000,
            "TB" => 1_000_000_000_000,
            "KiB" => 1 << 10,
            "MiB" => 1 << 20,
            "GiB" => 1 << 30,
            "TiB" => 1 << 40,
            _ => bail!("unknown byte unit {suffix}"),
        };
        Ok(Self::Bytes(
            value
                .checked_mul(multiplier)
                .ok_or_else(|| anyhow::anyhow!("quantity overflow"))?,
        ))
    }
}
pub type Needs = BTreeMap<String, Quantity>;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default)]
    pub resources: ResourceConfig,
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceConfig {
    #[serde(default)]
    pub providers: Vec<ProviderConfig>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderConfig {
    pub id: String,
    #[serde(flatten)]
    pub backend: Backend,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Backend {
    Static {
        capacity: Needs,
    },
    Command {
        keys: BTreeMap<String, Unit>,
        argv: Vec<String>,
        #[serde(default = "default_timeout")]
        timeout_ms: u64,
    },
    Nvidia {
        #[serde(default = "default_nvidia")]
        argv: Vec<String>,
        #[serde(default)]
        safety_margin_bytes: u64,
    },
}
fn default_timeout() -> u64 {
    3_000
}
fn default_nvidia() -> Vec<String> {
    vec!["nvidia-smi".into()]
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Unit {
    Count,
    Bytes,
}
impl ProviderConfig {
    pub fn keys(&self) -> BTreeMap<String, Unit> {
        match &self.backend {
            Backend::Static { capacity } => capacity
                .iter()
                .map(|(k, q)| {
                    (
                        k.clone(),
                        match q {
                            Quantity::Count(_) => Unit::Count,
                            Quantity::Bytes(_) => Unit::Bytes,
                        },
                    )
                })
                .collect(),
            Backend::Command { keys, .. } => keys.clone(),
            Backend::Nvidia { .. } => {
                BTreeMap::from([("gpu".into(), Unit::Count), ("gpu_mem".into(), Unit::Bytes)])
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Grant {
    pub id: String,
    #[serde(default)]
    pub environment: BTreeMap<String, String>,
    #[serde(default)]
    pub devices: Vec<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum ProviderReply {
    Granted { grant: Grant },
    Released,
    Absent,
    Rejected { reason: String },
    Unknown { reason: String },
    Snapshot { data: serde_json::Value },
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderRequest {
    pub version: u32,
    pub method: String,
    pub daemon_id: String,
    pub request_id: String,
    pub execution: cue_core::ExecutionId,
    pub needs: Needs,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    Waiting,
    Allocating,
    Allocated,
    Cleaning,
    Released,
    Isolated,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Attempt {
    pub provider: ProviderConfig,
    pub request: ProviderRequest,
    pub grant: Option<Grant>,
    pub uncertain: bool,
    pub released: bool,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Record {
    pub execution: cue_core::ExecutionId,
    pub needs: Needs,
    pub providers: Vec<ProviderConfig>,
    pub status: Status,
    pub attempts: Vec<Attempt>,
    pub reason: Option<String>,
    pub environment: BTreeMap<String, String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn config_and_quantities_are_strict() {
        let config:Config=toml::from_str("[[resources.providers]]\nid='slots'\nkind='static'\n[resources.providers.capacity]\nworker='2'\n").unwrap();
        assert_eq!(config.resources.providers[0].keys().len(), 1);
        for value in ["0", "-1", "1.5", "1foo", "18446744073709551615TiB", ""] {
            assert!(value.parse::<Quantity>().is_err(), "{value}");
        }
        assert_eq!(
            "24GiB".parse::<Quantity>().unwrap(),
            Quantity::Bytes(24 << 30)
        );
        assert!(toml::from_str::<Config>("unknown=true").is_err());
        assert!(toml::from_str::<Config>("[[resources.providers]]\nid='slots'\nkind='static'\nunknown=true\n[resources.providers.capacity]\nworker='2'\n").is_err());
    }
}
