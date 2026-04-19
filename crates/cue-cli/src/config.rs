use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use cue_tui::client::default_socket_path;
use serde::Deserialize;

const APP_DIR: &str = "cue-shell";
const CLIENT_CONFIG_FILE: &str = "client.toml";
const LEGACY_CONFIG_FILE: &str = "config.toml";

#[derive(Debug, Clone, Default, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub transport: TransportConfig,
}

impl Config {
    pub fn load() -> Result<Self> {
        let config_dir = config_dir();
        let client_path = config_dir.join(CLIENT_CONFIG_FILE);
        let legacy_path = config_dir.join(LEGACY_CONFIG_FILE);
        Self::load_from_sources(
            read_source(&client_path)?
                .as_deref()
                .map(|text| (client_path.as_path(), text)),
            read_source(&legacy_path)?
                .as_deref()
                .map(|text| (legacy_path.as_path(), text)),
        )
    }

    fn load_from_sources(
        client: Option<(&Path, &str)>,
        legacy: Option<(&Path, &str)>,
    ) -> Result<Self> {
        if let Some((path, text)) = client {
            return Self::parse(text, path);
        }
        if let Some((path, text)) = legacy {
            return Self::parse(text, path);
        }
        Ok(Self::default())
    }

    fn parse(text: &str, path: &Path) -> Result<Self> {
        let mut config: Self =
            toml::from_str(text).with_context(|| format!("parse config {}", path.display()))?;
        config.transport.fill_defaults();
        Ok(config)
    }

    pub fn resolve_transport(&self, socket_override: Option<PathBuf>) -> Result<ResolvedTransport> {
        if let Some(socket_path) = socket_override {
            return Ok(ResolvedTransport::Unix {
                profile_name: "env:CUE_SOCKET".into(),
                socket_path,
            });
        }

        let (profile_name, profile) = self.transport.default_profile()?;
        Ok(match profile {
            TransportProfile::Unix(profile) => ResolvedTransport::Unix {
                profile_name,
                socket_path: profile.socket.unwrap_or_else(default_socket_path),
            },
            TransportProfile::Ssh(profile) => ResolvedTransport::Ssh {
                profile_name,
                destination: profile.destination,
                gateway_command: profile.gateway_command,
                start_command: profile.start_command,
            },
        })
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct TransportConfig {
    #[serde(default = "default_profile_name")]
    pub default_profile: String,
    #[serde(default = "default_profiles")]
    pub profiles: BTreeMap<String, TransportProfile>,
}

impl Default for TransportConfig {
    fn default() -> Self {
        Self {
            default_profile: default_profile_name(),
            profiles: default_profiles(),
        }
    }
}

impl TransportConfig {
    fn fill_defaults(&mut self) {
        if self.default_profile.is_empty() {
            self.default_profile = default_profile_name();
        }
        if self.profiles.is_empty() {
            self.profiles = default_profiles();
        }
    }

    fn default_profile(&self) -> Result<(String, TransportProfile)> {
        let profile_name = self.default_profile.as_str();
        let profile =
            self.profiles.get(profile_name).cloned().ok_or_else(|| {
                anyhow::anyhow!("unknown client transport profile `{profile_name}`")
            })?;
        Ok((profile_name.to_string(), profile))
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "transport", rename_all = "snake_case")]
pub enum TransportProfile {
    Unix(UnixProfile),
    Ssh(SshProfile),
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct UnixProfile {
    #[serde(default)]
    pub socket: Option<PathBuf>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SshProfile {
    pub destination: String,
    #[serde(default = "default_gateway_command")]
    pub gateway_command: String,
    #[serde(default = "default_start_command")]
    pub start_command: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResolvedTransport {
    Unix {
        profile_name: String,
        socket_path: PathBuf,
    },
    Ssh {
        profile_name: String,
        destination: String,
        gateway_command: String,
        start_command: String,
    },
}

fn default_profile_name() -> String {
    "local".into()
}

fn default_gateway_command() -> String {
    "cued gateway --stdio".into()
}

fn default_start_command() -> String {
    "cued start".into()
}

fn default_profiles() -> BTreeMap<String, TransportProfile> {
    BTreeMap::from([(
        "local".into(),
        TransportProfile::Unix(UnixProfile::default()),
    )])
}

fn read_source(path: &Path) -> Result<Option<String>> {
    if !path.exists() {
        return Ok(None);
    }

    let text =
        std::fs::read_to_string(path).with_context(|| format!("read config {}", path.display()))?;
    Ok(Some(text))
}

fn config_dir() -> PathBuf {
    if let Ok(dir) = std::env::var("XDG_CONFIG_HOME") {
        PathBuf::from(dir).join(APP_DIR)
    } else {
        home_dir().join(".config").join(APP_DIR)
    }
}

fn home_dir() -> PathBuf {
    std::env::var("HOME")
        .map(PathBuf::from)
        .or_else(|_| std::env::current_dir())
        .unwrap_or_else(|_| PathBuf::from("."))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_transport_uses_local_unix_socket() {
        let config = Config::default();
        let transport = config.resolve_transport(None).expect("resolve transport");
        assert_eq!(
            transport,
            ResolvedTransport::Unix {
                profile_name: "local".into(),
                socket_path: default_socket_path(),
            }
        );
    }

    #[test]
    fn client_toml_takes_precedence_over_legacy_config_toml() {
        let config = Config::load_from_sources(
            Some((
                Path::new("client.toml"),
                r#"
[transport]
default_profile = "remote"

[transport.profiles.remote]
transport = "ssh"
destination = "devbox"
gateway_command = "cued gateway --stdio --socket ~/.cache/cue-shell/remote.sock"
start_command = "cued start --socket ~/.cache/cue-shell/remote.sock"
"#,
            )),
            Some((
                Path::new("config.toml"),
                r#"
[transport]
default_profile = "legacy"

[transport.profiles.legacy]
transport = "unix"
socket = "/legacy.sock"
"#,
            )),
        )
        .expect("load config");

        let transport = config.resolve_transport(None).expect("resolve transport");
        assert_eq!(
            transport,
            ResolvedTransport::Ssh {
                profile_name: "remote".into(),
                destination: "devbox".into(),
                gateway_command: "cued gateway --stdio --socket ~/.cache/cue-shell/remote.sock"
                    .into(),
                start_command: "cued start --socket ~/.cache/cue-shell/remote.sock".into(),
            }
        );
    }

    #[test]
    fn ssh_profile_defaults_remote_commands() {
        let config = Config::load_from_sources(
            Some((
                Path::new("client.toml"),
                r#"
[transport]
default_profile = "remote"

[transport.profiles.remote]
transport = "ssh"
destination = "devbox"
"#,
            )),
            None,
        )
        .expect("load config");

        let transport = config.resolve_transport(None).expect("resolve transport");
        assert_eq!(
            transport,
            ResolvedTransport::Ssh {
                profile_name: "remote".into(),
                destination: "devbox".into(),
                gateway_command: "cued gateway --stdio".into(),
                start_command: "cued start".into(),
            }
        );
    }

    #[test]
    fn legacy_config_toml_still_loads_transport_profiles() {
        let config = Config::load_from_sources(
            None,
            Some((
                Path::new("config.toml"),
                r#"
[transport]
default_profile = "legacy"

[transport.profiles.legacy]
transport = "unix"
socket = "/legacy.sock"
"#,
            )),
        )
        .expect("load config");

        let transport = config.resolve_transport(None).expect("resolve transport");
        assert_eq!(
            transport,
            ResolvedTransport::Unix {
                profile_name: "legacy".into(),
                socket_path: PathBuf::from("/legacy.sock"),
            }
        );
    }
}
