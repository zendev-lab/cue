use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde::Deserialize;

use crate::client::default_socket_path;
use crate::config_paths::{client_config_paths, read_client_config_sources};
use crate::host_discovery::HostDiscoveryConfig;
use crate::transport_discovery::detected_transport_hosts;
use crate::transport_schema::{
    LOCAL_PROFILE_NAME, SSH_DESTINATION_FIELD, SSH_GATEWAY_COMMAND_FIELD, SSH_START_COMMAND_FIELD,
    UNIX_SOCKET_FIELD, default_auto_detect_ssh, default_gateway_command, default_profile_name,
    default_start_command, transport_profile_field_path, transport_profile_path,
    validate_client_config_root_sections, validate_default_profile_name, validate_profile_name,
    validate_socket_path, validate_trimmed_non_empty,
};

#[derive(Debug, Clone, Default, Deserialize)]
struct TransportConfigFile {
    #[serde(default)]
    transport: TransportConfig,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransportConfig {
    #[serde(default = "default_profile_name")]
    default_profile: String,
    #[serde(default = "default_auto_detect_ssh")]
    auto_detect_ssh: bool,
    #[serde(default)]
    discovery: HostDiscoveryConfig,
    #[serde(default = "default_profiles")]
    profiles: BTreeMap<String, TransportProfile>,
}

impl Default for TransportConfig {
    fn default() -> Self {
        Self {
            default_profile: default_profile_name(),
            auto_detect_ssh: default_auto_detect_ssh(),
            discovery: HostDiscoveryConfig::default(),
            profiles: default_profiles(),
        }
    }
}

impl TransportConfig {
    pub fn resolve_transport(&self, socket_override: Option<PathBuf>) -> Result<ResolvedTransport> {
        self.resolve_transport_with_detection(socket_override, || self.detected_hosts())
    }

    fn resolve_transport_with_detection<F>(
        &self,
        socket_override: Option<PathBuf>,
        detect_hosts: F,
    ) -> Result<ResolvedTransport>
    where
        F: FnOnce() -> Result<BTreeSet<String>>,
    {
        self.validate()?;

        if let Some(transport) = resolve_socket_override(socket_override)? {
            return Ok(transport);
        }

        self.resolve_profile_with_detection_after_validate(&self.default_profile, detect_hosts)
    }

    #[cfg(test)]
    fn resolve_transport_with_detected(
        &self,
        socket_override: Option<PathBuf>,
        detected_hosts: BTreeSet<String>,
    ) -> Result<ResolvedTransport> {
        self.resolve_transport_with_detection(socket_override, || Ok(detected_hosts))
    }

    pub fn resolve_profile(&self, profile_name: &str) -> Result<ResolvedTransport> {
        self.validate()?;
        self.resolve_profile_with_detection_after_validate(profile_name, || self.detected_hosts())
    }

    fn resolve_profile_with_detection_after_validate<F>(
        &self,
        profile_name: &str,
        detect_hosts: F,
    ) -> Result<ResolvedTransport>
    where
        F: FnOnce() -> Result<BTreeSet<String>>,
    {
        if let Some(transport) = self.resolve_profile_from_detected(profile_name, &BTreeSet::new())
        {
            return Ok(transport);
        }

        if self.auto_detect_ssh {
            let detected_hosts = detect_hosts().with_context(|| {
                format!(
                    "auto-detect SSH profiles while resolving unknown client transport profile `{profile_name}`"
                )
            })?;
            if let Some(transport) =
                self.resolve_profile_from_detected(profile_name, &detected_hosts)
            {
                return Ok(transport);
            }
        }

        bail!("unknown client transport profile `{profile_name}`")
    }

    fn detected_hosts(&self) -> Result<BTreeSet<String>> {
        detected_transport_hosts(&self.discovery)
    }

    fn resolve_profile_from_detected(
        &self,
        profile_name: &str,
        detected_hosts: &BTreeSet<String>,
    ) -> Option<ResolvedTransport> {
        debug_assert!(
            self.validate().is_ok(),
            "resolve_profile_from_detected requires a validated transport config"
        );

        let profiles = self.merged_profiles(detected_hosts);
        let profile = profiles.get(profile_name).cloned()?;

        Some(match profile {
            TransportProfile::Unix(profile) => ResolvedTransport::Unix {
                profile_name: profile_name.to_string(),
                socket_path: profile.socket.unwrap_or_else(default_socket_path),
            },
            TransportProfile::Ssh(profile) => ResolvedTransport::Ssh {
                profile_name: profile_name.to_string(),
                destination: profile.destination,
                gateway_command: profile.gateway_command,
                start_command: profile.start_command,
            },
        })
    }

    fn merged_profiles(
        &self,
        detected_hosts: &BTreeSet<String>,
    ) -> BTreeMap<String, TransportProfile> {
        let mut profiles = self.profiles.clone();

        match profiles.get(LOCAL_PROFILE_NAME) {
            Some(TransportProfile::Unix(_)) => {}
            _ => {
                profiles.insert(LOCAL_PROFILE_NAME.into(), local_unix_profile());
            }
        }

        if self.auto_detect_ssh {
            for host in detected_hosts {
                if host == LOCAL_PROFILE_NAME {
                    continue;
                }
                profiles
                    .entry(host.clone())
                    .or_insert_with(|| detected_ssh_profile(host));
            }
        }

        profiles
    }

    pub fn validate(&self) -> Result<()> {
        validate_default_profile_name(&self.default_profile)?;
        if matches!(
            self.profiles.get(LOCAL_PROFILE_NAME),
            Some(TransportProfile::Ssh(_))
        ) {
            bail!(
                "{} is reserved for unix transport",
                transport_profile_path(LOCAL_PROFILE_NAME)
            );
        }
        for (name, profile) in &self.profiles {
            validate_profile_name(name)?;
            match profile {
                TransportProfile::Unix(profile) => validate_unix_profile(name, profile)?,
                TransportProfile::Ssh(profile) => validate_ssh_profile(name, profile)?,
            }
        }
        Ok(())
    }
}

fn resolve_socket_override(socket_override: Option<PathBuf>) -> Result<Option<ResolvedTransport>> {
    let Some(socket_path) = socket_override else {
        return Ok(None);
    };
    validate_socket_path("CUE_SOCKET", &socket_path)?;
    Ok(Some(ResolvedTransport::Unix {
        profile_name: "env:CUE_SOCKET".into(),
        socket_path,
    }))
}

fn validate_unix_profile(name: &str, profile: &UnixProfile) -> Result<()> {
    if let Some(socket) = &profile.socket {
        validate_socket_path(
            &transport_profile_field_path(name, UNIX_SOCKET_FIELD),
            socket,
        )?;
    }
    Ok(())
}

fn validate_ssh_profile(name: &str, profile: &SshProfile) -> Result<()> {
    validate_ssh_field(name, SSH_DESTINATION_FIELD, &profile.destination)?;
    validate_ssh_field(name, SSH_GATEWAY_COMMAND_FIELD, &profile.gateway_command)?;
    validate_ssh_field(name, SSH_START_COMMAND_FIELD, &profile.start_command)
}

fn validate_ssh_field(name: &str, field: &str, value: &str) -> Result<()> {
    let path = transport_profile_field_path(name, field);
    validate_trimmed_non_empty(
        value,
        &format!("{path} must not be empty"),
        &format!("{path} must not have leading or trailing whitespace"),
    )
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "transport", rename_all = "snake_case", deny_unknown_fields)]
pub enum TransportProfile {
    Unix(UnixProfile),
    Ssh(SshProfile),
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UnixProfile {
    #[serde(default)]
    pub socket: Option<PathBuf>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
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

pub fn load_transport_config() -> Result<TransportConfig> {
    let paths = client_config_paths()?;
    let sources = read_client_config_sources(&paths)?;
    load_transport_config_from_sources(
        sources
            .primary()
            .map(|source| (source.path(), source.text())),
    )
}

pub fn load_transport_config_from_sources(
    source: Option<(&Path, &str)>,
) -> Result<TransportConfig> {
    if let Some((path, text)) = source {
        return parse_transport_config(text, path);
    }
    Ok(TransportConfig::default())
}

pub fn parse_transport_config(text: &str, path: &Path) -> Result<TransportConfig> {
    validate_client_config_root_sections(text, path)?;
    let file: TransportConfigFile =
        toml::from_str(text).with_context(|| format!("parse config {}", path.display()))?;
    file.transport.validate()?;
    Ok(file.transport)
}

fn local_unix_profile() -> TransportProfile {
    TransportProfile::Unix(UnixProfile::default())
}

fn detected_ssh_profile(host: &str) -> TransportProfile {
    TransportProfile::Ssh(SshProfile {
        destination: host.to_string(),
        gateway_command: default_gateway_command(),
        start_command: default_start_command(),
    })
}

fn default_profiles() -> BTreeMap<String, TransportProfile> {
    BTreeMap::from([(LOCAL_PROFILE_NAME.into(), local_unix_profile())])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_transport_uses_local_unix_socket() {
        let config = TransportConfig::default();
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
    fn client_toml_takes_precedence() {
        let config = load_transport_config_from_sources(Some((
            Path::new("client.toml"),
            r#"
[transport]
default_profile = "remote"

[transport.profiles.remote]
transport = "ssh"
destination = "devbox"
"#,
        )))
        .expect("load config");

        assert_eq!(
            config.resolve_profile("remote").expect("resolve remote"),
            ResolvedTransport::Ssh {
                profile_name: "remote".into(),
                destination: "devbox".into(),
                gateway_command: default_gateway_command(),
                start_command: default_start_command(),
            }
        );
    }

    #[test]
    fn default_profile_can_resolve_detected_ssh_host() {
        let config = load_transport_config_from_sources(Some((
            Path::new("client.toml"),
            r#"
[transport]
default_profile = "devbox"
"#,
        )))
        .expect("load config");

        let detected = BTreeSet::from(["devbox".to_string()]);
        assert_eq!(
            config
                .resolve_transport_with_detected(None, detected)
                .expect("resolve detected"),
            ResolvedTransport::Ssh {
                profile_name: "devbox".into(),
                destination: "devbox".into(),
                gateway_command: default_gateway_command(),
                start_command: default_start_command(),
            }
        );
    }

    #[test]
    fn auto_detect_ssh_can_be_disabled() {
        let config = load_transport_config_from_sources(Some((
            Path::new("client.toml"),
            r#"
[transport]
default_profile = "devbox"
auto_detect_ssh = false
"#,
        )))
        .expect("load config");

        let detected = BTreeSet::from(["devbox".to_string()]);
        let error = config
            .resolve_transport_with_detected(None, detected)
            .expect_err("disabled detection should not resolve detected host");

        assert!(format!("{error:#}").contains("unknown client transport profile `devbox`"));
    }

    #[test]
    fn detected_ssh_hosts_extend_profiles_without_removing_local() {
        let config = TransportConfig::default();
        let profiles = config.merged_profiles(&BTreeSet::from(["devbox".to_string()]));

        assert!(matches!(
            profiles.get("local"),
            Some(TransportProfile::Unix(_))
        ));
        assert!(matches!(
            profiles.get("devbox"),
            Some(TransportProfile::Ssh(_))
        ));
    }

    #[test]
    fn ssh_profile_defaults_remote_commands() {
        let config = load_transport_config_from_sources(Some((
            Path::new("client.toml"),
            r#"
[transport.profiles.remote]
transport = "ssh"
destination = "devbox"
"#,
        )))
        .expect("load config");

        assert_eq!(
            config.resolve_profile("remote").expect("resolve remote"),
            ResolvedTransport::Ssh {
                profile_name: "remote".into(),
                destination: "devbox".into(),
                gateway_command: "cued gateway --stdio".into(),
                start_command: "cued start".into(),
            }
        );
    }

    #[test]
    fn parses_generic_host_discovery_config() {
        let config = load_transport_config_from_sources(Some((
            Path::new("client.toml"),
            r#"
[transport.discovery]
env_hosts = ["CLUSTER_HOSTS"]
env_endpoints = ["CLUSTER_ENDPOINTS"]
env_hostfiles = ["CLUSTER_HOSTFILE"]
env_bracket_ranges = ["CLUSTER_NODELIST"]
"#,
        )))
        .expect("load config");

        assert_eq!(config.discovery.env_hosts, vec!["CLUSTER_HOSTS"]);
        assert_eq!(config.discovery.env_endpoints, vec!["CLUSTER_ENDPOINTS"]);
        assert_eq!(config.discovery.env_hostfiles, vec!["CLUSTER_HOSTFILE"]);
        assert_eq!(
            config.discovery.env_bracket_ranges,
            vec!["CLUSTER_NODELIST"]
        );
    }

    #[test]
    fn local_profile_rejects_non_unix_transport() {
        let error = load_transport_config_from_sources(Some((
            Path::new("client.toml"),
            r#"
[transport.profiles.local]
transport = "ssh"
destination = "localhost"
"#,
        )))
        .expect_err("local ssh profile should fail");

        assert!(format!("{error:#}").contains("local"));
    }

    #[test]
    fn rejects_unknown_root_section() {
        let error = load_transport_config_from_sources(Some((
            Path::new("client.toml"),
            r#"
[daemon]
foo = true
"#,
        )))
        .expect_err("unknown client root section should fail");

        assert!(format!("{error:#}").contains("unknown top-level client config section `daemon`"));
    }
}
