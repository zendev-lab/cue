use std::collections::BTreeMap;
use std::path::Path;

use anyhow::{Context, Result};
use serde::Deserialize;

use crate::dirs;

const SERVER_CONFIG_FILE: &str = "server.toml";
const LEGACY_CONFIG_FILE: &str = "config.toml";

#[derive(Debug, Clone, Default, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub agent: AgentConfig,
}

impl Config {
    pub fn load() -> Result<Self> {
        let config_dir = dirs::config_dir();
        let server_path = config_dir.join(SERVER_CONFIG_FILE);
        let legacy_path = config_dir.join(LEGACY_CONFIG_FILE);
        Self::load_from_sources(
            read_source(&server_path)?
                .as_deref()
                .map(|text| (server_path.as_path(), text)),
            read_source(&legacy_path)?
                .as_deref()
                .map(|text| (legacy_path.as_path(), text)),
        )
    }

    fn load_from_sources(
        server: Option<(&Path, &str)>,
        legacy: Option<(&Path, &str)>,
    ) -> Result<Self> {
        if let Some((path, text)) = server {
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
        config.agent.fill_defaults();
        Ok(config)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct AgentConfig {
    #[serde(default = "default_backend_name")]
    pub default_backend: String,
    #[serde(default = "default_backends")]
    pub backends: BTreeMap<String, AgentBackendConfig>,
}

impl Default for AgentConfig {
    fn default() -> Self {
        Self {
            default_backend: default_backend_name(),
            backends: default_backends(),
        }
    }
}

impl AgentConfig {
    fn fill_defaults(&mut self) {
        if self.default_backend.is_empty() {
            self.default_backend = default_backend_name();
        }
        if self.backends.is_empty() {
            self.backends = default_backends();
        }
    }

    pub fn backend(&self, name: Option<&str>) -> Result<(String, AgentBackendConfig)> {
        if self.backends.is_empty() {
            anyhow::bail!(
                "no ACP agent backend configured; add [agent.backends.<name>] with command = \"...\" to server.toml (or legacy config.toml)"
            );
        }
        let backend_name = name.unwrap_or(&self.default_backend);
        let backend = self
            .backends
            .get(backend_name)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("unknown agent backend `{backend_name}`"))?;
        Ok((backend_name.to_string(), backend))
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct AgentBackendConfig {
    #[serde(default = "default_agent_command")]
    pub command: String,
    #[serde(default = "default_agent_args")]
    pub args: Vec<String>,
    #[serde(default)]
    pub model: Option<String>,
}

fn default_backend_name() -> String {
    "copilot".into()
}

fn default_agent_command() -> String {
    String::new()
}

fn default_agent_args() -> Vec<String> {
    Vec::new()
}

fn default_backends() -> BTreeMap<String, AgentBackendConfig> {
    BTreeMap::from([(
        "copilot".into(),
        AgentBackendConfig {
            command: "copilot".into(),
            args: vec!["--acp".into(), "--stdio".into()],
            model: None,
        },
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn default_agent_config_starts_copilot_acp_server() {
        let config = Config::default();
        let (name, backend) = config.agent.backend(None).expect("default backend");
        assert_eq!(name, "copilot");
        assert_eq!(backend.command, "copilot");
        assert_eq!(backend.args, vec!["--acp", "--stdio"]);
    }

    #[test]
    fn server_toml_takes_precedence_over_legacy_config_toml() {
        let config = Config::load_from_sources(
            Some((
                Path::new("server.toml"),
                r#"
[agent]
default_backend = "server"

[agent.backends.server]
command = "server-backend"
"#,
            )),
            Some((
                Path::new("config.toml"),
                r#"
[agent]
default_backend = "legacy"

[agent.backends.legacy]
command = "legacy-backend"
"#,
            )),
        )
        .expect("load config");

        let (name, backend) = config.agent.backend(None).expect("server backend");
        assert_eq!(name, "server");
        assert_eq!(backend.command, "server-backend");
    }

    #[test]
    fn legacy_config_toml_still_loads_server_agent_config() {
        let config = Config::load_from_sources(
            None,
            Some((
                Path::new("config.toml"),
                r#"
[agent]
default_backend = "legacy"

[agent.backends.legacy]
command = "legacy-backend"
"#,
            )),
        )
        .expect("load config");

        let (name, backend) = config.agent.backend(None).expect("legacy backend");
        assert_eq!(name, "legacy");
        assert_eq!(backend.command, "legacy-backend");
    }
}
