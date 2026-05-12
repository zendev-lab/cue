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
    #[serde(default)]
    pub aliases: AliasConfig,
    #[serde(default)]
    pub wrapper: WrapperConfig,
}

#[derive(Debug, Clone)]
pub struct AliasEntry {
    pub from: String,
    pub to: String,
}

#[derive(Debug, Clone, Default)]
pub struct AliasConfig {
    pub entries: Vec<AliasEntry>,
}

impl<'de> Deserialize<'de> for AliasConfig {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        let map = BTreeMap::<String, String>::deserialize(deserializer)?;
        let mut entries: Vec<AliasEntry> = map
            .into_iter()
            .map(|(from, to)| AliasEntry { from, to })
            .collect();
        entries.sort_by(|a, b| {
            b.from
                .split_whitespace()
                .count()
                .cmp(&a.from.split_whitespace().count())
        });
        Ok(AliasConfig { entries })
    }
}

impl AliasConfig {
    /// Apply the longest-matching alias to `input` and return the substituted string.
    ///
    /// Alias matching compares against the first N whitespace-separated tokens of
    /// `input`. Longer patterns take priority over shorter ones. Input that begins
    /// with `:` (an explicit colon-command such as `:run`) is intentionally **not**
    /// aliased — the caller is already using the explicit command syntax.
    pub fn apply(&self, input: &str) -> String {
        if self.entries.is_empty() || input.starts_with(':') {
            return input.to_string();
        }
        let input_tokens: Vec<&str> = input.split_whitespace().collect();
        for entry in &self.entries {
            let from_tokens: Vec<&str> = entry.from.split_whitespace().collect();
            let n = from_tokens.len();
            if input_tokens.len() >= n && input_tokens[..n] == from_tokens[..] {
                let rest = &input_tokens[n..];
                return if rest.is_empty() {
                    entry.to.clone()
                } else {
                    format!("{} {}", entry.to, rest.join(" "))
                };
            }
        }
        input.to_string()
    }
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

/// ── Wrapper config ──
///
/// Applies an external binary prefix to single-segment command spawns.
/// The wrapper is **idempotent**: if the program already matches
/// `binary`, or is in the denylist, or the spawn is a foreground attach,
/// the wrapper is skipped.
///
/// Example:
///
/// ```toml
/// [wrapper]
/// enabled = true
/// binary = "rtk"
///
/// [wrapper.denylist]
/// commands = ["vim", "ssh"]
/// interactive = true
/// ```
#[derive(Debug, Clone, Deserialize)]
pub struct WrapperConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_wrapper_binary")]
    pub binary: String,
    #[serde(default)]
    pub denylist: WrapperDenylist,
}

impl Default for WrapperConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            binary: default_wrapper_binary(),
            denylist: WrapperDenylist::default(),
        }
    }
}

impl WrapperConfig {
    /// Determine whether the wrapper should be applied for a given program.
    ///
    /// Returns `true` when:
    /// - wrapper is enabled (or explicitly overridden),
    /// - `program` is NOT the wrapper binary itself (idempotency),
    /// - `program` is NOT in the denylist,
    /// - the spawn is NOT a foreground attach (when `denylist.interactive`).
    pub fn should_wrap(
        &self,
        program: &str,
        is_foreground: bool,
        override_enabled: Option<bool>,
    ) -> bool {
        let enabled = override_enabled.unwrap_or(self.enabled);
        if !enabled {
            return false;
        }
        let base = std::path::Path::new(program)
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or(program);
        // Idempotency guard: already the wrapper binary.
        if base == self.binary_base() {
            return false;
        }
        // Interactive guard.
        if is_foreground && self.denylist.interactive {
            return false;
        }
        // Denylist guard.
        !self.denylist.matches(program)
    }

    /// Extract the file-name portion of the wrapper binary for comparison.
    fn binary_base(&self) -> &str {
        std::path::Path::new(&self.binary)
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or(&self.binary)
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct WrapperDenylist {
    #[serde(default)]
    pub commands: Vec<String>,
    #[serde(default = "default_true")]
    pub interactive: bool,
}

impl WrapperDenylist {
    pub fn matches(&self, program: &str) -> bool {
        let base = std::path::Path::new(program)
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or(program);
        self.commands.iter().any(|c| c == base)
    }
}

fn default_wrapper_binary() -> String {
    String::new()
}

fn default_true() -> bool {
    true
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

    #[test]
    fn alias_no_match_passthrough() {
        let cfg = AliasConfig::default();
        assert_eq!(cfg.apply("pip install foo"), "pip install foo");
    }

    #[test]
    fn alias_single_word() {
        let cfg: AliasConfig = toml::from_str(r#"pip = "uv pip""#).unwrap();
        assert_eq!(cfg.apply("pip install foo"), "uv pip install foo");
        assert_eq!(cfg.apply("pip"), "uv pip");
    }

    #[test]
    fn alias_multi_word() {
        let cfg: AliasConfig = toml::from_str(r#""git clone" = "ein clone""#).unwrap();
        assert_eq!(
            cfg.apply("git clone https://github.com/foo/bar"),
            "ein clone https://github.com/foo/bar"
        );
    }

    #[test]
    fn alias_longer_match_takes_priority() {
        let cfg: AliasConfig = toml::from_str(
            r#"
git = "alt-git"
"git clone" = "ein clone"
"#,
        )
        .unwrap();
        assert_eq!(
            cfg.apply("git clone https://github.com/foo/bar"),
            "ein clone https://github.com/foo/bar"
        );
        assert_eq!(cfg.apply("git status"), "alt-git status");
    }

    #[test]
    fn alias_no_match_in_middle() {
        let cfg: AliasConfig = toml::from_str(r#"pip = "uv pip""#).unwrap();
        assert_eq!(cfg.apply("run pip install foo"), "run pip install foo");
    }

    #[test]
    fn alias_empty_input() {
        let cfg: AliasConfig = toml::from_str(r#"pip = "uv pip""#).unwrap();
        assert_eq!(cfg.apply(""), "");
    }

    #[test]
    fn alias_parsed_from_server_toml() {
        let config = Config::load_from_sources(
            Some((
                Path::new("server.toml"),
                r#"
[aliases]
"git clone" = "ein clone"
pip = "uv pip"
"#,
            )),
            None,
        )
        .expect("load config");
        assert_eq!(
            config.aliases.apply("git clone https://example.com"),
            "ein clone https://example.com"
        );
        assert_eq!(
            config.aliases.apply("pip install foo"),
            "uv pip install foo"
        );
    }

    // ── WrapperConfig ──

    #[test]
    fn wrapper_default_disabled() {
        let cfg = WrapperConfig::default();
        assert!(!cfg.enabled);
        assert!(!cfg.should_wrap("git", false, None));
    }

    #[test]
    fn wrapper_enabled_wraps_command() {
        let cfg = WrapperConfig {
            enabled: true,
            binary: "rtk".into(),
            ..Default::default()
        };
        assert!(cfg.should_wrap("git", false, None));
    }

    #[test]
    fn wrapper_override_disabled() {
        let cfg = WrapperConfig {
            enabled: true,
            binary: "rtk".into(),
            ..Default::default()
        };
        assert!(!cfg.should_wrap("git", false, Some(false)));
    }

    #[test]
    fn wrapper_override_enabled() {
        let cfg = WrapperConfig {
            enabled: false,
            binary: "rtk".into(),
            ..Default::default()
        };
        assert!(cfg.should_wrap("git", false, Some(true)));
    }

    #[test]
    fn wrapper_idempotent_already_wrapped() {
        let cfg = WrapperConfig {
            enabled: true,
            binary: "rtk".into(),
            ..Default::default()
        };
        assert!(!cfg.should_wrap("rtk", false, None));
    }

    #[test]
    fn wrapper_idempotent_with_full_path() {
        let cfg = WrapperConfig {
            enabled: true,
            binary: "/usr/local/bin/rtk".into(),
            ..Default::default()
        };
        // rtk is the binary base name → skip
        assert!(!cfg.should_wrap("rtk", false, None));
        // git is not → wrap
        assert!(cfg.should_wrap("git", false, None));
    }

    #[test]
    fn wrapper_denylist_commands() {
        let cfg = WrapperConfig {
            enabled: true,
            binary: "rtk".into(),
            denylist: WrapperDenylist {
                commands: vec!["vim".into(), "nvim".into()],
                interactive: true,
            },
        };
        assert!(!cfg.should_wrap("vim", false, None));
        assert!(!cfg.should_wrap("nvim", false, None));
        assert!(cfg.should_wrap("git", false, None));
    }

    #[test]
    fn wrapper_denylist_interactive() {
        let cfg = WrapperConfig {
            enabled: true,
            binary: "rtk".into(),
            ..Default::default()
        };
        assert!(!cfg.should_wrap("git", true, None));
        assert!(cfg.should_wrap("git", false, None));
    }

    #[test]
    fn wrapper_denylist_interactive_disabled() {
        let cfg = WrapperConfig {
            enabled: true,
            binary: "rtk".into(),
            denylist: WrapperDenylist {
                commands: vec![],
                interactive: false,
            },
        };
        // Should still wrap in foreground when interactive: false
        assert!(cfg.should_wrap("git", true, None));
    }

    #[test]
    fn wrapper_parsed_from_server_toml() {
        let config = Config::load_from_sources(
            Some((
                Path::new("server.toml"),
                r#"
[wrapper]
enabled = true
binary = "rtk"

[wrapper.denylist]
commands = ["vim", "ssh"]
interactive = false
"#,
            )),
            None,
        )
        .expect("load config");
        assert!(config.wrapper.enabled);
        assert_eq!(config.wrapper.binary, "rtk");
        assert_eq!(config.wrapper.denylist.commands, vec!["vim", "ssh"]);
        assert!(!config.wrapper.denylist.interactive);
    }

    #[test]
    fn wrapper_absent_config_is_default() {
        let config = Config::load_from_sources(
            Some((
                Path::new("server.toml"),
                r#"
[aliases]
pip = "uv pip"
"#,
            )),
            None,
        )
        .expect("load config");
        assert!(!config.wrapper.enabled);
    }

    #[test]
    fn wrapper_guard_order_binary_first() {
        // Even if rtk is in the denylist, the idempotency guard fires first.
        let cfg = WrapperConfig {
            enabled: true,
            binary: "rtk".into(),
            denylist: WrapperDenylist {
                commands: vec!["rtk".into()],
                interactive: true,
            },
        };
        // rtk matches binary_base first → skip (idempotency)
        assert!(!cfg.should_wrap("rtk", false, None));
    }
}
