use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::Deserialize;

use crate::dirs;

const SERVER_CONFIG_FILE: &str = "server.toml";
const LEGACY_CONFIG_FILE: &str = "config.toml";

#[derive(Debug, Clone, Default, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub aliases: AliasConfig,
    #[serde(default)]
    pub weft: WeftConfig,
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
        toml::from_str(text).with_context(|| format!("parse config {}", path.display()))
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct WeftConfig {
    #[serde(default = "default_weft_socket_path")]
    pub socket_path: PathBuf,
}

impl Default for WeftConfig {
    fn default() -> Self {
        Self {
            socket_path: default_weft_socket_path(),
        }
    }
}

fn default_weft_socket_path() -> PathBuf {
    PathBuf::from("./weft.sock")
}

// ────────────────────────────────────────────────────────────────────
// Wrapper config
// ────────────────────────────────────────────────────────────────────

/// Wrapper configuration for binary-prefix injection.
///
/// When enabled, single-segment command spawns are prefixed with `binary`.
/// The wrapper is **idempotent**: if the program already matches `binary`,
/// or is in the denylist, or the spawn is a foreground attach, it is
/// skipped.
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
        if base == self.binary_base() {
            return false;
        }
        if is_foreground && self.denylist.interactive {
            return false;
        }
        !self.denylist.matches(program)
    }

    fn binary_base(&self) -> &str {
        std::path::Path::new(&self.binary)
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or(&self.binary)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct WrapperDenylist {
    #[serde(default)]
    pub commands: Vec<String>,
    #[serde(default = "default_true")]
    pub interactive: bool,
}

impl Default for WrapperDenylist {
    fn default() -> Self {
        Self {
            commands: Vec::new(),
            interactive: true,
        }
    }
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

// ────────────────────────────────────────────────────────────────────
// Helpers
// ────────────────────────────────────────────────────────────────────

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
    fn default_config_sets_weft_socket() {
        let config = Config::default();
        assert_eq!(config.weft.socket_path, PathBuf::from("./weft.sock"));
    }

    #[test]
    fn server_toml_takes_precedence_over_legacy_config_toml() {
        let config = Config::load_from_sources(
            Some((
                Path::new("server.toml"),
                r#"
[weft]
socket_path = "/tmp/server.sock"
"#,
            )),
            Some((
                Path::new("config.toml"),
                r#"
[weft]
socket_path = "/tmp/legacy.sock"
"#,
            )),
        )
        .expect("load config");

        assert_eq!(config.weft.socket_path, PathBuf::from("/tmp/server.sock"));
    }

    #[test]
    fn legacy_config_toml_still_loads_weft_config() {
        let config = Config::load_from_sources(
            None,
            Some((
                Path::new("config.toml"),
                r#"
[weft]
socket_path = "/tmp/legacy.sock"
"#,
            )),
        )
        .expect("load config");

        assert_eq!(config.weft.socket_path, PathBuf::from("/tmp/legacy.sock"));
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

    #[test]
    fn parses_weft_socket_path() {
        let config = Config::load_from_sources(
            Some((
                Path::new("server.toml"),
                r#"
[weft]
socket_path = "/var/run/weft.sock"
"#,
            )),
            None,
        )
        .expect("load config");

        assert_eq!(config.weft.socket_path, PathBuf::from("/var/run/weft.sock"));
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
    fn wrapper_idempotent_already_wrapped() {
        let cfg = WrapperConfig {
            enabled: true,
            binary: "rtk".into(),
            ..Default::default()
        };
        assert!(!cfg.should_wrap("rtk", false, None));
    }

    #[test]
    fn wrapper_denylist_commands() {
        let cfg = WrapperConfig {
            enabled: true,
            binary: "rtk".into(),
            denylist: WrapperDenylist {
                commands: vec!["vim".into()],
                interactive: true,
            },
        };
        assert!(!cfg.should_wrap("vim", false, None));
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
}
