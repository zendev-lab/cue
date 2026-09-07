use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde::Deserialize;

#[derive(Debug, Clone, Default, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub extensions: ExtensionsConfig,
}

impl Config {
    pub(crate) fn load_for_extension_dispatch() -> Result<Self> {
        let Some(path) = extension_config_path() else {
            return Ok(Self::default());
        };
        let text = match std::fs::read_to_string(&path) {
            Ok(text) => text,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(Self::default());
            }
            Err(error) => return Err(error).with_context(|| format!("read {}", path.display())),
        };
        Self::load_for_extension_dispatch_from_sources(Some((&path, &text)))
    }

    fn load_for_extension_dispatch_from_sources(source: Option<(&Path, &str)>) -> Result<Self> {
        let Some((path, text)) = source else {
            return Ok(Self::default());
        };
        validate_root_sections(text, path)?;
        let extension_config: ExtensionDispatchConfig =
            toml::from_str(text).with_context(|| format!("parse config {}", path.display()))?;
        extension_config.extensions.validate()?;
        Ok(Self {
            extensions: extension_config.extensions,
        })
    }
}

fn extension_config_path() -> Option<PathBuf> {
    if let Some(root) = std::env::var_os("XDG_CONFIG_HOME").filter(|value| !value.is_empty()) {
        return Some(PathBuf::from(root).join("cue/client.toml"));
    }
    std::env::var_os("HOME")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .map(|home| home.join(".config/cue/client.toml"))
}

fn validate_root_sections(text: &str, path: &Path) -> Result<()> {
    let root = toml::from_str::<toml::Table>(text)
        .with_context(|| format!("parse config {}", path.display()))?;
    for key in root.keys() {
        if key != "extensions" {
            bail!("unknown top-level client config section `{key}`")
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Default, Deserialize)]
struct ExtensionDispatchConfig {
    #[serde(default)]
    extensions: ExtensionsConfig,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExtensionsConfig {
    #[serde(default)]
    pub path_lookup: bool,
    #[serde(default)]
    pub commands: BTreeMap<String, ExtensionCommand>,
}

impl ExtensionsConfig {
    fn validate(&self) -> Result<()> {
        for (name, command) in &self.commands {
            if is_reserved_extension_name(name) {
                bail!(
                    "extension name `{name}` is reserved for a built-in or first-party cue subcommand"
                );
            }
            validate_extension_name(name, "extension name")?;
            if command.program.trim().is_empty() {
                bail!("extension `{name}` program must not be empty");
            }
            if command.program.trim() != command.program {
                bail!("extension `{name}` program must not have leading or trailing whitespace");
            }
        }
        Ok(())
    }
}

pub(crate) fn validate_extension_name(name: &str, label: &str) -> Result<()> {
    if !is_valid_extension_name(name) {
        bail!("{label} `{name}` must be kebab-case ASCII, for example `foo` or `foo-bar`");
    }
    Ok(())
}

fn is_reserved_extension_name(name: &str) -> bool {
    matches!(
        name,
        "client" | "daemon" | "help" | "run" | "target" | "tui" | "version"
    )
}

fn is_valid_extension_name(name: &str) -> bool {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !first.is_ascii_lowercase() && !first.is_ascii_digit() {
        return false;
    }
    let mut previous_was_dash = false;

    for ch in chars {
        match ch {
            'a'..='z' | '0'..='9' => previous_was_dash = false,
            '-' if !previous_was_dash => previous_was_dash = true,
            _ => return false,
        }
    }

    !previous_was_dash
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ExtensionCommand {
    pub program: String,
    #[serde(default)]
    pub description: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_external_extensions() {
        let config = Config::load_for_extension_dispatch_from_sources(Some((
            Path::new("client.toml"),
            r#"
[extensions]
path_lookup = true

[extensions.commands.foo]
program = "cue-foo"
description = "Foo extension"
"#,
        )))
        .expect("load config");

        assert!(config.extensions.path_lookup);
        assert_eq!(
            config.extensions.commands.get("foo"),
            Some(&ExtensionCommand {
                program: "cue-foo".into(),
                description: Some("Foo extension".into()),
            })
        );
    }

    #[test]
    fn extension_registry_requires_program_field() {
        let error = Config::load_for_extension_dispatch_from_sources(Some((
            Path::new("client.toml"),
            r#"
[extensions.commands.foo]
command = "cue-foo"
"#,
        )))
        .expect_err("command field should not be accepted as a program");

        let message = format!("{error:#}");
        assert!(message.contains("parse config client.toml"));
        assert!(message.contains("unknown field `command`"));
    }

    #[test]
    fn extension_registry_rejects_unknown_extension_fields() {
        let error = Config::load_for_extension_dispatch_from_sources(Some((
            Path::new("client.toml"),
            r#"
[extensions]
path_lookkup = true
"#,
        )))
        .expect_err("unknown extension config keys should fail during config loading");

        let message = format!("{error:#}");
        assert!(message.contains("parse config client.toml"));
        assert!(message.contains("unknown field `path_lookkup`"));
    }

    #[test]
    fn extension_registry_rejects_reserved_names() {
        let error = Config::load_for_extension_dispatch_from_sources(Some((
            Path::new("client.toml"),
            r#"
[extensions.commands.tui]
program = "custom-tui"
"#,
        )))
        .expect_err("first-party and built-in subcommands should not be configurable extensions");

        assert_eq!(
            format!("{error:#}"),
            "extension name `tui` is reserved for a built-in or first-party cue subcommand"
        );
    }

    #[test]
    fn extension_registry_rejects_non_kebab_case_names() {
        let error = Config::load_for_extension_dispatch_from_sources(Some((
            Path::new("client.toml"),
            r#"
[extensions.commands.foo_bar]
program = "cue-foo-bar"
"#,
        )))
        .expect_err("extension names should be stable CLI subcommand names");

        assert_eq!(
            format!("{error:#}"),
            "extension name `foo_bar` must be kebab-case ASCII, for example `foo` or `foo-bar`"
        );
    }

    #[test]
    fn extension_registry_rejects_empty_program() {
        let error = Config::load_for_extension_dispatch_from_sources(Some((
            Path::new("client.toml"),
            r#"
[extensions.commands.foo]
program = "   "
"#,
        )))
        .expect_err("extension program should be validated by config loading");

        assert_eq!(
            format!("{error:#}"),
            "extension `foo` program must not be empty"
        );
    }

    #[test]
    fn extension_registry_rejects_padded_program() {
        for program in [r#"" cue-foo""#, r#""cue-foo ""#] {
            let error = Config::load_for_extension_dispatch_from_sources(Some((
                Path::new("client.toml"),
                &format!(
                    r#"
[extensions.commands.foo]
program = {program}
"#
                ),
            )))
            .expect_err("extension program should be validated by config loading");

            assert_eq!(
                format!("{error:#}"),
                "extension `foo` program must not have leading or trailing whitespace"
            );
        }
    }

    #[test]
    fn extension_dispatch_config_rejects_removed_transport_section() {
        let error = Config::load_for_extension_dispatch_from_sources(Some((
            Path::new("client.toml"),
            r#"
[transport]
default_profile = " remote"

[extensions.commands.foo]
program = "cue-foo"
"#,
        )))
        .expect_err("removed transport config must not survive the hard cut");
        assert!(
            format!("{error:#}").contains("unknown top-level client config section `transport`")
        );
    }

    #[test]
    fn extension_dispatch_config_rejects_unknown_top_level_sections() {
        let error = Config::load_for_extension_dispatch_from_sources(Some((
            Path::new("client.toml"),
            r#"
[extensons]
path_lookup = true
"#,
        )))
        .expect_err("top-level extension config typos should not be silently defaulted");

        assert!(
            format!("{error:#}").contains("unknown top-level client config section `extensons`")
        );
    }
}
