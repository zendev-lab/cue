use std::path::Path;

use anyhow::{Context, Result, bail};
use toml::Value;
use toml::map::Map;

pub(crate) const TRANSPORT_SECTION: &str = "transport";
pub(crate) const TRANSPORT_AUTO_DETECT_SSH_FIELD: &str = "auto_detect_ssh";
pub(crate) const TRANSPORT_DEFAULT_PROFILE_FIELD: &str = "default_profile";
pub(crate) const TRANSPORT_DISCOVERY_FIELD: &str = "discovery";
pub(crate) const TRANSPORT_PROFILES_FIELD: &str = "profiles";
pub(crate) const PROFILE_TRANSPORT_FIELD: &str = "transport";
pub(crate) const UNIX_SOCKET_FIELD: &str = "socket";
pub(crate) const SSH_DESTINATION_FIELD: &str = "destination";
pub(crate) const SSH_GATEWAY_COMMAND_FIELD: &str = "gateway_command";
pub(crate) const SSH_START_COMMAND_FIELD: &str = "start_command";

pub(crate) const CLIENT_ROOT_SECTIONS: &[&str] = &["extensions", TRANSPORT_SECTION];
pub(crate) const TRANSPORT_KEYS: &[&str] = &[
    TRANSPORT_AUTO_DETECT_SSH_FIELD,
    TRANSPORT_DEFAULT_PROFILE_FIELD,
    TRANSPORT_DISCOVERY_FIELD,
    TRANSPORT_PROFILES_FIELD,
];
pub(crate) const UNIX_PROFILE_KEYS: &[&str] = &[PROFILE_TRANSPORT_FIELD, UNIX_SOCKET_FIELD];
pub(crate) const SSH_PROFILE_KEYS: &[&str] = &[
    PROFILE_TRANSPORT_FIELD,
    SSH_DESTINATION_FIELD,
    SSH_GATEWAY_COMMAND_FIELD,
    SSH_START_COMMAND_FIELD,
];

pub(crate) const LOCAL_PROFILE_NAME: &str = "local";
pub(crate) const UNIX_TRANSPORT: &str = "unix";
pub(crate) const SSH_TRANSPORT: &str = "ssh";

pub(crate) fn default_profile_name() -> String {
    LOCAL_PROFILE_NAME.into()
}

pub(crate) fn default_auto_detect_ssh() -> bool {
    true
}

pub(crate) fn default_gateway_command() -> String {
    "cued gateway --stdio".into()
}

pub(crate) fn default_start_command() -> String {
    "cued start".into()
}

pub(crate) fn transport_field_path(field: &str) -> String {
    format!("{TRANSPORT_SECTION}.{field}")
}

pub(crate) fn transport_profiles_path() -> String {
    transport_field_path(TRANSPORT_PROFILES_FIELD)
}

pub(crate) fn transport_profile_path(profile: &str) -> String {
    format!("{}.{}", transport_profiles_path(), profile)
}

pub(crate) fn transport_profile_field_path(profile: &str, field: &str) -> String {
    format!("{}.{}", transport_profile_path(profile), field)
}

pub(crate) fn validate_default_profile_name(name: &str) -> Result<()> {
    let path = transport_field_path(TRANSPORT_DEFAULT_PROFILE_FIELD);
    validate_trimmed_non_empty(
        name,
        &format!("{path} must not be empty"),
        &format!("{path} must not have leading or trailing whitespace"),
    )
}

pub(crate) fn validate_profile_name(name: &str) -> Result<()> {
    validate_trimmed_non_empty(
        name,
        "transport profile names must not be empty",
        "transport profile names must not have leading or trailing whitespace",
    )
}

pub(crate) fn validate_socket_path(field: &str, socket: &Path) -> Result<()> {
    let Some(socket) = socket.to_str() else {
        bail!("{field} must be valid UTF-8");
    };
    validate_trimmed_non_empty(
        socket,
        &format!("{field} must not be empty"),
        &format!("{field} must not have leading or trailing whitespace"),
    )
}

pub(crate) fn validate_trimmed_non_empty(
    value: &str,
    empty_message: &str,
    padded_message: &str,
) -> Result<()> {
    if value.trim().is_empty() {
        bail!("{empty_message}");
    }
    if value.trim() != value {
        bail!("{padded_message}");
    }
    Ok(())
}

pub(crate) fn validate_known_keys(
    table: &Map<String, Value>,
    scope: &str,
    allowed: &[&str],
) -> Result<()> {
    if let Some(key) = table.keys().find(|key| !allowed.contains(&key.as_str())) {
        bail!("unknown field `{key}` in {scope}");
    }
    Ok(())
}

pub(crate) fn unknown_field_detail(table: &Map<String, Value>, allowed: &[&str]) -> Option<String> {
    table
        .keys()
        .find(|key| !allowed.contains(&key.as_str()))
        .map(|key| format!("unknown field `{key}`"))
}

pub fn validate_client_config_root_sections(text: &str, path: &Path) -> Result<()> {
    let value: Value = toml::from_str(text)
        .with_context(|| format!("parse config root sections {}", path.display()))?;
    let Some(table) = value.as_table() else {
        bail!("config root must be a TOML table");
    };
    for key in table.keys() {
        if !CLIENT_ROOT_SECTIONS.contains(&key.as_str()) {
            bail!(
                "unknown top-level client config section `{key}` in {}; supported top-level sections: {}",
                path.display(),
                CLIENT_ROOT_SECTIONS.join(", ")
            );
        }
    }

    Ok(())
}
