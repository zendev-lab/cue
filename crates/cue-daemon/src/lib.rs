//! Cue's IPC v4 local execution daemon.

mod dirs;
mod host;
pub mod service;

pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

pub(crate) fn daemon_instance_id() -> &'static str {
    static INSTANCE_ID: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    INSTANCE_ID
        .get_or_init(|| {
            std::env::var("CUE_DAEMON_INSTANCE_ID")
                .ok()
                .filter(|value| !value.is_empty())
                .unwrap_or_else(|| uuid::Uuid::new_v4().to_string())
        })
        .as_str()
}

pub fn run_cli() -> i32 {
    match host::run_cli() {
        Ok(code) => code,
        Err(error) => {
            eprintln!("cued: {error:#}");
            1
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn version_is_set() {
        assert!(!crate::version().is_empty());
    }
}
