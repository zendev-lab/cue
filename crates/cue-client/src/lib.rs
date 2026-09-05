//! IPC v4 client and executable frontend support.

pub mod cli;
pub mod execution;
pub mod script_runner;

use std::ffi::OsString;
use std::path::PathBuf;

pub use execution::{
    ExecutionClient, MultiplexedClient, PreparedCommand, SurfaceOutcome, process_scope,
};

/// Resolve `$XDG_RUNTIME_DIR/cue/cued.sock`, falling back to the process temp
/// directory when no runtime directory is configured.
pub fn default_socket_path() -> PathBuf {
    let root = std::env::var_os("XDG_RUNTIME_DIR")
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| OsString::from(std::env::temp_dir()));
    PathBuf::from(root).join("cue/cued.sock")
}

#[cfg(test)]
mod tests {
    #[test]
    fn default_socket_has_v4_daemon_name() {
        assert_eq!(
            crate::default_socket_path().file_name().unwrap(),
            "cued.sock"
        );
    }
}
