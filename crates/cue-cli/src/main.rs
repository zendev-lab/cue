//! `cue` — TUI entry point for cue-shell.
//!
//! 1. Try to connect to the cued daemon (reusing the connection for TUI).
//! 2. If not running, auto-start `cued start` and retry with backoff.
//! 3. Run the TUI event loop with auto-reconnect on disconnect.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use anyhow::{Context, Result};
use tracing::info;

use cue_tui::CuedClient;
use cue_tui::client::default_socket_path;

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .with_writer(std::io::stderr)
        .init();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("build tokio runtime")?;

    rt.block_on(async_main())
}

async fn async_main() -> Result<()> {
    let socket_path = socket_path_from_env();

    // Connect (auto-starting daemon if needed). The client is reused by TUI.
    let client = ensure_daemon_running(&socket_path).await;

    // Run the TUI with socket_path for auto-reconnect on disconnect.
    cue_tui::run(&socket_path, client).await
}

fn socket_path_from_env() -> PathBuf {
    if let Ok(path) = std::env::var("CUE_SOCKET") {
        PathBuf::from(path)
    } else {
        default_socket_path()
    }
}

/// Try to connect to the daemon, auto-starting it if needed.
///
/// Returns the connected client for the TUI to reuse (no double-connect).
/// Returns `None` for offline mode with auto-reconnect.
async fn ensure_daemon_running(socket_path: &Path) -> Option<CuedClient> {
    // Try direct connect first.
    if let Ok(client) = CuedClient::connect(socket_path).await {
        info!("cued already running");
        return Some(client);
    }

    // Connection failed. Clean up stale socket if present.
    if socket_path.exists() {
        info!("stale socket detected, removing {}", socket_path.display());
        std::fs::remove_file(socket_path).ok();
    }

    // Auto-start the daemon.
    info!("cued not running, attempting to start");
    let cued_bin = std::env::var("CUE_DAEMON_BIN").unwrap_or_else(|_| "cued".into());
    let _child = Command::new(&cued_bin)
        .arg("start")
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn();

    // Retry connect with backoff: 100ms, 200ms, 400ms, 800ms, 1600ms.
    let mut delay = Duration::from_millis(100);
    for _ in 0..5 {
        tokio::time::sleep(delay).await;
        if let Ok(client) = CuedClient::connect(socket_path).await {
            info!("connected after auto-start");
            return Some(client);
        }
        delay *= 2;
    }

    tracing::warn!("cued did not start in time, entering offline mode");
    None
}
