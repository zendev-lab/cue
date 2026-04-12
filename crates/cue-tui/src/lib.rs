//! TUI frontend for cue-shell.
//!
//! Architecture: TEA (The Elm Architecture) + Component hybrid.
//! - Global [`AppState`] + [`AppMsg`] enum + pure `update` function
//! - Panels rendered by independent [`Component`] implementors
//! - ratatui 0.30 + crossterm 0.29

pub mod app;
pub mod client;
pub mod component;
pub mod event;
pub mod ui;

pub use app::{AppMsg, AppState, FocusArea};
pub use client::CuedClient;

use anyhow::{Context, Result};

/// Run the TUI application.
///
/// Accepts an optional pre-connected client (from `ensure_daemon_running`)
/// to avoid double-connecting. If `None`, starts in offline mode.
/// Auto-reconnects on disconnect using `socket_path`.
pub async fn run(
    socket_path: &std::path::Path,
    client: Option<CuedClient>,
) -> Result<()> {
    // Split client into reader/writer handle if connected.
    let (socket_reader, writer_handle, connected) = match client {
        Some(c) => {
            let (reader, writer) = c.into_split();
            (Some(reader), Some(client::spawn_writer_task(writer)), true)
        }
        None => (None, None, false),
    };

    // Initialize terminal.
    let mut terminal = ratatui::init();
    crossterm::execute!(std::io::stdout(), crossterm::event::EnableMouseCapture)
        .context("enable mouse capture")?;

    // Install a panic hook that also disables mouse capture.
    let original_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let _ = crossterm::execute!(std::io::stdout(), crossterm::event::DisableMouseCapture);
        original_hook(info);
    }));

    // Build app state.
    let mut state = AppState::new();
    let (w, h) = crossterm::terminal::size().unwrap_or((80, 24));
    state.terminal_width = w;
    state.terminal_height = h;

    if let Some(wh) = writer_handle {
        state.writer = Some(wh);
        state.connected = connected;
        state
            .status_bar
            .update(component::status_bar::StatusBarMsg::SetConnected(connected));
    }

    // Spawn event loop with socket_path for auto-reconnect.
    let mut rx = event::spawn_event_loop(socket_reader, socket_path.to_path_buf())?;

    // Main loop.
    let result = loop {
        if let Err(e) = terminal.draw(|frame| ui::draw(frame, &state)) {
            break Err(e).context("draw frame");
        }

        match rx.recv().await {
            Some(msg) => state.update(msg),
            None => break Ok(()),
        }

        if state.should_quit {
            break Ok(());
        }
    };

    // Restore terminal.
    crossterm::execute!(std::io::stdout(), crossterm::event::DisableMouseCapture)
        .context("disable mouse capture")?;
    ratatui::restore();

    result
}

use component::Component as _;
