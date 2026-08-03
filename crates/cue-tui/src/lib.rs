//! TUI frontend for cue-shell.
//!
//! Architecture: TEA (The Elm Architecture) + Component hybrid.
//! - Global app state plus message-driven update function
//! - Panels rendered by independent component implementors
//! - ratatui 0.30 + crossterm 0.29

mod ansi;
mod app;
mod card_action;
pub mod cli;
mod client;
mod clipboard;
mod completion;
mod component;
mod display;
mod event;
mod focus;
mod footer;
mod geometry;
mod history;
mod job_picker;
mod message;
mod mouse_mode;
mod record_format;
mod script_summary;
mod session_binding;
mod sidebar_action;
mod status_view;
mod submission;
mod target_config;
mod target_settings;
mod terminal;
mod tui_debug;
mod ui;

use std::path::PathBuf;

use anyhow::{Context, Result};
use app::AppState;
use crossterm::event::{
    DisableMouseCapture, EnableBracketedPaste, EnableMouseCapture, KeyboardEnhancementFlags,
    PushKeyboardEnhancementFlags,
};
use cue_client::{ClientConnector, CuedClient, RestartHandle};
use message::AppMsg;
use terminal::{PanicHookGuard, TerminalRestoreGuard, initial_terminal_size};

const MAX_READY_EVENTS_PER_FRAME: usize = 64;

/// Entry point used by the thin `src/main.rs` binary.
pub fn run_cli() -> anyhow::Result<()> {
    cli::run()
}

/// Inputs needed to start the TUI.
///
/// Keeping this as a named boundary avoids a long positional `run(...)`
/// signature and lets the CLI assemble transport details without making the
/// TUI crate re-export `cue-client` types as its own public API.
pub struct RunOptions {
    client_connector: ClientConnector,
    client: Option<CuedClient>,
    session_profile_name: Option<String>,
    named_session_selector: Option<String>,
    named_session_refresh: bool,
    restart_handle: Option<RestartHandle>,
    debug_socket: Option<PathBuf>,
}

impl RunOptions {
    pub fn new(client_connector: ClientConnector) -> Self {
        Self {
            client_connector,
            client: None,
            session_profile_name: None,
            named_session_selector: None,
            named_session_refresh: false,
            restart_handle: None,
            debug_socket: None,
        }
    }

    pub fn with_client(mut self, client: CuedClient) -> Self {
        self.client = Some(client);
        self
    }

    pub fn with_optional_client(mut self, client: Option<CuedClient>) -> Self {
        self.client = client;
        self
    }

    pub fn with_session_profile_name(mut self, session_profile_name: Option<String>) -> Self {
        self.session_profile_name = session_profile_name;
        self
    }

    /// Select a durable named daemon session for the initial connection and
    /// every subsequent reconnect.
    pub fn with_named_session_selector(mut self, selector: Option<String>) -> Self {
        self.named_session_selector = selector;
        self
    }

    /// Permit recovery of a selected named session whose volatile scope was
    /// lost during daemon restart. Ready scopes are never replaced: each
    /// connection first attempts a normal attach and refreshes only after the
    /// daemon confirms `needs_refresh`.
    pub fn with_named_session_refresh(mut self, refresh: bool) -> Self {
        self.named_session_refresh = refresh;
        self
    }

    pub fn with_restart_handle(mut self, restart_handle: Option<RestartHandle>) -> Self {
        self.restart_handle = restart_handle;
        self
    }

    pub fn with_debug_socket(mut self, debug_socket: Option<PathBuf>) -> Self {
        self.debug_socket = debug_socket;
        self
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct EventBatchStats {
    received: usize,
    applied: usize,
}

fn apply_app_message(state: &mut AppState, message: AppMsg) -> Result<(), String> {
    if let AppMsg::FatalError { message } = message {
        state.update(AppMsg::FatalError {
            message: message.clone(),
        });
        return Err(message);
    }
    state.update(message);
    Ok(())
}

/// Apply one ready batch between two draw calls.
///
/// Messages retain their exact order and update boundary, including terminal
/// reply drains and attachment fences, but a burst causes only one full frame.
fn apply_ready_event_batch(
    state: &mut AppState,
    rx: &mut tokio::sync::mpsc::Receiver<AppMsg>,
    first: AppMsg,
) -> Result<EventBatchStats, String> {
    let mut stats = EventBatchStats {
        received: 1,
        applied: 0,
    };
    apply_app_message(state, first)?;
    stats.applied = 1;
    if state.should_quit {
        return Ok(stats);
    }

    while stats.received < MAX_READY_EVENTS_PER_FRAME {
        let message = match rx.try_recv() {
            Ok(message) => message,
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
            | Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => break,
        };
        stats.received += 1;
        apply_app_message(state, message)?;
        stats.applied += 1;
        if state.should_quit {
            break;
        }
    }

    Ok(stats)
}

/// Run the TUI application.
///
/// [`RunOptions`] accepts an optional pre-connected client (from
/// `ensure_daemon_running`) to avoid double-connecting. If `None`, the app
/// starts in offline mode and auto-reconnects using the provided connector. A
/// selected named session is attached before the initial client is split and
/// after every connector-created reconnect.
pub async fn run(options: RunOptions) -> Result<()> {
    let RunOptions {
        client_connector,
        mut client,
        session_profile_name,
        named_session_selector,
        named_session_refresh,
        restart_handle,
        debug_socket,
    } = options;

    if let Some(selector) = named_session_selector.as_deref()
        && let Some(connected_client) = client.take()
    {
        client = Some(
            session_binding::attach_named_session(
                connected_client,
                selector,
                named_session_refresh,
            )
            .await
            .context("bind initial TUI connection to named session")?,
        );
    }
    let client_connector = session_binding::connector_with_named_session(
        client_connector,
        named_session_selector.clone(),
        named_session_refresh,
    );

    // Split client into reader/writer handle if connected.
    let (socket_reader, writer_handle, connected) = match client {
        Some(c) => {
            let (reader, writer) = c.into_reader_and_writer_handle();
            (Some(reader), Some(writer), true)
        }
        None => (None, None, false),
    };

    // Initialize terminal.
    let mut terminal = ratatui::init();
    let mut terminal_restore = TerminalRestoreGuard::new();
    crossterm::execute!(std::io::stdout(), EnableBracketedPaste)
        .context("enable bracketed paste")?;
    let keyboard_enhancements_enabled = crossterm::execute!(
        std::io::stdout(),
        PushKeyboardEnhancementFlags(KeyboardEnhancementFlags::DISAMBIGUATE_ESCAPE_CODES)
    )
    .is_ok();
    terminal_restore.set_keyboard_enhancements_enabled(keyboard_enhancements_enabled);
    let mut mouse_capture_enabled = false;

    // Install a panic hook that also restores terminal input modes.
    let _panic_hook_guard = PanicHookGuard::install(keyboard_enhancements_enabled);

    // Build app state.
    let mut state = AppState::new();
    state.set_session_profile_name(session_profile_name);
    state.set_named_session_selector(named_session_selector);
    state.set_named_session_refresh(named_session_refresh);
    state.set_restart_handle(restart_handle);
    if let Err(error) = history::load_history().map(|items| state.input.replace_history(items)) {
        tracing::warn!(%error, "failed to load prompt history");
    }
    let (w, h) = initial_terminal_size(crossterm::terminal::size)?;
    state.terminal_width = w;
    state.terminal_height = h;
    let mut persisted_history = state.input.history.clone();

    if let Some(wh) = writer_handle {
        state.writer = Some(wh);
        if connected {
            state.update(AppMsg::Connected);
        }
    }

    if state.mouse_capture_enabled() {
        crossterm::execute!(std::io::stdout(), EnableMouseCapture)
            .context("enable initial mouse capture")?;
        mouse_capture_enabled = true;
    }

    // Spawn event loop with the shared connector for auto-reconnect.
    let (mut rx, connection_controller, inject_tx) =
        event::spawn_event_loop(socket_reader, client_connector)?;
    state.set_connection_controller(connection_controller);

    let mut debug_server = None;
    let debug_control = if let Some(socket_path) = debug_socket {
        let snapshots = tui_debug::shared_debug_snapshots();
        let control = tui_debug::DebugControl {
            snapshots: snapshots.clone(),
            inject_tx,
        };
        debug_server = Some(
            tui_debug::spawn_debug_server(socket_path, control.clone())
                .context("start debug control server")?,
        );
        tui_debug::update_state_snapshot(&control.snapshots, &state);
        Some(control)
    } else {
        None
    };

    // Main loop.
    let result = loop {
        if let Some(control) = debug_control.as_ref() {
            if let Err(e) = terminal.draw(|frame| {
                ui::draw(frame, &mut state);
                tui_debug::record_frame_snapshot(control, frame.buffer_mut());
            }) {
                break Err(e).context("draw frame");
            }
        } else if let Err(e) = terminal.draw(|frame| ui::draw(frame, &mut state)) {
            break Err(e).context("draw frame");
        }

        let Some(first) = rx.recv().await else {
            break Ok(());
        };
        if let Err(message) = apply_ready_event_batch(&mut state, &mut rx, first) {
            break Err(anyhow::anyhow!(message));
        }
        if let Some(control) = debug_control.as_ref() {
            tui_debug::update_state_snapshot(&control.snapshots, &state);
        }

        if state.input.history != persisted_history {
            if let Err(error) = history::save_history(&state.input.history) {
                tracing::warn!(%error, "failed to save prompt history");
            } else {
                persisted_history = state.input.history.clone();
            }
        }

        let desired_mouse_capture = state.mouse_capture_enabled();
        if desired_mouse_capture != mouse_capture_enabled {
            if desired_mouse_capture {
                crossterm::execute!(std::io::stdout(), EnableMouseCapture)
                    .context("enable mouse capture")?;
            } else {
                crossterm::execute!(std::io::stdout(), DisableMouseCapture)
                    .context("disable mouse capture")?;
            }
            mouse_capture_enabled = desired_mouse_capture;
        }

        if state.should_quit {
            break Ok(());
        }
    };

    terminal_restore.restore()?;

    if let Some(server) = debug_server {
        server.shutdown().await;
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use cue_core::ipc::EventPayload;

    fn fg_output(attachment_id: u64, data: &[u8]) -> AppMsg {
        AppMsg::ServerEvent(EventPayload::FgOutput {
            id: "J1".to_string(),
            attachment_id,
            data: data.to_vec(),
        })
    }

    #[tokio::test]
    async fn ready_foreground_chunks_batch_within_one_frame_boundary() {
        let mut state = AppState::new();
        let (tx, mut rx) = tokio::sync::mpsc::channel(8);
        tx.try_send(fg_output(7, b"one")).expect("first output");
        tx.try_send(fg_output(7, b"two")).expect("second output");
        tx.try_send(fg_output(7, b"three")).expect("third output");

        let first = rx.recv().await.expect("first ready event");
        let stats =
            apply_ready_event_batch(&mut state, &mut rx, first).expect("apply ready event batch");

        assert_eq!(
            stats,
            EventBatchStats {
                received: 3,
                applied: 3,
            }
        );
        assert!(rx.is_empty());
    }

    #[tokio::test]
    async fn foreground_batch_preserves_attachment_event_boundaries() {
        let mut state = AppState::new();
        let (tx, mut rx) = tokio::sync::mpsc::channel(8);
        tx.try_send(fg_output(7, b"old")).expect("old output");
        tx.try_send(fg_output(8, b"new")).expect("new output");

        let first = rx.recv().await.expect("first ready event");
        let stats =
            apply_ready_event_batch(&mut state, &mut rx, first).expect("apply ready event batch");

        assert_eq!(
            stats,
            EventBatchStats {
                received: 2,
                applied: 2,
            }
        );
    }

    #[tokio::test]
    async fn ready_batch_has_a_fixed_work_bound() {
        let mut state = AppState::new();
        let (tx, mut rx) = tokio::sync::mpsc::channel(MAX_READY_EVENTS_PER_FRAME + 8);
        for _ in 0..(MAX_READY_EVENTS_PER_FRAME + 6) {
            tx.try_send(AppMsg::Tick).expect("queue tick");
        }

        let first = rx.recv().await.expect("first ready event");
        let stats =
            apply_ready_event_batch(&mut state, &mut rx, first).expect("apply ready event batch");

        assert_eq!(stats.received, MAX_READY_EVENTS_PER_FRAME);
        assert_eq!(stats.applied, MAX_READY_EVENTS_PER_FRAME);
        assert_eq!(rx.len(), 6);
    }
}
