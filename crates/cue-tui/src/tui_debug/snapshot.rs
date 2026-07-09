//! Shared debug snapshots and app-state summaries.

use std::sync::{Arc, RwLock};

use cue_core::tui_debug::{TuiDebugCapture, TuiDebugState};

use crate::app::AppState;
use crate::focus::FocusArea;

use super::buffer::FrameText;

#[derive(Debug, Clone)]
pub(crate) struct FrameSnapshot {
    pub text: FrameText,
}

#[derive(Debug, Clone)]
pub(crate) struct DebugSnapshots {
    pub frame: Option<FrameSnapshot>,
    pub state: TuiDebugState,
}

impl DebugSnapshots {
    pub(crate) fn new() -> Self {
        Self {
            frame: None,
            state: empty_state(),
        }
    }
}

pub(crate) type SharedDebugSnapshots = Arc<RwLock<DebugSnapshots>>;

pub(crate) fn shared_debug_snapshots() -> SharedDebugSnapshots {
    Arc::new(RwLock::new(DebugSnapshots::new()))
}

pub(crate) fn update_frame_snapshot(shared: &SharedDebugSnapshots, text: FrameText) {
    if let Ok(mut guard) = shared.write() {
        guard.frame = Some(FrameSnapshot { text });
    }
}

pub(crate) fn update_state_snapshot(shared: &SharedDebugSnapshots, state: &AppState) {
    if let Ok(mut guard) = shared.write() {
        guard.state = state_summary(state);
    }
}

pub(crate) fn capture_from_snapshots(
    shared: &SharedDebugSnapshots,
    styled: bool,
) -> Result<TuiDebugCapture, String> {
    let guard = shared
        .read()
        .map_err(|_| "debug snapshot lock poisoned".to_string())?;
    let frame = guard
        .frame
        .as_ref()
        .ok_or_else(|| "no rendered frame is available yet".to_string())?;
    Ok(TuiDebugCapture {
        text: frame.text.plain.clone(),
        width: frame.text.width,
        height: frame.text.height,
        styled: styled.then(|| frame.text.styled.clone()),
    })
}

pub(crate) fn state_from_snapshots(shared: &SharedDebugSnapshots) -> Result<TuiDebugState, String> {
    let guard = shared
        .read()
        .map_err(|_| "debug snapshot lock poisoned".to_string())?;
    Ok(guard.state.clone())
}

pub(crate) fn state_summary(state: &AppState) -> TuiDebugState {
    TuiDebugState {
        mode: format!("{:?}", state.mode).to_ascii_uppercase(),
        focus: focus_label(state.focus).into(),
        input: state.input.content.clone(),
        sidebar_visible: state.sidebar_visible(),
        connected: state.connected,
        job_count: state.debug_job_count(),
        cron_count: state.debug_cron_count(),
        active_display_tab: state.active_display_tab(),
        display_tab_labels: state.display_tab_labels(),
        fg_active: state.fg_active(),
        should_quit: state.should_quit,
        terminal_width: state.terminal_width,
        terminal_height: state.terminal_height,
    }
}

fn focus_label(focus: FocusArea) -> &'static str {
    match focus {
        FocusArea::Input => "input",
        FocusArea::MainView => "main",
        FocusArea::Sidebar => "sidebar",
    }
}

fn empty_state() -> TuiDebugState {
    TuiDebugState {
        mode: "JOB".into(),
        focus: "input".into(),
        input: String::new(),
        sidebar_visible: false,
        connected: false,
        job_count: 0,
        cron_count: 0,
        active_display_tab: None,
        display_tab_labels: Vec::new(),
        fg_active: false,
        should_quit: false,
        terminal_width: 0,
        terminal_height: 0,
    }
}

#[cfg(test)]
mod tests {
    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    use crate::message::AppMsg;
    use crate::tui_debug::buffer::frame_text_from_buffer;
    use crate::ui;

    use super::*;

    #[test]
    fn debug_capture_reflects_rendered_app_state() {
        let snapshots = shared_debug_snapshots();
        let mut state = AppState::new();
        state.terminal_width = 40;
        state.terminal_height = 12;
        state.update(AppMsg::KeyEvent(KeyEvent::new(
            KeyCode::Char('h'),
            KeyModifiers::NONE,
        )));
        update_state_snapshot(&snapshots, &state);

        let backend = TestBackend::new(40, 12);
        let mut terminal = Terminal::new(backend).expect("terminal");
        terminal
            .draw(|frame| {
                ui::draw(frame, &state);
                update_frame_snapshot(&snapshots, frame_text_from_buffer(frame.buffer_mut()));
            })
            .expect("draw");

        let capture = capture_from_snapshots(&snapshots, false).expect("capture frame");
        assert!(
            capture.text.contains('h'),
            "capture should include typed input"
        );
        assert_eq!(capture.width, 40);
        assert_eq!(capture.height, 12);

        let summary = state_summary(&state);
        assert_eq!(summary.input, "h");
        assert_eq!(summary.mode, "JOB");
    }
}
