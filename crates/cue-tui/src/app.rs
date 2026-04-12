//! App state and TEA update loop.
//!
//! Central state machine: all mutations flow through [`AppState::update`]
//! which pattern-matches on [`AppMsg`] and delegates to components.

use crossterm::event::{KeyCode, KeyEvent, KeyEventKind, KeyModifiers, MouseEvent};

use cue_core::Mode;
use cue_core::ipc::{EventPayload, OkPayload, RequestPayload, ResponsePayload};

use crate::client::WriterHandle;
use crate::component::Component;
use crate::component::input_line::{InputLine, InputMsg};
use crate::component::main_view::{Card, CardStatus, MainView, MainViewMsg};
use crate::component::sidebar::{OverviewCounts, Sidebar, SidebarMsg};
use crate::component::status_bar::{StatusBar, StatusBarMsg};

// ── Focus ──

/// Which panel currently owns keyboard focus.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FocusArea {
    Input,
    MainView,
    Sidebar,
}

// ── App-level message ──

/// All events that can mutate [`AppState`].
#[allow(clippy::large_enum_variant)]
pub enum AppMsg {
    // Raw terminal events
    KeyEvent(KeyEvent),
    MouseEvent(MouseEvent),
    Resize(u16, u16),

    // User actions
    Submit(String),
    ModeSwitch,
    ToggleSidebar,

    // Socket lifecycle
    Connected,
    Disconnected,
    Reconnected { writer: WriterHandle },
    Response { id: u32, payload: ResponsePayload },
    ServerEvent(EventPayload),

    // System
    Tick,
    Quit,
}

// ── App state ──

/// Root application state.  Owns all component state and connection info.
pub struct AppState {
    // Components
    pub input: InputLine,
    pub main_view: MainView,
    pub sidebar: Sidebar,
    pub status_bar: StatusBar,

    // Connection
    pub writer: Option<WriterHandle>,
    pub connected: bool,

    // UI state
    pub mode: Mode,
    /// `None` = auto (show when width ≥ 100), `Some` = manual override.
    pub show_sidebar: Option<bool>,
    pub focus: FocusArea,
    pub should_quit: bool,
    pub terminal_width: u16,
    pub terminal_height: u16,
}

impl AppState {
    pub fn new() -> Self {
        Self {
            input: InputLine::new(),
            main_view: MainView::new(),
            sidebar: Sidebar::new(),
            status_bar: StatusBar::new(),
            writer: None,
            connected: false,
            mode: Mode::default(),
            show_sidebar: None,
            focus: FocusArea::Input,
            should_quit: false,
            terminal_width: 80,
            terminal_height: 24,
        }
    }

    /// Whether the sidebar should be visible for the current terminal width.
    pub fn sidebar_visible(&self) -> bool {
        match self.show_sidebar {
            Some(v) => v,
            None => self.terminal_width >= 100,
        }
    }

    /// TEA update: apply a message to the state.
    pub fn update(&mut self, msg: AppMsg) {
        match msg {
            AppMsg::Quit => {
                self.should_quit = true;
            }

            AppMsg::Resize(w, h) => {
                self.terminal_width = w;
                self.terminal_height = h;
            }

            AppMsg::Tick => {
                // Status bar re-renders on every draw, clock updates automatically.
            }

            AppMsg::ModeSwitch => {
                self.mode = self.mode.next();
                self.input.update(InputMsg::SetMode(self.mode));
            }

            AppMsg::ToggleSidebar => {
                let currently_visible = self.sidebar_visible();
                self.show_sidebar = Some(!currently_visible);
            }

            AppMsg::Submit(text) => {
                // Push a new card.
                let card = Card::new(text.clone());
                self.main_view.update(MainViewMsg::PushCard(card));
                self.input.update(InputMsg::Clear);

                // Send to daemon if connected.
                if let Some(ref writer) = self.writer {
                    let payload = RequestPayload::Eval {
                        input: text,
                        mode: self.mode,
                    };
                    if let Err(e) = writer.try_send(payload) {
                        tracing::warn!("failed to send command: {e}");
                    }
                }
            }

            AppMsg::Connected => {
                self.connected = true;
                self.status_bar.update(StatusBarMsg::SetConnected(true));
            }

            AppMsg::Disconnected => {
                self.connected = false;
                self.writer = None;
                self.status_bar.update(StatusBarMsg::SetConnected(false));
            }

            AppMsg::Reconnected { writer } => {
                self.writer = Some(writer);
                self.connected = true;
                self.status_bar.update(StatusBarMsg::SetConnected(true));
            }

            AppMsg::Response { id: _, payload } => {
                match payload {
                    ResponsePayload::Ok(ok) => match ok {
                        OkPayload::JobCreated { job_id } => {
                            self.main_view.update(MainViewMsg::SetLatestLabel(job_id));
                            self.main_view
                                .update(MainViewMsg::SetLatestStatus(CardStatus::Streaming));
                        }
                        OkPayload::EvalText { text } => {
                            self.main_view
                                .update(MainViewMsg::AppendOutput { data: text });
                            self.main_view
                                .update(MainViewMsg::SetLatestStatus(CardStatus::Success));
                        }
                        OkPayload::Pong {} => {
                            tracing::debug!("pong received");
                        }
                        _ => {
                            // Other Ok variants — show as text.
                            let text = format!("{ok:?}");
                            self.main_view
                                .update(MainViewMsg::AppendOutput { data: text });
                            self.main_view
                                .update(MainViewMsg::SetLatestStatus(CardStatus::Success));
                        }
                    },
                    ResponsePayload::Err { code, message } => {
                        self.main_view.update(MainViewMsg::AppendOutput {
                            data: format!("Error [{code}]: {message}"),
                        });
                        self.main_view
                            .update(MainViewMsg::SetLatestStatus(CardStatus::Error));
                    }
                }
            }

            AppMsg::ServerEvent(event) => match event {
                EventPayload::OutputChunk {
                    id: _,
                    stream: _,
                    data,
                } => {
                    self.main_view.update(MainViewMsg::AppendOutput { data });
                }
                EventPayload::OutputEof { id: _ } => {
                    self.main_view
                        .update(MainViewMsg::SetLatestStatus(CardStatus::Success));
                }
                EventPayload::JobStateChanged {
                    job_id: _,
                    old_state: _,
                    new_state,
                } => {
                    use cue_core::job::JobStatus;
                    let status = match new_state {
                        JobStatus::Done => CardStatus::Success,
                        JobStatus::Failed | JobStatus::Killed | JobStatus::Cancelled(_) => {
                            CardStatus::Error
                        }
                        JobStatus::Running => CardStatus::Streaming,
                        JobStatus::Pending => CardStatus::Pending,
                    };
                    self.main_view.update(MainViewMsg::SetLatestStatus(status));
                }
                EventPayload::ShuttingDown { reason } => {
                    self.main_view.update(MainViewMsg::AppendOutput {
                        data: format!("⚠ Daemon shutting down: {reason}"),
                    });
                    self.connected = false;
                    self.status_bar.update(StatusBarMsg::SetConnected(false));
                }
                _ => {
                    tracing::debug!(?event, "unhandled server event");
                }
            },

            AppMsg::KeyEvent(key) => {
                // Intercept focus-management keys before dispatching.
                if key.kind == KeyEventKind::Press {
                    if key.code == KeyCode::Tab && !key.modifiers.contains(KeyModifiers::SHIFT) {
                        self.focus = match self.focus {
                            FocusArea::Input => FocusArea::MainView,
                            FocusArea::MainView => {
                                if self.sidebar_visible() {
                                    FocusArea::Sidebar
                                } else {
                                    FocusArea::Input
                                }
                            }
                            FocusArea::Sidebar => FocusArea::Input,
                        };
                        return;
                    }
                    if key.code == KeyCode::Esc && self.focus != FocusArea::Input {
                        self.focus = FocusArea::Input;
                        return;
                    }
                }

                // Dispatch to focused component; bubble up any returned AppMsg.
                let child_msg = match self.focus {
                    FocusArea::Input => self.input.handle_key(key),
                    FocusArea::MainView => self.main_view.handle_key(key),
                    FocusArea::Sidebar => self.sidebar.handle_key(key),
                };
                if let Some(msg) = child_msg {
                    self.update(msg);
                }
            }

            AppMsg::MouseEvent(mouse) => {
                // For now, forward mouse to main_view for scrolling.
                if let Some(msg) = self.main_view.handle_mouse(mouse) {
                    self.update(msg);
                }
            }
        }
    }
}

impl Default for AppState {
    fn default() -> Self {
        Self::new()
    }
}

// Sync overview counts from sidebar to status bar.
impl AppState {
    /// Propagate overview counts to both sidebar and status bar.
    pub fn set_overview(&mut self, counts: OverviewCounts) {
        self.sidebar.update(SidebarMsg::SetOverview(counts.clone()));
        self.status_bar.update(StatusBarMsg::SetOverview(counts));
    }
}
