//! App-level messages shared by the event loop, components, and state machine.

use crossterm::event::{KeyEvent, MouseEvent};
use cue_core::ipc::{EventPayload, ResponsePayload};

use crate::client::WriterHandle;

/// All events that can mutate app state.
pub(crate) enum AppMsg {
    // Raw terminal events
    KeyEvent(KeyEvent),
    MouseEvent(MouseEvent),
    Paste(String),
    Resize(u16, u16),

    // User actions
    Submit(String),
    ModeSwitch,
    ToggleSidebar,
    ToggleMouseMode,
    CopyFocus,
    ClearDisplay,
    OpenTargetSettings,
    OpenJobPicker,
    KillSelection,

    // Socket lifecycle
    Connected,
    Disconnected,
    ReconnectFailed { message: String },
    Reconnected { writer: WriterHandle },
    Response { id: u32, payload: ResponsePayload },
    ServerEvent(EventPayload),

    // System
    FatalError { message: String },
    Tick,
    Quit,
}
