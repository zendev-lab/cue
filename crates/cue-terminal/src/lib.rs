//! A concrete Ghostty terminal model for cue-shell foreground sessions.
//!
//! [`ForegroundTerminal`] owns only client-local VT state: parsing, input
//! encoding, scrollback, selection, and Ratatui projection. The cue daemon
//! remains the sole owner of child processes, PTYs, resize authority, and the
//! observer/controller lease.
//!
//! `libghostty-vt` types are deliberately kept private. They are `!Send` and
//! `!Sync`, so a terminal must remain on the TUI thread that created it.

mod error;
mod input;
mod render;
mod selection;
mod terminal;

pub use error::{Error, Result};
pub use render::{CursorState, CursorStyle};
pub use selection::SelectionRange;
pub use terminal::{
    EffectMode, ForegroundTerminal, ReplyAuthority, TerminalEffect, ViewportScroll,
};
