use std::{cell::RefCell, mem, rc::Rc};

use crossterm::event::{KeyEvent, MouseEvent};
use libghostty_vt::{
    RenderState, Terminal, focus, key,
    mouse::{self, EncoderSize},
    paste,
    render::{CellIterator, RowIterator},
    terminal::{Mode, Options, ScrollViewport},
};
use ratatui::{buffer::Buffer, layout::Rect};

use crate::{
    Error, Result,
    input::{self, NormalizedMouse},
    render::{self, CursorState},
    selection::{self, SelectionRange, SelectionState},
};

const LOGICAL_CELL_WIDTH_PX: u32 = 1;
const LOGICAL_CELL_HEIGHT_PX: u32 = 1;

/// Whether terminal callbacks may escape the local VT model.
///
/// Snapshot bytes must be fed as [`Replay`](Self::Replay), while bytes crossing
/// the daemon's live-output boundary use [`Live`](Self::Live).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum EffectMode {
    #[default]
    Replay,
    Live,
}

/// The caller's current foreground-write authority.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReplyAuthority {
    Observer,
    Controller,
}

/// A user-visible effect emitted while processing live terminal bytes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TerminalEffect {
    Bell,
    TitleChanged(String),
    WorkingDirectoryChanged(String),
}

/// A local scrollback operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ViewportScroll {
    /// Negative values move toward older output; positive values move down.
    Lines(isize),
    Top,
    Bottom,
}

#[derive(Debug, Default)]
struct CallbackState {
    mode: EffectMode,
    effects: Vec<TerminalEffect>,
    replies: Vec<u8>,
    reply_checkpoint: usize,
    error: Option<libghostty_vt::Error>,
}

impl CallbackState {
    fn begin(&mut self, mode: EffectMode) {
        self.mode = mode;
        self.effects.clear();
        self.error = None;
        self.reply_checkpoint = self.replies.len();
    }

    fn finish(&mut self) -> Result<Vec<TerminalEffect>> {
        self.mode = EffectMode::Replay;
        if let Some(error) = self.error.take() {
            self.replies.truncate(self.reply_checkpoint);
            self.effects.clear();
            return Err(error.into());
        }
        Ok(mem::take(&mut self.effects))
    }

    fn abort(&mut self) {
        self.mode = EffectMode::Replay;
        self.replies.truncate(self.reply_checkpoint);
        self.effects.clear();
        self.error = None;
    }

    fn live(&self) -> bool {
        self.mode == EffectMode::Live
    }
}

/// Ghostty-backed terminal state for one attached foreground job.
///
/// This type intentionally contains no PTY or transport handle. The outer TUI
/// feeds daemon snapshots/output into it and sends encoded input or drained
/// replies through the existing foreground IPC.
#[derive(Debug)]
pub struct ForegroundTerminal {
    terminal: Terminal<'static, 'static>,
    render_state: RenderState<'static>,
    row_iterator: RowIterator<'static>,
    cell_iterator: CellIterator<'static>,
    key_encoder: key::Encoder<'static>,
    mouse_encoder: mouse::Encoder<'static>,
    callbacks: Rc<RefCell<CallbackState>>,
    selection: SelectionState,
    pressed_mouse_buttons: u16,
}

impl ForegroundTerminal {
    /// Create a terminal with a cell-based Ratatui surface.
    pub fn new(cols: u16, rows: u16, max_scrollback: usize) -> Result<Self> {
        validate_size(cols, rows)?;
        let callbacks = Rc::new(RefCell::new(CallbackState::default()));
        let mut terminal = Terminal::new(Options {
            cols,
            rows,
            max_scrollback,
        })?;
        install_callbacks(&mut terminal, &callbacks)?;

        let mut mouse_encoder = mouse::Encoder::new()?;
        mouse_encoder.set_track_last_cell(true);
        Ok(Self {
            terminal,
            render_state: RenderState::new()?,
            row_iterator: RowIterator::new()?,
            cell_iterator: CellIterator::new()?,
            key_encoder: key::Encoder::new()?,
            mouse_encoder,
            callbacks,
            selection: SelectionState::default(),
            pressed_mouse_buttons: 0,
        })
    }

    /// Feed raw foreground output into the VT model.
    ///
    /// Replay mode updates screen state but suppresses replies, bells, title
    /// updates, and working-directory effects. Live mode records them.
    pub fn feed(&mut self, bytes: &[u8], mode: EffectMode) -> Result<Vec<TerminalEffect>> {
        self.selection.detach_before_mutation(&self.terminal)?;
        self.callbacks.borrow_mut().begin(mode);
        self.terminal.vt_write(bytes);
        if let Err(error) = self.selection.refresh(&self.terminal) {
            self.callbacks.borrow_mut().abort();
            return Err(error);
        }
        self.callbacks.borrow_mut().finish()
    }

    /// Resize the local model while suppressing terminal effects.
    pub fn resize(&mut self, cols: u16, rows: u16) -> Result<()> {
        self.resize_with_effects(cols, rows, EffectMode::Replay)
            .map(drop)
    }

    /// Resize the local model with an explicit effect gate.
    ///
    /// A controller may use live mode when in-band resize reports must be
    /// forwarded. Observers and initial snapshot setup must use replay mode.
    pub fn resize_with_effects(
        &mut self,
        cols: u16,
        rows: u16,
        mode: EffectMode,
    ) -> Result<Vec<TerminalEffect>> {
        validate_size(cols, rows)?;
        if self.size()? == (cols, rows) {
            return Ok(Vec::new());
        }

        self.selection.detach_before_mutation(&self.terminal)?;
        self.callbacks.borrow_mut().begin(mode);
        if let Err(error) =
            self.terminal
                .resize(cols, rows, LOGICAL_CELL_WIDTH_PX, LOGICAL_CELL_HEIGHT_PX)
        {
            self.callbacks.borrow_mut().abort();
            return Err(error.into());
        }
        if let Err(error) = self.selection.refresh(&self.terminal) {
            self.callbacks.borrow_mut().abort();
            return Err(error);
        }
        self.callbacks.borrow_mut().finish()
    }

    /// Render the visible viewport into a Ratatui buffer.
    ///
    /// The returned cursor position is absolute within `buffer`.
    pub fn render_into(
        &mut self,
        area: Rect,
        buffer: &mut Buffer,
        focused: bool,
    ) -> Result<CursorState> {
        self.selection.refresh(&self.terminal)?;
        render::render_into(
            &self.terminal,
            &mut self.render_state,
            &mut self.row_iterator,
            &mut self.cell_iterator,
            area,
            buffer,
            focused,
        )
    }

    /// Encode a crossterm key using the terminal's current keyboard modes.
    pub fn encode_key(&mut self, event: KeyEvent) -> Result<Vec<u8>> {
        self.key_encoder.set_options_from_terminal(&self.terminal);
        let event = input::key_event(event)?;
        let mut encoded = Vec::new();
        self.key_encoder.encode_to_vec(&event, &mut encoded)?;
        Ok(encoded)
    }

    /// Encode a crossterm mouse event relative to the rendered terminal area.
    ///
    /// Events outside `viewport` and events ignored by the active terminal
    /// mouse protocol produce an empty vector.
    pub fn encode_mouse(&mut self, event: MouseEvent, viewport: Rect) -> Result<Vec<u8>> {
        let Some(NormalizedMouse {
            event,
            button_change,
        }) = input::mouse_event(
            event,
            viewport,
            LOGICAL_CELL_WIDTH_PX,
            LOGICAL_CELL_HEIGHT_PX,
        )?
        else {
            return Ok(Vec::new());
        };

        if let Some((button, pressed)) = button_change {
            if pressed {
                self.pressed_mouse_buttons |= button;
            } else {
                self.pressed_mouse_buttons &= !button;
            }
        }

        let (cols, rows) = self.size()?;
        let screen_width = u32::from(cols)
            .checked_mul(LOGICAL_CELL_WIDTH_PX)
            .ok_or(Error::MouseCoordinateOverflow)?;
        let screen_height = u32::from(rows)
            .checked_mul(LOGICAL_CELL_HEIGHT_PX)
            .ok_or(Error::MouseCoordinateOverflow)?;
        self.mouse_encoder
            .set_options_from_terminal(&self.terminal)
            .set_size(EncoderSize {
                screen_width,
                screen_height,
                cell_width: LOGICAL_CELL_WIDTH_PX,
                cell_height: LOGICAL_CELL_HEIGHT_PX,
                padding_top: 0,
                padding_bottom: 0,
                padding_right: 0,
                padding_left: 0,
            })
            .set_any_button_pressed(self.pressed_mouse_buttons != 0);

        let mut encoded = Vec::new();
        self.mouse_encoder.encode_to_vec(&event, &mut encoded)?;
        Ok(encoded)
    }

    /// Encode pasted text using Ghostty's sanitization and bracketed-paste mode.
    pub fn encode_paste(&self, text: &str) -> Result<Vec<u8>> {
        let bracketed = self.terminal.mode(Mode::BRACKETED_PASTE)?;
        let capacity = text.len().checked_add(12).ok_or(Error::InputTooLarge)?;
        let mut input = text.as_bytes().to_vec();
        let mut output = vec![0_u8; capacity];
        let written = paste::encode(&mut input, bracketed, &mut output)?;
        output.truncate(written);
        Ok(output)
    }

    /// Encode a focus transition only when the foreground enabled mode 1004.
    pub fn encode_focus(&self, focused: bool) -> Result<Vec<u8>> {
        if !self.terminal.mode(Mode::FOCUS_EVENT)? {
            return Ok(Vec::new());
        }
        let mut output = [0_u8; 8];
        let written = if focused {
            focus::Event::Gained
        } else {
            focus::Event::Lost
        }
        .encode(&mut output)?;
        Ok(output[..written].to_vec())
    }

    /// Drain terminal-generated PTY replies through an explicit role gate.
    ///
    /// Observer drains discard pending bytes so they cannot become stale input
    /// if the client later claims control.
    pub fn drain_replies(&mut self, authority: ReplyAuthority) -> Vec<u8> {
        let mut callbacks = self.callbacks.borrow_mut();
        match authority {
            ReplyAuthority::Controller => mem::take(&mut callbacks.replies),
            ReplyAuthority::Observer => {
                callbacks.replies.clear();
                Vec::new()
            }
        }
    }

    pub fn pending_reply_bytes(&self) -> usize {
        self.callbacks.borrow().replies.len()
    }

    pub fn size(&self) -> Result<(u16, u16)> {
        Ok((self.terminal.cols()?, self.terminal.rows()?))
    }

    pub fn title(&self) -> Result<&str> {
        self.terminal.title().map_err(Into::into)
    }

    pub fn working_directory(&self) -> Result<&str> {
        self.terminal.pwd().map_err(Into::into)
    }

    pub fn bracketed_paste_enabled(&self) -> Result<bool> {
        self.terminal
            .mode(Mode::BRACKETED_PASTE)
            .map_err(Into::into)
    }

    pub fn mouse_tracking_enabled(&self) -> Result<bool> {
        self.terminal.is_mouse_tracking().map_err(Into::into)
    }

    pub fn scroll(&mut self, scroll: ViewportScroll) -> Result<()> {
        self.selection.detach_before_mutation(&self.terminal)?;
        self.terminal.scroll_viewport(match scroll {
            ViewportScroll::Lines(lines) => ScrollViewport::Delta(lines),
            ViewportScroll::Top => ScrollViewport::Top,
            ViewportScroll::Bottom => ScrollViewport::Bottom,
        });
        self.selection.refresh(&self.terminal)
    }

    pub fn begin_selection(&mut self, row: u16, col: u16) -> Result<()> {
        self.selection.begin(&self.terminal, row, col)
    }

    pub fn update_selection(&mut self, row: u16, col: u16, rectangular: bool) -> Result<()> {
        self.selection.update(&self.terminal, row, col, rectangular)
    }

    pub fn clear_selection(&mut self) -> Result<()> {
        self.selection.clear(&self.terminal)
    }

    pub fn has_selection(&self) -> bool {
        self.selection.has_selection()
    }

    pub fn selection_range(&self) -> Option<SelectionRange> {
        self.selection.range()
    }

    pub fn copy_selection(&mut self) -> Result<Option<String>> {
        self.selection.copy_selection(&self.terminal)
    }

    pub fn copy_screen(&self) -> Result<String> {
        selection::copy_screen(&self.terminal)
    }

    /// Copy the active selection, or the visible terminal content when no
    /// selection exists.
    pub fn copy_text(&mut self) -> Result<String> {
        if let Some(selection) = self.copy_selection()? {
            return Ok(selection);
        }
        self.copy_screen()
    }
}

fn install_callbacks(
    terminal: &mut Terminal<'static, 'static>,
    state: &Rc<RefCell<CallbackState>>,
) -> Result<()> {
    let replies = Rc::clone(state);
    terminal.on_pty_write(move |_terminal, bytes| {
        let mut state = replies.borrow_mut();
        if state.live() {
            state.replies.extend_from_slice(bytes);
        }
    })?;

    let bell = Rc::clone(state);
    terminal.on_bell(move |_terminal| {
        let mut state = bell.borrow_mut();
        if state.live() {
            state.effects.push(TerminalEffect::Bell);
        }
    })?;

    let title = Rc::clone(state);
    terminal.on_title_changed(move |terminal| {
        let mut state = title.borrow_mut();
        if !state.live() {
            return;
        }
        match terminal.title() {
            Ok(title) => state
                .effects
                .push(TerminalEffect::TitleChanged(title.to_owned())),
            Err(error) => state.error = Some(error),
        }
    })?;

    let working_directory = Rc::clone(state);
    terminal.on_pwd_changed(move |terminal| {
        let mut state = working_directory.borrow_mut();
        if !state.live() {
            return;
        }
        match terminal.pwd() {
            Ok(path) => state
                .effects
                .push(TerminalEffect::WorkingDirectoryChanged(path.to_owned())),
            Err(error) => state.error = Some(error),
        }
    })?;
    Ok(())
}

fn validate_size(cols: u16, rows: u16) -> Result<()> {
    if cols == 0 || rows == 0 {
        return Err(Error::InvalidSize { cols, rows });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crossterm::event::{KeyCode, KeyModifiers, MouseButton, MouseEventKind};
    use ratatui::{buffer::Buffer, style::Color};

    use super::*;

    fn terminal() -> ForegroundTerminal {
        ForegroundTerminal::new(20, 5, 100).expect("terminal")
    }

    #[test]
    fn replay_updates_state_without_emitting_effects() {
        let mut terminal = terminal();
        let effects = terminal
            .feed(b"\x07\x1b]2;replayed title\x07", EffectMode::Replay)
            .expect("feed replay");
        assert!(effects.is_empty());
        assert_eq!(terminal.title().expect("title"), "replayed title");
    }

    #[test]
    fn live_feed_emits_bell_and_title_effects() {
        let mut terminal = terminal();
        let effects = terminal
            .feed(b"\x07\x1b]2;live title\x07", EffectMode::Live)
            .expect("feed live");
        assert_eq!(
            effects,
            vec![
                TerminalEffect::Bell,
                TerminalEffect::TitleChanged("live title".to_string())
            ]
        );
    }

    #[test]
    fn replies_require_live_feed_and_controller_drain() {
        let mut terminal = terminal();
        terminal
            .feed(b"\x1b[5n", EffectMode::Replay)
            .expect("replay query");
        assert_eq!(terminal.pending_reply_bytes(), 0);

        terminal
            .feed(b"\x1b[5n", EffectMode::Live)
            .expect("live query");
        assert!(terminal.pending_reply_bytes() > 0);
        assert_eq!(
            terminal.drain_replies(ReplyAuthority::Controller),
            b"\x1b[0n"
        );
    }

    #[test]
    fn observer_drain_discards_replies_before_control_changes() {
        let mut terminal = terminal();
        terminal
            .feed(b"\x1b[5n", EffectMode::Live)
            .expect("live query");
        assert!(terminal.drain_replies(ReplyAuthority::Observer).is_empty());
        assert!(
            terminal
                .drain_replies(ReplyAuthority::Controller)
                .is_empty()
        );
    }

    #[test]
    fn keyboard_encoding_uses_terminal_application_cursor_mode() {
        let mut terminal = terminal();
        let up = KeyEvent::new(KeyCode::Up, KeyModifiers::NONE);
        assert_eq!(terminal.encode_key(up).expect("normal cursor"), b"\x1b[A");

        terminal
            .feed(b"\x1b[?1h", EffectMode::Replay)
            .expect("application cursor mode");
        assert_eq!(
            terminal.encode_key(up).expect("application cursor"),
            b"\x1bOA"
        );
    }

    #[test]
    fn paste_and_focus_follow_terminal_modes() {
        let mut terminal = terminal();
        assert!(
            terminal
                .encode_focus(true)
                .expect("focus disabled")
                .is_empty()
        );
        assert_eq!(
            terminal.encode_paste("hello\nworld").expect("plain paste"),
            b"hello\rworld"
        );

        terminal
            .feed(b"\x1b[?1004h\x1b[?2004h", EffectMode::Replay)
            .expect("enable modes");
        assert_eq!(terminal.encode_focus(true).expect("focus"), b"\x1b[I");
        assert_eq!(
            terminal
                .encode_paste("hello\nworld")
                .expect("bracketed paste"),
            b"\x1b[200~hello\nworld\x1b[201~"
        );
    }

    #[test]
    fn mouse_encoding_uses_viewport_relative_coordinates() {
        let mut terminal = terminal();
        terminal
            .feed(b"\x1b[?1000h\x1b[?1006h", EffectMode::Replay)
            .expect("enable mouse");
        let event = MouseEvent {
            kind: MouseEventKind::Down(MouseButton::Left),
            column: 7,
            row: 6,
            modifiers: KeyModifiers::NONE,
        };
        assert_eq!(
            terminal
                .encode_mouse(event, Rect::new(5, 3, 20, 5))
                .expect("mouse"),
            b"\x1b[<0;3;4M"
        );
    }

    #[test]
    fn selection_copy_uses_tracked_grid_references() {
        let mut terminal = terminal();
        terminal
            .feed(b"hello world\r\n", EffectMode::Replay)
            .expect("feed");
        terminal.begin_selection(0, 0).expect("begin selection");
        terminal
            .update_selection(0, 4, false)
            .expect("update selection");
        assert_eq!(
            terminal.copy_selection().expect("copy").as_deref(),
            Some("hello")
        );
        assert!(terminal.has_selection());
    }

    #[test]
    fn render_is_fallible_and_clears_stale_cells() {
        let mut terminal = terminal();
        terminal
            .feed(b"\x1b[38;2;12;34;56mLong text", EffectMode::Replay)
            .expect("feed");
        let area = Rect::new(2, 1, 20, 5);
        let mut buffer = Buffer::empty(Rect::new(0, 0, 24, 8));
        let cursor = terminal
            .render_into(area, &mut buffer, true)
            .expect("render");
        assert_eq!(buffer[(2, 1)].symbol(), "L");
        assert_eq!(buffer[(2, 1)].fg, Color::Rgb(12, 34, 56));
        assert_eq!(cursor.position, Some(ratatui::layout::Position::new(11, 1)));

        terminal
            .feed(b"\x1b[2J\x1b[HShort", EffectMode::Replay)
            .expect("replace screen");
        terminal
            .render_into(area, &mut buffer, true)
            .expect("render replacement");
        assert_eq!(buffer[(2, 1)].symbol(), "S");
        assert_eq!(buffer[(8, 1)].symbol(), " ");
    }

    #[test]
    fn zero_sized_terminals_fail_before_ffi() {
        assert!(matches!(
            ForegroundTerminal::new(0, 5, 0),
            Err(Error::InvalidSize { cols: 0, rows: 5 })
        ));
    }
}
