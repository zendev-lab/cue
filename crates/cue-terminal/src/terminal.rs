use std::{cell::RefCell, mem, rc::Rc, sync::OnceLock};

use crossterm::event::{KeyEvent, MouseEvent, MouseEventKind};
use libghostty_vt::{
    RenderState, Terminal,
    fmt::Format,
    focus, key,
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
const MAX_REPLY_BYTES_PER_UPDATE: usize = 64 * 1024;
static GHOSTTY_INITIALIZED: OnceLock<std::result::Result<(), String>> = OnceLock::new();

/// Whether terminal-generated PTY replies may escape the local VT model.
///
/// Snapshot bytes must be fed as [`Replay`](Self::Replay), while bytes crossing
/// the daemon's live-output boundary use [`Live`](Self::Live) with the current
/// observer/controller authority. Observer mode rejects PTY replies at callback
/// time.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum FeedMode {
    #[default]
    Replay,
    Live(ReplyAuthority),
}

/// The caller's current foreground-write authority.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReplyAuthority {
    Observer,
    Controller,
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
    mode: FeedMode,
    replies: Vec<u8>,
    reply_overflow: bool,
}

impl CallbackState {
    fn begin(&mut self, mode: FeedMode) {
        self.mode = mode;
        // A reply belongs only to the update that produced it. Clearing here
        // prevents a missed drain or a later controller claim from reviving it.
        self.replies.clear();
        self.reply_overflow = false;
    }

    fn finish(&mut self) -> Result<()> {
        self.mode = FeedMode::Replay;
        if self.reply_overflow {
            self.replies.clear();
            self.reply_overflow = false;
            return Err(Error::ReplyOverflow {
                limit: MAX_REPLY_BYTES_PER_UPDATE,
            });
        }
        Ok(())
    }

    fn abort(&mut self) {
        self.mode = FeedMode::Replay;
        self.replies.clear();
        self.reply_overflow = false;
    }

    fn may_reply(&self) -> bool {
        self.mode == FeedMode::Live(ReplyAuthority::Controller)
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
        initialize_ghostty()?;
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
    /// Replay mode updates screen state but suppresses terminal-generated PTY
    /// replies. Live mode applies the current observer/controller reply gate.
    pub fn feed(&mut self, bytes: &[u8], mode: FeedMode) -> Result<()> {
        self.callbacks.borrow_mut().begin(mode);
        if let Err(error) = self.selection.detach_before_mutation(&self.terminal) {
            self.callbacks.borrow_mut().abort();
            return Err(error);
        }
        self.terminal.vt_write(bytes);
        if let Err(error) = self.selection.refresh(&self.terminal) {
            self.callbacks.borrow_mut().abort();
            return Err(error);
        }
        self.callbacks.borrow_mut().finish()
    }

    /// Resize the local model while suppressing terminal-generated PTY replies.
    pub fn resize(&mut self, cols: u16, rows: u16) -> Result<()> {
        self.resize_with_mode(cols, rows, FeedMode::Replay)
    }

    /// Resize the local model with an explicit terminal-reply gate.
    ///
    /// A controller may use live mode when in-band resize reports must be
    /// forwarded. Observers and initial snapshot setup must use replay mode.
    pub fn resize_with_mode(&mut self, cols: u16, rows: u16, mode: FeedMode) -> Result<()> {
        validate_size(cols, rows)?;
        self.callbacks.borrow_mut().begin(mode);
        let current_size = match self.size() {
            Ok(size) => size,
            Err(error) => {
                self.callbacks.borrow_mut().abort();
                return Err(error);
            }
        };
        if current_size == (cols, rows) {
            return self.callbacks.borrow_mut().finish();
        }

        if let Err(error) = self.selection.detach_before_mutation(&self.terminal) {
            self.callbacks.borrow_mut().abort();
            return Err(error);
        }
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
    /// Uncaptured events outside `viewport` and events ignored by the active
    /// terminal mouse protocol produce an empty vector. Captured drag/release
    /// events are clamped to the nearest viewport cell.
    pub fn encode_mouse(&mut self, mut event: MouseEvent, viewport: Rect) -> Result<Vec<u8>> {
        // Once a button goes down inside the terminal, keep routing drag and
        // release events to it even when the pointer crosses the pane edge.
        // Clamping gives the child a valid coordinate and, critically, the
        // matching release instead of leaving its mouse capture stuck.
        let captured = match event.kind {
            MouseEventKind::Up(button) | MouseEventKind::Drag(button) => {
                self.pressed_mouse_buttons & input::mouse_button_bit(button) != 0
            }
            MouseEventKind::Moved => self.pressed_mouse_buttons != 0,
            _ => false,
        };
        if captured && viewport.width > 0 && viewport.height > 0 {
            event.column = event
                .column
                .clamp(viewport.left(), viewport.right().saturating_sub(1));
            event.row = event
                .row
                .clamp(viewport.top(), viewport.bottom().saturating_sub(1));
        }

        let normalized = input::mouse_event(
            event,
            viewport,
            LOGICAL_CELL_WIDTH_PX,
            LOGICAL_CELL_HEIGHT_PX,
        )?;
        let Some(NormalizedMouse {
            event,
            button_change,
        }) = normalized
        else {
            if let MouseEventKind::Up(button) = event.kind {
                self.pressed_mouse_buttons &= !input::mouse_button_bit(button);
                self.mouse_encoder
                    .set_any_button_pressed(self.pressed_mouse_buttons != 0);
            }
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

    /// Clear pointer capture and Ghostty's motion-deduplication state.
    ///
    /// Call this whenever controller authority or mouse capture is lost.
    pub fn reset_pointer_state(&mut self) {
        self.pressed_mouse_buttons = 0;
        self.mouse_encoder.set_any_button_pressed(false).reset();
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

    /// Format the visible screen as VT bytes for durable card output.
    pub fn formatted_text(&self) -> Result<Vec<u8>> {
        selection::format_screen(&self.terminal, Format::Vt)
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
        if !state.may_reply() || state.reply_overflow {
            return;
        }
        if state.replies.len().saturating_add(bytes.len()) <= MAX_REPLY_BYTES_PER_UPDATE {
            state.replies.extend_from_slice(bytes);
        } else {
            state.replies.clear();
            state.reply_overflow = true;
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

fn initialize_ghostty() -> Result<()> {
    match GHOSTTY_INITIALIZED
        .get_or_init(|| libghostty_vt::set_logger(None).map_err(|error| error.to_string()))
    {
        Ok(()) => Ok(()),
        Err(message) => Err(Error::Initialization(message.clone())),
    }
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
    fn replay_updates_terminal_state_without_replies() {
        let mut terminal = terminal();
        terminal
            .feed(b"\x07\x1b]2;replayed title\x07", FeedMode::Replay)
            .expect("feed replay");
        assert_eq!(terminal.pending_reply_bytes(), 0);
        assert_eq!(terminal.title().expect("title"), "replayed title");
    }

    #[test]
    fn live_observer_feed_updates_state_without_replies() {
        let mut terminal = terminal();
        terminal
            .feed(
                b"\x07\x1b]2;live title\x07",
                FeedMode::Live(ReplyAuthority::Observer),
            )
            .expect("feed live");
        assert_eq!(terminal.pending_reply_bytes(), 0);
        assert_eq!(terminal.title().expect("title"), "live title");
    }

    #[test]
    fn replies_require_live_feed_and_controller_drain() {
        let mut terminal = terminal();
        terminal
            .feed(b"\x1b[5n", FeedMode::Replay)
            .expect("replay query");
        assert_eq!(terminal.pending_reply_bytes(), 0);

        terminal
            .feed(b"\x1b[5n", FeedMode::Live(ReplyAuthority::Controller))
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
            .feed(b"\x1b[5n", FeedMode::Live(ReplyAuthority::Observer))
            .expect("live query");
        assert_eq!(terminal.pending_reply_bytes(), 0);
        assert!(
            terminal
                .drain_replies(ReplyAuthority::Controller)
                .is_empty()
        );
    }

    #[test]
    fn a_noop_resize_cannot_revive_an_undrained_reply() {
        let mut terminal = terminal();
        terminal
            .feed(b"\x1b[5n", FeedMode::Live(ReplyAuthority::Controller))
            .expect("live query");
        assert!(terminal.pending_reply_bytes() > 0);

        terminal
            .resize_with_mode(20, 5, FeedMode::Live(ReplyAuthority::Observer))
            .expect("no-op observer resize");
        assert_eq!(terminal.pending_reply_bytes(), 0);
    }

    #[test]
    fn oversized_controller_reply_batch_is_rejected_without_retaining_bytes() {
        let mut terminal = terminal();
        let queries = b"\x1b[5n".repeat((MAX_REPLY_BYTES_PER_UPDATE / 4) + 1);
        assert!(matches!(
            terminal.feed(&queries, FeedMode::Live(ReplyAuthority::Controller)),
            Err(Error::ReplyOverflow {
                limit: MAX_REPLY_BYTES_PER_UPDATE
            })
        ));
        assert_eq!(terminal.pending_reply_bytes(), 0);
    }

    #[test]
    fn keyboard_encoding_uses_terminal_application_cursor_mode() {
        let mut terminal = terminal();
        let up = KeyEvent::new(KeyCode::Up, KeyModifiers::NONE);
        assert_eq!(terminal.encode_key(up).expect("normal cursor"), b"\x1b[A");

        terminal
            .feed(b"\x1b[?1h", FeedMode::Replay)
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
            .feed(b"\x1b[?1004h\x1b[?2004h", FeedMode::Replay)
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
            .feed(b"\x1b[?1000h\x1b[?1006h", FeedMode::Replay)
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
    fn pointer_state_resets_on_demotion_and_outside_release() {
        let mut terminal = terminal();
        terminal
            .feed(b"\x1b[?1002h\x1b[?1006h", FeedMode::Replay)
            .expect("enable button mouse tracking");
        let viewport = Rect::new(5, 3, 20, 5);
        let left_down = MouseEvent {
            kind: MouseEventKind::Down(MouseButton::Left),
            column: 7,
            row: 4,
            modifiers: KeyModifiers::NONE,
        };
        let moved = MouseEvent {
            kind: MouseEventKind::Moved,
            column: 8,
            row: 4,
            modifiers: KeyModifiers::NONE,
        };

        assert!(
            !terminal
                .encode_mouse(left_down, viewport)
                .expect("mouse down")
                .is_empty()
        );
        terminal.reset_pointer_state();
        assert!(
            terminal
                .encode_mouse(moved, viewport)
                .expect("move after demotion")
                .is_empty()
        );

        terminal
            .encode_mouse(left_down, viewport)
            .expect("second mouse down");
        let outside_release = MouseEvent {
            kind: MouseEventKind::Up(MouseButton::Left),
            column: 1,
            row: 1,
            modifiers: KeyModifiers::NONE,
        };
        let release = terminal
            .encode_mouse(outside_release, viewport)
            .expect("outside release");
        assert!(!release.is_empty());
        assert_eq!(release.last(), Some(&b'm'));
        assert!(
            terminal
                .encode_mouse(moved, viewport)
                .expect("move after outside release")
                .is_empty()
        );
    }

    #[test]
    fn selection_copy_uses_tracked_grid_references() {
        let mut terminal = terminal();
        terminal
            .feed(b"hello world\r\n", FeedMode::Replay)
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
            .feed(b"\x1b[38;2;12;34;56mLong text", FeedMode::Replay)
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
            .render_into(area, &mut buffer, true)
            .expect("render unchanged frame");
        assert_eq!(buffer[(2, 1)].symbol(), "L");
        assert_eq!(buffer[(2, 1)].fg, Color::Rgb(12, 34, 56));

        terminal
            .feed(b"\x1b[2J\x1b[HShort", FeedMode::Replay)
            .expect("replace screen");
        terminal
            .render_into(area, &mut buffer, true)
            .expect("render replacement");
        assert_eq!(buffer[(2, 1)].symbol(), "S");
        assert_eq!(buffer[(8, 1)].symbol(), " ");
    }

    #[test]
    fn utf8_chunks_wide_cells_and_reflow_keep_text() {
        let mut terminal = ForegroundTerminal::new(6, 3, 100).expect("terminal");
        let text = "ab界cd";
        let bytes = text.as_bytes();
        terminal
            .feed(&bytes[..3], FeedMode::Replay)
            .expect("first UTF-8 chunk");
        terminal
            .feed(&bytes[3..], FeedMode::Replay)
            .expect("second UTF-8 chunk");
        assert_eq!(terminal.copy_screen().expect("copy"), text);

        let mut buffer = Buffer::empty(Rect::new(0, 0, 6, 3));
        terminal
            .render_into(Rect::new(0, 0, 6, 3), &mut buffer, true)
            .expect("render");
        assert_eq!(buffer[(2, 0)].symbol(), "界");

        terminal.resize(3, 4).expect("resize");
        assert_eq!(terminal.size().expect("size"), (3, 4));
        assert_eq!(terminal.copy_screen().expect("copy after reflow"), text);
    }

    #[test]
    fn formatted_text_preserves_terminal_style() {
        let mut terminal = terminal();
        terminal
            .feed(b"\x1b[31mred\x1b[0m", FeedMode::Replay)
            .expect("feed styled text");
        let formatted = terminal.formatted_text().expect("formatted text");
        assert!(formatted.windows(3).any(|window| window == b"red"));
        assert!(formatted.contains(&0x1b));
    }

    #[test]
    fn zero_sized_terminals_fail_before_ffi() {
        assert!(matches!(
            ForegroundTerminal::new(0, 5, 0),
            Err(Error::InvalidSize { cols: 0, rows: 5 })
        ));
    }
}
