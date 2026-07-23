// Ratatui projection adapted from ratatui-ghostty and turborepo-ghostty.
// Both sources are MIT licensed; see ../THIRD_PARTY_NOTICES.md.

use libghostty_vt::{
    RenderState, Terminal,
    render::{CellIterator, CursorViewport, CursorVisualStyle, Dirty, RowIterator, Snapshot},
    style::{RgbColor, Style as GhosttyStyle, StyleColor, Underline},
};
use ratatui::{
    buffer::Buffer,
    layout::{Position, Rect},
    style::{Color, Modifier, Style},
};

use crate::Result;

/// Cursor information extracted from the latest terminal frame.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CursorState {
    /// Absolute buffer position, or `None` when the cursor should not be drawn.
    pub position: Option<Position>,
    pub style: CursorStyle,
    pub blinking: bool,
}

/// Visual shape requested by the foreground program.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum CursorStyle {
    #[default]
    Block,
    HollowBlock,
    Bar,
    Underline,
}

pub(crate) fn render_into(
    terminal: &Terminal<'static, 'static>,
    render_state: &mut RenderState<'static>,
    row_iterator: &mut RowIterator<'static>,
    cell_iterator: &mut CellIterator<'static>,
    area: Rect,
    buffer: &mut Buffer,
    focused: bool,
) -> Result<CursorState> {
    clear_area(area, buffer);
    let snapshot = render_state.update(terminal)?;
    let cursor = cursor_state(&snapshot, area, *buffer.area(), focused)?;
    let colors = snapshot.colors()?;
    {
        let mut rows = row_iterator.update(&snapshot)?;
        let mut row_index = 0_u16;
        let mut grapheme = String::new();

        while let Some(row) = rows.next() {
            if row_index >= area.height {
                break;
            }
            let selection = row.selection()?;
            {
                let mut cells = cell_iterator.update(row)?;
                let mut column_index = 0_u16;
                while let Some(cell) = cells.next() {
                    if column_index >= area.width {
                        break;
                    }

                    grapheme.clear();
                    if cell.graphemes_len()? == 0 {
                        grapheme.push(' ');
                    } else {
                        cell.graphemes_utf8(&mut grapheme)?;
                    }

                    let ghostty_style = cell.style()?;
                    let mut style = to_ratatui_style(&ghostty_style, &colors.palette);
                    if let Some(color) = cell.fg_color()? {
                        style = style.fg(rgb(color));
                    }
                    if let Some(color) = cell.bg_color()? {
                        style = style.bg(rgb(color));
                    }
                    if selection.is_some_and(|range| {
                        column_index >= range.start_x && column_index <= range.end_x
                    }) {
                        style = style.add_modifier(Modifier::REVERSED);
                    }

                    let x = area.x.saturating_add(column_index);
                    let y = area.y.saturating_add(row_index);
                    if buffer.area().contains(Position::new(x, y)) {
                        buffer[(x, y)].set_symbol(&grapheme).set_style(style);
                    }
                    column_index = column_index.saturating_add(1);
                }
            }
            row.set_dirty(false)?;
            row_index = row_index.saturating_add(1);
        }
    }

    snapshot.set_dirty(Dirty::Clean)?;
    Ok(cursor)
}

fn clear_area(area: Rect, buffer: &mut Buffer) {
    let clipped = area.intersection(*buffer.area());
    for y in clipped.top()..clipped.bottom() {
        for x in clipped.left()..clipped.right() {
            buffer[(x, y)].reset();
        }
    }
}

fn cursor_state(
    snapshot: &Snapshot<'_, '_>,
    area: Rect,
    buffer_area: Rect,
    focused: bool,
) -> Result<CursorState> {
    if !focused || !snapshot.cursor_visible()? {
        return Ok(CursorState::default());
    }

    let Some(CursorViewport {
        x,
        y,
        at_wide_tail: false,
    }) = snapshot.cursor_viewport()?
    else {
        return Ok(CursorState::default());
    };
    if x >= area.width || y >= area.height {
        return Ok(CursorState::default());
    }

    let position = Position::new(area.x.saturating_add(x), area.y.saturating_add(y));
    if !buffer_area.contains(position) {
        return Ok(CursorState::default());
    }

    let style = match snapshot.cursor_visual_style()? {
        CursorVisualStyle::Bar => CursorStyle::Bar,
        CursorVisualStyle::Block => CursorStyle::Block,
        CursorVisualStyle::Underline => CursorStyle::Underline,
        CursorVisualStyle::BlockHollow => CursorStyle::HollowBlock,
        _ => CursorStyle::Block,
    };
    Ok(CursorState {
        position: Some(position),
        style,
        blinking: snapshot.cursor_blinking()?,
    })
}

fn to_ratatui_style(style: &GhosttyStyle, palette: &[RgbColor; 256]) -> Style {
    let mut result = Style::default();
    if let Some(color) = resolve_color(style.fg_color, palette) {
        result = result.fg(color);
    }
    if let Some(color) = resolve_color(style.bg_color, palette) {
        result = result.bg(color);
    }
    if let Some(color) = resolve_color(style.underline_color, palette) {
        result = result.underline_color(color);
    }

    let mut modifiers = Modifier::empty();
    if style.bold {
        modifiers |= Modifier::BOLD;
    }
    if style.italic {
        modifiers |= Modifier::ITALIC;
    }
    if style.faint {
        modifiers |= Modifier::DIM;
    }
    if style.blink {
        modifiers |= Modifier::SLOW_BLINK;
    }
    if style.inverse {
        modifiers |= Modifier::REVERSED;
    }
    if style.invisible {
        modifiers |= Modifier::HIDDEN;
    }
    if style.strikethrough {
        modifiers |= Modifier::CROSSED_OUT;
    }
    if !matches!(style.underline, Underline::None) {
        modifiers |= Modifier::UNDERLINED;
    }
    result.add_modifier(modifiers)
}

fn resolve_color(color: StyleColor, palette: &[RgbColor; 256]) -> Option<Color> {
    match color {
        StyleColor::None => None,
        StyleColor::Rgb(color) => Some(rgb(color)),
        StyleColor::Palette(index) => Some(rgb(palette[usize::from(index.0)])),
    }
}

fn rgb(color: RgbColor) -> Color {
    Color::Rgb(color.r, color.g, color.b)
}
