//! Convert ratatui buffers into plain text and ANSI snapshots.

use ratatui::buffer::Buffer;
use ratatui::style::{Color, Modifier};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FrameText {
    pub width: u16,
    pub height: u16,
    pub plain: String,
    pub styled: String,
}

pub(crate) fn frame_text_from_buffer(buffer: &Buffer) -> FrameText {
    let width = buffer.area.width;
    let height = buffer.area.height;
    let mut plain_lines = Vec::with_capacity(height as usize);
    let mut styled_lines = Vec::with_capacity(height as usize);

    for y in 0..height {
        let mut plain = String::new();
        let mut styled = String::new();
        for x in 0..width {
            let cell = &buffer[(x, y)];
            let symbol = cell.symbol();
            plain.push_str(symbol);
            let styled_cell = style_prefix(cell.modifier, cell.fg, cell.bg);
            styled.push_str(&styled_cell);
            styled.push_str(symbol);
            if !styled_cell.is_empty() {
                styled.push_str("\x1b[0m");
            }
        }
        plain_lines.push(trim_trailing_spaces(&plain));
        styled_lines.push(trim_trailing_spaces(&styled));
    }

    FrameText {
        width,
        height,
        plain: plain_lines.join("\n"),
        styled: styled_lines.join("\n"),
    }
}

fn trim_trailing_spaces(line: &str) -> String {
    line.trim_end_matches(' ').to_string()
}

fn style_prefix(modifier: Modifier, fg: Color, bg: Color) -> String {
    let mut codes = Vec::new();
    if modifier.contains(Modifier::BOLD) {
        codes.push("1");
    }
    if modifier.contains(Modifier::DIM) {
        codes.push("2");
    }
    if modifier.contains(Modifier::ITALIC) {
        codes.push("3");
    }
    if modifier.contains(Modifier::UNDERLINED) {
        codes.push("4");
    }
    if fg != Color::Reset
        && let Some(code) = fg_code(fg)
    {
        codes.push(code);
    }
    if bg != Color::Reset
        && let Some(code) = bg_code(bg)
    {
        codes.push(code);
    }
    if codes.is_empty() {
        String::new()
    } else {
        format!("\x1b[{}m", codes.join(";"))
    }
}

fn fg_code(color: Color) -> Option<&'static str> {
    match color {
        Color::Black => Some("30"),
        Color::Red => Some("31"),
        Color::Green => Some("32"),
        Color::Yellow => Some("33"),
        Color::Blue => Some("34"),
        Color::Magenta => Some("35"),
        Color::Cyan => Some("36"),
        Color::White => Some("37"),
        Color::Gray => Some("90"),
        Color::DarkGray => Some("90"),
        Color::LightRed => Some("91"),
        Color::LightGreen => Some("92"),
        Color::LightYellow => Some("93"),
        Color::LightBlue => Some("94"),
        Color::LightMagenta => Some("95"),
        Color::LightCyan => Some("96"),
        Color::Indexed(_) | Color::Rgb(_, _, _) => None,
        _ => None,
    }
}

fn bg_code(color: Color) -> Option<&'static str> {
    match color {
        Color::Black => Some("40"),
        Color::Red => Some("41"),
        Color::Green => Some("42"),
        Color::Yellow => Some("43"),
        Color::Blue => Some("44"),
        Color::Magenta => Some("45"),
        Color::Cyan => Some("46"),
        Color::White => Some("47"),
        Color::Gray => Some("100"),
        Color::DarkGray => Some("100"),
        Color::LightRed => Some("101"),
        Color::LightGreen => Some("102"),
        Color::LightYellow => Some("103"),
        Color::LightBlue => Some("104"),
        Color::LightMagenta => Some("105"),
        Color::LightCyan => Some("106"),
        Color::Indexed(_) | Color::Rgb(_, _, _) => None,
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use ratatui::buffer::Buffer;
    use ratatui::layout::Rect;
    use ratatui::prelude::Widget;
    use ratatui::style::{Color, Style};
    use ratatui::text::{Line, Span};
    use ratatui::widgets::Paragraph;

    use super::*;

    #[test]
    fn buffer_to_plain_text_joins_rows() {
        let area = Rect::new(0, 0, 5, 2);
        let mut buffer = Buffer::empty(area);
        Paragraph::new("hello").render(area, &mut buffer);
        Paragraph::new("world").render(Rect::new(0, 1, 5, 1), &mut buffer);

        let frame = frame_text_from_buffer(&buffer);
        assert_eq!(frame.width, 5);
        assert_eq!(frame.height, 2);
        assert_eq!(frame.plain, "hello\nworld");
    }

    #[test]
    fn buffer_to_styled_text_includes_ansi_prefix() {
        let area = Rect::new(0, 0, 3, 1);
        let mut buffer = Buffer::empty(area);
        Paragraph::new(Line::from(Span::styled(
            "ok",
            Style::default().fg(Color::Cyan),
        )))
        .render(area, &mut buffer);

        let frame = frame_text_from_buffer(&buffer);
        assert!(frame.styled.contains("\x1b[36m"), "{:?}", frame.styled);
        assert_eq!(frame.plain.trim(), "ok");
    }
}
