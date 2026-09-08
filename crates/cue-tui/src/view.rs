use ansi_to_tui::IntoText as _;
use cue_core::ExecutionState;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::{Block, Borders, Clear, List, ListItem, Paragraph};

use crate::workbench::{Focus, State, Tab, leaves};

fn border(title: impl Into<Line<'static>>, focused: bool) -> Block<'static> {
    Block::default()
        .borders(Borders::ALL)
        .title(title)
        .border_style(Style::default().fg(if focused {
            Color::Cyan
        } else {
            Color::DarkGray
        }))
}

pub(crate) fn rendered_text(text: &str) -> Text<'static> {
    text.as_bytes().into_text().unwrap_or_else(|_| {
        Text::from(
            text.chars()
                .filter(|ch| !ch.is_control() || matches!(ch, '\n' | '\t'))
                .collect::<String>(),
        )
    })
}

pub(crate) fn draw(frame: &mut ratatui::Frame<'_>, state: &mut State) {
    let area = frame.area();
    if area.width < 12 || area.height < 8 {
        frame.render_widget(Paragraph::new("Enlarge terminal\nCtrl-C quits"), area);
        return;
    }
    let input_height = (state.editor.text.lines().count().max(1) as u16 + 2)
        .min(6)
        .min(area.height.saturating_sub(6));
    let regions = Layout::vertical([
        Constraint::Length(1),
        Constraint::Min(3),
        Constraint::Length(input_height),
        Constraint::Length(1),
        Constraint::Length(1),
    ])
    .split(area);
    let running = state
        .executions
        .iter()
        .filter(|execution| !execution.state.is_terminal())
        .count();
    let connection = if state.connected {
        "connected"
    } else {
        "disconnected · reconnecting"
    };
    frame.render_widget(
        Paragraph::new(Line::from(vec![
            Span::styled(
                " CUE ",
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::raw(format!(
                "{} executions · {running} active · {connection}",
                state.executions.len()
            )),
        ])),
        regions[0],
    );

    let narrow_sidebar = area.width < 55 && state.focus == Focus::Sidebar;
    let show_sidebar = state.sidebar_visible && (area.width >= 55 || narrow_sidebar);
    let panels = Layout::horizontal([
        Constraint::Length(if show_sidebar {
            if narrow_sidebar {
                area.width
            } else {
                (area.width / 3).clamp(24, 38)
            }
        } else {
            0
        }),
        Constraint::Min(0),
    ])
    .split(regions[1]);
    state.sidebar_area = panels[0];
    if show_sidebar {
        let items = state
            .executions
            .iter()
            .map(|execution| {
                let color = match execution.state {
                    ExecutionState::Failed => Color::Red,
                    ExecutionState::Succeeded => Color::Green,
                    ExecutionState::Cancelled => Color::Yellow,
                    _ => Color::Cyan,
                };
                let description = leaves(execution)
                    .into_iter()
                    .map(|(label, _)| label)
                    .collect::<Vec<_>>()
                    .join(" → ");
                ListItem::new(vec![
                    Line::from(vec![
                        Span::styled(
                            execution.snapshot.id.to_string(),
                            Style::default().add_modifier(Modifier::BOLD),
                        ),
                        Span::styled(
                            format!("  {:?}", execution.state),
                            Style::default().fg(color),
                        ),
                    ]),
                    Line::from(description),
                ])
            })
            .collect::<Vec<_>>();
        let title = if state.next_page.is_some() {
            " Executions · more ↓ "
        } else {
            " Executions "
        };
        let list = List::new(items)
            .block(border(title, state.focus == Focus::Sidebar))
            .highlight_style(
                Style::default()
                    .bg(Color::DarkGray)
                    .add_modifier(Modifier::BOLD),
            )
            .highlight_symbol("› ");
        frame.render_stateful_widget(list, panels[0], &mut state.sidebar);
    }

    if !narrow_sidebar {
        let right = Layout::vertical([
            Constraint::Length(3),
            Constraint::Length(1),
            Constraint::Min(0),
        ])
        .split(panels[1]);
        let summary = state
            .selected()
            .map(|execution| {
                let label = leaves(execution)
                    .get(state.step_index)
                    .map(|(label, _)| label.clone())
                    .unwrap_or_default();
                format!(
                    "{}  {:?}  · step {}/{}\n{label}",
                    execution.snapshot.id,
                    execution.state,
                    state.step_index + 1,
                    execution.snapshot.steps.len()
                )
            })
            .unwrap_or_else(|| {
                "Your execution workspace\nSubmit a command to see its output here.".into()
            });
        frame.render_widget(Paragraph::new(summary), right[0]);
        state.tabs_area = right[1];
        state.tab_hits.clear();
        let compact = right[1].width < 66;
        let short = ["All", "Out", "Err", "TTY", "Info", "Log"];
        let mut x = right[1].x;
        let mut tabs = Vec::new();
        for (index, tab) in Tab::ALL.iter().enumerate() {
            if right[1].width < 42 && *tab != state.tab {
                continue;
            }
            let label = if compact { short[index] } else { tab.label() };
            let text = format!(" {}:{} ", index + 1, label);
            let width = text.len() as u16;
            state.tab_hits.push((
                *tab,
                Rect::new(
                    x,
                    right[1].y,
                    width.min(right[1].right().saturating_sub(x)),
                    1,
                ),
            ));
            x = x.saturating_add(width);
            tabs.push(Span::styled(
                text,
                if *tab == state.tab {
                    Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD | Modifier::UNDERLINED)
                } else {
                    Style::default()
                },
            ));
        }
        if right[1].width < 42 {
            tabs.push(Span::raw(" 1–6 switch tabs"));
        }
        frame.render_widget(Paragraph::new(Line::from(tabs)), right[1]);
        state.output_area = right[2];
        let text = rendered_text(&state.output_text());
        let height = right[2].height.saturating_sub(2) as usize;
        let maximum = text.lines.len().saturating_sub(height);
        state.scroll = if state.follow {
            maximum
        } else {
            state.scroll.min(maximum)
        };
        let status = if state.follow {
            "following"
        } else {
            "scroll paused"
        };
        let title = format!(
            " {} · {} · {status} ",
            state
                .step()
                .map_or_else(|| "Execution".into(), |step| step.to_string()),
            state.tab.label()
        );
        let visible = Text::from(
            text.lines
                .into_iter()
                .skip(state.scroll)
                .take(height)
                .collect::<Vec<_>>(),
        );
        frame.render_widget(
            Paragraph::new(visible)
                .scroll((0, state.horizontal))
                .block(border(title, state.focus == Focus::Output)),
            right[2],
        );
    }

    state.input_area = regions[2];
    let input_block = border(
        " Command · Enter run · Tab complete ",
        state.focus == Focus::Input,
    );
    let inner = input_block.inner(regions[2]);
    let cursor = state.editor.cursor.min(state.editor.text.len());
    let before = &state.editor.text[..cursor];
    let row = before.bytes().filter(|byte| *byte == b'\n').count() as u16;
    let column = Line::from(before.rsplit('\n').next().unwrap_or(""))
        .width()
        .min(u16::MAX as usize) as u16;
    let top = row.saturating_sub(inner.height.saturating_sub(1));
    let left = column.saturating_sub(inner.width.saturating_sub(1));
    frame.render_widget(
        Paragraph::new(state.editor.text.as_str())
            .scroll((top, left))
            .block(input_block),
        regions[2],
    );
    if state.focus == Focus::Input && !state.help && inner.width > 0 && inner.height > 0 {
        frame.set_cursor_position((
            inner.x + column.saturating_sub(left),
            inner.y + row.saturating_sub(top),
        ));
    }
    let notice = if state.editor.candidates.is_empty() {
        state.notice.clone()
    } else {
        state
            .editor
            .candidates
            .iter()
            .take(8)
            .cloned()
            .collect::<Vec<_>>()
            .join("  ")
    };
    frame.render_widget(
        Paragraph::new(notice).style(Style::default().fg(if state.connected {
            Color::Gray
        } else {
            Color::Yellow
        })),
        regions[3],
    );
    let footer = match state.focus {
        Focus::Input => "F2 tasks  F3 output  ↑/↓ history  F1 help  Ctrl-C quit",
        Focus::Sidebar => "↑/↓ select  Enter output  Del cancel  f attach  Ctrl-B sidebar",
        Focus::Output => "1–6 tabs  [/] steps  PgUp/PgDn scroll  End follow  Ctrl-Y copy  F4 input",
    };
    frame.render_widget(
        Paragraph::new(footer).style(Style::default().fg(Color::Cyan)),
        regions[4],
    );
    if state.help {
        draw_help(frame);
    }
}

fn draw_help(frame: &mut ratatui::Frame<'_>) {
    let area = frame.area();
    let width = area.width.saturating_sub(4).min(78);
    let height = area.height.saturating_sub(2).min(24);
    let dialog = Rect::new(
        (area.width - width) / 2,
        (area.height - height) / 2,
        width,
        height,
    );
    frame.render_widget(Clear, dialog);
    frame.render_widget(Paragraph::new(
        "Browse\n  F2 tasks · ↑/↓ select · Enter open · Ctrl-B toggle sidebar\n  F3 output · F4 command · Shift-Tab changes focus\n  Click a task or tab; mouse wheel scrolls the panel\n\nOutput\n  1–6 Output / Stdout / Stderr / Terminal / Details / Activity\n  [ / ] previous / next step · PgUp/PgDn scroll\n  Home beginning · End follow live · ←/→ horizontal scroll\n  Ctrl-Y copies the active view (terminal OSC 52 support)\n\nActions (tasks or output focused)\n  Del cancels selected execution · K force-cancels\n  f attaches selected PTY · o observes · Ctrl-] returns\n\nCommand\n  Tab completes · ↑/↓ recalls history · ←/→ edits\n  Home/End · Ctrl-A/E · Ctrl-U/K · Shift-Enter newline\n  Paste inserts text; Enter submits · F5 refresh\n\n  :executions · :log E1 · :out E1/S1 · :err E1/S1 · :cancel E1\n  Esc closes help / returns to command · Ctrl-C quits")
        .block(border(" Keyboard help · Esc closes ", true)), dialog);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compact_and_unicode_layouts_keep_input_and_tabs_accessible() {
        for (width, height) in [(80, 24), (55, 12), (40, 10), (12, 8), (8, 4)] {
            let backend = ratatui::backend::TestBackend::new(width, height);
            let mut terminal = ratatui::Terminal::new(backend).unwrap();
            let mut state = State::default();
            state.editor.insert("中文🙂\nsecond line");
            terminal.draw(|frame| draw(frame, &mut state)).unwrap();
            if width >= 80 {
                assert_eq!(state.tab_hits.len(), 6);
                assert!(state.tab_hits.iter().all(|(_, rect)| rect.right() <= width));
            }
            state.focus = Focus::Sidebar;
            terminal.draw(|frame| draw(frame, &mut state)).unwrap();
        }
    }
}
