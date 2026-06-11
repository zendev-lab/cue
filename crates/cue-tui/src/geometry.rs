use ratatui::layout::{Constraint, Layout, Rect};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct UiRegions {
    pub(crate) header: Rect,
    pub(crate) input: Rect,
    pub(crate) main: Rect,
    pub(crate) display: Rect,
    pub(crate) results: Rect,
    pub(crate) results_inner: Rect,
    pub(crate) sidebar: Option<Rect>,
    pub(crate) sidebar_list: Option<Rect>,
}

pub(crate) fn layout_regions(
    terminal_width: u16,
    terminal_height: u16,
    input_desired_height: u16,
    sidebar_visible: bool,
) -> UiRegions {
    let area = Rect::new(0, 0, terminal_width, terminal_height);
    let input_height = input_desired_height.min(terminal_height.saturating_sub(5).max(1));
    let vertical = Layout::vertical([
        Constraint::Length(1),
        Constraint::Min(3),
        Constraint::Length(input_height),
        Constraint::Length(1),
    ])
    .split(area);

    let header = vertical[0];
    let body = vertical[1];
    let input = vertical[2];

    if sidebar_visible {
        let sidebar_width = (terminal_width / 4)
            .clamp(20, 40)
            .min(terminal_width.saturating_sub(30));
        let horizontal =
            Layout::horizontal([Constraint::Length(sidebar_width), Constraint::Min(30)])
                .split(body);
        let sidebar = horizontal[0];
        let main = horizontal[1];
        let panes = Layout::vertical([Constraint::Percentage(60), Constraint::Min(6)]).split(main);
        return UiRegions {
            header,
            input,
            main,
            display: panes[0],
            results: panes[1],
            results_inner: inner_rect(panes[1]),
            sidebar: Some(sidebar),
            sidebar_list: Some(inner_rect(sidebar)),
        };
    }

    let panes = Layout::vertical([Constraint::Percentage(60), Constraint::Min(6)]).split(body);
    UiRegions {
        header,
        input,
        main: body,
        display: panes[0],
        results: panes[1],
        results_inner: inner_rect(panes[1]),
        sidebar: None,
        sidebar_list: None,
    }
}

pub(crate) fn contains(area: Rect, point: Rect) -> bool {
    point.x >= area.x
        && point.x < area.x + area.width
        && point.y >= area.y
        && point.y < area.y + area.height
}

pub(crate) fn inner_rect(area: Rect) -> Rect {
    Rect::new(
        area.x.saturating_add(1),
        area.y.saturating_add(1),
        area.width.saturating_sub(2),
        area.height.saturating_sub(2),
    )
}

pub(crate) fn centered_rect(area: Rect, width_pct: u16, height_pct: u16) -> Rect {
    let vertical = Layout::vertical([
        Constraint::Percentage((100 - height_pct) / 2),
        Constraint::Percentage(height_pct),
        Constraint::Percentage((100 - height_pct) / 2),
    ])
    .split(area);
    let horizontal = Layout::horizontal([
        Constraint::Percentage((100 - width_pct) / 2),
        Constraint::Percentage(width_pct),
        Constraint::Percentage((100 - width_pct) / 2),
    ])
    .split(vertical[1]);
    horizontal[1]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn contains_uses_half_open_rect_bounds() {
        let area = Rect::new(2, 3, 4, 5);

        assert!(contains(area, Rect::new(2, 3, 1, 1)));
        assert!(contains(area, Rect::new(5, 7, 1, 1)));
        assert!(!contains(area, Rect::new(6, 7, 1, 1)));
        assert!(!contains(area, Rect::new(5, 8, 1, 1)));
    }

    #[test]
    fn inner_rect_saturates_tiny_areas() {
        assert_eq!(inner_rect(Rect::new(5, 7, 1, 1)), Rect::new(6, 8, 0, 0));
    }

    #[test]
    fn centered_rect_returns_expected_percentage_slice() {
        assert_eq!(
            centered_rect(Rect::new(0, 0, 100, 50), 70, 60),
            Rect::new(15, 10, 70, 30)
        );
    }

    #[test]
    fn layout_regions_splits_sidebar_and_workspace() {
        let regions = layout_regions(120, 40, 3, true);

        assert_eq!(regions.header, Rect::new(0, 0, 120, 1));
        assert_eq!(regions.input.height, 3);
        assert_eq!(regions.sidebar, Some(Rect::new(0, 1, 30, 35)));
        assert_eq!(regions.main, Rect::new(30, 1, 90, 35));
        assert_eq!(regions.sidebar_list, Some(Rect::new(1, 2, 28, 33)));
    }

    #[test]
    fn layout_regions_omits_sidebar_when_hidden() {
        let regions = layout_regions(80, 24, 2, false);

        assert_eq!(regions.main, Rect::new(0, 1, 80, 20));
        assert_eq!(regions.sidebar, None);
        assert_eq!(regions.sidebar_list, None);
    }
}
