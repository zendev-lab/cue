use ratatui::layout::Rect;

const EMPTY_DISPLAY_MESSAGE: &str =
    "Select an execution, schedule, or command card to inspect its details.";

#[derive(Debug, Clone)]
struct DisplayTab {
    key: String,
    title: String,
    content: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DisplayCopyTarget {
    pub(crate) label: String,
    pub(crate) content: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DisplayPreview {
    pub(crate) key: String,
    pub(crate) title: String,
    pub(crate) content: String,
}

impl DisplayPreview {
    pub(crate) fn new(
        key: impl Into<String>,
        title: impl Into<String>,
        content: impl Into<String>,
    ) -> Self {
        Self {
            key: key.into(),
            title: title.into(),
            content: content.into(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DisplayTabHit {
    Activate(usize),
    Close(usize),
}

#[derive(Debug, Default, Clone)]
pub(crate) struct DisplayPane {
    tabs: Vec<DisplayTab>,
    active: Option<usize>,
}

impl DisplayPane {
    pub(crate) fn content(&self) -> &str {
        self.active_tab()
            .map(|tab| tab.content.as_str())
            .unwrap_or(EMPTY_DISPLAY_MESSAGE)
    }

    pub(crate) fn has_target(&self) -> bool {
        self.active.is_some()
    }

    pub(crate) fn labels(&self) -> Vec<String> {
        self.tabs
            .iter()
            .map(|tab| format!(" {}  × ", tab.title))
            .collect()
    }

    pub(crate) fn active(&self) -> Option<usize> {
        self.active
    }

    pub(crate) fn copy_target(&self) -> Option<DisplayCopyTarget> {
        let tab = self.active_tab()?;
        Some(DisplayCopyTarget {
            label: tab.title.clone(),
            content: tab.content.clone(),
        })
    }

    pub(crate) fn open_preview(&mut self, preview: DisplayPreview) {
        if let Some(index) = self
            .tabs
            .iter()
            .position(|tab| tab.key == preview.key && tab.title == preview.title)
        {
            self.tabs[index].content = preview.content;
            self.active = Some(index);
            return;
        }

        self.tabs.push(DisplayTab {
            key: preview.key,
            title: preview.title,
            content: preview.content,
        });
        self.active = Some(self.tabs.len() - 1);
    }

    pub(crate) fn clear(&mut self) {
        self.tabs.clear();
        self.active = None;
    }

    pub(crate) fn activate(&mut self, index: usize) {
        if index < self.tabs.len() {
            self.active = Some(index);
        }
    }

    pub(crate) fn close(&mut self, index: usize) -> bool {
        if self.tabs.get(index).is_none() {
            return false;
        }
        self.tabs.remove(index);
        self.active = match self.tabs.is_empty() {
            true => None,
            false if index >= self.tabs.len() => Some(self.tabs.len() - 1),
            false => Some(index),
        };
        true
    }

    pub(crate) fn hit(&self, display_area: Rect, point: Rect) -> Option<DisplayTabHit> {
        let tab_bar = self.tab_bar_rect(display_area)?;
        if !contains(tab_bar, point) {
            return None;
        }

        let mut x = tab_bar.x;
        for (index, label) in self.labels().into_iter().enumerate() {
            let width = label.chars().count() as u16;
            let start = x;
            let close_x = start + width.saturating_sub(3);
            let end = start + width;
            if point.x >= start && point.x < end {
                return if point.x >= close_x {
                    Some(DisplayTabHit::Close(index))
                } else {
                    Some(DisplayTabHit::Activate(index))
                };
            }
            x = end;
        }
        None
    }

    fn active_tab(&self) -> Option<&DisplayTab> {
        self.active.and_then(|index| self.tabs.get(index))
    }

    fn tab_bar_rect(&self, display_area: Rect) -> Option<Rect> {
        if self.tabs.is_empty() || display_area.width <= 2 || display_area.height <= 2 {
            return None;
        }
        Some(Rect::new(
            display_area.x + 1,
            display_area.y + 1,
            display_area.width.saturating_sub(2),
            1,
        ))
    }
}

fn contains(area: Rect, point: Rect) -> bool {
    point.x >= area.x
        && point.x < area.x + area.width
        && point.y >= area.y
        && point.y < area.y + area.height
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preview_tab_is_reused_by_key_and_title() {
        let mut pane = DisplayPane::default();
        pane.open_preview(DisplayPreview::new("card:1", "record", "old"));
        pane.open_preview(DisplayPreview::new("card:1", "record", "new"));

        assert_eq!(pane.labels(), vec![" record  × ".to_string()]);
        assert_eq!(pane.content(), "new");
    }

    #[test]
    fn closing_active_tab_selects_next_available_tab() {
        let mut pane = DisplayPane::default();
        pane.open_preview(DisplayPreview::new("one", "first", "one"));
        pane.open_preview(DisplayPreview::new("two", "second", "two"));
        pane.activate(0);

        assert!(pane.close(0));

        assert_eq!(pane.active(), Some(0));
        assert_eq!(pane.content(), "two");
    }

    #[test]
    fn tab_hit_distinguishes_activation_and_close_region() {
        let mut pane = DisplayPane::default();
        pane.open_preview(DisplayPreview::new("one", "first", "one"));
        let area = Rect::new(0, 0, 30, 5);

        assert_eq!(
            pane.hit(area, Rect::new(2, 1, 1, 1)),
            Some(DisplayTabHit::Activate(0))
        );
        assert_eq!(
            pane.hit(area, Rect::new(9, 1, 1, 1)),
            Some(DisplayTabHit::Close(0))
        );
    }
}
