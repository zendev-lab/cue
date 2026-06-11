#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MouseMode {
    TextSelect,
    UiCapture,
}

impl MouseMode {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::TextSelect => "text",
            Self::UiCapture => "ui",
        }
    }

    pub(crate) fn capture_enabled(self) -> bool {
        matches!(self, Self::UiCapture)
    }

    pub(crate) fn toggle(self) -> Self {
        match self {
            Self::TextSelect => Self::UiCapture,
            Self::UiCapture => Self::TextSelect,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn labels_match_terminal_mouse_mode() {
        assert_eq!(MouseMode::TextSelect.label(), "text");
        assert_eq!(MouseMode::UiCapture.label(), "ui");
    }

    #[test]
    fn capture_enabled_only_for_ui_mode() {
        assert!(!MouseMode::TextSelect.capture_enabled());
        assert!(MouseMode::UiCapture.capture_enabled());
    }

    #[test]
    fn toggle_switches_between_modes() {
        assert_eq!(MouseMode::TextSelect.toggle(), MouseMode::UiCapture);
        assert_eq!(MouseMode::UiCapture.toggle(), MouseMode::TextSelect);
    }
}
