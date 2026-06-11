use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FocusArea {
    Input,
    MainView,
    Sidebar,
}

pub(crate) fn is_mode_switch_key(key: KeyEvent) -> bool {
    matches!(key.code, KeyCode::BackTab)
        || (key.code == KeyCode::Tab && key.modifiers.contains(KeyModifiers::SHIFT))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mode_switch_key_accepts_shift_tab_forms() {
        assert!(is_mode_switch_key(KeyEvent::new(
            KeyCode::BackTab,
            KeyModifiers::SHIFT
        )));
        assert!(is_mode_switch_key(KeyEvent::new(
            KeyCode::Tab,
            KeyModifiers::SHIFT
        )));
        assert!(!is_mode_switch_key(KeyEvent::new(
            KeyCode::Tab,
            KeyModifiers::NONE
        )));
    }
}
