use cue_core::Mode;

use crate::focus::FocusArea;
use crate::target_settings::target_settings_footer_text;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FooterContext {
    JobPicker {
        mode: Mode,
    },
    TargetSettings {
        can_save: bool,
        has_pending_reconnect: bool,
    },
    Main {
        focus: FocusArea,
        mode: Mode,
        display_has_target: bool,
    },
}

pub(crate) fn footer_text(context: FooterContext) -> String {
    match context {
        FooterContext::JobPicker { mode } => match mode {
            Mode::Job => "Kill picker: Enter kill  •  Esc close".to_string(),
            Mode::Cron => "Remove picker: Enter remove  •  Esc close".to_string(),
        },
        FooterContext::TargetSettings {
            can_save,
            has_pending_reconnect,
        } => target_settings_footer_text(can_save, has_pending_reconnect),
        FooterContext::Main {
            focus,
            mode,
            display_has_target,
        } => main_footer_text(focus, mode, display_has_target).to_string(),
    }
}

fn main_footer_text(focus: FocusArea, mode: Mode, display_has_target: bool) -> &'static str {
    match focus {
        FocusArea::Input => match mode {
            Mode::Job => {
                "JOB: Enter submit  •  Shift+Enter newline  •  Tab complete  •  Shift+Tab mode"
            }
            Mode::Cron => {
                "CRON: Enter schedule + command  •  Shift+Enter newline  •  Tab complete  •  Shift+Tab mode"
            }
        },
        FocusArea::Sidebar => {
            "Sidebar: Click row to open  •  Del/Backspace kill or remove  •  Up/Down move  •  Enter open  •  Shift+Tab mode  •  Ctrl+B toggle"
        }
        FocusArea::MainView if display_has_target => {
            "Display: Click tab to switch  •  × closes tab  •  Ctrl+Y copy active tab  •  Shift+Tab mode  •  Ctrl+L clears when idle"
        }
        FocusArea::MainView => {
            "Command log: Click cards to inspect  •  Ctrl+Y copy latest record  •  Shift+Tab mode  •  :out/:err snapshot  •  :tail follows live output"
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn picker_footer_matches_mode_action() {
        assert!(footer_text(FooterContext::JobPicker { mode: Mode::Job }).contains("Kill picker"));
        assert!(
            footer_text(FooterContext::JobPicker { mode: Mode::Cron }).contains("Remove picker")
        );
    }

    #[test]
    fn main_footer_distinguishes_input_modes() {
        assert!(
            footer_text(FooterContext::Main {
                focus: FocusArea::Input,
                mode: Mode::Job,
                display_has_target: false,
            })
            .starts_with("JOB:")
        );
        assert!(
            footer_text(FooterContext::Main {
                focus: FocusArea::Input,
                mode: Mode::Cron,
                display_has_target: false,
            })
            .starts_with("CRON:")
        );
    }

    #[test]
    fn main_footer_distinguishes_display_and_command_log() {
        assert!(
            footer_text(FooterContext::Main {
                focus: FocusArea::MainView,
                mode: Mode::Job,
                display_has_target: true,
            })
            .contains("Display:")
        );
        assert!(
            footer_text(FooterContext::Main {
                focus: FocusArea::MainView,
                mode: Mode::Job,
                display_has_target: false,
            })
            .contains("Command log:")
        );
    }

    #[test]
    fn target_settings_footer_delegates_action_hints() {
        let text = footer_text(FooterContext::TargetSettings {
            can_save: true,
            has_pending_reconnect: true,
        });

        assert!(text.contains("Enter save default"));
        assert!(text.contains("R reconnect now"));
    }
}
