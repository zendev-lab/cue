use crossterm::event::{
    KeyCode, KeyEvent, KeyEventKind, KeyEventState, KeyModifiers, MediaKeyCode, ModifierKeyCode,
    MouseButton, MouseEvent, MouseEventKind,
};
use libghostty_vt::{key, mouse};
use ratatui::layout::{Position, Rect};

use crate::{Error, Result};

pub(crate) fn key_event(input: KeyEvent) -> Result<key::Event<'static>> {
    let mut event = key::Event::new()?;
    let mut modifiers = key_modifiers(input.modifiers);
    if input.state.contains(KeyEventState::CAPS_LOCK) {
        modifiers |= key::Mods::CAPS_LOCK;
    }
    if input.state.contains(KeyEventState::NUM_LOCK) {
        modifiers |= key::Mods::NUM_LOCK;
    }
    if matches!(input.code, KeyCode::BackTab) {
        modifiers |= key::Mods::SHIFT;
    }
    modifiers |= modifier_side(input.code);

    event
        .set_action(match input.kind {
            KeyEventKind::Press => key::Action::Press,
            KeyEventKind::Repeat => key::Action::Repeat,
            KeyEventKind::Release => key::Action::Release,
        })
        .set_key(key_code(input.code))
        .set_mods(modifiers);

    if let KeyCode::Char(ch) = input.code {
        event
            .set_utf8(Some(ch.to_string()))
            .set_unshifted_codepoint(unshifted_char(ch));
    }

    Ok(event)
}

fn key_code(code: KeyCode) -> key::Key {
    match code {
        KeyCode::Backspace => key::Key::Backspace,
        KeyCode::Enter => key::Key::Enter,
        KeyCode::Left => key::Key::ArrowLeft,
        KeyCode::Right => key::Key::ArrowRight,
        KeyCode::Up => key::Key::ArrowUp,
        KeyCode::Down => key::Key::ArrowDown,
        KeyCode::Home => key::Key::Home,
        KeyCode::End => key::Key::End,
        KeyCode::PageUp => key::Key::PageUp,
        KeyCode::PageDown => key::Key::PageDown,
        KeyCode::Tab | KeyCode::BackTab => key::Key::Tab,
        KeyCode::Delete => key::Key::Delete,
        KeyCode::Insert => key::Key::Insert,
        KeyCode::Esc => key::Key::Escape,
        KeyCode::CapsLock => key::Key::CapsLock,
        KeyCode::ScrollLock => key::Key::ScrollLock,
        KeyCode::NumLock => key::Key::NumLock,
        KeyCode::PrintScreen => key::Key::PrintScreen,
        KeyCode::Pause => key::Key::Pause,
        KeyCode::Menu => key::Key::ContextMenu,
        KeyCode::KeypadBegin => key::Key::NumpadBegin,
        KeyCode::Null => key::Key::Unidentified,
        KeyCode::F(number) => function_key(number),
        KeyCode::Char(ch) => character_key(ch),
        KeyCode::Media(code) => media_key(code),
        KeyCode::Modifier(code) => modifier_key(code),
    }
}

fn function_key(number: u8) -> key::Key {
    match number {
        1 => key::Key::F1,
        2 => key::Key::F2,
        3 => key::Key::F3,
        4 => key::Key::F4,
        5 => key::Key::F5,
        6 => key::Key::F6,
        7 => key::Key::F7,
        8 => key::Key::F8,
        9 => key::Key::F9,
        10 => key::Key::F10,
        11 => key::Key::F11,
        12 => key::Key::F12,
        13 => key::Key::F13,
        14 => key::Key::F14,
        15 => key::Key::F15,
        16 => key::Key::F16,
        17 => key::Key::F17,
        18 => key::Key::F18,
        19 => key::Key::F19,
        20 => key::Key::F20,
        21 => key::Key::F21,
        22 => key::Key::F22,
        23 => key::Key::F23,
        24 => key::Key::F24,
        25 => key::Key::F25,
        _ => key::Key::Unidentified,
    }
}

fn character_key(ch: char) -> key::Key {
    match ch.to_ascii_lowercase() {
        'a' => key::Key::A,
        'b' => key::Key::B,
        'c' => key::Key::C,
        'd' => key::Key::D,
        'e' => key::Key::E,
        'f' => key::Key::F,
        'g' => key::Key::G,
        'h' => key::Key::H,
        'i' => key::Key::I,
        'j' => key::Key::J,
        'k' => key::Key::K,
        'l' => key::Key::L,
        'm' => key::Key::M,
        'n' => key::Key::N,
        'o' => key::Key::O,
        'p' => key::Key::P,
        'q' => key::Key::Q,
        'r' => key::Key::R,
        's' => key::Key::S,
        't' => key::Key::T,
        'u' => key::Key::U,
        'v' => key::Key::V,
        'w' => key::Key::W,
        'x' => key::Key::X,
        'y' => key::Key::Y,
        'z' => key::Key::Z,
        '0' | ')' => key::Key::Digit0,
        '1' | '!' => key::Key::Digit1,
        '2' | '@' => key::Key::Digit2,
        '3' | '#' => key::Key::Digit3,
        '4' | '$' => key::Key::Digit4,
        '5' | '%' => key::Key::Digit5,
        '6' | '^' => key::Key::Digit6,
        '7' | '&' => key::Key::Digit7,
        '8' | '*' => key::Key::Digit8,
        '9' | '(' => key::Key::Digit9,
        ' ' => key::Key::Space,
        '`' | '~' => key::Key::Backquote,
        '\\' | '|' => key::Key::Backslash,
        '[' | '{' => key::Key::BracketLeft,
        ']' | '}' => key::Key::BracketRight,
        ',' | '<' => key::Key::Comma,
        '=' | '+' => key::Key::Equal,
        '-' | '_' => key::Key::Minus,
        '.' | '>' => key::Key::Period,
        '\'' | '"' => key::Key::Quote,
        ';' | ':' => key::Key::Semicolon,
        '/' | '?' => key::Key::Slash,
        _ => key::Key::Unidentified,
    }
}

fn unshifted_char(ch: char) -> char {
    match ch {
        'A'..='Z' => ch.to_ascii_lowercase(),
        ')' => '0',
        '!' => '1',
        '@' => '2',
        '#' => '3',
        '$' => '4',
        '%' => '5',
        '^' => '6',
        '&' => '7',
        '*' => '8',
        '(' => '9',
        '~' => '`',
        '|' => '\\',
        '{' => '[',
        '}' => ']',
        '<' => ',',
        '+' => '=',
        '_' => '-',
        '>' => '.',
        '"' => '\'',
        ':' => ';',
        '?' => '/',
        _ => ch,
    }
}

fn media_key(code: MediaKeyCode) -> key::Key {
    match code {
        MediaKeyCode::Play | MediaKeyCode::Pause | MediaKeyCode::PlayPause => {
            key::Key::MediaPlayPause
        }
        MediaKeyCode::Stop => key::Key::MediaStop,
        MediaKeyCode::TrackNext => key::Key::MediaTrackNext,
        MediaKeyCode::TrackPrevious => key::Key::MediaTrackPrevious,
        MediaKeyCode::LowerVolume => key::Key::AudioVolumeDown,
        MediaKeyCode::RaiseVolume => key::Key::AudioVolumeUp,
        MediaKeyCode::MuteVolume => key::Key::AudioVolumeMute,
        MediaKeyCode::Reverse
        | MediaKeyCode::FastForward
        | MediaKeyCode::Rewind
        | MediaKeyCode::Record => key::Key::Unidentified,
    }
}

fn modifier_key(code: ModifierKeyCode) -> key::Key {
    match code {
        ModifierKeyCode::LeftShift => key::Key::ShiftLeft,
        ModifierKeyCode::RightShift => key::Key::ShiftRight,
        ModifierKeyCode::LeftControl => key::Key::ControlLeft,
        ModifierKeyCode::RightControl => key::Key::ControlRight,
        ModifierKeyCode::LeftAlt => key::Key::AltLeft,
        ModifierKeyCode::RightAlt => key::Key::AltRight,
        ModifierKeyCode::LeftSuper | ModifierKeyCode::LeftMeta => key::Key::MetaLeft,
        ModifierKeyCode::RightSuper | ModifierKeyCode::RightMeta => key::Key::MetaRight,
        ModifierKeyCode::LeftHyper
        | ModifierKeyCode::RightHyper
        | ModifierKeyCode::IsoLevel3Shift
        | ModifierKeyCode::IsoLevel5Shift => key::Key::Unidentified,
    }
}

fn key_modifiers(modifiers: KeyModifiers) -> key::Mods {
    let mut result = key::Mods::empty();
    if modifiers.contains(KeyModifiers::SHIFT) {
        result |= key::Mods::SHIFT;
    }
    if modifiers.contains(KeyModifiers::CONTROL) {
        result |= key::Mods::CTRL;
    }
    if modifiers.contains(KeyModifiers::ALT) {
        result |= key::Mods::ALT;
    }
    if modifiers.contains(KeyModifiers::SUPER) {
        result |= key::Mods::SUPER;
    }
    result
}

fn modifier_side(code: KeyCode) -> key::Mods {
    match code {
        KeyCode::Modifier(ModifierKeyCode::RightShift) => key::Mods::SHIFT | key::Mods::SHIFT_SIDE,
        KeyCode::Modifier(ModifierKeyCode::RightControl) => key::Mods::CTRL | key::Mods::CTRL_SIDE,
        KeyCode::Modifier(ModifierKeyCode::RightAlt) => key::Mods::ALT | key::Mods::ALT_SIDE,
        KeyCode::Modifier(ModifierKeyCode::RightSuper | ModifierKeyCode::RightMeta) => {
            key::Mods::SUPER | key::Mods::SUPER_SIDE
        }
        KeyCode::Modifier(ModifierKeyCode::LeftShift) => key::Mods::SHIFT,
        KeyCode::Modifier(ModifierKeyCode::LeftControl) => key::Mods::CTRL,
        KeyCode::Modifier(ModifierKeyCode::LeftAlt) => key::Mods::ALT,
        KeyCode::Modifier(ModifierKeyCode::LeftSuper | ModifierKeyCode::LeftMeta) => {
            key::Mods::SUPER
        }
        _ => key::Mods::empty(),
    }
}

#[derive(Debug)]
pub(crate) struct NormalizedMouse {
    pub event: mouse::Event<'static>,
    pub button_change: Option<(u16, bool)>,
}

pub(crate) fn mouse_event(
    input: MouseEvent,
    viewport: Rect,
    cell_width_px: u32,
    cell_height_px: u32,
) -> Result<Option<NormalizedMouse>> {
    let position = Position::new(input.column, input.row);
    if !viewport.contains(position) {
        return Ok(None);
    }

    let col = input.column - viewport.x;
    let row = input.row - viewport.y;
    let (action, button, button_change) = match input.kind {
        MouseEventKind::Down(button) => {
            let button = mouse_button(button);
            (
                mouse::Action::Press,
                Some(button),
                Some((button_bit(button), true)),
            )
        }
        MouseEventKind::Up(button) => {
            let button = mouse_button(button);
            (
                mouse::Action::Release,
                Some(button),
                Some((button_bit(button), false)),
            )
        }
        MouseEventKind::Drag(button) => (mouse::Action::Motion, Some(mouse_button(button)), None),
        MouseEventKind::Moved => (mouse::Action::Motion, None, None),
        MouseEventKind::ScrollUp => (mouse::Action::Press, Some(mouse::Button::Four), None),
        MouseEventKind::ScrollDown => (mouse::Action::Press, Some(mouse::Button::Five), None),
        MouseEventKind::ScrollLeft => (mouse::Action::Press, Some(mouse::Button::Six), None),
        MouseEventKind::ScrollRight => (mouse::Action::Press, Some(mouse::Button::Seven), None),
    };

    let x = u32::from(col)
        .checked_mul(cell_width_px)
        .ok_or(Error::MouseCoordinateOverflow)?;
    let y = u32::from(row)
        .checked_mul(cell_height_px)
        .ok_or(Error::MouseCoordinateOverflow)?;
    let mut event = mouse::Event::new()?;
    event
        .set_action(action)
        .set_button(button)
        .set_mods(key_modifiers(input.modifiers))
        .set_position(mouse::Position {
            x: x as f32,
            y: y as f32,
        });

    Ok(Some(NormalizedMouse {
        event,
        button_change,
    }))
}

fn mouse_button(button: MouseButton) -> mouse::Button {
    match button {
        MouseButton::Left => mouse::Button::Left,
        MouseButton::Right => mouse::Button::Right,
        MouseButton::Middle => mouse::Button::Middle,
    }
}

fn button_bit(button: mouse::Button) -> u16 {
    match button {
        mouse::Button::Left => 1 << 0,
        mouse::Button::Right => 1 << 1,
        mouse::Button::Middle => 1 << 2,
        mouse::Button::Four => 1 << 3,
        mouse::Button::Five => 1 << 4,
        mouse::Button::Six => 1 << 5,
        mouse::Button::Seven => 1 << 6,
        mouse::Button::Eight => 1 << 7,
        mouse::Button::Nine => 1 << 8,
        mouse::Button::Ten => 1 << 9,
        mouse::Button::Eleven => 1 << 10,
        _ => 1 << 15,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shifted_punctuation_keeps_its_physical_key() {
        assert_eq!(character_key('!'), key::Key::Digit1);
        assert_eq!(unshifted_char('!'), '1');
        assert_eq!(character_key('?'), key::Key::Slash);
        assert_eq!(unshifted_char('?'), '/');
    }

    #[test]
    fn right_modifiers_keep_side_information() {
        let mods = modifier_side(KeyCode::Modifier(ModifierKeyCode::RightControl));
        assert!(mods.contains(key::Mods::CTRL));
        assert!(mods.contains(key::Mods::CTRL_SIDE));
    }

    #[test]
    fn mouse_coordinates_are_viewport_relative() {
        let input = MouseEvent {
            kind: MouseEventKind::Down(MouseButton::Left),
            column: 12,
            row: 8,
            modifiers: KeyModifiers::NONE,
        };
        let normalized = mouse_event(input, Rect::new(10, 5, 20, 10), 2, 4)
            .expect("normalize")
            .expect("inside viewport");
        let position = normalized.event.position();
        assert_eq!(position.x, 4.0);
        assert_eq!(position.y, 12.0);
    }
}
