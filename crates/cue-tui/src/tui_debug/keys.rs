//! Parse human-readable key names into crossterm key events.

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct KeyParseError {
    pub message: String,
}

impl KeyParseError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

pub(crate) fn parse_key_token(token: &str) -> Result<KeyEvent, KeyParseError> {
    let token = token.trim();
    if token.is_empty() {
        return Err(KeyParseError::new("key token must not be empty"));
    }

    let (modifiers, body) = split_modifiers(token)?;
    let code = parse_key_code(body)?;
    Ok(KeyEvent::new(code, modifiers))
}

pub(crate) fn parse_key_tokens(tokens: &[String]) -> Result<Vec<KeyEvent>, KeyParseError> {
    tokens.iter().map(|token| parse_key_token(token)).collect()
}

pub(crate) fn char_key_events(text: &str) -> Vec<KeyEvent> {
    text.chars()
        .map(|ch| KeyEvent::new(KeyCode::Char(ch), KeyModifiers::NONE))
        .collect()
}

fn split_modifiers(token: &str) -> Result<(KeyModifiers, &str), KeyParseError> {
    let mut modifiers = KeyModifiers::NONE;
    let mut body = token;
    while let Some((prefix, rest)) = body.split_once('+') {
        let prefix = prefix.trim();
        if prefix.is_empty() {
            return Err(KeyParseError::new(format!(
                "invalid key token `{token}`: empty modifier segment"
            )));
        }
        modifiers |= modifier_from_name(prefix).ok_or_else(|| {
            KeyParseError::new(format!("unknown modifier `{prefix}` in `{token}`"))
        })?;
        body = rest.trim();
        if body.is_empty() {
            return Err(KeyParseError::new(format!(
                "invalid key token `{token}`: missing key after modifiers"
            )));
        }
    }
    Ok((modifiers, body))
}

fn modifier_from_name(name: &str) -> Option<KeyModifiers> {
    match name.to_ascii_lowercase().as_str() {
        "ctrl" | "control" => Some(KeyModifiers::CONTROL),
        "alt" => Some(KeyModifiers::ALT),
        "shift" => Some(KeyModifiers::SHIFT),
        _ => None,
    }
}

fn parse_key_code(body: &str) -> Result<KeyCode, KeyParseError> {
    if body.len() == 1 {
        let ch = body.chars().next().expect("single char");
        return Ok(KeyCode::Char(ch));
    }

    match body.to_ascii_lowercase().as_str() {
        "enter" | "return" => Ok(KeyCode::Enter),
        "esc" | "escape" => Ok(KeyCode::Esc),
        "tab" => Ok(KeyCode::Tab),
        "backtab" | "shift-tab" | "shifttab" => Ok(KeyCode::BackTab),
        "backspace" => Ok(KeyCode::Backspace),
        "delete" | "del" => Ok(KeyCode::Delete),
        "home" => Ok(KeyCode::Home),
        "end" => Ok(KeyCode::End),
        "pageup" | "page-up" => Ok(KeyCode::PageUp),
        "pagedown" | "page-down" => Ok(KeyCode::PageDown),
        "up" | "arrow-up" => Ok(KeyCode::Up),
        "down" | "arrow-down" => Ok(KeyCode::Down),
        "left" | "arrow-left" => Ok(KeyCode::Left),
        "right" | "arrow-right" => Ok(KeyCode::Right),
        "space" => Ok(KeyCode::Char(' ')),
        other if other.len() == 1 => Ok(KeyCode::Char(other.chars().next().expect("char"))),
        other => Err(KeyParseError::new(format!("unknown key `{other}`"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_named_keys() {
        assert_eq!(
            parse_key_token("enter").unwrap(),
            KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE)
        );
        assert_eq!(
            parse_key_token("esc").unwrap(),
            KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE)
        );
        assert_eq!(
            parse_key_token("shift+tab").unwrap(),
            KeyEvent::new(KeyCode::Tab, KeyModifiers::SHIFT)
        );
        assert_eq!(
            parse_key_token("ctrl+c").unwrap(),
            KeyEvent::new(KeyCode::Char('c'), KeyModifiers::CONTROL)
        );
    }

    #[test]
    fn parse_single_char_key() {
        assert_eq!(
            parse_key_token("x").unwrap(),
            KeyEvent::new(KeyCode::Char('x'), KeyModifiers::NONE)
        );
    }

    #[test]
    fn parse_unknown_key_reports_error() {
        let error = parse_key_token("not-a-key").expect_err("unknown key");
        assert!(error.message.contains("unknown key"));
    }

    #[test]
    fn char_key_events_emit_one_event_per_char() {
        let events = char_key_events("ab");
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].code, KeyCode::Char('a'));
        assert_eq!(events[1].code, KeyCode::Char('b'));
    }
}
