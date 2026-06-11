use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

pub(crate) fn terminal_size(terminal_width: u16, terminal_height: u16) -> (u16, u16) {
    let cols = terminal_width.saturating_sub(2).max(1);
    let rows = terminal_height.saturating_sub(3).max(1);
    (cols, rows)
}

/// Encode a key press for a foreground PTY session.
pub(crate) fn key_bytes(key: KeyEvent, application_cursor: bool) -> Option<Vec<u8>> {
    match key.code {
        KeyCode::Char(ch) => {
            if key.modifiers.contains(KeyModifiers::CONTROL) {
                if ch.is_ascii_alphabetic() {
                    Some(vec![(ch.to_ascii_lowercase() as u8) & 0x1f])
                } else {
                    None
                }
            } else {
                Some(ch.to_string().into_bytes())
            }
        }
        KeyCode::Enter => Some(vec![b'\r']),
        KeyCode::Tab => Some(vec![b'\t']),
        KeyCode::Backspace => Some(vec![0x7f]),
        KeyCode::Esc => Some(vec![0x1b]),
        KeyCode::Left => Some(if application_cursor {
            b"\x1bOD".to_vec()
        } else {
            b"\x1b[D".to_vec()
        }),
        KeyCode::Right => Some(if application_cursor {
            b"\x1bOC".to_vec()
        } else {
            b"\x1b[C".to_vec()
        }),
        KeyCode::Up => Some(if application_cursor {
            b"\x1bOA".to_vec()
        } else {
            b"\x1b[A".to_vec()
        }),
        KeyCode::Down => Some(if application_cursor {
            b"\x1bOB".to_vec()
        } else {
            b"\x1b[B".to_vec()
        }),
        KeyCode::Home => Some(if application_cursor {
            b"\x1bOH".to_vec()
        } else {
            b"\x1b[H".to_vec()
        }),
        KeyCode::End => Some(if application_cursor {
            b"\x1bOF".to_vec()
        } else {
            b"\x1b[F".to_vec()
        }),
        KeyCode::Delete => Some(b"\x1b[3~".to_vec()),
        KeyCode::BackTab => Some(b"\x1b[Z".to_vec()),
        _ => None,
    }
}

/// Encode pasted text for a foreground PTY session.
pub(crate) fn paste_bytes(text: &str, bracketed: bool) -> Vec<u8> {
    if bracketed {
        let mut wrapped = b"\x1b[200~".to_vec();
        wrapped.extend_from_slice(text.as_bytes());
        wrapped.extend_from_slice(b"\x1b[201~");
        wrapped
    } else {
        text.as_bytes().to_vec()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn terminal_size_accounts_for_chrome_and_saturates() {
        assert_eq!(terminal_size(80, 24), (78, 21));
        assert_eq!(terminal_size(2, 3), (1, 1));
        assert_eq!(terminal_size(0, 0), (1, 1));
    }

    #[test]
    fn key_bytes_use_application_cursor_sequences_when_enabled() {
        let key = KeyEvent::new(KeyCode::Up, KeyModifiers::NONE);
        assert_eq!(key_bytes(key, false), Some(b"\x1b[A".to_vec()));
        assert_eq!(key_bytes(key, true), Some(b"\x1bOA".to_vec()));
    }

    #[test]
    fn control_letter_keys_encode_control_bytes() {
        let key = KeyEvent::new(KeyCode::Char('C'), KeyModifiers::CONTROL);
        assert_eq!(key_bytes(key, false), Some(vec![0x03]));
    }

    #[test]
    fn non_letter_control_keys_are_not_encoded() {
        let key = KeyEvent::new(KeyCode::Char('1'), KeyModifiers::CONTROL);
        assert_eq!(key_bytes(key, false), None);
    }

    #[test]
    fn paste_bytes_wrap_when_bracketed_paste_is_enabled() {
        assert_eq!(paste_bytes("echo hi", false), b"echo hi".to_vec());
        assert_eq!(
            paste_bytes("echo hi", true),
            b"\x1b[200~echo hi\x1b[201~".to_vec()
        );
    }
}
