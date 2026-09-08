use crossterm::event::{KeyCode, KeyModifiers};
use cue_language::{CompletionScope, Mode, completion_candidates, completion_replacement};

const INPUT_LIMIT: usize = 64 * 1024;

#[derive(Default)]
pub(crate) struct Editor {
    pub text: String,
    pub cursor: usize,
    pub history: Vec<String>,
    history_index: Option<usize>,
    draft: String,
    pub candidates: Vec<String>,
}

impl Editor {
    pub fn set(&mut self, text: String) {
        self.cursor = text.len();
        self.text = text;
        self.candidates.clear();
    }

    pub fn insert(&mut self, text: &str) {
        let clean = text.replace('\r', "\n");
        let clean = clean
            .chars()
            .filter(|ch| !ch.is_control() || matches!(ch, '\n' | '\t'))
            .collect::<String>();
        if self.text.len().saturating_add(clean.len()) > INPUT_LIMIT {
            return;
        }
        self.text.insert_str(self.cursor, &clean);
        self.cursor += clean.len();
        self.candidates.clear();
    }

    pub fn take(&mut self) -> String {
        self.cursor = 0;
        self.history_index = None;
        self.draft.clear();
        self.candidates.clear();
        std::mem::take(&mut self.text)
    }

    pub fn remember(&mut self, source: &str) {
        if self.history.last().is_none_or(|last| last != source) {
            self.history.push(source.to_owned());
            if self.history.len() > 500 {
                self.history.remove(0);
            }
        }
    }

    pub fn key(&mut self, code: KeyCode, modifiers: KeyModifiers) {
        match code {
            KeyCode::Char('a') if modifiers.contains(KeyModifiers::CONTROL) => self.cursor = 0,
            KeyCode::Char('e') if modifiers.contains(KeyModifiers::CONTROL) => {
                self.cursor = self.text.len()
            }
            KeyCode::Char('u') if modifiers.contains(KeyModifiers::CONTROL) => {
                self.text.drain(..self.cursor);
                self.cursor = 0;
            }
            KeyCode::Char('k') if modifiers.contains(KeyModifiers::CONTROL) => {
                self.text.truncate(self.cursor)
            }
            KeyCode::Char(ch)
                if !modifiers.intersects(KeyModifiers::CONTROL | KeyModifiers::ALT) =>
            {
                self.insert(&ch.to_string())
            }
            KeyCode::Left => self.cursor = self.previous(),
            KeyCode::Right => self.cursor = self.next(),
            KeyCode::Home => {
                self.cursor = self.text[..self.cursor]
                    .rfind('\n')
                    .map_or(0, |index| index + 1)
            }
            KeyCode::End => {
                self.cursor = self.text[self.cursor..]
                    .find('\n')
                    .map_or(self.text.len(), |index| self.cursor + index)
            }
            KeyCode::Backspace if self.cursor > 0 => {
                let previous = self.previous();
                self.text.drain(previous..self.cursor);
                self.cursor = previous;
            }
            KeyCode::Delete if self.cursor < self.text.len() => {
                self.text.drain(self.cursor..self.next());
            }
            KeyCode::Up => self.history_move(-1),
            KeyCode::Down => self.history_move(1),
            _ => {}
        }
    }

    fn previous(&self) -> usize {
        self.text[..self.cursor]
            .char_indices()
            .next_back()
            .map_or(0, |(index, _)| index)
    }

    fn next(&self) -> usize {
        self.cursor
            + self.text[self.cursor..]
                .chars()
                .next()
                .map_or(0, char::len_utf8)
    }

    fn history_move(&mut self, delta: isize) {
        if self.history.is_empty() {
            return;
        }
        if self.history_index.is_none() {
            self.draft = self.text.clone();
        }
        let next = self
            .history_index
            .unwrap_or(self.history.len())
            .saturating_add_signed(delta)
            .min(self.history.len());
        self.history_index = (next < self.history.len()).then_some(next);
        self.set(
            self.history
                .get(next)
                .cloned()
                .unwrap_or_else(|| self.draft.clone()),
        );
    }

    pub fn complete(
        &mut self,
        execution_ids: &[String],
        step_ids: &[String],
    ) -> anyhow::Result<()> {
        let start = self.text[..self.cursor]
            .rfind(char::is_whitespace)
            .map_or(0, |index| index + 1);
        let candidates = completion_candidates(CompletionScope {
            mode: Mode::Job,
            content: &self.text,
            cursor: self.cursor,
            word_range: start..self.cursor,
            execution_ids,
            step_ids,
            schedule_ids: &[],
        })?;
        if let Some(replacement) =
            completion_replacement(&candidates, &self.text[start..self.cursor])
        {
            self.text.replace_range(start..self.cursor, &replacement);
            self.cursor = start + replacement.len();
        }
        self.candidates = candidates;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unicode_edits_and_history_round_trip_preserve_the_draft() {
        let mut editor = Editor::default();
        editor.remember("old command");
        editor.insert("中文🙂");
        editor.key(KeyCode::Left, KeyModifiers::NONE);
        editor.key(KeyCode::Backspace, KeyModifiers::NONE);
        editor.insert("字");
        assert_eq!(editor.text, "中字🙂");
        editor.key(KeyCode::Up, KeyModifiers::NONE);
        assert_eq!(editor.text, "old command");
        editor.key(KeyCode::Down, KeyModifiers::NONE);
        assert_eq!(editor.text, "中字🙂");
    }
}
