use std::collections::BTreeMap;
use std::fmt;

/// One pipe token in the Cue surface language.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PipeOp {
    Stdout,
    StdoutStderr,
    StderrOnly,
}

impl fmt::Display for PipeOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Stdout => "|>",
            Self::StdoutStderr => "|&>",
            Self::StderrOnly => "|!>",
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ParamValue {
    Str(String),
    Bool(bool),
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ModeParams {
    pub(crate) params: BTreeMap<String, ParamValue>,
}

impl ModeParams {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.params.is_empty()
    }

    pub(crate) fn get(&self, key: &str) -> Option<&ParamValue> {
        self.params.get(key)
    }

    pub(crate) fn insert(&mut self, key: impl Into<String>, value: ParamValue) {
        self.params.insert(key.into(), value);
    }

    #[cfg(test)]
    pub(crate) fn pty_enabled(&self) -> bool {
        !matches!(self.get("pty"), Some(ParamValue::Bool(false)))
    }
}

/// Surface heuristic used only to choose the default I/O mode.
pub(crate) fn command_prefers_foreground(command_line: &[String]) -> bool {
    let Some(command_word) = command_line.first() else {
        return false;
    };
    let command = std::path::Path::new(command_word)
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or(command_word.as_str());
    let args: Vec<&str> = command_line.iter().skip(1).map(String::as_str).collect();

    match command {
        "vim" | "nvim" | "vi" | "nano" | "less" | "more" | "man" | "top" | "htop" | "watch"
        | "fzf" | "tig" | "lazygit" | "tmux" | "zellij" => true,
        "bash" | "zsh" | "sh" | "fish" => {
            args.is_empty()
                || args.contains(&"-i")
                || args.contains(&"--interactive")
                || args.contains(&"-l")
        }
        "python" | "python3" | "node" | "ipython" | "bpython" | "irb" => {
            args.is_empty()
                || args
                    .first()
                    .is_some_and(|arg| matches!(*arg, "-i" | "--interactive"))
        }
        "ssh" | "psql" | "mysql" | "sqlite3" => true,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn foreground_detection_stays_in_the_surface_language() {
        assert!(command_prefers_foreground(&["vim".into()]));
        assert!(command_prefers_foreground(&["python".into()]));
        assert!(!command_prefers_foreground(&[
            "cargo".into(),
            "test".into()
        ]));
    }
}
