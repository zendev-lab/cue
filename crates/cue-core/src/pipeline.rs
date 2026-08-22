use std::collections::BTreeMap;
use std::fmt;

use serde::{Deserialize, Serialize};

/// One executable process pipeline.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Pipeline {
    /// At least one segment.
    pub segments: Vec<PipeSegment>,
}

/// One process in a pipeline.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PipeSegment {
    /// Environment overrides applied only to this process segment.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub env: BTreeMap<String, String>,
    /// Command words, e.g. `["cargo", "test", "--release"]`.
    pub command: Vec<String>,
    /// How this segment's output connects to the next (None for last segment).
    pub pipe_to_next: Option<PipeOp>,
}

/// Pipe operator connecting two process segments.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PipeOp {
    /// `|>` — stdout → next stdin.
    Stdout,
    /// `|&>` — stdout + stderr → next stdin.
    StdoutStderr,
    /// `|!>` — stderr only → next stdin.
    StderrOnly,
}

impl Pipeline {
    /// Create a simple single-command pipeline.
    pub fn simple(command: Vec<String>) -> Self {
        Self {
            segments: vec![PipeSegment {
                env: BTreeMap::new(),
                command,
                pipe_to_next: None,
            }],
        }
    }
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

impl fmt::Display for Pipeline {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (idx, segment) in self.segments.iter().enumerate() {
            if idx > 0 {
                f.write_str(" ")?;
            }

            for (key, value) in &segment.env {
                write!(f, "{key}={value} ")?;
            }
            let cmd = segment.command.join(" ");
            match segment.pipe_to_next {
                Some(op) => write!(f, "{cmd} {op}")?,
                None => f.write_str(&cmd)?,
            }
        }
        Ok(())
    }
}

/// Return true when a command is likely to need immediate foreground/TTY use.
pub fn command_prefers_foreground(command_line: &[String]) -> bool {
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
    fn simple_pipeline() {
        let p = Pipeline::simple(vec!["cargo".into(), "test".into()]);
        assert_eq!(p.segments.len(), 1);
        assert!(p.segments[0].pipe_to_next.is_none());
    }

    #[test]
    fn segment_environment_roundtrips_and_defaults_for_existing_specs() {
        let existing: PipeSegment =
            serde_json::from_str(r#"{"command":["echo","ok"],"pipe_to_next":null}"#)
                .expect("decode pre-assignment segment");
        assert!(existing.env.is_empty());

        let segment = PipeSegment {
            env: BTreeMap::from([("FOO".into(), "bar".into())]),
            command: vec!["printenv".into(), "FOO".into()],
            pipe_to_next: None,
        };
        let encoded = serde_json::to_string(&segment).expect("encode segment environment");
        let decoded: PipeSegment = serde_json::from_str(&encoded).expect("decode environment");
        assert_eq!(decoded, segment);
    }

    #[test]
    fn foreground_command_detection() {
        assert!(command_prefers_foreground(&[
            "vim".into(),
            "src/main.rs".into()
        ]));
        assert!(command_prefers_foreground(&[
            "/usr/bin/ssh".into(),
            "host".into()
        ]));
        assert!(command_prefers_foreground(&["python".into()]));
        assert!(command_prefers_foreground(&[
            "bash".into(),
            "--interactive".into(),
        ]));
        assert!(!command_prefers_foreground(&[
            "cargo".into(),
            "test".into(),
        ]));
        assert!(!command_prefers_foreground(&[
            "python".into(),
            "script.py".into(),
        ]));
    }

    #[test]
    fn display_pipeline() {
        let pipeline = Pipeline {
            segments: vec![
                PipeSegment {
                    env: BTreeMap::new(),
                    command: vec!["printf".into(), "hi".into()],
                    pipe_to_next: Some(PipeOp::Stdout),
                },
                PipeSegment {
                    env: BTreeMap::new(),
                    command: vec!["grep".into(), "h".into()],
                    pipe_to_next: Some(PipeOp::StderrOnly),
                },
                PipeSegment {
                    env: BTreeMap::new(),
                    command: vec!["wc".into(), "-l".into()],
                    pipe_to_next: None,
                },
            ],
        };
        assert_eq!(pipeline.to_string(), "printf hi |> grep h |!> wc -l");
    }
}
