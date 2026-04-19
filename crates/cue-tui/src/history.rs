use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

const APP_DIR: &str = "cue-shell";
const HISTORY_FILE: &str = "input-history.json";

fn home_dir() -> PathBuf {
    std::env::var("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("/tmp"))
}

fn data_dir() -> PathBuf {
    if let Ok(dir) = std::env::var("XDG_DATA_HOME") {
        PathBuf::from(dir).join(APP_DIR)
    } else {
        home_dir().join(".local/share").join(APP_DIR)
    }
}

pub fn history_path() -> PathBuf {
    data_dir().join(HISTORY_FILE)
}

fn load_history_from(path: &Path) -> Result<Vec<String>> {
    match std::fs::read_to_string(path) {
        Ok(content) => serde_json::from_str(&content)
            .with_context(|| format!("parse history file {}", path.display())),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(error) => Err(error).with_context(|| format!("read history file {}", path.display())),
    }
}

fn save_history_to(path: &Path, history: &[String]) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create history directory {}", parent.display()))?;
    }
    let content = serde_json::to_string(history).context("serialize history")?;
    std::fs::write(path, content).with_context(|| format!("write history file {}", path.display()))
}

pub fn load_history() -> Result<Vec<String>> {
    load_history_from(&history_path())
}

pub fn save_history(history: &[String]) -> Result<()> {
    save_history_to(&history_path(), history)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_path(name: &str) -> PathBuf {
        let root = std::env::temp_dir().join(format!(
            "cue-tui-history-test-{}-{}",
            std::process::id(),
            name
        ));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        root.join(HISTORY_FILE)
    }

    #[test]
    fn missing_history_file_loads_as_empty() {
        let path = temp_path("missing");
        assert!(load_history_from(&path).unwrap().is_empty());
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn history_roundtrip_preserves_multiline_entries() {
        let path = temp_path("roundtrip");
        let history = vec!["ls".into(), "echo hi\npwd".into()];
        save_history_to(&path, &history).unwrap();
        assert_eq!(load_history_from(&path).unwrap(), history);
        let _ = std::fs::remove_file(path);
    }
}
