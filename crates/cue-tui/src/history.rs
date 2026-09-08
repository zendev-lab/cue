//! Frontend command recall. Append-only records let independent TUIs coexist.
use std::fs::OpenOptions;
use std::io::{BufRead as _, BufReader, Write as _};
use std::os::unix::fs::{DirBuilderExt as _, OpenOptionsExt as _};
use std::path::PathBuf;

use anyhow::{Context as _, Result};

fn directory() -> Option<PathBuf> {
    std::env::var_os("XDG_DATA_HOME")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .or_else(|| {
            std::env::var_os("HOME")
                .filter(|value| !value.is_empty())
                .map(|home| PathBuf::from(home).join(".local/share"))
        })
        .map(|root| root.join("cue"))
}

pub(crate) fn load() -> Result<Vec<String>> {
    let Some(root) = directory() else {
        return Ok(Vec::new());
    };
    let mut history = Vec::new();
    // Keep the previous UI's input history available without changing its file.
    if let Ok(text) = std::fs::read_to_string(root.join("input-history.json")) {
        history = serde_json::from_str(&text).context("read previous TUI input history")?;
    }
    match std::fs::File::open(root.join("input-history-v4.jsonl")) {
        Ok(file) => {
            for line in BufReader::new(file).lines() {
                history.push(
                    serde_json::from_str::<String>(&line?).context("read TUI input history")?,
                );
                if history.len() > 500 {
                    history.remove(0);
                }
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    if history.len() > 500 {
        history.drain(..history.len() - 500);
    }
    Ok(history)
}

pub(crate) fn append(source: &str) -> Result<()> {
    let Some(root) = directory() else {
        return Ok(());
    };
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(&root)?;
    let mut file = OpenOptions::new()
        .append(true)
        .create(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(root.join("input-history-v4.jsonl"))?;
    anyhow::ensure!(
        file.metadata()?.is_file(),
        "TUI history must be a regular file"
    );
    let mut record = serde_json::to_vec(source)?;
    record.push(b'\n');
    file.write_all(&record).context("save TUI input history")
}
