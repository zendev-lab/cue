use std::io::{Write as _, stdout};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::Duration;

use anyhow::{Context as _, Result};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use crossterm::event::{
    DisableBracketedPaste, DisableMouseCapture, EnableBracketedPaste, EnableMouseCapture, Event,
};
use cue_core::StepId;

pub(crate) struct Events(Arc<AtomicU8>);

impl Events {
    pub fn start() -> (Self, tokio::sync::mpsc::Receiver<Event>) {
        let control = Arc::new(AtomicU8::new(0));
        let worker = control.clone();
        let (sender, receiver) = tokio::sync::mpsc::channel(64);
        std::thread::spawn(move || {
            loop {
                match worker.load(Ordering::Acquire) {
                    3 => break,
                    1 | 2 => {
                        worker
                            .compare_exchange(1, 2, Ordering::AcqRel, Ordering::Acquire)
                            .ok();
                        std::thread::sleep(Duration::from_millis(10));
                        continue;
                    }
                    _ => {}
                }
                match crossterm::event::poll(Duration::from_millis(50)) {
                    Ok(true) => {
                        if worker.load(Ordering::Acquire) != 0 {
                            continue;
                        }
                        match crossterm::event::read() {
                            Ok(event) => {
                                if sender.blocking_send(event).is_err() {
                                    break;
                                }
                            }
                            Err(_) => break,
                        }
                    }
                    Ok(false) => {}
                    Err(_) => break,
                }
            }
        });
        (Self(control), receiver)
    }

    pub async fn pause(&self, receiver: &mut tokio::sync::mpsc::Receiver<Event>) -> Result<()> {
        self.0.store(1, Ordering::Release);
        tokio::time::timeout(Duration::from_secs(2), async {
            while self.0.load(Ordering::Acquire) != 2 {
                while receiver.try_recv().is_ok() {}
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .context("pause TUI keyboard reader before PTY passthrough")
    }

    pub fn resume(&self) {
        self.0.store(0, Ordering::Release);
    }
}

impl Drop for Events {
    fn drop(&mut self) {
        self.0.store(3, Ordering::Release);
    }
}

pub(crate) fn enable() -> Result<()> {
    crossterm::execute!(stdout(), EnableMouseCapture, EnableBracketedPaste)?;
    Ok(())
}

pub(crate) struct Restore;
impl Drop for Restore {
    fn drop(&mut self) {
        let _ = crossterm::execute!(stdout(), DisableMouseCapture, DisableBracketedPaste);
        ratatui::restore();
    }
}

pub(crate) fn copy(text: &str) -> Result<()> {
    let text = crate::view::rendered_text(text)
        .lines
        .into_iter()
        .map(|line| {
            line.spans
                .into_iter()
                .map(|span| span.content.into_owned())
                .collect::<String>()
        })
        .collect::<Vec<_>>()
        .join("\n");
    let mut output = stdout();
    write!(output, "\x1b]52;c;{}\x07", STANDARD.encode(text.as_bytes()))?;
    output.flush()?;
    Ok(())
}

pub(crate) async fn passthrough(socket: &Path, step: StepId, observe: bool) -> Result<()> {
    crossterm::execute!(stdout(), DisableMouseCapture, DisableBracketedPaste)?;
    ratatui::restore();
    let result = async {
        let executable = std::env::current_exe()?;
        let sibling = executable.with_file_name("cue-client");
        anyhow::ensure!(
            sibling.is_file(),
            "cue-client is missing beside cue-tui; install the full Cue command set"
        );
        let mut command = tokio::process::Command::new(sibling);
        command
            .args(["fg", &step.to_string()])
            .env("CUE_SOCKET", socket);
        if observe {
            command.arg("--observe");
        }
        let status = command.status().await.context("run PTY passthrough")?;
        anyhow::ensure!(status.success(), "PTY passthrough ended with {status}");
        Ok(())
    }
    .await;
    crossterm::terminal::enable_raw_mode()?;
    crossterm::execute!(stdout(), crossterm::terminal::EnterAlternateScreen)?;
    enable()?;
    result
}
