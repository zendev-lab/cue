//! Small IPC v4 execution TUI.

pub mod cli;

use std::path::PathBuf;

use anyhow::{Context as _, Result, bail};
use crossterm::event::{Event, KeyCode, KeyEventKind, KeyModifiers};
use cue_client::{VnextClient, VnextMultiplexedClient, process_scope};
use cue_core::vnext::{CancelMode, Fact, OutputStream};
use cue_core::{ExecutionId, StepId};
use cue_language::{
    Mode, OutputSelection, OutputTarget, VnextCommand, VnextFrontendAction, compile_vnext_command,
};
use cue_protocol::{Command, EventPayload, ExecutionView, OutputRange, Query, ResultPayload};
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, List, ListItem, Paragraph, Wrap};

pub fn run_cli() -> Result<()> {
    cli::run()
}

pub async fn run(socket: PathBuf) -> Result<()> {
    let client = VnextClient::connect(&socket)
        .await
        .with_context(|| format!("connect to {}", socket.display()))?
        .into_multiplexed();
    let mut state = State::default();
    refresh(&client, &mut state).await?;

    let mut terminal = ratatui::init();
    let _restore = TerminalRestore;
    let (key_tx, mut key_rx) = tokio::sync::mpsc::channel(32);
    std::thread::spawn(move || {
        while let Ok(event) = crossterm::event::read() {
            if key_tx.blocking_send(event).is_err() {
                break;
            }
        }
    });

    loop {
        terminal.draw(|frame| draw(frame, &state))?;
        tokio::select! {
            event = key_rx.recv() => {
                let Some(event) = event else {
                    break;
                };
                if handle_event(event, &client, &mut state).await? {
                    break;
                }
            }
            event = client.next_event() => {
                match event {
                    Some(EventPayload::Fact(fact)) => {
                        state.notice = fact_summary(&fact.fact);
                        refresh(&client, &mut state).await?;
                    }
                    Some(EventPayload::ServerDraining { reason }) => {
                        state.notice = format!("daemon draining: {reason}");
                    }
                    Some(_) => {}
                    None => {
                        state.notice = "daemon disconnected".into();
                        break;
                    }
                }
            }
        }
    }
    Ok(())
}

#[derive(Default)]
struct State {
    input: String,
    executions: Vec<ExecutionView>,
    log: Vec<String>,
    notice: String,
}

async fn handle_event(
    event: Event,
    client: &VnextMultiplexedClient,
    state: &mut State,
) -> Result<bool> {
    let Event::Key(key) = event else {
        return Ok(false);
    };
    if key.kind != KeyEventKind::Press {
        return Ok(false);
    }
    if key.code == KeyCode::Esc
        || (key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL))
    {
        return Ok(true);
    }
    match key.code {
        KeyCode::Char(character) => state.input.push(character),
        KeyCode::Backspace => {
            state.input.pop();
        }
        KeyCode::Enter => {
            let source = std::mem::take(&mut state.input);
            if !source.trim().is_empty() && dispatch(client, state, &source).await? {
                return Ok(true);
            }
        }
        _ => {}
    }
    Ok(false)
}

async fn dispatch(
    client: &VnextMultiplexedClient,
    state: &mut State,
    source: &str,
) -> Result<bool> {
    let scope = process_scope()?;
    let stored = client
        .command(Command::PutScope {
            scope: Box::new(scope),
        })
        .await?;
    let ResultPayload::ScopeStored { hash, .. } = stored else {
        bail!("daemon returned an unexpected PutScope response")
    };
    let command = match compile_vnext_command(source, Mode::Job, hash) {
        Ok(command) => command,
        Err(error) => {
            state.log.push(error.to_string());
            return Ok(false);
        }
    };
    match command {
        VnextCommand::Submit(spec) => {
            let submitted = client
                .command(Command::SubmitExecution {
                    spec: Box::new(spec),
                })
                .await?;
            let ResultPayload::ExecutionSubmitted { execution } = submitted else {
                bail!("daemon returned an unexpected SubmitExecution response")
            };
            client
                .command(Command::WatchExecution {
                    id: execution.snapshot.id,
                    after_event: None,
                })
                .await?;
            state
                .log
                .push(format!("submitted {}", execution.snapshot.id));
            refresh(client, state).await?;
        }
        VnextCommand::ListExecutions => refresh(client, state).await?,
        VnextCommand::GetExecution { id } => {
            append_json(state, client.query(Query::GetExecution { id }).await?)?;
        }
        VnextCommand::WaitExecution { id } => {
            append_json(state, client.query(Query::WaitExecution { id }).await?)?;
            refresh(client, state).await?;
        }
        VnextCommand::ReadOutput {
            target,
            stream,
            tail_bytes,
        } => read_output(client, state, target, stream, tail_bytes).await?,
        VnextCommand::CancelExecution { id, force } => {
            client
                .command(Command::CancelExecution {
                    id,
                    mode: if force {
                        CancelMode::Force
                    } else {
                        CancelMode::Graceful
                    },
                })
                .await?;
            refresh(client, state).await?;
        }
        VnextCommand::AttachPty { step, .. } => state.log.push(format!(
            "PTY {step} needs terminal passthrough; run `cue fg {step}`"
        )),
        VnextCommand::Frontend(VnextFrontendAction::Clear) => state.log.clear(),
        VnextCommand::Frontend(VnextFrontendAction::Quit) => return Ok(true),
        VnextCommand::Frontend(VnextFrontendAction::Restart) => {
            append_json(state, client.command(Command::Restart).await?)?;
        }
        VnextCommand::Frontend(VnextFrontendAction::Help { .. }) => state.log.push(
            "run commands directly; :jobs, :log E1, :wait E1, :out E1/S1, :cancel E1, :fg E1/S1"
                .into(),
        ),
    }
    Ok(false)
}

async fn refresh(client: &VnextMultiplexedClient, state: &mut State) -> Result<()> {
    let result = client
        .query(Query::ListExecutions {
            before: None,
            limit: 100,
        })
        .await?;
    let ResultPayload::Executions { executions, .. } = result else {
        bail!("daemon returned an unexpected ListExecutions response")
    };
    state.executions = executions;
    Ok(())
}

async fn read_output(
    client: &VnextMultiplexedClient,
    state: &mut State,
    target: OutputTarget,
    stream: OutputSelection,
    tail_bytes: Option<usize>,
) -> Result<()> {
    let step = match target {
        OutputTarget::Step(step) => step,
        OutputTarget::Execution(id) => last_step(client, id).await?,
    };
    let range = OutputRange {
        offset: 0,
        max_bytes: tail_bytes.unwrap_or(1024 * 1024).min(u32::MAX as usize) as u32,
    };
    let empty = OutputRange {
        offset: 0,
        max_bytes: 0,
    };
    let result = client
        .query(Query::ReadOutput {
            step,
            stdout: if stream == OutputSelection::Stdout {
                range.clone()
            } else {
                empty.clone()
            },
            stderr: if stream == OutputSelection::Stderr {
                range
            } else {
                empty.clone()
            },
            terminal: empty,
        })
        .await?;
    let ResultPayload::Output { chunks } = result else {
        bail!("daemon returned an unexpected ReadOutput response")
    };
    let selected = if stream == OutputSelection::Stdout {
        OutputStream::Stdout
    } else {
        OutputStream::Stderr
    };
    let bytes = cue_client::vnext::output_bytes(&chunks, selected);
    state
        .log
        .extend(String::from_utf8_lossy(&bytes).lines().map(str::to_owned));
    Ok(())
}

async fn last_step(client: &VnextMultiplexedClient, id: ExecutionId) -> Result<StepId> {
    let ResultPayload::Execution { execution } = client.query(Query::GetExecution { id }).await?
    else {
        bail!("daemon returned an unexpected GetExecution response")
    };
    execution
        .snapshot
        .steps
        .last()
        .map(|step| step.id())
        .ok_or_else(|| anyhow::anyhow!("execution {id} has no steps"))
}

fn append_json(state: &mut State, value: impl serde::Serialize) -> Result<()> {
    state.log.extend(
        serde_json::to_string_pretty(&value)?
            .lines()
            .map(str::to_owned),
    );
    Ok(())
}

fn fact_summary(fact: &Fact) -> String {
    match fact {
        Fact::ExecutionCreated { id, .. } => format!("created {id}"),
        Fact::StepStateChanged { id, next, .. } => format!("{id}: {next:?}"),
        Fact::ExecutionStateChanged { id, next, .. } => format!("{id}: {next:?}"),
        Fact::OutputAppended { step, .. } => format!("output from {step}"),
        Fact::ExecutionFinished { id, state } => format!("{id}: {state:?}"),
    }
}

fn draw(frame: &mut ratatui::Frame<'_>, state: &State) {
    let regions = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage(52),
            Constraint::Min(5),
            Constraint::Length(3),
            Constraint::Length(1),
        ])
        .split(frame.area());
    let executions = state
        .executions
        .iter()
        .map(|execution| {
            ListItem::new(Line::from(vec![
                Span::styled(
                    execution.snapshot.id.to_string(),
                    Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD),
                ),
                Span::raw(format!("  {:?}", execution.state)),
            ]))
        })
        .collect::<Vec<_>>();
    frame.render_widget(
        List::new(executions).block(Block::default().borders(Borders::ALL).title("Executions")),
        regions[0],
    );
    let log = state
        .log
        .iter()
        .rev()
        .take(regions[1].height.saturating_sub(2) as usize)
        .rev()
        .cloned()
        .collect::<Vec<_>>()
        .join("\n");
    frame.render_widget(
        Paragraph::new(log)
            .wrap(Wrap { trim: false })
            .block(Block::default().borders(Borders::ALL).title("Activity")),
        regions[1],
    );
    frame.render_widget(
        Paragraph::new(state.input.as_str())
            .block(Block::default().borders(Borders::ALL).title("Cue")),
        regions[2],
    );
    frame.render_widget(
        Paragraph::new(format!("{}  Esc/Ctrl-C quit", state.notice)),
        regions[3],
    );
    frame.set_cursor_position((
        regions[2].x + 1 + state.input.chars().count() as u16,
        regions[2].y + 1,
    ));
}

struct TerminalRestore;

impl Drop for TerminalRestore {
    fn drop(&mut self) {
        ratatui::restore();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fact_summary_uses_vnext_execution_identity() {
        assert_eq!(
            fact_summary(&Fact::ExecutionFinished {
                id: ExecutionId(7),
                state: cue_core::vnext::ExecutionState::Succeeded,
            }),
            "E7: Succeeded"
        );
    }
}
