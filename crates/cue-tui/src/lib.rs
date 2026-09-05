//! Small IPC v4 execution TUI.

pub mod cli;

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context as _, Result, bail};
use crossterm::event::{Event, KeyCode, KeyEventKind, KeyModifiers};
use cue_client::{ExecutionClient, MultiplexedClient, SurfaceOutcome, process_scope};
use cue_core::Fact;
use cue_language::{FrontendAction, Mode, SurfaceCommand, compile_command};
use cue_protocol::{Command, EventPayload, ExecutionView, Query, ResultPayload};
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, List, ListItem, Paragraph, Wrap};

pub fn run_cli() -> Result<()> {
    cli::run()
}

pub async fn run(socket: PathBuf) -> Result<()> {
    let client = Arc::new(
        ExecutionClient::connect(&socket)
            .await
            .with_context(|| format!("connect to {}", socket.display()))?
            .into_multiplexed(),
    );
    let mut state = State::default();
    let mut pending = tokio::task::JoinSet::<Result<ResultPayload>>::new();
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
            completed = pending.join_next(), if !pending.is_empty() => {
                match completed.unwrap() {
                    Ok(Ok(result)) => apply_result(&client, &mut state, result).await?,
                    Ok(Err(error)) => state.log.push(error.to_string()),
                    Err(error) => state.log.push(format!("request failed: {error}")),
                }
            }
            event = key_rx.recv() => {
                let Some(event) = event else {
                    break;
                };
                if handle_event(event, &client, &mut state, &mut pending)? {
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

fn handle_event(
    event: Event,
    client: &Arc<MultiplexedClient>,
    state: &mut State,
    pending: &mut tokio::task::JoinSet<Result<ResultPayload>>,
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
            if !source.trim().is_empty() && dispatch(client, state, &source, pending)? {
                return Ok(true);
            }
        }
        _ => {}
    }
    Ok(false)
}

fn dispatch(
    client: &Arc<MultiplexedClient>,
    state: &mut State,
    source: &str,
    pending: &mut tokio::task::JoinSet<Result<ResultPayload>>,
) -> Result<bool> {
    let scope = process_scope()?;
    let command = match compile_command(source, Mode::Job, scope.compute_hash()) {
        Ok(command) => command,
        Err(error) => {
            state.log.push(error.to_string());
            return Ok(false);
        }
    };
    match command {
        SurfaceCommand::AttachPty { step, .. } => state.log.push(format!(
            "PTY {step} needs terminal passthrough; run `cue fg {step}`"
        )),
        SurfaceCommand::Frontend(FrontendAction::Clear) => state.log.clear(),
        SurfaceCommand::Frontend(FrontendAction::Quit) => return Ok(true),
        SurfaceCommand::Frontend(FrontendAction::Help { .. }) => state.log.push(
            "run commands directly; :jobs, :log E1, :wait E1, :out E1/S1, :cancel E1, :fg E1/S1"
                .into(),
        ),
        command => {
            if pending.len() >= 64 {
                state
                    .log
                    .push("too many pending requests; wait for one to finish".into());
                return Ok(false);
            }
            let client = client.clone();
            pending.spawn(async move {
                if matches!(command, SurfaceCommand::Frontend(FrontendAction::Restart)) {
                    return client.command(Command::Restart).await;
                }
                let SurfaceOutcome::Response(result) =
                    client.execute_compiled(scope, command).await?
                else {
                    bail!("unexpected frontend action");
                };
                if let ResultPayload::ExecutionSubmitted { execution } = &result {
                    client
                        .command(Command::WatchExecution {
                            id: execution.snapshot.id,
                            after_event: None,
                        })
                        .await?;
                }
                Ok(result)
            });
        }
    }
    Ok(false)
}

async fn apply_result(
    client: &MultiplexedClient,
    state: &mut State,
    result: ResultPayload,
) -> Result<()> {
    match result {
        ResultPayload::Executions { executions, .. } => state.executions = executions,
        ResultPayload::ExecutionSubmitted { execution } => {
            state
                .log
                .push(format!("submitted {}", execution.snapshot.id));
            refresh(client, state).await?;
        }
        result @ ResultPayload::Execution { .. } => {
            append_json(state, result)?;
            refresh(client, state).await?;
        }
        ResultPayload::Output { chunks } => {
            let bytes = chunks
                .into_iter()
                .flat_map(|chunk| chunk.data)
                .collect::<Vec<_>>();
            state
                .log
                .extend(String::from_utf8_lossy(&bytes).lines().map(str::to_owned));
        }
        result => append_json(state, result)?,
    }
    Ok(())
}

async fn refresh(client: &MultiplexedClient, state: &mut State) -> Result<()> {
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
    use cue_core::ExecutionId;
    use std::time::Duration;

    async fn client() -> (Arc<MultiplexedClient>, tokio::task::JoinHandle<Result<()>>) {
        let service = cue_daemon::service::DaemonService::in_memory().unwrap();
        let (stream, server_stream) = tokio::io::duplex(128 * 1024);
        let server = tokio::spawn(async move {
            cue_daemon::service::serve_stream(service, server_stream)
                .await
                .map_err(anyhow::Error::from)
        });
        let client = ExecutionClient::connect_stream(
            stream,
            cue_protocol::ClientId::new("tui-regression").unwrap(),
        )
        .await
        .unwrap()
        .into_multiplexed();
        (Arc::new(client), server)
    }

    async fn completed(pending: &mut tokio::task::JoinSet<Result<ResultPayload>>) -> ResultPayload {
        tokio::time::timeout(Duration::from_secs(3), pending.join_next())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .unwrap()
    }

    #[tokio::test]
    async fn dispatch_keeps_queries_help_and_errors_read_only_and_tails_the_suffix() {
        let (client, server) = client().await;
        let mut state = State::default();
        let mut pending = tokio::task::JoinSet::new();
        let hash = process_scope().unwrap().compute_hash();
        for source in [
            ":jobs",
            ":help",
            ":log E999",
            ":wait E999",
            ":out E999",
            ":cancel",
        ] {
            assert!(!dispatch(&client, &mut state, source, &mut pending).unwrap());
            while !pending.is_empty() {
                let _ = tokio::time::timeout(Duration::from_secs(3), pending.join_next())
                    .await
                    .unwrap();
            }
            assert!(
                client.query(Query::GetScope { hash }).await.is_err(),
                "{source}"
            );
        }
        dispatch(
            &client,
            &mut state,
            "/usr/bin/printf abcdefghij",
            &mut pending,
        )
        .unwrap();
        let result = completed(&mut pending).await;
        let ResultPayload::ExecutionSubmitted { ref execution } = result else {
            panic!("submit")
        };
        let id = execution.snapshot.id;
        apply_result(&client, &mut state, result).await.unwrap();
        client.query(Query::WaitExecution { id }).await.unwrap();
        state.log.clear();
        dispatch(&client, &mut state, &format!(":tail {id} 4"), &mut pending).unwrap();
        apply_result(&client, &mut state, completed(&mut pending).await)
            .await
            .unwrap();
        assert_eq!(state.log, vec!["ghij"]);
        drop(client);
        server.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn waiting_leaves_editing_cancel_and_escape_responsive() {
        let (client, server) = client().await;
        let mut state = State::default();
        let mut pending = tokio::task::JoinSet::new();
        dispatch(&client, &mut state, "/bin/sleep 30", &mut pending).unwrap();
        let ResultPayload::ExecutionSubmitted { execution } = completed(&mut pending).await else {
            panic!("submit")
        };
        let id = execution.snapshot.id;
        state.input = format!(":wait {id}");
        let key = |code| Event::Key(crossterm::event::KeyEvent::new(code, KeyModifiers::NONE));
        assert!(!handle_event(key(KeyCode::Enter), &client, &mut state, &mut pending).unwrap());
        assert!(
            tokio::time::timeout(Duration::from_millis(30), pending.join_next())
                .await
                .is_err()
        );
        assert!(!handle_event(key(KeyCode::Char('x')), &client, &mut state, &mut pending).unwrap());
        assert_eq!(state.input, "x");
        assert!(handle_event(key(KeyCode::Esc), &client, &mut state, &mut pending).unwrap());
        state.input = format!(":cancel {id}");
        assert!(!handle_event(key(KeyCode::Enter), &client, &mut state, &mut pending).unwrap());
        for _ in 0..2 {
            let _ = completed(&mut pending).await;
        }
        drop(client);
        server.await.unwrap().unwrap();
    }

    #[test]
    fn fact_summary_uses_execution_identity() {
        assert_eq!(
            fact_summary(&Fact::ExecutionFinished {
                id: ExecutionId(7),
                state: cue_core::ExecutionState::Succeeded,
            }),
            "E7: Succeeded"
        );
    }
}
