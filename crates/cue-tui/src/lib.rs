//! IPC v4 execution workbench: browse, inspect, follow output, and control jobs.

pub mod cli;
mod editor;
mod history;
mod requests;
mod terminal;
mod view;
mod workbench;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context as _, Result, bail};
use crossterm::event::{Event, KeyCode, KeyEventKind, KeyModifiers, MouseButton, MouseEventKind};
use cue_client::{ExecutionClient, MultiplexedClient, SurfaceOutcome, process_scope};
use cue_core::{CancelMode, Fact, IoMode};
use cue_language::{FrontendAction, Mode, SurfaceCommand, compile_command, compile_file};
use cue_protocol::{Command, EventPayload, ResultPayload};
use requests::{Kind, PendingRequests, list_executions};
use workbench::{Focus, State, Tab, leaves};

pub fn run_cli() -> Result<()> {
    cli::run()
}

async fn connect(socket: &std::path::Path) -> Result<Arc<MultiplexedClient>> {
    Ok(Arc::new(
        ExecutionClient::connect(socket).await?.into_multiplexed(),
    ))
}

pub async fn run(socket: PathBuf) -> Result<()> {
    let mut client = connect(&socket)
        .await
        .with_context(|| format!("connect to {}", socket.display()))?;
    let mut state = State::default();
    let mut pending = PendingRequests::default();
    match history::load() {
        Ok(history) => state.editor.history = history,
        Err(error) => state.log(format!("History unavailable: {error:#}")),
    }
    apply_result(
        &client,
        &mut state,
        &mut pending,
        list_executions(&client).await?,
    )?;
    let mut terminal = ratatui::init();
    let _restore = terminal::Restore;
    terminal::enable()?;
    let (events, mut key_rx) = terminal::Events::start();
    let mut poll = tokio::time::interval(Duration::from_millis(250));
    poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut tick = 0u64;
    let mut reconnect = tokio::task::JoinSet::new();
    let mut paginated = false;
    loop {
        terminal.draw(|frame| view::draw(frame, &mut state))?;
        tokio::select! {
            _ = poll.tick() => {
                tick = tick.wrapping_add(1);
                if state.connected {
                    pending.request_output(&client, &state);
                    if tick.is_multiple_of(3) {
                        pending.request_refresh(&client);
                        if state.sidebar.selected().is_some_and(|index| index >= 100) || (state.selection.is_some() && state.selected().is_none()) {
                            pending.inspect_selected(&client, &state);
                        }
                    }
                } else if reconnect.is_empty() && tick.is_multiple_of(8) {
                    let socket = socket.clone();
                    reconnect.spawn(async move { connect(&socket).await });
                }
            }
            completed = reconnect.join_next(), if !reconnect.is_empty() => {
                match completed.unwrap() {
                    Ok(Ok(reconnected)) => {
                        client = reconnected;
                        state.connected = true;
                        state.reset_output();
                        state.log("Reconnected; refreshing execution state");
                        pending.request_refresh(&client);
                    }
                    Ok(Err(error)) => state.notice = format!("Disconnected; retrying: {error:#}"),
                    Err(error) => state.notice = format!("Reconnect failed: {error}"),
                }
            }
            completed = pending.join_next(), if !pending.is_empty() => {
                let (kind, completed) = completed.unwrap();
                match completed {
                    Ok(Ok(result)) => {
                        match (kind, result) {
                            (Kind::Output(generation), ResultPayload::Output { chunks }) => { if generation == state.output_generation { state.apply_output(chunks); } },
                            (Kind::Inspect, ResultPayload::Execution { execution }) => state.merge_executions(vec![*execution]),
                            (Kind::Refresh | Kind::Page, ResultPayload::Executions { executions, next_before }) => {
                                if kind == Kind::Page { paginated = true; }
                                if kind == Kind::Page || !paginated { state.next_page = next_before; }
                                state.merge_executions(executions);
                            }
                            (_, result) => apply_result(&client, &mut state, &mut pending, result)?,
                        }
                    }
                    Ok(Err(error)) => state.log(format!("{error:#}")),
                    Err(error) => state.log(format!("Request failed: {error}")),
                }
                if kind == Kind::Refresh && pending.refresh_again { pending.request_refresh(&client); }
            }
            event = key_rx.recv() => {
                let Some(event) = event else { bail!("terminal input disconnected"); };
                if handle_event(event, &client, &mut state, &mut pending)? { break; }
            }
            event = client.next_event(), if state.connected => {
                match event {
                    Some(EventPayload::Fact(fact)) => { state.notice = fact_summary(&fact.fact); pending.request_refresh(&client); }
                    Some(EventPayload::ServerDraining { reason }) => state.log(format!("Daemon draining: {reason}")),
                    Some(_) => {},
                    None => {
                        state.connected = false;
                        pending = PendingRequests::default();
                        state.log("Daemon disconnected; showing last snapshot. Pending commands will not be resubmitted.");
                    }
                }
            }
        }
        for source in std::mem::take(&mut state.history_pending) {
            if let Err(error) = history::append(&source) {
                state.log(format!("Could not save history: {error:#}"));
            }
        }
        if let Some((step, observe)) = state.foreground.take() {
            match events.pause(&mut key_rx).await {
                Ok(()) => {
                    while key_rx.try_recv().is_ok() {}
                    if let Err(error) = terminal::passthrough(&socket, step, observe).await {
                        state.log(format!("{error:#}"));
                    }
                    // Fullscreen resize also invalidates the buffers without a cursor query.
                    terminal.resize(terminal.size()?.into())?;
                    pending.request_refresh(&client);
                }
                Err(error) => state.log(format!("{error:#}")),
            }
            events.resume();
        }
    }
    Ok(())
}

fn handle_event(
    event: Event,
    client: &Arc<MultiplexedClient>,
    state: &mut State,
    pending: &mut PendingRequests,
) -> Result<bool> {
    if let Event::Paste(text) = event {
        state.focus = Focus::Input;
        state.editor.insert(&text);
        return Ok(false);
    }
    if let Event::Mouse(mouse) = event {
        let position = ratatui::layout::Position::new(mouse.column, mouse.row);
        match mouse.kind {
            MouseEventKind::Down(MouseButton::Left) if state.sidebar_area.contains(position) => {
                let row = mouse.row.saturating_sub(state.sidebar_area.y + 1) as usize / 2
                    + state.sidebar.offset();
                if let Some(id) = state
                    .executions
                    .get(row)
                    .map(|execution| execution.snapshot.id)
                {
                    state.select(id);
                    state.focus = Focus::Sidebar;
                }
            }
            MouseEventKind::Down(MouseButton::Left) if state.tabs_area.contains(position) => {
                if let Some(tab) = state
                    .tab_hits
                    .iter()
                    .find(|(_, rect)| rect.contains(position))
                    .map(|(tab, _)| *tab)
                {
                    state.tab = tab;
                    state.scroll = 0;
                    state.horizontal = 0;
                    state.focus = Focus::Output;
                }
            }
            MouseEventKind::Down(MouseButton::Left) if state.output_area.contains(position) => {
                state.focus = Focus::Output
            }
            MouseEventKind::Down(MouseButton::Left) if state.input_area.contains(position) => {
                state.focus = Focus::Input
            }
            MouseEventKind::ScrollUp | MouseEventKind::ScrollDown => {
                let delta = if mouse.kind == MouseEventKind::ScrollUp {
                    -3
                } else {
                    3
                };
                if state.sidebar_area.contains(position) {
                    state.move_selection(delta);
                } else {
                    state.follow = false;
                    state.scroll = state.scroll.saturating_add_signed(delta);
                }
            }
            _ => {}
        }
        return Ok(false);
    }
    let Event::Key(key) = event else {
        return Ok(false);
    };
    if !matches!(key.kind, KeyEventKind::Press | KeyEventKind::Repeat) {
        return Ok(false);
    }
    if key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL) {
        return Ok(true);
    }
    if state.help {
        if matches!(key.code, KeyCode::Esc | KeyCode::F(1)) {
            state.help = false;
        }
        return Ok(false);
    }
    match key.code {
        KeyCode::F(1) => state.help = true,
        KeyCode::F(2) => {
            state.focus = Focus::Sidebar;
            state.sidebar_visible = true;
        }
        KeyCode::F(3) => state.focus = Focus::Output,
        KeyCode::F(4) => state.focus = Focus::Input,
        KeyCode::F(5) if state.connected => pending.request_refresh(client),
        KeyCode::Char('b') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            state.sidebar_visible = !state.sidebar_visible;
            if !state.sidebar_visible && state.focus == Focus::Sidebar {
                state.focus = Focus::Output;
            }
        }
        KeyCode::Char('y') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            match terminal::copy(&state.output_text()) {
                Ok(()) => state.log("Sent active view to terminal clipboard"),
                Err(error) => state.log(format!("Copy failed: {error:#}")),
            }
        }
        KeyCode::BackTab => {
            state.focus = match state.focus {
                Focus::Input => Focus::Sidebar,
                Focus::Sidebar => Focus::Output,
                Focus::Output => Focus::Input,
            }
        }
        KeyCode::Esc => {
            if !state.editor.candidates.is_empty() {
                state.editor.candidates.clear();
            } else if state.focus != Focus::Input {
                state.focus = Focus::Input;
            } else if state.editor.text.is_empty() {
                return Ok(true);
            } else {
                state.editor.take();
            }
        }
        code if state.focus == Focus::Input => match code {
            KeyCode::Enter if key.modifiers.contains(KeyModifiers::SHIFT) => {
                state.editor.insert("\n")
            }
            KeyCode::Enter => {
                let source = state.editor.take();
                if !source.trim().is_empty() {
                    if !state.connected {
                        state.editor.set(source);
                        state.log("Disconnected; command kept in input. Wait for reconnect before submitting.");
                    } else {
                        state.editor.remember(&source);
                        state.history_pending.push(source.clone());
                        if dispatch(client, state, &source, pending)? {
                            return Ok(true);
                        }
                    }
                }
            }
            KeyCode::Tab => {
                let ids = state
                    .executions
                    .iter()
                    .map(|execution| execution.snapshot.id.to_string())
                    .collect::<Vec<_>>();
                let steps = state
                    .executions
                    .iter()
                    .flat_map(|execution| {
                        execution
                            .snapshot
                            .steps
                            .iter()
                            .map(|step| step.id().to_string())
                    })
                    .collect::<Vec<_>>();
                if let Err(error) = state.editor.complete(&ids, &steps) {
                    state.log(format!("Completion failed: {error:#}"));
                }
            }
            _ => state.editor.key(code, key.modifiers),
        },
        KeyCode::Tab => {
            state.focus = if state.focus == Focus::Sidebar {
                Focus::Output
            } else {
                Focus::Input
            }
        }
        KeyCode::Up if state.focus == Focus::Sidebar => state.move_selection(-1),
        KeyCode::Down if state.focus == Focus::Sidebar => {
            if state.sidebar.selected() == state.executions.len().checked_sub(1) {
                pending.older(client, state);
            }
            state.move_selection(1);
        }
        KeyCode::Enter if state.focus == Focus::Sidebar => state.focus = Focus::Output,
        KeyCode::Char(ch @ '1'..='6') => {
            state.tab = Tab::ALL[(ch as u8 - b'1') as usize];
            state.scroll = 0;
            state.horizontal = 0;
        }
        KeyCode::Char('[') => state.move_step(-1),
        KeyCode::Char(']') => state.move_step(1),
        KeyCode::Home => {
            state.scroll = 0;
            state.follow = false;
        }
        KeyCode::End => state.follow = true,
        KeyCode::PageUp | KeyCode::Up => {
            state.follow = false;
            state.scroll = state.scroll.saturating_sub(if key.code == KeyCode::PageUp {
                state.output_area.height.saturating_sub(3) as usize
            } else {
                1
            });
        }
        KeyCode::PageDown | KeyCode::Down => {
            state.follow = false;
            state.scroll += if key.code == KeyCode::PageDown {
                state.output_area.height.saturating_sub(3) as usize
            } else {
                1
            };
        }
        KeyCode::Left => state.horizontal = state.horizontal.saturating_sub(4),
        KeyCode::Right => state.horizontal = state.horizontal.saturating_add(4),
        KeyCode::Delete | KeyCode::Char('K') if state.connected => {
            if let Some(id) = state.selection {
                let client = client.clone();
                let mode = if key.code == KeyCode::Char('K') {
                    CancelMode::Force
                } else {
                    CancelMode::Graceful
                };
                pending.commands.spawn(async move {
                    client.command(Command::CancelExecution { id, mode }).await
                });
                state.log(format!("Cancellation requested for {id}"));
            }
        }
        KeyCode::Char('f' | 'o') if state.connected => {
            if let Some(step) = state.step() {
                if state.selected().is_some_and(|execution| {
                    leaves(execution)
                        .get(state.step_index)
                        .is_some_and(|(_, io)| *io == Some(IoMode::Pty))
                }) {
                    state.foreground = Some((step, key.code == KeyCode::Char('o')));
                } else {
                    state.log("This step uses captured output; select a PTY step to attach.");
                }
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
    pending: &mut PendingRequests,
) -> Result<bool> {
    let scope = process_scope()?;
    let compiled = if source.contains('\n') {
        compile_file(source, scope.compute_hash()).map(SurfaceCommand::Submit)
    } else {
        compile_command(source, Mode::Job, scope.compute_hash())
    };
    let command = match compiled {
        Ok(command) => command,
        Err(error) => {
            state.log(error.to_string());
            state.editor.set(source.to_owned());
            return Ok(false);
        }
    };
    match command {
        SurfaceCommand::AttachPty {
            step,
            claim_control,
        } => state.foreground = Some((step, !claim_control)),
        SurfaceCommand::Frontend(FrontendAction::Clear) => {
            state.log.clear();
            state.tab = Tab::Activity;
        }
        SurfaceCommand::Frontend(FrontendAction::Quit) => return Ok(true),
        SurfaceCommand::Frontend(FrontendAction::Help { .. }) => state.help = true,
        command => {
            let requests = if matches!(command, SurfaceCommand::WaitExecution { .. }) {
                &mut pending.waits
            } else {
                &mut pending.commands
            };
            if requests.len() >= 64 {
                state.log("Too many pending requests; wait for one to finish");
                state.editor.set(source.to_owned());
                return Ok(false);
            }
            let client = client.clone();
            let is_wait = matches!(command, SurfaceCommand::WaitExecution { .. });
            requests.spawn(async move {
                let work = async {
                    if matches!(command, SurfaceCommand::Frontend(FrontendAction::Restart)) { return client.command(Command::Restart).await; }
                    let SurfaceOutcome::Response(result) = client.execute_compiled(scope, command).await? else { bail!("unexpected frontend action"); };
                    Ok(result)
                };
                if is_wait { work.await }
                else { tokio::time::timeout(Duration::from_secs(10), work).await.context("request timed out; outcome is unknown, inspect executions before retrying")? }
            });
        }
    }
    Ok(false)
}

fn apply_result(
    client: &Arc<MultiplexedClient>,
    state: &mut State,
    pending: &mut PendingRequests,
    result: ResultPayload,
) -> Result<()> {
    match result {
        ResultPayload::Executions {
            executions,
            next_before,
        } => {
            state.next_page = next_before;
            state.merge_executions(executions);
        }
        ResultPayload::ExecutionSubmitted { execution } => {
            let id = execution.snapshot.id;
            state.merge_executions(vec![*execution]);
            state.select(id);
            state.tab = Tab::Output;
            state.log(format!("Submitted {id}; output follows automatically"));
            pending.request_refresh(client);
        }
        ResultPayload::Execution { execution } => {
            let id = execution.snapshot.id;
            state.merge_executions(vec![*execution]);
            state.select(id);
            state.tab = Tab::Details;
        }
        ResultPayload::Output { chunks } => {
            if let Some(chunk) = chunks.first() {
                state.select(chunk.step.execution);
                let index = chunk.step.index.saturating_sub(1) as usize;
                if state.step_index != index {
                    state.step_index = index;
                    state.reset_output();
                }
                state.tab = match chunk.stream {
                    cue_core::OutputStream::Stdout => Tab::Stdout,
                    cue_core::OutputStream::Stderr => Tab::Stderr,
                    cue_core::OutputStream::Terminal => Tab::Terminal,
                };
                if state.selected().is_none() {
                    pending.inspect_selected(client, state);
                }
            }
            for chunk in &chunks {
                state.log.extend(
                    String::from_utf8_lossy(&chunk.data)
                        .lines()
                        .map(str::to_owned),
                );
            }
            state.apply_output(chunks);
        }
        ResultPayload::Ack => {
            state.notice = "Done; refreshing state".into();
            pending.request_refresh(client);
        }
        result => state.log(serde_json::to_string(&result)?),
    }
    Ok(())
}

fn fact_summary(fact: &Fact) -> String {
    match fact {
        Fact::ExecutionCreated { id, .. } => format!("Created {id}"),
        Fact::StepStateChanged { id, next, .. } => format!("{id}: {next:?}"),
        Fact::ExecutionStateChanged { id, next, .. } => format!("{id}: {next:?}"),
        Fact::OutputAppended { step, .. } => format!("Output from {step}"),
        Fact::ExecutionFinished { id, state } => format!("{id}: {state:?}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cue_core::ExecutionId;
    use cue_protocol::Query;
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

    async fn completed(pending: &mut PendingRequests) -> ResultPayload {
        tokio::time::timeout(Duration::from_secs(3), pending.join_next())
            .await
            .unwrap()
            .unwrap()
            .1
            .unwrap()
            .unwrap()
    }

    #[tokio::test]
    async fn dispatch_keeps_queries_help_and_errors_read_only_and_tails_the_suffix() {
        let (client, server) = client().await;
        let mut state = State::default();
        let mut pending = PendingRequests::default();
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
        apply_result(&client, &mut state, &mut pending, result).unwrap();
        while !pending.is_empty() {
            let _ = completed(&mut pending).await;
        }
        client.query(Query::WaitExecution { id }).await.unwrap();
        state.log.clear();
        dispatch(&client, &mut state, &format!(":tail {id} 4"), &mut pending).unwrap();
        let result = completed(&mut pending).await;
        apply_result(&client, &mut state, &mut pending, result).unwrap();
        assert_eq!(state.log, vec!["ghij"]);
        drop(client);
        server.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn waiting_leaves_editing_cancel_and_escape_responsive() {
        let (client, server) = client().await;
        let mut state = State::default();
        let mut pending = PendingRequests::default();
        dispatch(&client, &mut state, "/bin/sleep 30", &mut pending).unwrap();
        let ResultPayload::ExecutionSubmitted { execution } = completed(&mut pending).await else {
            panic!("submit")
        };
        let id = execution.snapshot.id;
        state.editor.text = format!(":wait {id}");
        let key = |code| Event::Key(crossterm::event::KeyEvent::new(code, KeyModifiers::NONE));
        assert!(!handle_event(key(KeyCode::Enter), &client, &mut state, &mut pending).unwrap());
        assert!(
            tokio::time::timeout(Duration::from_millis(30), pending.join_next())
                .await
                .is_err()
        );
        assert!(!handle_event(key(KeyCode::Char('x')), &client, &mut state, &mut pending).unwrap());
        assert_eq!(state.editor.text, "x");
        assert!(!handle_event(key(KeyCode::Esc), &client, &mut state, &mut pending).unwrap());
        assert!(state.editor.text.is_empty());
        assert!(handle_event(key(KeyCode::Esc), &client, &mut state, &mut pending).unwrap());
        state.editor.text = format!(":cancel {id}");
        assert!(!handle_event(key(KeyCode::Enter), &client, &mut state, &mut pending).unwrap());
        for _ in 0..2 {
            let _ = completed(&mut pending).await;
        }
        drop(client);
        server.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn full_wait_queue_still_accepts_cancel_and_refresh() {
        let (client, server) = client().await;
        let mut state = State::default();
        let mut pending = PendingRequests::default();
        dispatch(&client, &mut state, "/bin/sleep 30", &mut pending).unwrap();
        let ResultPayload::ExecutionSubmitted { execution } = completed(&mut pending).await else {
            panic!("submit")
        };
        let id = execution.snapshot.id;
        for _ in 0..64 {
            dispatch(&client, &mut state, &format!(":wait {id}"), &mut pending).unwrap();
        }
        assert_eq!(pending.waits.len(), 64);
        pending.request_refresh(&client);
        dispatch(&client, &mut state, &format!(":cancel {id}"), &mut pending).unwrap();
        assert_eq!(pending.commands.len(), 1);
        while !pending.is_empty() {
            let _ = completed(&mut pending).await;
        }
        let ResultPayload::Execution { execution } =
            client.query(Query::GetExecution { id }).await.unwrap()
        else {
            panic!("execution")
        };
        assert_eq!(execution.state, cue_core::ExecutionState::Cancelled);
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
