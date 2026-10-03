//! Thin IPC v5 command-line frontend.

use std::collections::VecDeque;
use std::ffi::OsString;
use std::future::Future;
use std::io::{Read as _, Write as _};
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;

use anyhow::{Context as _, Result, bail};
use cue_core::{CancelMode, Fact, OutputStream};
use cue_core::{ExecutionId, StepId};
use cue_language::Mode;
use cue_protocol::{AttachmentId, Command, EventPayload, OutputRange, Query, ResultPayload};

use crate::default_socket_path;
use crate::execution::{
    ExecutionClient, MultiplexedClient, SurfaceOutcome, output_bytes, process_scope, wait_execution,
};
use crate::script_runner::{
    execution_exit_code, warn_missing_output_prefix, write_execution_output,
};

#[derive(Debug, Clone, PartialEq, Eq)]
enum ClientCommand {
    Help,
    Version,
    Run(PathBuf),
    Exec(String),
    RunNeeds {
        path: PathBuf,
        needs: std::collections::BTreeMap<String, String>,
    },
    ExecNeeds {
        source: String,
        needs: std::collections::BTreeMap<String, String>,
    },
    Resources {
        providers: bool,
    },
    List,
    Show(ExecutionId),
    Wait(ExecutionId),
    Output {
        step: StepId,
        stream: OutputStream,
    },
    Cancel {
        id: ExecutionId,
        force: bool,
    },
    Foreground {
        step: StepId,
        observe: bool,
    },
    Restart,
    Shutdown,
}

pub fn run() -> Result<()> {
    match parse_command(std::env::args_os())? {
        ClientCommand::Help => print_help(),
        ClientCommand::Version => println!("cue-client {}", env!("CARGO_PKG_VERSION")),
        ClientCommand::Run(path) => std::process::exit(crate::script_runner::run(path)?),
        command => {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .context("build client runtime")?;
            let code = runtime.block_on(run_connected(command))?;
            if code != 0 {
                std::process::exit(code);
            }
        }
    }
    Ok(())
}

async fn run_connected(command: ClientCommand) -> Result<i32> {
    let socket = std::env::var_os("CUE_SOCKET")
        .map(PathBuf::from)
        .unwrap_or_else(default_socket_path);
    let mut client = ExecutionClient::connect(&socket).await?;
    match command {
        ClientCommand::RunNeeds { path, needs } => {
            crate::script_runner::run_with_needs(path, needs).await
        }
        ClientCommand::ExecNeeds { source, needs } => {
            let scope = process_scope()?;
            let cue_language::SurfaceCommand::Submit(spec) =
                cue_language::compile_command(&source, Mode::Job, scope.compute_hash())?
            else {
                bail!("--need requires executable Cue source")
            };
            client.put_scope(scope).await?;
            let submitted = client.submit_with_needs(spec, needs).await?;
            let execution = wait_execution(&mut client, submitted.snapshot.id).await?;
            write_execution_output(&mut client, &execution).await?;
            Ok(execution_exit_code(&execution))
        }
        ClientCommand::Resources { providers } => {
            print_json(
                client
                    .resource_query(
                        if providers { "providers" } else { "list" },
                        serde_json::json!({}),
                    )
                    .await?,
            )?;
            Ok(0)
        }
        ClientCommand::Exec(source) => {
            match client
                .execute_surface(process_scope()?, &source, Mode::Job)
                .await?
            {
                SurfaceOutcome::Response(ResultPayload::ExecutionSubmitted { execution }) => {
                    let execution = wait_execution(&mut client, execution.snapshot.id).await?;
                    write_execution_output(&mut client, &execution).await?;
                    Ok(execution_exit_code(&execution))
                }
                SurfaceOutcome::Response(result) => {
                    print_json(result)?;
                    Ok(0)
                }
                SurfaceOutcome::Frontend(action) => {
                    bail!("frontend-only action {action:?} is invalid in non-interactive mode")
                }
            }
        }
        ClientCommand::List => {
            print_json(
                client
                    .query(Query::ListExecutions {
                        before: None,
                        limit: 100,
                    })
                    .await?,
            )?;
            Ok(0)
        }
        ClientCommand::Show(id) => {
            let mut result = serde_json::to_value(client.query(Query::GetExecution { id }).await?)?;
            result["resources"] = client
                .resource_query("show", serde_json::json!({"execution":id}))
                .await?;
            print_json(result)?;
            Ok(0)
        }
        ClientCommand::Wait(id) => {
            let execution = wait_execution(&mut client, id).await?;
            print_json(&execution)?;
            Ok(execution_exit_code(&execution))
        }
        ClientCommand::Output { step, stream } => {
            let response = client
                .query(Query::ReadOutput {
                    step,
                    stdout: selected_range(stream == OutputStream::Stdout),
                    stderr: selected_range(stream == OutputStream::Stderr),
                    terminal: selected_range(stream == OutputStream::Terminal),
                })
                .await?;
            let ResultPayload::Output { chunks } = response else {
                bail!("daemon returned an unexpected output response")
            };
            let chunks = chunks
                .into_iter()
                .filter(|chunk| chunk.stream == stream)
                .collect::<Vec<_>>();
            warn_missing_output_prefix(&chunks);
            std::io::stdout().write_all(&output_bytes(&chunks, stream))?;
            Ok(0)
        }
        ClientCommand::Cancel { id, force } => {
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
            Ok(0)
        }
        ClientCommand::Foreground { step, observe } => foreground(client, step, observe).await,
        ClientCommand::Restart => {
            print_json(client.command(Command::Restart).await?)?;
            Ok(0)
        }
        ClientCommand::Shutdown => {
            client.command(Command::Shutdown).await?;
            Ok(0)
        }
        ClientCommand::Help | ClientCommand::Version | ClientCommand::Run(_) => unreachable!(),
    }
}

fn selected_range(selected: bool) -> OutputRange {
    OutputRange {
        offset: 0,
        max_bytes: if selected { 16 * 1024 * 1024 } else { 0 },
    }
}

async fn foreground(mut client: ExecutionClient, step: StepId, observe: bool) -> Result<i32> {
    client
        .command(Command::WatchExecution {
            id: step.execution,
            after_event: None,
        })
        .await?;
    let attached = client
        .command(Command::AttachPty {
            step,
            replay_bytes: 64 * 1024,
        })
        .await?;
    let ResultPayload::PtyAttached {
        attachment,
        snapshot,
        ..
    } = attached
    else {
        bail!("daemon returned an unexpected PTY attach response")
    };
    std::io::stdout().write_all(&snapshot)?;
    std::io::stdout().flush()?;
    if !observe {
        client
            .command(Command::ClaimPtyControl { attachment })
            .await?;
    }
    let client = Arc::new(client.into_multiplexed());
    let _raw = (!observe).then(TerminalRawMode::enter).transpose()?;
    forward_terminal(client, step, attachment, (!observe).then(terminal_input)).await
}

type TerminalInput = tokio::sync::mpsc::Receiver<std::io::Result<Vec<u8>>>;
type PendingPtyInput = Pin<Box<dyn Future<Output = Result<ResultPayload>> + Send>>;
const PENDING_INPUT_LIMIT: usize = 64 * 1024;

fn terminal_input() -> TerminalInput {
    let (sender, receiver) = tokio::sync::mpsc::channel(8);
    // A blocking stdin read cannot be cancelled. Keep it outside Tokio's
    // blocking pool so an idle terminal cannot hold runtime shutdown open.
    std::thread::spawn(move || {
        let mut stdin = std::io::stdin().lock();
        let mut bytes = [0; 1024];
        loop {
            let result = match stdin.read(&mut bytes) {
                Ok(0) => break,
                Ok(count) => Ok(bytes[..count].to_vec()),
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(error) => Err(error),
            };
            let failed = result.is_err();
            if sender.blocking_send(result).is_err() || failed {
                break;
            }
        }
    });
    receiver
}

async fn forward_terminal(
    client: Arc<MultiplexedClient>,
    step: StepId,
    attachment: AttachmentId,
    mut input: Option<TerminalInput>,
) -> Result<i32> {
    let mut queued = VecDeque::<Vec<u8>>::new();
    let mut queued_bytes = 0;
    let mut pending: Option<PendingPtyInput> = None;
    loop {
        if pending.is_none()
            && let Some(data) = queued.pop_front()
        {
            queued_bytes -= data.len();
            let client = client.clone();
            pending = Some(Box::pin(async move {
                client.command(Command::PtyInput { attachment, data }).await
            }));
        }
        tokio::select! {
            result = async { pending.as_mut().unwrap().await }, if pending.is_some() => {
                pending = None;
                result?;
            }
            event = client.next_event() => {
                match event {
                    Some(EventPayload::PtyOutput { attachment: event_attachment, data, .. })
                        if event_attachment == attachment => {
                            std::io::stdout().write_all(&data)?;
                            std::io::stdout().flush()?;
                        }
                    Some(EventPayload::PtyDetached { attachment: event_attachment, .. })
                        if event_attachment == attachment => return Ok(0),
                    Some(EventPayload::Fact(event))
                        if matches!(event.fact, Fact::ExecutionFinished { id, .. } if id == step.execution) => {
                            return Ok(0);
                        }
                    Some(_) => {}
                    None => bail!("daemon disconnected while PTY was attached"),
                }
            }
            read = async { input.as_mut().unwrap().recv().await }, if input.is_some() => {
                let data = read.transpose().context("read terminal input")?;
                if data.as_ref().is_none_or(|data| data.contains(&0x1d)) {
                    detach_terminal(&client, attachment, &mut pending).await?;
                    return Ok(0);
                }
                let data = data.unwrap();
                if data.len() > PENDING_INPUT_LIMIT - queued_bytes {
                    detach_terminal(&client, attachment, &mut pending).await?;
                    bail!("PTY input buffer filled; detached because the process is not reading input");
                }
                queued_bytes += data.len();
                queued.push_back(data);
            }
        }
    }
}

async fn detach_terminal(
    client: &MultiplexedClient,
    attachment: AttachmentId,
    pending: &mut Option<PendingPtyInput>,
) -> Result<()> {
    let detach = client.command(Command::DetachPty { attachment });
    tokio::pin!(detach);
    loop {
        tokio::select! {
            result = &mut detach => return result.map(|_| ()),
            _ = async { pending.as_mut().unwrap().await }, if pending.is_some() => {
                // Finish sending any partially written frame before Detach can
                // acquire the writer; lease revocation releases a blocked reply.
                *pending = None;
            }
        }
    }
}

struct TerminalRawMode;

impl TerminalRawMode {
    fn enter() -> Result<Self> {
        crossterm::terminal::enable_raw_mode().context("enable terminal raw mode")?;
        Ok(Self)
    }
}

impl Drop for TerminalRawMode {
    fn drop(&mut self) {
        let _ = crossterm::terminal::disable_raw_mode();
    }
}

fn parse_command(args: impl IntoIterator<Item = OsString>) -> Result<ClientCommand> {
    let mut args = args.into_iter();
    let _program = args.next();
    let Some(command) = args.next() else {
        return Ok(ClientCommand::Help);
    };
    let command = command
        .into_string()
        .map_err(|_| anyhow::anyhow!("cue-client command must be valid UTF-8"))?;
    match command.as_str() {
        "help" | "-h" | "--help" => no_args(args, ClientCommand::Help),
        "version" | "-V" | "--version" => no_args(args, ClientCommand::Version),
        "run" | "exec" => parse_submission(&command, args),
        "resources" | "providers" => {
            let rest = args.collect::<Vec<_>>();
            if !rest.is_empty() && rest != [OsString::from("--json")] {
                bail!("expected optional --json")
            }
            Ok(ClientCommand::Resources {
                providers: command == "providers",
            })
        }
        "list" => no_args(args, ClientCommand::List),
        "show" => Ok(ClientCommand::Show(parse_one(
            args,
            "show",
            "execution ID",
        )?)),
        "wait" => Ok(ClientCommand::Wait(parse_one(
            args,
            "wait",
            "execution ID",
        )?)),
        "out" | "err" | "terminal" => Ok(ClientCommand::Output {
            step: parse_one(args, &command, "step ID")?,
            stream: match command.as_str() {
                "out" => OutputStream::Stdout,
                "err" => OutputStream::Stderr,
                _ => OutputStream::Terminal,
            },
        }),
        "cancel" | "kill" => Ok(ClientCommand::Cancel {
            id: parse_one(args, &command, "execution ID")?,
            force: command == "kill",
        }),
        "fg" => parse_foreground(args),
        "restart" => no_args(args, ClientCommand::Restart),
        "shutdown" => no_args(args, ClientCommand::Shutdown),
        "session" | "cron" | "retry" => bail!(
            "`{command}` is not owned by the Cue execution kernel; use an external producer or orchestration layer"
        ),
        _ => bail!("unknown cue-client command `{command}`"),
    }
}

fn parse_submission(command: &str, args: impl Iterator<Item = OsString>) -> Result<ClientCommand> {
    let mut needs = std::collections::BTreeMap::new();
    let mut rest = Vec::new();
    let mut args = args.peekable();
    let mut literal = false;
    while let Some(arg) = args.next() {
        if !literal && arg == "--" {
            literal = true;
            continue;
        }
        if !literal && arg == "--need" {
            let pair = args
                .next()
                .context("--need expects KEY=QUANTITY")?
                .into_string()
                .map_err(|_| anyhow::anyhow!("need must be UTF-8"))?;
            let (key, value) = pair
                .split_once('=')
                .context("--need expects KEY=QUANTITY")?;
            if key.is_empty()
                || value.is_empty()
                || needs.insert(key.to_owned(), value.to_owned()).is_some()
            {
                bail!("empty or duplicate resource need")
            }
        } else {
            rest.push(arg)
        }
    }
    if command == "run" {
        let path = PathBuf::from(one_string(rest.into_iter(), "run", "a .cue file")?);
        if path.extension().and_then(|v| v.to_str()) != Some("cue") {
            bail!("cue-client run expects a .cue file")
        }
        return Ok(if needs.is_empty() {
            ClientCommand::Run(path)
        } else {
            ClientCommand::RunNeeds { path, needs }
        });
    }
    let source = if literal && rest.len() > 1 {
        rest.into_iter()
            .map(|s| {
                s.into_string()
                    .map_err(|_| anyhow::anyhow!("source must be UTF-8"))
                    .and_then(|s| serde_json::to_string(&s).map_err(Into::into))
            })
            .collect::<Result<Vec<_>>>()?
            .join(" ")
    } else {
        one_string(rest.into_iter(), "exec", "Cue source")?
    };
    Ok(if needs.is_empty() {
        ClientCommand::Exec(source)
    } else {
        ClientCommand::ExecNeeds { source, needs }
    })
}

fn parse_foreground(args: impl IntoIterator<Item = OsString>) -> Result<ClientCommand> {
    let mut step = None;
    let mut observe = false;
    for argument in args {
        match argument.to_str() {
            Some("--observe") if !observe => observe = true,
            Some(value) if value.starts_with('-') => bail!("unknown fg option `{value}`"),
            Some(value) if step.is_none() => step = Some(value.parse::<StepId>()?),
            Some(_) => bail!("fg accepts one step ID"),
            None => bail!("step ID must be valid UTF-8"),
        }
    }
    Ok(ClientCommand::Foreground {
        step: step.ok_or_else(|| anyhow::anyhow!("fg expects a step ID such as E1/S1"))?,
        observe,
    })
}

fn no_args(
    mut args: impl Iterator<Item = OsString>,
    command: ClientCommand,
) -> Result<ClientCommand> {
    if args.next().is_some() {
        bail!("command does not accept extra arguments")
    }
    Ok(command)
}

fn one_string(
    mut args: impl Iterator<Item = OsString>,
    command: &str,
    expected: &str,
) -> Result<String> {
    let value = args
        .next()
        .ok_or_else(|| anyhow::anyhow!("`{command}` expects {expected}"))?
        .into_string()
        .map_err(|_| anyhow::anyhow!("{expected} must be valid UTF-8"))?;
    if args.next().is_some() {
        bail!("`{command}` accepts exactly one argument")
    }
    Ok(value)
}

fn parse_one<T>(args: impl Iterator<Item = OsString>, command: &str, expected: &str) -> Result<T>
where
    T: std::str::FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    one_string(args, command, expected)?
        .parse()
        .with_context(|| format!("parse {expected}"))
}

fn print_json(value: impl serde::Serialize) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(&value)?);
    Ok(())
}

fn print_help() {
    println!(
        "cue-client {}\n\nUsage:\n  cue-client run FILE.cue [--need KEY=QUANTITY]...\n  cue-client exec [--need KEY=QUANTITY]... -- SOURCE\n  cue-client resources|providers [--json]\n  cue-client list\n  cue-client show|wait EXECUTION\n  cue-client out|err|terminal STEP\n  cue-client cancel|kill EXECUTION\n  cue-client fg STEP [--observe]\n  cue-client restart|shutdown\n\nEnvironment:\n  CUE_SOCKET  Override the local cued socket\n\nPTY control: Ctrl-] detaches. Session, schedule, retry and approval policy are external owners. Resources are execution-scoped daemon extensions.",
        env!("CARGO_PKG_VERSION")
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(values: &[&str]) -> Vec<OsString> {
        values.iter().map(OsString::from).collect()
    }

    #[test]
    fn parser_exposes_only_kernel_commands() {
        assert_eq!(
            parse_command(args(&["cue-client", "show", "E7"])).unwrap(),
            ClientCommand::Show(ExecutionId(7))
        );
        assert_eq!(
            parse_command(args(&["cue-client", "fg", "E7/S2", "--observe"])).unwrap(),
            ClientCommand::Foreground {
                step: "E7/S2".parse().unwrap(),
                observe: true,
            }
        );
        assert!(parse_command(args(&["cue-client", "session", "list"])).is_err());
        assert!(parse_command(args(&["cue-client", "retry", "E1"])).is_err());
    }

    #[test]
    fn run_requires_exactly_one_cue_file() {
        assert!(parse_command(args(&["cue-client", "run", "script.cue"])).is_ok());
        assert!(parse_command(args(&["cue-client", "run", "script.sh"])).is_err());
        assert!(parse_command(args(&["cue-client", "run"])).is_err());
    }
    #[tokio::test]
    async fn foreground_can_detach_with_an_unacknowledged_input_write() {
        use cue_protocol::{
            ClientId, Message, PROTOCOL_VERSION, ProtocolErrorCode, ResponsePayload, encode_message,
        };
        use std::time::Duration;
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

        async fn message(peer: &mut tokio::io::DuplexStream) -> Message {
            let mut header = [0; 4];
            peer.read_exact(&mut header).await.unwrap();
            let mut body = vec![0; u32::from_be_bytes(header) as usize];
            peer.read_exact(&mut body).await.unwrap();
            serde_json::from_slice(&body).unwrap()
        }

        for exit in ["escape", "eof", "overflow"] {
            let (stream, mut peer) = tokio::io::duplex(4096);
            let (seen, input_seen) = tokio::sync::oneshot::channel();
            let server = tokio::spawn(async move {
                let Message::Query { request_id, .. } = message(&mut peer).await else {
                    panic!("expected Hello")
                };
                peer.write_all(
                    &encode_message(&Message::Response {
                        request_id,
                        payload: ResponsePayload::Ok(ResultPayload::Hello {
                            protocol_version: PROTOCOL_VERSION,
                            server_version: "test".into(),
                            instance_id: "foreground".into(),
                            capabilities: Vec::new(),
                        }),
                    })
                    .unwrap(),
                )
                .await
                .unwrap();
                let Message::Command {
                    request_id: input_request,
                    command: Command::PtyInput { .. },
                    ..
                } = message(&mut peer).await
                else {
                    panic!("expected PTY input")
                };
                seen.send(()).unwrap();
                let Message::Command {
                    request_id,
                    command: Command::DetachPty { .. },
                    ..
                } = message(&mut peer).await
                else {
                    panic!("detach must arrive before input acknowledgement")
                };
                for response in [
                    Message::Response {
                        request_id: input_request,
                        payload: ResponsePayload::error(
                            ProtocolErrorCode::Conflict,
                            "control released",
                        ),
                    },
                    Message::Response {
                        request_id,
                        payload: ResponsePayload::ack(),
                    },
                ] {
                    peer.write_all(&encode_message(&response).unwrap())
                        .await
                        .unwrap();
                }
            });
            let client = Arc::new(
                ExecutionClient::connect_stream(stream, ClientId::new("foreground").unwrap())
                    .await
                    .unwrap()
                    .into_multiplexed(),
            );
            let (sender, receiver) = tokio::sync::mpsc::channel(8);
            let foreground = tokio::spawn(forward_terminal(
                client,
                "E1/S1".parse().unwrap(),
                AttachmentId::new(1).unwrap(),
                Some(receiver),
            ));
            sender.send(Ok(vec![b'x'; 1024])).await.unwrap();
            tokio::time::timeout(Duration::from_secs(2), input_seen)
                .await
                .unwrap()
                .unwrap();
            match exit {
                "escape" => sender.send(Ok(vec![0x1d])).await.unwrap(),
                "overflow" => {
                    for _ in 0..=PENDING_INPUT_LIMIT / 1024 {
                        sender.send(Ok(vec![b'x'; 1024])).await.unwrap();
                    }
                }
                _ => {}
            }
            drop(sender);
            let result = tokio::time::timeout(Duration::from_secs(2), foreground)
                .await
                .unwrap()
                .unwrap();
            if exit == "overflow" {
                assert!(
                    result
                        .unwrap_err()
                        .to_string()
                        .contains("input buffer filled")
                );
            } else {
                assert_eq!(result.unwrap(), 0);
            }
            server.await.unwrap();
        }
    }
}
