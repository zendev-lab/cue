//! Thin IPC v4 command-line frontend.

use std::ffi::OsString;
use std::io::Write as _;
use std::path::PathBuf;

use anyhow::{Context as _, Result, bail};
use cue_core::{CancelMode, Fact, OutputStream};
use cue_core::{ExecutionId, StepId};
use cue_language::Mode;
use cue_protocol::{Command, EventPayload, OutputRange, Query, ResultPayload};

use crate::default_socket_path;
use crate::execution::{
    ExecutionClient, SurfaceOutcome, output_bytes, process_scope, wait_execution,
};
use crate::script_runner::{execution_exit_code, write_execution_output};

#[derive(Debug, Clone, PartialEq, Eq)]
enum ClientCommand {
    Help,
    Version,
    Run(PathBuf),
    Exec(String),
    List,
    Show(ExecutionId),
    Wait(ExecutionId),
    Output { step: StepId, stream: OutputStream },
    Cancel { id: ExecutionId, force: bool },
    Foreground { step: StepId, observe: bool },
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
            print_json(client.query(Query::GetExecution { id }).await?)?;
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
    if !observe {
        client
            .command(Command::ClaimPtyControl { attachment })
            .await?;
    }
    let client = client.into_multiplexed();
    let _raw = (!observe).then(TerminalRawMode::enter).transpose()?;
    let mut stdin = tokio::io::stdin();
    let mut input = [0u8; 1024];
    loop {
        tokio::select! {
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
            read = tokio::io::AsyncReadExt::read(&mut stdin, &mut input), if !observe => {
                let count = read.context("read terminal input")?;
                if count == 0 || input[..count].contains(&0x1d) {
                    client.command(Command::DetachPty { attachment }).await?;
                    return Ok(0);
                }
                client.command(Command::PtyInput {
                    attachment,
                    data: input[..count].to_vec(),
                }).await?;
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
        "run" => {
            let path = one_string(args, "run", "a .cue file")?;
            let path = PathBuf::from(path);
            if path.extension().and_then(|value| value.to_str()) != Some("cue") {
                bail!("`cue-client run` expects a .cue file")
            }
            Ok(ClientCommand::Run(path))
        }
        "exec" => Ok(ClientCommand::Exec(one_string(args, "exec", "Cue source")?)),
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
        "session" | "cron" | "retry" | "resources" => bail!(
            "`{command}` is not owned by the Cue execution kernel; use an external producer or orchestration layer"
        ),
        _ => bail!("unknown cue-client command `{command}`"),
    }
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
        "cue-client {}\n\nUsage:\n  cue-client run FILE.cue\n  cue-client exec SOURCE\n  cue-client list\n  cue-client show|wait EXECUTION\n  cue-client out|err|terminal STEP\n  cue-client cancel|kill EXECUTION\n  cue-client fg STEP [--observe]\n  cue-client restart|shutdown\n\nEnvironment:\n  CUE_SOCKET  Override the local cued socket\n\nPTY control: Ctrl-] detaches. Session, schedule, retry, resource and approval policy are external owners.",
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
}
