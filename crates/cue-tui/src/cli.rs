use std::ffi::OsString;
use std::path::PathBuf;

use anyhow::{Result, bail};

enum TuiCommand {
    Help,
    Version,
    Run { socket: PathBuf },
}

pub fn run() -> Result<()> {
    match parse(std::env::args_os().skip(1))? {
        TuiCommand::Help => print_help(),
        TuiCommand::Version => println!("cue-tui {}", env!("CARGO_PKG_VERSION")),
        TuiCommand::Run { socket } => {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?;
            runtime.block_on(crate::run(socket))?;
        }
    }
    Ok(())
}

fn parse(args: impl IntoIterator<Item = OsString>) -> Result<TuiCommand> {
    let mut args = args.into_iter();
    let mut socket = std::env::var_os("CUE_SOCKET").map(PathBuf::from);
    while let Some(argument) = args.next() {
        match argument.to_str() {
            Some("-h" | "--help" | "help") => {
                if args.next().is_some() {
                    bail!("help does not accept extra arguments")
                }
                return Ok(TuiCommand::Help);
            }
            Some("-V" | "--version" | "version") => {
                if args.next().is_some() {
                    bail!("version does not accept extra arguments")
                }
                return Ok(TuiCommand::Version);
            }
            Some("--socket") => {
                let value = args
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("--socket expects a path"))?;
                socket = Some(PathBuf::from(value));
            }
            Some(value) => bail!("unknown cue-tui argument `{value}`"),
            None => bail!("cue-tui arguments must be valid UTF-8"),
        }
    }
    Ok(TuiCommand::Run {
        socket: socket.unwrap_or_else(cue_client::default_socket_path),
    })
}

fn print_help() {
    println!(
        "cue-tui {}\n\nUsage:\n  cue-tui [--socket PATH]\n\nThe TUI submits typed IPC v4 executions from an explicit frontend Scope.\nSession, schedule, retry, resource, and approval policy are external owners.\n\nKeys:\n  Enter   compile and dispatch input\n  Esc     quit\n  Ctrl-C  quit",
        env!("CARGO_PKG_VERSION")
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn removed_session_flags_are_rejected() {
        assert!(parse([OsString::from("--session"), OsString::from("dev")]).is_err());
        assert!(parse([OsString::from("--session-refresh")]).is_err());
    }
}
