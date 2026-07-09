//! Thin debug-control client used by `cue-tui debug ...`.

use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use cue_core::tui_debug::{
    TuiDebugFrameEvent, TuiDebugOkPayload, TuiDebugRequest, TuiDebugRequestBody, TuiDebugResponse,
    TuiDebugResponseBody,
};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DebugCliCommand {
    Capture { styled: bool },
    SendKeys { keys: Vec<String> },
    WriteChars { text: String },
    State,
    Subscribe { styled: bool },
}

pub(crate) fn run_debug_command(socket_path: PathBuf, command: DebugCliCommand) -> Result<()> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("build tokio runtime for debug client")?;
    rt.block_on(run_debug_command_async(socket_path, command))
}

async fn run_debug_command_async(socket_path: PathBuf, command: DebugCliCommand) -> Result<()> {
    let mut stream = UnixStream::connect(&socket_path)
        .await
        .with_context(|| format!("connect to debug socket {}", socket_path.display()))?;

    match command {
        DebugCliCommand::Subscribe { styled } => {
            let request = TuiDebugRequest {
                id: 1,
                body: TuiDebugRequestBody::Subscribe { styled },
            };
            write_request(&mut stream, &request).await?;
            let (reader, _) = stream.into_split();
            let mut lines = BufReader::new(reader).lines();
            let mut saw_ack = false;
            while let Some(line) = lines.next_line().await? {
                if line.trim().is_empty() {
                    continue;
                }
                if !saw_ack {
                    let response: TuiDebugResponse =
                        serde_json::from_str(&line).context("decode subscribe acknowledgement")?;
                    ensure_ok_response(&response)?;
                    saw_ack = true;
                    continue;
                }
                let event: TuiDebugFrameEvent =
                    serde_json::from_str(&line).context("decode frame event")?;
                print_frame_event(&event, styled);
            }
            if !saw_ack {
                bail!("debug subscribe ended before acknowledgement");
            }
        }
        other => {
            let (request, styled) = request_for_command(other);
            let response = exchange(&mut stream, &request).await?;
            print_ok_response(&response, styled)?;
        }
    }

    Ok(())
}

fn request_for_command(command: DebugCliCommand) -> (TuiDebugRequest, bool) {
    match command {
        DebugCliCommand::Capture { styled } => (
            TuiDebugRequest {
                id: 1,
                body: TuiDebugRequestBody::Capture { styled },
            },
            styled,
        ),
        DebugCliCommand::SendKeys { keys } => (
            TuiDebugRequest {
                id: 1,
                body: TuiDebugRequestBody::SendKeys { keys },
            },
            false,
        ),
        DebugCliCommand::WriteChars { text } => (
            TuiDebugRequest {
                id: 1,
                body: TuiDebugRequestBody::WriteChars { text },
            },
            false,
        ),
        DebugCliCommand::State => (
            TuiDebugRequest {
                id: 1,
                body: TuiDebugRequestBody::State,
            },
            false,
        ),
        DebugCliCommand::Subscribe { styled } => (
            TuiDebugRequest {
                id: 1,
                body: TuiDebugRequestBody::Subscribe { styled },
            },
            styled,
        ),
    }
}

async fn exchange(stream: &mut UnixStream, request: &TuiDebugRequest) -> Result<TuiDebugResponse> {
    write_request(stream, request).await?;
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    reader
        .read_line(&mut line)
        .await
        .context("read debug response")?;
    serde_json::from_str(&line).context("decode debug response")
}

async fn write_request(stream: &mut UnixStream, request: &TuiDebugRequest) -> Result<()> {
    let json = serde_json::to_string(request).context("encode debug request")?;
    stream
        .write_all(json.as_bytes())
        .await
        .context("write debug request")?;
    stream
        .write_all(b"\n")
        .await
        .context("write debug request newline")?;
    stream.flush().await.context("flush debug request")?;
    Ok(())
}

fn ensure_ok_response(response: &TuiDebugResponse) -> Result<()> {
    match &response.body {
        TuiDebugResponseBody::Ok { .. } => Ok(()),
        TuiDebugResponseBody::Err { err } => {
            bail!("debug error [{}]: {}", err.code, err.message)
        }
    }
}

fn print_ok_response(response: &TuiDebugResponse, styled: bool) -> Result<()> {
    ensure_ok_response(response)?;
    match &response.body {
        TuiDebugResponseBody::Ok { ok } => match ok {
            TuiDebugOkPayload::Ack {} => {}
            TuiDebugOkPayload::Capture(capture) => {
                if styled {
                    if let Some(styled_text) = &capture.styled {
                        print!("{styled_text}");
                    } else {
                        print!("{}", capture.text);
                    }
                } else {
                    println!("{}", capture.text);
                }
            }
            TuiDebugOkPayload::State(state) => {
                println!("{}", serde_json::to_string_pretty(state)?);
            }
        },
        TuiDebugResponseBody::Err { .. } => unreachable!("checked above"),
    }
    Ok(())
}

fn print_frame_event(event: &TuiDebugFrameEvent, styled: bool) {
    if styled && let Some(styled_text) = &event.styled {
        print!("{styled_text}");
        return;
    }
    println!("{}", event.text);
}
