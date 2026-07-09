//! Unix-socket debug control server for external harnesses.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use cue_core::tui_debug::{
    TuiDebugError, TuiDebugFrameEvent, TuiDebugOkPayload, TuiDebugRequest, TuiDebugRequestBody,
    TuiDebugResponse, error_code,
};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::mpsc;
use tokio::time::{Duration, sleep};
use tracing::warn;

use crate::message::AppMsg;

use super::keys::{char_key_events, parse_key_tokens};
use super::snapshot::{
    SharedDebugSnapshots, capture_from_snapshots, state_from_snapshots, update_frame_snapshot,
};

#[derive(Clone)]
pub(crate) struct DebugControl {
    pub snapshots: SharedDebugSnapshots,
    pub inject_tx: mpsc::UnboundedSender<AppMsg>,
}

pub(crate) struct DebugServerHandle {
    shutdown_tx: mpsc::UnboundedSender<()>,
}

impl DebugServerHandle {
    pub(crate) async fn shutdown(self) {
        let _ = self.shutdown_tx.send(());
    }
}

pub(crate) fn spawn_debug_server(
    socket_path: PathBuf,
    control: DebugControl,
) -> Result<DebugServerHandle> {
    prepare_socket_path(&socket_path)?;
    let listener = UnixListener::bind(&socket_path)
        .with_context(|| format!("bind debug socket {}", socket_path.display()))?;
    restrict_socket_permissions(&socket_path)?;

    let (shutdown_tx, mut shutdown_rx) = mpsc::unbounded_channel();
    let control_for_accept = control.clone();

    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = shutdown_rx.recv() => break,
                accepted = listener.accept() => {
                    match accepted {
                        Ok((stream, _)) => {
                            let control = control_for_accept.clone();
                            tokio::spawn(handle_client(stream, control));
                        }
                        Err(error) => {
                            warn!(%error, "debug server accept failed");
                        }
                    }
                }
            }
        }
    });

    Ok(DebugServerHandle { shutdown_tx })
}

async fn handle_client(mut stream: UnixStream, control: DebugControl) {
    let (reader, mut writer) = stream.split();
    let mut lines = BufReader::new(reader).lines();

    while let Ok(Some(line)) = lines.next_line().await {
        if line.trim().is_empty() {
            continue;
        }
        let request = match serde_json::from_str::<TuiDebugRequest>(&line) {
            Ok(request) => request,
            Err(error) => {
                let response = TuiDebugResponse::err(
                    0,
                    TuiDebugError::invalid_request(format!("invalid JSON request: {error}")),
                );
                if write_response(&mut writer, &response).await.is_err() {
                    break;
                }
                continue;
            }
        };

        if let TuiDebugRequestBody::Subscribe { styled } = request.body {
            let ack = TuiDebugResponse::ok(request.id, TuiDebugOkPayload::Ack {});
            if write_response(&mut writer, &ack).await.is_err() {
                break;
            }
            if run_subscribe(&control, &mut writer, styled).await.is_err() {
                break;
            }
            break;
        }

        let response = dispatch_request(request, &control);
        if write_response(&mut writer, &response).await.is_err() {
            break;
        }
    }
}

fn dispatch_request(request: TuiDebugRequest, control: &DebugControl) -> TuiDebugResponse {
    match request.body {
        TuiDebugRequestBody::Capture { styled } => {
            match capture_from_snapshots(&control.snapshots, styled) {
                Ok(capture) => {
                    TuiDebugResponse::ok(request.id, TuiDebugOkPayload::Capture(capture))
                }
                Err(message) => {
                    TuiDebugResponse::err(request.id, TuiDebugError::unavailable(message))
                }
            }
        }
        TuiDebugRequestBody::SendKeys { keys } => match parse_key_tokens(&keys) {
            Ok(events) => {
                for event in events {
                    if control.inject_tx.send(AppMsg::KeyEvent(event)).is_err() {
                        return TuiDebugResponse::err(
                            request.id,
                            TuiDebugError::unavailable("cue-tui event loop is not running"),
                        );
                    }
                }
                TuiDebugResponse::ok(request.id, TuiDebugOkPayload::Ack {})
            }
            Err(error) => {
                TuiDebugResponse::err(request.id, TuiDebugError::invalid_request(error.message))
            }
        },
        TuiDebugRequestBody::WriteChars { text } => {
            for event in char_key_events(&text) {
                if control.inject_tx.send(AppMsg::KeyEvent(event)).is_err() {
                    return TuiDebugResponse::err(
                        request.id,
                        TuiDebugError::unavailable("cue-tui event loop is not running"),
                    );
                }
            }
            TuiDebugResponse::ok(request.id, TuiDebugOkPayload::Ack {})
        }
        TuiDebugRequestBody::State => match state_from_snapshots(&control.snapshots) {
            Ok(state) => TuiDebugResponse::ok(request.id, TuiDebugOkPayload::State(state)),
            Err(message) => TuiDebugResponse::err(
                request.id,
                TuiDebugError {
                    code: error_code::INTERNAL.into(),
                    message,
                },
            ),
        },
        TuiDebugRequestBody::Subscribe { .. } => TuiDebugResponse::err(
            request.id,
            TuiDebugError::invalid_request("subscribe must be handled by the server session loop"),
        ),
    }
}

async fn run_subscribe(
    control: &DebugControl,
    writer: &mut tokio::net::unix::WriteHalf<'_>,
    styled: bool,
) -> Result<()> {
    let mut last_plain = String::new();
    loop {
        let capture =
            capture_from_snapshots(&control.snapshots, styled).map_err(anyhow::Error::msg)?;
        if capture.text != last_plain {
            last_plain = capture.text.clone();
            let event = TuiDebugFrameEvent::frame(
                capture.text,
                capture.width,
                capture.height,
                capture.styled,
            );
            write_event(writer, &event).await?;
        }
        sleep(Duration::from_millis(50)).await;
    }
}

async fn write_response(
    writer: &mut tokio::net::unix::WriteHalf<'_>,
    response: &TuiDebugResponse,
) -> Result<()> {
    let json = serde_json::to_string(response).context("encode debug response")?;
    writer
        .write_all(json.as_bytes())
        .await
        .context("write debug response")?;
    writer
        .write_all(b"\n")
        .await
        .context("write debug newline")?;
    writer.flush().await.context("flush debug response")?;
    Ok(())
}

async fn write_event(
    writer: &mut tokio::net::unix::WriteHalf<'_>,
    event: &TuiDebugFrameEvent,
) -> Result<()> {
    let json = serde_json::to_string(event).context("encode debug event")?;
    writer
        .write_all(json.as_bytes())
        .await
        .context("write event")?;
    writer
        .write_all(b"\n")
        .await
        .context("write event newline")?;
    writer.flush().await.context("flush event")?;
    Ok(())
}

fn prepare_socket_path(socket_path: &Path) -> Result<()> {
    if socket_path.exists() {
        std::fs::remove_file(socket_path)
            .with_context(|| format!("remove stale debug socket {}", socket_path.display()))?;
    }
    if let Some(parent) = socket_path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create debug socket parent {}", parent.display()))?;
    }
    Ok(())
}

#[cfg(unix)]
fn restrict_socket_permissions(socket_path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let permissions = std::fs::Permissions::from_mode(0o600);
    std::fs::set_permissions(socket_path, permissions)
        .with_context(|| format!("chmod debug socket {}", socket_path.display()))?;
    Ok(())
}

#[cfg(not(unix))]
fn restrict_socket_permissions(_socket_path: &Path) -> Result<()> {
    Ok(())
}

pub(crate) fn record_frame_snapshot(control: &DebugControl, buffer: &ratatui::buffer::Buffer) {
    let text = super::buffer::frame_text_from_buffer(buffer);
    update_frame_snapshot(&control.snapshots, text);
}
