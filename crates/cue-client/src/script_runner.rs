use std::ffi::OsString;
use std::io::Write;
use std::path::PathBuf;

use crate::{CuedClient, ResolvedTransport, connect_ssh_transport, load_transport_config};
use anyhow::{Context, Result, bail};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use cue_core::execution::{ExecutionState, StepFailure, StepState};
use cue_core::ipc::{EventPayload, ExecutionInfo, Message, OkPayload, ResponsePayload, Stream};

use crate::daemon_lifecycle::{
    check_local_daemon_version, ensure_daemon_running, version_from_ping,
    warn_on_remote_version_mismatch,
};

pub fn run(path: PathBuf, session_refresh_flag: bool) -> Result<i32> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .with_writer(std::io::stderr)
        .init();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("build tokio runtime")?;

    rt.block_on(async_run(path, session_refresh_flag))
}

async fn async_run(path: PathBuf, session_refresh_flag: bool) -> Result<i32> {
    let input = std::fs::read_to_string(&path)
        .with_context(|| format!("read .cue script `{}`", path.display()))?;
    let display_path = path.display().to_string();
    let selector = cue_session_selector(std::env::var_os("CUE_SESSION"))?;
    let refresh_if_needed =
        session_refresh_flag || cue_session_refresh(std::env::var_os("CUE_SESSION_REFRESH"))?;
    if refresh_if_needed && selector.is_none() {
        bail!("session refresh requires CUE_SESSION to select a named session");
    }
    let mut client = connect_for_script().await?;
    run_in_session_with_client(
        &mut client,
        &display_path,
        &input,
        selector,
        refresh_if_needed,
    )
    .await
}

async fn run_in_session_with_client(
    client: &mut CuedClient,
    path: &str,
    input: &str,
    selector: Option<String>,
    refresh_if_needed: bool,
) -> Result<i32> {
    if let Some(selector) = selector {
        let attach = client
            .attach_session_with_refresh_if_needed(&selector, refresh_if_needed)
            .await;
        if refresh_if_needed {
            attach.with_context(|| {
                format!("attach cue script to session `{selector}` with explicit restart recovery")
            })?;
        } else {
            attach.with_context(|| {
                format!(
                    "attach cue script to session `{selector}`; if it reports needs_refresh after a daemon restart, rerun with `--session-refresh` or `CUE_SESSION_REFRESH=1` to explicitly replace its scope from this process environment"
                )
            })?;
        }
    }
    run_with_client(client, path, input).await
}

fn cue_session_selector(value: Option<OsString>) -> Result<Option<String>> {
    let Some(value) = value.filter(|value| !value.is_empty()) else {
        return Ok(None);
    };
    value
        .into_string()
        .map(Some)
        .map_err(|_| anyhow::anyhow!("CUE_SESSION must be valid UTF-8"))
}

fn cue_session_refresh(value: Option<OsString>) -> Result<bool> {
    let Some(value) = value else {
        return Ok(false);
    };
    let value = value
        .into_string()
        .map_err(|_| anyhow::anyhow!("CUE_SESSION_REFRESH must be valid UTF-8"))?;
    match value.as_str() {
        "1" | "true" => Ok(true),
        "0" | "false" => Ok(false),
        _ => bail!("CUE_SESSION_REFRESH must be one of: 1, true, 0, false"),
    }
}

async fn connect_for_script() -> Result<CuedClient> {
    let transport = load_transport_config()?
        .resolve_transport(std::env::var_os("CUE_SOCKET").map(PathBuf::from))?;
    match transport {
        ResolvedTransport::Unix { socket_path, .. } => {
            let client = ensure_daemon_running(&socket_path).await.ok_or_else(|| {
                anyhow::anyhow!("cued is not available at {}", socket_path.display())
            })?;
            check_local_daemon_version(Some(client), &socket_path)
                .await
                .ok_or_else(|| {
                    anyhow::anyhow!("cued is not available at {}", socket_path.display())
                })
        }
        ssh_transport @ ResolvedTransport::Ssh { .. } => {
            let (client, daemon_version) = connect_ssh_transport(&ssh_transport).await?;
            warn_on_remote_version_mismatch(version_from_ping(daemon_version));
            Ok(client)
        }
    }
}

async fn run_with_client(client: &mut CuedClient, path: &str, input: &str) -> Result<i32> {
    let spec = cue_language::compile_file(input, path)
        .with_context(|| format!("compile .cue script `{path}`"))?;
    let submit_id = client.submit_execution(spec).await?;
    let mut wait_id = None;

    loop {
        match client.recv().await? {
            Message::Response { id, payload } if id == submit_id => match payload {
                ResponsePayload::Ok(OkPayload::ExecutionCreated { execution }) => {
                    wait_id = Some(client.wait_execution(execution.id).await?);
                }
                ResponsePayload::Err { code, message } => {
                    bail!("cue run failed [{code}]: {message}");
                }
                other => bail!("unexpected cue run response: {other:?}"),
            },
            Message::Response { id, payload } if wait_id == Some(id) => match payload {
                ResponsePayload::Ok(OkPayload::ExecutionInfo(execution)) => {
                    return Ok(execution_exit_code(&execution));
                }
                ResponsePayload::Err { code, message } => {
                    bail!("cue run wait failed [{code}]: {message}");
                }
                other => bail!("unexpected cue run wait response: {other:?}"),
            },
            Message::Response { .. } => {}
            Message::Request { .. } => {
                bail!("unexpected request message from cued");
            }
            Message::Event { payload } => match payload {
                EventPayload::OutputChunk { stream, data, .. } => {
                    write_stream(stream, data.as_bytes())?;
                }
                EventPayload::OutputChunkBinary { stream, base64, .. } => {
                    let bytes = decode_binary_output_chunk(&base64)?;
                    write_stream(stream, &bytes)?;
                }
                _ => {}
            },
        }
    }
}

fn execution_exit_code(execution: &ExecutionInfo) -> i32 {
    match execution.state {
        ExecutionState::Succeeded => 0,
        ExecutionState::Failed => execution
            .steps
            .iter()
            .find_map(|step| match &step.state {
                StepState::Failed {
                    failure: StepFailure::Exit { code },
                } => Some(*code),
                StepState::Failed { .. } => Some(1),
                _ => None,
            })
            .unwrap_or(1),
        ExecutionState::Cancelled { .. } => 130,
        ExecutionState::Queued | ExecutionState::Running => 1,
    }
}

fn write_stream(stream: Stream, bytes: &[u8]) -> Result<()> {
    match stream {
        Stream::Stdout => std::io::stdout().write_all(bytes)?,
        Stream::Stderr => std::io::stderr().write_all(bytes)?,
    }
    Ok(())
}

fn decode_binary_output_chunk(base64: &str) -> Result<Vec<u8>> {
    BASE64_STANDARD
        .decode(base64.as_bytes())
        .context("decode binary output chunk")
}

#[cfg(test)]
mod tests {
    use super::*;
    use cue_core::ExecutionId;
    use cue_core::execution::{ExecutionPlan, ExecutionSpec, LaunchContext};
    use cue_core::ipc::{
        MAX_MESSAGE_SIZE, RequestPayload, SessionInfo, SessionScopeState, encode_message,
    };
    use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

    async fn read_test_message<R>(stream: &mut R) -> Message
    where
        R: AsyncRead + Unpin,
    {
        let mut len_buf = [0u8; 4];
        stream
            .read_exact(&mut len_buf)
            .await
            .expect("read message length");
        let len = u32::from_be_bytes(len_buf) as usize;
        assert!(len <= MAX_MESSAGE_SIZE, "test message too large: {len}");
        let mut body = vec![0u8; len];
        stream
            .read_exact(&mut body)
            .await
            .expect("read message body");
        serde_json::from_slice(&body).expect("decode message")
    }

    async fn write_test_message<W>(stream: &mut W, message: Message)
    where
        W: AsyncWrite + Unpin,
    {
        let encoded = encode_message(&message).expect("encode message");
        stream
            .write_all(&encoded)
            .await
            .expect("write test message");
    }

    fn execution_info(id: ExecutionId, state: ExecutionState) -> ExecutionInfo {
        ExecutionInfo {
            id,
            state,
            steps: Vec::new(),
            spec: ExecutionSpec {
                plan: ExecutionPlan::pipeline(cue_core::pipeline::Pipeline::simple(vec![
                    "true".into(),
                ])),
                start_scope: None,
                launch_context: LaunchContext::default(),
                source: None,
                retry_of: None,
            },
        }
    }

    #[test]
    fn binary_output_chunks_decode_to_original_bytes() {
        let encoded = BASE64_STANDARD.encode([0, 159, 146, 150, b'\n']);

        let decoded = decode_binary_output_chunk(&encoded).expect("decode binary chunk");

        assert_eq!(decoded, vec![0, 159, 146, 150, b'\n']);
    }

    #[test]
    fn cue_session_selector_ignores_missing_and_empty_values() {
        assert_eq!(cue_session_selector(None).unwrap(), None);
        assert_eq!(cue_session_selector(Some(OsString::new())).unwrap(), None);
    }

    #[test]
    fn cue_session_selector_accepts_name_or_id() {
        assert_eq!(
            cue_session_selector(Some(OsString::from("shared-bench"))).unwrap(),
            Some("shared-bench".into())
        );
        assert_eq!(
            cue_session_selector(Some(OsString::from("S42"))).unwrap(),
            Some("S42".into())
        );
    }

    #[test]
    fn cue_session_refresh_requires_an_explicit_boolean() {
        assert!(!cue_session_refresh(None).unwrap());
        assert!(!cue_session_refresh(Some(OsString::from("0"))).unwrap());
        assert!(!cue_session_refresh(Some(OsString::from("false"))).unwrap());
        assert!(cue_session_refresh(Some(OsString::from("1"))).unwrap());
        assert!(cue_session_refresh(Some(OsString::from("true"))).unwrap());

        let error = cue_session_refresh(Some(OsString::new()))
            .expect_err("an empty opt-in must not silently enable refresh");
        assert!(
            format!("{error:#}").contains("CUE_SESSION_REFRESH must be one of"),
            "{error:#}"
        );
    }

    #[tokio::test]
    async fn configured_session_attaches_before_script_submission() {
        let (client_stream, mut server_stream) = tokio::io::duplex(4096);
        let mut client = CuedClient::from_stream(client_stream);
        let runner = tokio::spawn(async move {
            run_in_session_with_client(
                &mut client,
                "shared.cue",
                "echo shared\n",
                Some("shared-bench".into()),
                false,
            )
            .await
        });

        let attach_id = match read_test_message(&mut server_stream).await {
            Message::Request {
                id,
                payload: RequestPayload::AttachSession { selector, refresh },
                ..
            } => {
                assert_eq!(selector, "shared-bench");
                assert!(!refresh);
                id
            }
            other => panic!("expected AttachSession before SubmitExecution, got {other:?}"),
        };
        write_test_message(
            &mut server_stream,
            Message::Response {
                id: attach_id,
                payload: ResponsePayload::Ok(OkPayload::SessionInfo(Box::new(SessionInfo {
                    id: "S42".into(),
                    name: "shared-bench".into(),
                    scope_state: SessionScopeState::ReadyDurable,
                    scope_hash: Some("abc123".into()),
                    connected_clients: 2,
                    restart_safe: true,
                    current: true,
                    created_at_ms: 10,
                    updated_at_ms: 20,
                    archived_at_ms: None,
                }))),
            },
        )
        .await;

        let submit_id = match read_test_message(&mut server_stream).await {
            Message::Request {
                id,
                payload: RequestPayload::SubmitExecution { spec },
                ..
            } => {
                assert_eq!(spec.source.as_ref().unwrap().name, "shared.cue");
                id
            }
            other => panic!("expected SubmitExecution after attach confirmation, got {other:?}"),
        };
        let execution_id = ExecutionId(1);
        write_test_message(
            &mut server_stream,
            Message::Response {
                id: submit_id,
                payload: ResponsePayload::Ok(OkPayload::ExecutionCreated {
                    execution: Box::new(execution_info(execution_id, ExecutionState::Running)),
                }),
            },
        )
        .await;
        let wait_id = match read_test_message(&mut server_stream).await {
            Message::Request {
                id,
                payload: RequestPayload::WaitExecution { id: requested },
                ..
            } => {
                assert_eq!(requested, execution_id);
                id
            }
            other => panic!("expected WaitExecution, got {other:?}"),
        };
        write_test_message(
            &mut server_stream,
            Message::Response {
                id: wait_id,
                payload: ResponsePayload::Ok(OkPayload::ExecutionInfo(Box::new(execution_info(
                    execution_id,
                    ExecutionState::Succeeded,
                )))),
            },
        )
        .await;

        assert_eq!(runner.await.unwrap().unwrap(), 0);
    }

    #[tokio::test]
    async fn session_attach_failure_explains_explicit_restart_recovery() {
        let (client_stream, mut server_stream) = tokio::io::duplex(4096);
        let mut client = CuedClient::from_stream(client_stream);
        let runner = tokio::spawn(async move {
            run_in_session_with_client(
                &mut client,
                "shared.cue",
                ":help\n",
                Some("shared-bench".into()),
                false,
            )
            .await
        });

        let attach_id = match read_test_message(&mut server_stream).await {
            Message::Request {
                id,
                payload: RequestPayload::AttachSession { selector, refresh },
                ..
            } => {
                assert_eq!(selector, "shared-bench");
                assert!(!refresh);
                id
            }
            other => panic!("expected AttachSession, got {other:?}"),
        };
        write_test_message(
            &mut server_stream,
            Message::Response {
                id: attach_id,
                payload: ResponsePayload::Err {
                    code: "INVALID_STATE".into(),
                    message: "named session needs_refresh after daemon restart".into(),
                },
            },
        )
        .await;

        let error = runner
            .await
            .expect("join script runner")
            .expect_err("non-refreshing run must fail closed");
        let message = format!("{error:#}");
        assert!(message.contains("--session-refresh"), "{message}");
        assert!(message.contains("CUE_SESSION_REFRESH=1"), "{message}");
        assert!(message.contains("needs_refresh"), "{message}");
    }

    #[tokio::test]
    async fn run_with_client_uses_typed_wait_without_event_subscription() {
        let (client_stream, mut server_stream) = tokio::io::duplex(4096);
        let mut client = CuedClient::from_stream(client_stream);
        let runner =
            tokio::spawn(
                async move { run_with_client(&mut client, "fast.cue", "echo fast\n").await },
            );

        match read_test_message(&mut server_stream).await {
            Message::Request {
                id,
                payload: RequestPayload::SubmitExecution { spec },
                ..
            } => {
                assert_eq!(id, 1);
                assert_eq!(spec.source.as_ref().unwrap().name, "fast.cue");
            }
            other => panic!("expected first request to be SubmitExecution, got {other:?}"),
        }

        let execution_id = ExecutionId(7);
        write_test_message(
            &mut server_stream,
            Message::Event {
                payload: EventPayload::OutputChunk {
                    id: "E7/S1".into(),
                    stream: Stream::Stdout,
                    data: "fast\n".into(),
                },
            },
        )
        .await;
        write_test_message(
            &mut server_stream,
            Message::Response {
                id: 1,
                payload: ResponsePayload::Ok(OkPayload::ExecutionCreated {
                    execution: Box::new(execution_info(execution_id, ExecutionState::Succeeded)),
                }),
            },
        )
        .await;
        let wait_id = match read_test_message(&mut server_stream).await {
            Message::Request {
                id,
                payload: RequestPayload::WaitExecution { id: requested },
                ..
            } => {
                assert_eq!(requested, execution_id);
                id
            }
            other => panic!("expected WaitExecution, got {other:?}"),
        };
        write_test_message(
            &mut server_stream,
            Message::Response {
                id: wait_id,
                payload: ResponsePayload::Ok(OkPayload::ExecutionInfo(Box::new(execution_info(
                    execution_id,
                    ExecutionState::Succeeded,
                )))),
            },
        )
        .await;

        let exit_code = runner
            .await
            .expect("runner task")
            .expect("run_with_client succeeds");
        assert_eq!(exit_code, 0);
    }
}
