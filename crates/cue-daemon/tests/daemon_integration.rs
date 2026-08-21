//! End-to-end contract tests for the typed `cued` IPC.
//!
//! Every test runs a real daemon with isolated XDG roots. The suite stays at
//! the public Execution/Step/Schedule boundary: legacy Job/Chain/Script state
//! is intentionally not observable through IPC v3.

use std::collections::BTreeMap;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use cue_core::cron::{CronSchedule, CronStatus};
use cue_core::execution::{
    CancelMode, ExecutionCancelReason, ExecutionPlan, ExecutionSpec, ExecutionState, LaunchContext,
    StepState,
};
use cue_core::ipc::{
    self, ForegroundRole, Message, OkPayload, RequestPayload, ResponsePayload, error_code,
};
use cue_core::pipeline::Pipeline;
use cue_core::{ExecutionId, ScheduleId, StepId};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::UnixStream;
use tokio::process::{Child, Command};
use tokio::sync::Semaphore;
use tokio::time::{Instant, timeout};

const TEST_TIMEOUT: Duration = Duration::from_secs(20);
static DAEMON_TEST_PERMIT: Semaphore = Semaphore::const_new(1);

async fn run_daemon_test(test: impl Future<Output = ()>) {
    let _permit = DAEMON_TEST_PERMIT
        .acquire()
        .await
        .expect("daemon integration test permit is never closed");
    timeout(TEST_TIMEOUT, test).await.expect("test timed out");
}

struct TestEnv {
    root: PathBuf,
    socket: PathBuf,
}

impl TestEnv {
    fn new(label: &str) -> Self {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock")
            .as_nanos();
        let root = PathBuf::from(format!(
            "/tmp/cue-itest-{label}-{}-{nonce}",
            std::process::id()
        ));
        std::fs::create_dir_all(&root).expect("create test root");
        let socket = root.join("cued.sock");
        Self { root, socket }
    }

    fn spawn_daemon(&self) -> Child {
        let mut command = Command::new(env!("CARGO_BIN_EXE_cued"));
        command
            .args(["start", "--fg", "--socket"])
            .arg(&self.socket)
            .env("XDG_RUNTIME_DIR", &self.root)
            .env("XDG_DATA_HOME", self.root.join("data"))
            .env("XDG_STATE_HOME", self.root.join("state"))
            .env("XDG_CONFIG_HOME", self.root.join("config"))
            .env("HOME", &self.root)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .expect("spawn cued")
    }
}

impl Drop for TestEnv {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

async fn read_child_stderr(child: &mut Child) -> String {
    let Some(mut stderr) = child.stderr.take() else {
        return String::new();
    };
    let mut output = String::new();
    match timeout(
        Duration::from_millis(200),
        stderr.read_to_string(&mut output),
    )
    .await
    {
        Ok(Ok(_)) => output,
        Ok(Err(error)) => format!("<failed to read stderr: {error}>"),
        Err(_) if output.is_empty() => "<stderr still open>".into(),
        Err(_) => output,
    }
}

async fn wait_for_raw_socket(socket: &Path, child: &mut Child) -> UnixStream {
    for _ in 0..80 {
        if socket.exists()
            && let Ok(stream) = UnixStream::connect(socket).await
        {
            return stream;
        }
        if let Some(status) = child.try_wait().expect("poll daemon startup") {
            let stderr = read_child_stderr(child).await;
            panic!(
                "daemon exited before creating {} with {status}; stderr:\n{stderr}",
                socket.display()
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let stderr = read_child_stderr(child).await;
    panic!(
        "daemon did not create {} within 8 seconds; stderr:\n{stderr}",
        socket.display()
    );
}

async fn wait_for_socket(socket: &Path, child: &mut Child) -> UnixStream {
    let mut stream = wait_for_raw_socket(socket, child).await;
    let cwd = socket.parent().expect("socket parent");
    let response = roundtrip(
        &mut stream,
        0,
        RequestPayload::Handshake {
            protocol_version: ipc::IPC_PROTOCOL_VERSION,
            session_id: format!("itest:{}", socket.display()),
            cwd: cwd.display().to_string(),
            env: BTreeMap::new(),
            refresh: false,
        },
    )
    .await;
    assert!(matches!(response, ResponsePayload::Ok(OkPayload::Ack {})));
    stream
}

fn message(id: u32, operation_id: Option<&str>, payload: RequestPayload) -> Message {
    Message::Request {
        id,
        operation_id: operation_id.map(str::to_owned),
        payload,
    }
}

async fn send<S>(stream: &mut S, message: &Message)
where
    S: AsyncWrite + Unpin,
{
    let encoded = ipc::encode_message(message).expect("encode IPC message");
    stream.write_all(&encoded).await.expect("write IPC message");
    stream.flush().await.expect("flush IPC message");
}

async fn receive<S>(stream: &mut S) -> Message
where
    S: AsyncRead + Unpin,
{
    let length = stream.read_u32().await.expect("read IPC frame length");
    let mut body = vec![0; length as usize];
    stream
        .read_exact(&mut body)
        .await
        .expect("read IPC frame body");
    serde_json::from_slice(&body).expect("decode IPC message")
}

async fn roundtrip<S>(stream: &mut S, id: u32, payload: RequestPayload) -> ResponsePayload
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    roundtrip_with_operation(stream, id, None, payload).await
}

async fn roundtrip_with_operation<S>(
    stream: &mut S,
    id: u32,
    operation_id: Option<&str>,
    payload: RequestPayload,
) -> ResponsePayload
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    send(stream, &message(id, operation_id, payload)).await;
    loop {
        if let Message::Response {
            id: response_id,
            payload,
        } = receive(stream).await
            && response_id == id
        {
            return payload;
        }
    }
}

async fn stop_daemon(stream: &mut UnixStream, child: &mut Child) {
    let _ = roundtrip(stream, u32::MAX, RequestPayload::Shutdown {}).await;
    if let Some(pid) = child.id() {
        unsafe {
            libc::kill(pid as i32, libc::SIGTERM);
        }
    }
    if timeout(Duration::from_secs(5), child.wait()).await.is_err() {
        child.kill().await.expect("force stop daemon");
    }
}

fn spec(command: &[&str], pty: bool) -> ExecutionSpec {
    ExecutionSpec {
        plan: ExecutionPlan::pipeline(Pipeline::simple(
            command.iter().map(|part| (*part).to_string()).collect(),
        )),
        start_scope: None,
        launch_context: LaunchContext {
            pty: Some(pty),
            ..LaunchContext::default()
        },
        source: None,
        retry_of: None,
    }
}

fn created_execution(response: ResponsePayload) -> ExecutionId {
    match response {
        ResponsePayload::Ok(OkPayload::ExecutionCreated { execution }) => execution.id,
        other => panic!("expected ExecutionCreated, got {other:?}"),
    }
}

async fn wait_succeeded(stream: &mut UnixStream, request_id: u32, id: ExecutionId) {
    match roundtrip(stream, request_id, RequestPayload::WaitExecution { id }).await {
        ResponsePayload::Ok(OkPayload::ExecutionInfo(execution)) => {
            assert_eq!(execution.state, ExecutionState::Succeeded);
        }
        other => panic!("expected successful ExecutionInfo, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lifecycle_requires_handshake_and_reports_v3() {
    run_daemon_test(async {
        let env = TestEnv::new("lifecycle");
        let mut child = env.spawn_daemon();
        let mut raw = wait_for_raw_socket(&env.socket, &mut child).await;

        let missing = roundtrip(&mut raw, 1, RequestPayload::ListExecutions { limit: None }).await;
        assert!(matches!(
            missing,
            ResponsePayload::Err { code, .. } if code == error_code::INVALID_REQUEST
        ));
        drop(raw);

        let mut stream = wait_for_socket(&env.socket, &mut child).await;
        match roundtrip(&mut stream, 2, RequestPayload::Ping {}).await {
            ResponsePayload::Ok(OkPayload::Pong {
                protocol_version,
                capabilities,
                ..
            }) => {
                assert_eq!(protocol_version, ipc::IPC_PROTOCOL_VERSION);
                assert!(capabilities.iter().any(|item| item == "execution-v3"));
            }
            other => panic!("expected Pong, got {other:?}"),
        }

        stop_daemon(&mut stream, &mut child).await;
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn old_protocol_receives_explicit_upgrade_error() {
    run_daemon_test(async {
        let env = TestEnv::new("protocol-upgrade");
        let mut child = env.spawn_daemon();
        let mut raw = wait_for_raw_socket(&env.socket, &mut child).await;

        let response = roundtrip(
            &mut raw,
            1,
            RequestPayload::Handshake {
                protocol_version: ipc::IPC_PROTOCOL_VERSION - 1,
                session_id: "old-client".into(),
                cwd: env.root.display().to_string(),
                env: BTreeMap::new(),
                refresh: false,
            },
        )
        .await;
        match response {
            ResponsePayload::Err { code, message } => {
                assert_eq!(code, error_code::PROTOCOL_UPGRADE_REQUIRED);
                assert!(message.contains("upgrade to protocol 3"));
            }
            other => panic!("expected protocol upgrade error, got {other:?}"),
        }
        drop(raw);

        let mut stream = wait_for_socket(&env.socket, &mut child).await;
        stop_daemon(&mut stream, &mut child).await;
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn execution_submit_wait_list_output_and_segment_env() {
    run_daemon_test(async {
        let env = TestEnv::new("execution");
        let mut child = env.spawn_daemon();
        let mut stream = wait_for_socket(&env.socket, &mut child).await;

        let mut first = Pipeline::simple(vec!["printenv".into(), "CUE_LOCAL_VALUE".into()]);
        first.segments[0]
            .env
            .insert("CUE_LOCAL_VALUE".into(), "alpha".into());
        let plan = ExecutionPlan::OnSuccess {
            left: Box::new(ExecutionPlan::pipeline(first)),
            right: Box::new(ExecutionPlan::pipeline(Pipeline::simple(vec![
                "printf".into(),
                "beta".into(),
            ]))),
        };
        let id = created_execution(
            roundtrip(
                &mut stream,
                10,
                RequestPayload::SubmitExecution {
                    spec: Box::new(ExecutionSpec {
                        plan,
                        start_scope: None,
                        launch_context: LaunchContext {
                            pty: Some(false),
                            ..LaunchContext::default()
                        },
                        source: None,
                        retry_of: None,
                    }),
                },
            )
            .await,
        );
        assert_eq!(id, ExecutionId(1));
        wait_succeeded(&mut stream, 11, id).await;

        match roundtrip(
            &mut stream,
            12,
            RequestPayload::ReadExecutionOutput {
                id,
                step_id: None,
                stdout_bytes: Some(1024),
                stderr_bytes: Some(1024),
            },
        )
        .await
        {
            ResponsePayload::Ok(OkPayload::ExecutionOutput { steps, .. }) => {
                assert_eq!(steps.len(), 2);
                assert_eq!(
                    steps
                        .iter()
                        .map(|step| step.stdout.data.as_str())
                        .collect::<String>(),
                    "alpha\nbeta"
                );
            }
            other => panic!("expected ExecutionOutput, got {other:?}"),
        }
        assert!(matches!(
            roundtrip(
                &mut stream,
                13,
                RequestPayload::ListExecutions { limit: Some(1) }
            )
            .await,
            ResponsePayload::Ok(OkPayload::ExecutionList(executions))
                if executions.len() == 1 && executions[0].id == id
        ));

        stop_daemon(&mut stream, &mut child).await;
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn submit_is_idempotent_by_operation_id() {
    run_daemon_test(async {
        let env = TestEnv::new("idempotency");
        let mut child = env.spawn_daemon();
        let mut stream = wait_for_socket(&env.socket, &mut child).await;
        let payload = RequestPayload::SubmitExecution {
            spec: Box::new(spec(&["true"], false)),
        };

        let first = created_execution(
            roundtrip_with_operation(&mut stream, 10, Some("submit-once"), payload.clone()).await,
        );
        let replay = created_execution(
            roundtrip_with_operation(&mut stream, 11, Some("submit-once"), payload).await,
        );
        assert_eq!(first, replay);
        wait_succeeded(&mut stream, 12, first).await;

        stop_daemon(&mut stream, &mut child).await;
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn force_cancel_is_terminal_and_keeps_forced_reason() {
    run_daemon_test(async {
        let env = TestEnv::new("cancel");
        let mut child = env.spawn_daemon();
        let mut stream = wait_for_socket(&env.socket, &mut child).await;
        let id = created_execution(
            roundtrip(
                &mut stream,
                10,
                RequestPayload::SubmitExecution {
                    spec: Box::new(spec(&["sleep", "60"], false)),
                },
            )
            .await,
        );

        assert!(matches!(
            roundtrip(
                &mut stream,
                11,
                RequestPayload::CancelExecution {
                    id,
                    mode: CancelMode::Force,
                }
            )
            .await,
            ResponsePayload::Ok(OkPayload::Ack {})
        ));
        match roundtrip(&mut stream, 12, RequestPayload::WaitExecution { id }).await {
            ResponsePayload::Ok(OkPayload::ExecutionInfo(execution)) => {
                assert_eq!(
                    execution.state,
                    ExecutionState::Cancelled {
                        reason: ExecutionCancelReason::Forced,
                    }
                );
                assert!(matches!(
                    execution.steps[0].state,
                    StepState::Cancelled { .. }
                ));
            }
            other => panic!("expected cancelled ExecutionInfo, got {other:?}"),
        }

        stop_daemon(&mut stream, &mut child).await;
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn step_attach_uses_stable_identity_and_connection_scoped_control() {
    run_daemon_test(async {
        let env = TestEnv::new("step-attach");
        let mut child = env.spawn_daemon();
        let mut stream = wait_for_socket(&env.socket, &mut child).await;
        let id = created_execution(
            roundtrip(
                &mut stream,
                10,
                RequestPayload::SubmitExecution {
                    spec: Box::new(spec(
                        &["sh", "-c", "IFS= read -r ignored; printf 'got:hello\\n'"],
                        true,
                    )),
                },
            )
            .await,
        );
        let step_id = StepId {
            execution: id,
            index: 1,
        };

        match roundtrip(&mut stream, 11, RequestPayload::StepAttach { id: step_id }).await {
            ResponsePayload::Ok(OkPayload::FgAttached(attachment)) => {
                assert_eq!(attachment.id, step_id.to_string());
                assert_eq!(attachment.role, ForegroundRole::Controller);
            }
            other => panic!("expected step attachment, got {other:?}"),
        }
        assert!(matches!(
            roundtrip(
                &mut stream,
                12,
                RequestPayload::StepInput {
                    data: b"hello\n".to_vec(),
                }
            )
            .await,
            ResponsePayload::Ok(OkPayload::Ack {})
        ));
        wait_succeeded(&mut stream, 13, id).await;

        match roundtrip(
            &mut stream,
            14,
            RequestPayload::ReadExecutionOutput {
                id,
                step_id: Some(step_id),
                stdout_bytes: Some(1024),
                stderr_bytes: Some(1024),
            },
        )
        .await
        {
            ResponsePayload::Ok(OkPayload::ExecutionOutput { steps, .. }) => {
                assert_eq!(steps.len(), 1);
                assert!(steps[0].stdout.data.contains("got:hello"));
            }
            other => panic!("expected PTY output, got {other:?}"),
        }

        stop_daemon(&mut stream, &mut child).await;
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delay_schedule_triggers_a_fresh_execution() {
    run_daemon_test(async {
        let env = TestEnv::new("schedule");
        let mut child = env.spawn_daemon();
        let mut stream = wait_for_socket(&env.socket, &mut child).await;

        let schedule_id = match roundtrip(
            &mut stream,
            10,
            RequestPayload::CreateSchedule {
                schedule: CronSchedule::Delay(Duration::from_millis(150)),
                execution: Box::new(spec(&["printf", "triggered"], false)),
            },
        )
        .await
        {
            ResponsePayload::Ok(OkPayload::ScheduleCreated { schedule }) => schedule.id,
            other => panic!("expected ScheduleCreated, got {other:?}"),
        };
        assert_eq!(schedule_id, ScheduleId(1));

        let deadline = Instant::now() + Duration::from_secs(5);
        let execution_id = loop {
            match roundtrip(
                &mut stream,
                11,
                RequestPayload::ListExecutions { limit: Some(1) },
            )
            .await
            {
                ResponsePayload::Ok(OkPayload::ExecutionList(executions))
                    if !executions.is_empty() =>
                {
                    break executions[0].id;
                }
                ResponsePayload::Ok(OkPayload::ExecutionList(_)) => {}
                other => panic!("expected ExecutionList, got {other:?}"),
            }
            assert!(Instant::now() < deadline, "schedule did not fire");
            tokio::time::sleep(Duration::from_millis(50)).await;
        };
        wait_succeeded(&mut stream, 12, execution_id).await;
        assert!(matches!(
            roundtrip(
                &mut stream,
                13,
                RequestPayload::ListSchedules { limit: None }
            )
            .await,
            ResponsePayload::Ok(OkPayload::ScheduleList(schedules))
                if schedules.len() == 1
                    && schedules[0].id == schedule_id
                    && schedules[0].status == CronStatus::Completed
        ));

        stop_daemon(&mut stream, &mut child).await;
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn terminal_execution_survives_daemon_restart() {
    run_daemon_test(async {
        let env = TestEnv::new("persistence");
        let mut first = env.spawn_daemon();
        let mut stream = wait_for_socket(&env.socket, &mut first).await;
        let id = created_execution(
            roundtrip(
                &mut stream,
                10,
                RequestPayload::SubmitExecution {
                    spec: Box::new(spec(&["printf", "durable"], false)),
                },
            )
            .await,
        );
        wait_succeeded(&mut stream, 11, id).await;
        stop_daemon(&mut stream, &mut first).await;
        drop(stream);

        let mut second = env.spawn_daemon();
        let mut restored = wait_for_socket(&env.socket, &mut second).await;
        match roundtrip(&mut restored, 20, RequestPayload::GetExecution { id }).await {
            ResponsePayload::Ok(OkPayload::ExecutionInfo(execution)) => {
                assert_eq!(execution.id, id);
                assert_eq!(execution.state, ExecutionState::Succeeded);
            }
            other => panic!("expected restored ExecutionInfo, got {other:?}"),
        }
        assert!(matches!(
            roundtrip(
                &mut restored,
                21,
                RequestPayload::ReadExecutionOutput {
                    id,
                    step_id: None,
                    stdout_bytes: Some(1024),
                    stderr_bytes: Some(1024),
                }
            )
            .await,
            ResponsePayload::Ok(OkPayload::ExecutionOutput { steps, .. })
                if steps.len() == 1 && steps[0].stdout.data == "durable"
        ));

        stop_daemon(&mut restored, &mut second).await;
    })
    .await;
}
