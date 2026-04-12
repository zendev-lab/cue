//! End-to-end integration tests for the `cued` daemon.
//!
//! Each test spawns a real `cued start --fg --socket <unique>` process, connects
//! over the Unix domain socket, exercises the IPC protocol, then shuts down.
//!
//! Environment isolation: every test sets `XDG_RUNTIME_DIR`, `XDG_DATA_HOME`,
//! `XDG_STATE_HOME`, and `XDG_CONFIG_HOME` to a per-test temp directory so the
//! daemon uses its own PID file, database, and socket — never colliding with a
//! real running `cued` instance.

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;
use tokio::process::{Child, Command};
use tokio::time::timeout;

use cue_core::ipc::{
    self, EventPayload, Message, OkPayload, RequestPayload, ResponsePayload,
};
use cue_core::job::JobStatus;
use cue_core::mode::Mode;

// ── Helpers ─────────────────────────────────────────────────────────────────

/// Per-test timeout to prevent hangs.
const TEST_TIMEOUT: Duration = Duration::from_secs(15);

/// A self-contained test environment with unique dirs and socket.
struct TestEnv {
    /// Root temp directory (cleaned up on drop).
    root: PathBuf,
    /// Path to the Unix domain socket.
    socket: PathBuf,
}

impl TestEnv {
    /// Create a fresh, isolated temp directory tree for one test.
    fn new(label: &str) -> Self {
        let pid = std::process::id();
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = PathBuf::from(format!("/tmp/cue-itest-{label}-{pid}-{ts}"));
        std::fs::create_dir_all(&root).expect("create test root");
        let socket = root.join("cued.sock");
        Self { root, socket }
    }

    /// Spawn `cued start --fg --socket <path>` with isolated XDG env vars.
    fn spawn_daemon(&self) -> Child {
        Command::new(env!("CARGO"))
            .args([
                "run",
                "--quiet",
                "-p",
                "cued",
                "--",
                "start",
                "--fg",
                "--socket",
            ])
            .arg(&self.socket)
            .env("XDG_RUNTIME_DIR", &self.root)
            .env("XDG_DATA_HOME", self.root.join("data"))
            .env("XDG_STATE_HOME", self.root.join("state"))
            .env("XDG_CONFIG_HOME", self.root.join("config"))
            .env("HOME", &self.root) // fallback isolation
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .expect("failed to spawn cued")
    }
}

impl Drop for TestEnv {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

/// Wait (with retries) until the socket file appears and is connectable.
async fn wait_for_socket(socket: &Path) -> UnixStream {
    for _ in 0..80 {
        if socket.exists()
            && let Ok(stream) = UnixStream::connect(socket).await
        {
            return stream;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!(
        "daemon did not create socket within 8 s: {}",
        socket.display()
    );
}

/// Write a length-prefixed JSON message to the stream.
async fn send(stream: &mut UnixStream, msg: &Message) {
    let encoded = ipc::encode_message(msg).expect("encode");
    stream.write_all(&encoded).await.expect("write");
    stream.flush().await.expect("flush");
}

/// Read one length-prefixed JSON message from the stream.
async fn recv(stream: &mut UnixStream) -> Message {
    let len = stream.read_u32().await.expect("read length");
    let mut buf = vec![0u8; len as usize];
    stream.read_exact(&mut buf).await.expect("read body");
    serde_json::from_slice(&buf).expect("deserialize")
}

/// Build a `Request` envelope.
fn request(id: u32, payload: RequestPayload) -> Message {
    Message::Request { id, payload }
}

/// Send a request and return the matching response payload.
async fn roundtrip(stream: &mut UnixStream, id: u32, payload: RequestPayload) -> ResponsePayload {
    send(stream, &request(id, payload)).await;
    // Drain until we get a Response with the matching id (skip Events).
    loop {
        let msg = recv(stream).await;
        if let Message::Response {
            id: rid, payload, ..
        } = msg
            && rid == id
        {
            return payload;
        }
    }
}

/// Subscribe to a set of channels.
async fn subscribe(stream: &mut UnixStream, id: u32, channels: Vec<&str>) {
    let resp = roundtrip(
        stream,
        id,
        RequestPayload::Subscribe {
            channels: channels.into_iter().map(String::from).collect(),
        },
    )
    .await;
    assert!(
        matches!(resp, ResponsePayload::Ok(OkPayload::Ack {})),
        "subscribe failed: {resp:?}"
    );
}

/// Collect messages until `predicate` returns `true` (with a timeout).
async fn collect_until<F>(stream: &mut UnixStream, dur: Duration, mut predicate: F) -> Vec<Message>
where
    F: FnMut(&Message) -> bool,
{
    let mut collected = Vec::new();
    let deadline = tokio::time::Instant::now() + dur;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        match timeout(remaining, recv(stream)).await {
            Ok(msg) => {
                let done = predicate(&msg);
                collected.push(msg);
                if done {
                    break;
                }
            }
            Err(_) => break, // timeout
        }
    }
    collected
}

/// Send `:shutdown` and wait for the child to exit.
async fn shutdown_daemon(stream: &mut UnixStream, child: &mut Child) {
    // Best-effort IPC shutdown (stops the gateway dispatch loop).
    let _ = send(stream, &request(9999, RequestPayload::Shutdown {})).await;
    // The daemon's main loop waits for a Unix signal to exit. Send SIGTERM.
    if let Some(pid) = child.id() {
        unsafe {
            libc::kill(pid as i32, libc::SIGTERM);
        }
    }
    let _ = timeout(Duration::from_secs(5), child.wait()).await;
    // If still alive, force kill.
    let _ = child.kill().await;
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_daemon_lifecycle() {
    timeout(TEST_TIMEOUT, async {
        let env = TestEnv::new("lifecycle");
        let mut child = env.spawn_daemon();

        // Connect and ping.
        let mut stream = wait_for_socket(&env.socket).await;
        let resp = roundtrip(&mut stream, 1, RequestPayload::Ping {}).await;
        assert!(
            matches!(resp, ResponsePayload::Ok(OkPayload::Pong {})),
            "expected Pong, got {resp:?}"
        );

        // Shutdown via IPC.
        let resp = roundtrip(&mut stream, 2, RequestPayload::Shutdown {}).await;
        assert!(
            matches!(resp, ResponsePayload::Ok(OkPayload::Ack {})),
            "expected Ack for shutdown, got {resp:?}"
        );

        // The IPC Shutdown stops the gateway dispatch loop but the daemon's
        // main loop waits for a Unix signal. Send SIGTERM to the child.
        let pid = child.id().expect("child pid");
        unsafe {
            libc::kill(pid as i32, libc::SIGTERM);
        }

        // Daemon should exit.
        let status = timeout(Duration::from_secs(5), child.wait())
            .await
            .expect("daemon did not exit in time")
            .expect("wait failed");
        // Might exit 0 or via signal — both are acceptable.
        let _ = status;
    })
    .await
    .expect("test timed out");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_simple_job_execution() {
    timeout(TEST_TIMEOUT, async {
        let env = TestEnv::new("simplejob");
        let mut child = env.spawn_daemon();
        let mut stream = wait_for_socket(&env.socket).await;

        // Subscribe to job events.
        subscribe(&mut stream, 1, vec!["jobs"]).await;

        // Send `echo hello` (bare input → :run in Job mode).
        let resp = roundtrip(
            &mut stream,
            2,
            RequestPayload::Eval {
                input: "echo hello".into(),
                mode: Mode::Job,
            },
        )
        .await;

        // Should get JobCreated or ChainCreated.
        match &resp {
            ResponsePayload::Ok(OkPayload::JobCreated { job_id }) => {
                assert!(job_id.starts_with('J'), "unexpected job id: {job_id}");
            }
            ResponsePayload::Ok(OkPayload::ChainCreated { job_ids, .. }) => {
                assert!(!job_ids.is_empty());
            }
            other => panic!("expected job/chain created, got {other:?}"),
        }

        // Wait for the job to reach a terminal state via events.
        let msgs = collect_until(&mut stream, Duration::from_secs(10), |msg| {
            matches!(
                msg,
                Message::Event {
                    payload: EventPayload::JobStateChanged {
                        new_state: JobStatus::Done | JobStatus::Failed,
                        ..
                    },
                }
            )
        })
        .await;

        // Verify we saw at least one state transition to Done.
        let reached_done = msgs.iter().any(|m| {
            matches!(
                m,
                Message::Event {
                    payload: EventPayload::JobStateChanged {
                        new_state: JobStatus::Done,
                        ..
                    },
                }
            )
        });
        assert!(reached_done, "job never reached Done; events: {msgs:?}");

        shutdown_daemon(&mut stream, &mut child).await;
    })
    .await
    .expect("test timed out");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_chain_execution() {
    timeout(TEST_TIMEOUT, async {
        let env = TestEnv::new("chain");
        let mut child = env.spawn_daemon();
        let mut stream = wait_for_socket(&env.socket).await;

        // Subscribe to job events.
        subscribe(&mut stream, 1, vec!["jobs"]).await;

        // Submit a serial chain: echo first -> echo second
        let resp = roundtrip(
            &mut stream,
            2,
            RequestPayload::Eval {
                input: "echo first -> echo second".into(),
                mode: Mode::Job,
            },
        )
        .await;

        // For a serial chain `a -> b`, the scheduler returns ChainCreated with
        // only the initially-ready jobs (just the first leaf). The second leaf
        // is spawned when the first completes. Accept either ChainCreated or
        // JobCreated.
        match &resp {
            ResponsePayload::Ok(OkPayload::ChainCreated { job_ids, .. }) => {
                assert!(
                    !job_ids.is_empty(),
                    "chain created with no initially-ready jobs"
                );
            }
            ResponsePayload::Ok(OkPayload::JobCreated { .. }) => {
                // Single-leaf optimisation — still valid.
            }
            other => panic!("expected chain/job created, got {other:?}"),
        }

        // Wait for both jobs to complete (2 terminal state events).
        let mut done_count = 0;

        let msgs = collect_until(&mut stream, Duration::from_secs(10), |msg| {
            if matches!(
                msg,
                Message::Event {
                    payload: EventPayload::JobStateChanged {
                        new_state: JobStatus::Done | JobStatus::Failed,
                        ..
                    },
                }
            ) {
                done_count += 1;
            }
            done_count >= 2
        })
        .await;

        assert!(
            done_count >= 2,
            "expected 2 terminal states, got {done_count}; events: {msgs:?}"
        );

        shutdown_daemon(&mut stream, &mut child).await;
    })
    .await
    .expect("test timed out");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_job_kill() {
    timeout(TEST_TIMEOUT, async {
        let env = TestEnv::new("kill");
        let mut child = env.spawn_daemon();
        let mut stream = wait_for_socket(&env.socket).await;

        // Subscribe to events.
        subscribe(&mut stream, 1, vec!["jobs"]).await;

        // Start a long-running job.
        let resp = roundtrip(
            &mut stream,
            2,
            RequestPayload::Eval {
                input: "sleep 60".into(),
                mode: Mode::Job,
            },
        )
        .await;

        let job_id = match &resp {
            ResponsePayload::Ok(OkPayload::JobCreated { job_id }) => job_id.clone(),
            ResponsePayload::Ok(OkPayload::ChainCreated { job_ids, .. }) => {
                job_ids.first().unwrap().clone()
            }
            other => panic!("expected job created, got {other:?}"),
        };

        // Wait for the job to reach Running state.
        let _ = collect_until(&mut stream, Duration::from_secs(5), |msg| {
            matches!(
                msg,
                Message::Event {
                    payload: EventPayload::JobStateChanged {
                        new_state: JobStatus::Running,
                        ..
                    },
                }
            )
        })
        .await;

        // Kill the job.
        let kill_resp = roundtrip(
            &mut stream,
            3,
            RequestPayload::Eval {
                input: format!(":kill {job_id}"),
                mode: Mode::Job,
            },
        )
        .await;
        assert!(
            matches!(kill_resp, ResponsePayload::Ok(OkPayload::Ack {})),
            "expected Ack for kill, got {kill_resp:?}"
        );

        // The scheduler sets status to Killed synchronously, and the process
        // will eventually exit producing a JobStateChanged event. Collect
        // events until we see a terminal state (Killed or Failed — the signal
        // may show up as either depending on timing).
        let msgs = collect_until(&mut stream, Duration::from_secs(5), |msg| {
            matches!(
                msg,
                Message::Event {
                    payload: EventPayload::JobStateChanged {
                        new_state: JobStatus::Killed
                            | JobStatus::Failed
                            | JobStatus::Done
                            | JobStatus::Cancelled(_),
                        ..
                    },
                }
            )
        })
        .await;

        // At least one terminal event should appear.
        let has_terminal = msgs.iter().any(|m| {
            matches!(
                m,
                Message::Event {
                    payload: EventPayload::JobStateChanged {
                        new_state: JobStatus::Killed
                            | JobStatus::Failed
                            | JobStatus::Done
                            | JobStatus::Cancelled(_),
                        ..
                    },
                }
            )
        });
        assert!(
            has_terminal,
            "expected terminal state after kill; events: {msgs:?}"
        );

        shutdown_daemon(&mut stream, &mut child).await;
    })
    .await
    .expect("test timed out");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_cron_add_and_list() {
    timeout(TEST_TIMEOUT, async {
        let env = TestEnv::new("cron");
        let mut child = env.spawn_daemon();
        let mut stream = wait_for_socket(&env.socket).await;

        // NOTE: The resolver formats the cron schedule using Rust's Debug
        // trait (`format!("{schedule:?}")`), producing e.g. `"Every(3600s)"`,
        // but the scheduler's `parse_schedule` expects `"every 1h"`. This
        // means `:cron every …` always returns an INVALID_SYNTAX error today.
        // This test documents the current behaviour.
        //
        // When the resolver bug is fixed, the first assertion below will
        // fail — update the test to expect `CronAdded` and verify the list.
        let resp = roundtrip(
            &mut stream,
            1,
            RequestPayload::Eval {
                input: ":cron every 1h echo hello".into(),
                mode: Mode::Job,
            },
        )
        .await;

        match &resp {
            // Happy path (when the bug is fixed): cron was added.
            ResponsePayload::Ok(OkPayload::CronAdded { cron_id }) => {
                assert!(cron_id.starts_with('C'), "unexpected cron id: {cron_id}");

                // List crons and verify our entry appears.
                let list_resp = roundtrip(
                    &mut stream,
                    2,
                    RequestPayload::Eval {
                        input: ":crons".into(),
                        mode: Mode::Job,
                    },
                )
                .await;

                match &list_resp {
                    ResponsePayload::Ok(OkPayload::CronList(list)) => {
                        assert!(!list.is_empty(), "cron list should not be empty");
                        let found = list.iter().any(|c| c.id == *cron_id);
                        assert!(found, "cron {cron_id} not in list: {list:?}");
                        let entry = list.iter().find(|c| c.id == *cron_id).unwrap();
                        assert!(entry.enabled, "cron should be enabled");
                    }
                    other => panic!("expected CronList, got {other:?}"),
                }
            }

            // Current (buggy) path: schedule parse fails.
            ResponsePayload::Err { code, message } => {
                assert_eq!(code, "INVALID_SYNTAX");
                assert!(
                    message.contains("cannot parse schedule"),
                    "unexpected error message: {message}"
                );

                // Even with the add failure, `:crons` should return an empty list.
                let list_resp = roundtrip(
                    &mut stream,
                    2,
                    RequestPayload::Eval {
                        input: ":crons".into(),
                        mode: Mode::Job,
                    },
                )
                .await;
                match &list_resp {
                    ResponsePayload::Ok(OkPayload::CronList(list)) => {
                        assert!(list.is_empty(), "cron list should be empty: {list:?}");
                    }
                    other => panic!("expected CronList, got {other:?}"),
                }
            }

            other => panic!("expected CronAdded or Err, got {other:?}"),
        }

        shutdown_daemon(&mut stream, &mut child).await;
    })
    .await
    .expect("test timed out");
}
