use std::io::{Read as _, Write as _};
use std::os::unix::net::UnixListener;
use std::path::PathBuf;
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

const BINARY: &str = env!("CARGO_BIN_EXE_cued");

struct Fixture {
    root: PathBuf,
    socket: PathBuf,
    child: Child,
}

impl Fixture {
    fn start(mode: &str) -> Self {
        let root = PathBuf::from("/tmp").join(format!("cue-recovery-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&root).unwrap();
        let socket = root.join("peer.sock");
        let child = if mode == "v4" {
            Command::new(BINARY)
                .arg("start")
                .arg("--fg")
                .arg("--socket")
                .arg(&socket)
                .arg("--db")
                .arg(root.join("test.db"))
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap()
        } else {
            Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "socket_fixture", "--ignored", "--nocapture"])
                .env("CUE_RECOVERY_TEST_SOCKET", &socket)
                .env("CUE_RECOVERY_TEST_MODE", mode)
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap()
        };
        let mut fixture = Self {
            root,
            socket,
            child,
        };
        let deadline = Instant::now() + Duration::from_secs(5);
        while !fixture.socket.exists() {
            assert!(
                fixture.child.try_wait().unwrap().is_none(),
                "fixture exited before binding"
            );
            assert!(Instant::now() < deadline, "fixture did not bind");
            std::thread::sleep(Duration::from_millis(10));
        }
        fixture
    }

    fn command(&self, verb: &str) -> Command {
        let mut command = Command::new(BINARY);
        command.arg(verb).arg("--socket").arg(&self.socket);
        command
    }

    fn force_stop(&mut self) -> Output {
        let mut cli = self
            .command("stop")
            .arg("--force")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            // Reap our fixture so the CLI can verify exit rather than a zombie.
            let _ = self.child.try_wait().unwrap();
            if cli.try_wait().unwrap().is_some() {
                break;
            }
            if Instant::now() >= deadline {
                let _ = cli.kill();
                let _ = cli.wait();
                panic!("force stop did not return");
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        cli.wait_with_output().unwrap()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

// A separate process gives the socket kernel credentials a real target PID.
// It intentionally implements no Cue protocol. No installed daemon is used.
#[test]
#[ignore]
fn socket_fixture() {
    let Some(socket) = std::env::var_os("CUE_RECOVERY_TEST_SOCKET") else {
        return;
    };
    let mode = std::env::var("CUE_RECOVERY_TEST_MODE").unwrap();
    if mode == "ignore-term" {
        // SAFETY: installs the platform's SIG_IGN disposition in this isolated fixture.
        unsafe {
            libc::signal(libc::SIGTERM, libc::SIG_IGN);
        }
    }
    let listener = UnixListener::bind(socket).unwrap();
    for stream in listener.incoming() {
        let mut stream = stream.unwrap();
        if mode == "hang" {
            std::thread::sleep(Duration::from_secs(30));
        } else {
            // Consume a complete v4 frame before closing, like the v3 decoder
            // rejecting the envelope. This avoids connection-reset ambiguity.
            let mut header = [0; 4];
            if stream.read_exact(&mut header).is_ok() {
                let size = u32::from_be_bytes(header) as usize;
                if size < 16384 {
                    let mut body = vec![0; size];
                    let _ = stream.read_exact(&mut body);
                    if mode == "lost-ack" {
                        let response = cue_protocol::Message::Response {
                            request_id: cue_protocol::RequestId::new(1).unwrap(),
                            payload: cue_protocol::ResponsePayload::Ok(
                                cue_protocol::ResultPayload::Hello {
                                    protocol_version: cue_protocol::PROTOCOL_VERSION,
                                    server_version: "fixture".into(),
                                    instance_id: "fixture".into(),
                                    capabilities: vec![],
                                },
                            ),
                        };
                        stream
                            .write_all(&cue_protocol::encode_message(&response).unwrap())
                            .unwrap();
                        stream.read_exact(&mut header).unwrap();
                        let mut body = vec![0; u32::from_be_bytes(header) as usize];
                        stream.read_exact(&mut body).unwrap();
                        assert!(matches!(
                            serde_json::from_slice::<cue_protocol::Message>(&body).unwrap(),
                            cue_protocol::Message::Command { .. }
                        ));
                        // Command was received, but its acknowledgement is lost.
                    }
                }
            }
        }
    }
}

#[test]
fn incompatible_listener_has_actionable_status_and_control_errors() {
    let mut fixture = Fixture::start("eof");
    for verb in ["status", "stop", "restart"] {
        let output = fixture.command(verb).output().unwrap();
        assert!(!output.status.success());
        let text = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(text.contains("IPC v4"), "{text}");
        assert!(text.contains("cued stop --force --socket"), "{text}");
        assert!(text.contains(fixture.socket.to_str().unwrap()), "{text}");
        assert!(!text.contains("not running"), "{text}");
        assert!(fixture.child.try_wait().unwrap().is_none());
    }
    let output = fixture.force_stop();
    assert!(output.status.success(), "{output:?}");
    assert!(fixture.child.try_wait().unwrap().is_some());
}

#[test]
fn hung_handshake_is_bounded_and_recovery_needs_no_handshake() {
    let mut fixture = Fixture::start("hang");
    let start = Instant::now();
    let output = fixture.command("status").output().unwrap();
    assert!(!output.status.success());
    assert!(start.elapsed() < Duration::from_secs(6));
    let text = String::from_utf8_lossy(&output.stdout);
    assert!(
        text.contains("timed out") && text.contains("stop --force"),
        "{text}"
    );
    assert!(fixture.force_stop().status.success());
}

#[test]
fn force_stop_does_not_claim_success_or_escalate_when_signal_is_ignored() {
    let mut fixture = Fixture::start("ignore-term");
    let output = fixture.force_stop();
    assert!(!output.status.success());
    let text = String::from_utf8_lossy(&output.stderr);
    assert!(
        text.contains("stop is not confirmed") && text.contains("no SIGKILL"),
        "{text}"
    );
    assert!(fixture.child.try_wait().unwrap().is_none());
}

#[test]
fn v4_signal_shutdown_cleans_up_socket_and_missing_stop_is_idempotent() {
    let mut fixture = Fixture::start("v4");
    assert!(fixture.command("status").output().unwrap().status.success());
    assert!(fixture.force_stop().status.success());
    assert!(
        !fixture.socket.exists(),
        "v4 must drain and remove its socket on SIGTERM"
    );
    assert!(fixture.root.join("test.db").exists());
    assert!(fixture.command("stop").output().unwrap().status.success());
    assert!(
        fixture
            .command("stop")
            .arg("--force")
            .output()
            .unwrap()
            .status
            .success()
    );
}

#[test]
fn force_stop_rejects_symlinked_socket_without_signalling_peer() {
    let mut fixture = Fixture::start("eof");
    let alias = fixture.root.join("alias.sock");
    std::os::unix::fs::symlink(&fixture.socket, &alias).unwrap();
    let output = Command::new(BINARY)
        .args(["stop", "--force", "--socket"])
        .arg(alias)
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("non-socket"));
    assert!(fixture.child.try_wait().unwrap().is_none());
}

#[test]
fn lost_control_ack_reports_unknown_outcome_without_signal_fallback() {
    let mut fixture = Fixture::start("lost-ack");
    let output = fixture.command("stop").output().unwrap();
    assert!(!output.status.success());
    let text = String::from_utf8_lossy(&output.stderr);
    assert!(text.contains("command outcome is unknown"), "{text}");
    assert!(!text.contains("stop --force"), "{text}");
    assert!(fixture.child.try_wait().unwrap().is_none());
}
