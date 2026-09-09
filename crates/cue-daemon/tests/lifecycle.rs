use std::collections::BTreeMap;
use std::os::unix::fs::PermissionsExt as _;
use std::path::PathBuf;
use std::process::{Output, Stdio};
use std::time::Duration;

use cue_core::{
    AbsolutePath, Argv, EnvKey, EnvValue, ExecutionPlan, ExecutionSpec, FileModeMask, IoMode,
    Pipeline, Process, Scope,
};
use cue_protocol::{
    ClientId, Command, Hello, Message, OperationId, PROTOCOL_VERSION, Query, RequestId,
    ResponsePayload, ResultPayload,
};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::UnixStream;

const BINARY: &str = env!("CARGO_BIN_EXE_cued");

struct Fixture {
    root: PathBuf,
    socket: PathBuf,
    database: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let root = PathBuf::from("/tmp").join(format!("cue-lifecycle-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&root).unwrap();
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o755)).unwrap();
        Self {
            socket: root.join("peer.sock"),
            database: root.join("data.db"),
            root,
        }
    }

    fn command(&self, verb: &str) -> tokio::process::Command {
        let mut command = tokio::process::Command::new(BINARY);
        command
            .arg(verb)
            .arg("--socket")
            .arg(&self.socket)
            .current_dir(&self.root)
            .env("XDG_CONFIG_HOME", self.root.join("config"))
            .kill_on_drop(true)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if verb == "start" {
            command.arg("--db").arg(&self.database);
        }
        command
    }

    async fn run(&self, verb: &str) -> Output {
        output(self.command(verb)).await
    }

    async fn start(&self) {
        let result = self.run("start").await;
        assert!(result.status.success(), "{result:?}");
        assert!(String::from_utf8_lossy(&result.stdout).contains("IPC v5 ready"));
    }

    async fn ready(&self) -> (String, i32) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Ok(stream) = UnixStream::connect(&self.socket).await {
                    let (_, instance, pid) = hello(stream).await;
                    break (instance, pid);
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("daemon must become ready")
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        // Each fixture has its own socket and database. Never touch the installed daemon.
        let status = std::process::Command::new(BINARY)
            .args(["stop", "--force", "--socket"])
            .arg(&self.socket)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
        if status.is_ok_and(|status| status.success()) {
            let _ = std::fs::remove_dir_all(&self.root);
        }
    }
}

async fn output(mut command: tokio::process::Command) -> Output {
    tokio::time::timeout(Duration::from_secs(20), command.output())
        .await
        .expect("CLI must return within its lifecycle deadline")
        .expect("run CLI")
}

async fn exchange(stream: &mut UnixStream, message: Message) -> ResultPayload {
    stream
        .write_all(&cue_protocol::encode_message(&message).unwrap())
        .await
        .unwrap();
    let mut header = [0; 4];
    stream.read_exact(&mut header).await.unwrap();
    let length = u32::from_be_bytes(header) as usize;
    assert!(length < cue_protocol::MAX_MESSAGE_SIZE);
    let mut body = vec![0; length];
    stream.read_exact(&mut body).await.unwrap();
    let message: Message = serde_json::from_slice(&body).unwrap();
    let Message::Response {
        payload: ResponsePayload::Ok(result),
        ..
    } = message
    else {
        panic!("{message:?}");
    };
    result
}

async fn hello(mut stream: UnixStream) -> (UnixStream, String, i32) {
    let pid = stream.peer_cred().unwrap().pid().unwrap();
    let result = exchange(
        &mut stream,
        Message::Query {
            request_id: RequestId::new(1).unwrap(),
            query: Query::Hello(Hello {
                protocol_version: PROTOCOL_VERSION,
                client_id: ClientId::new(format!("lifecycle-test:{}", uuid::Uuid::new_v4()))
                    .unwrap(),
            }),
        },
    )
    .await;
    let ResultPayload::Hello { instance_id, .. } = result else {
        panic!("{result:?}");
    };
    (stream, instance_id, pid)
}

async fn command(stream: &mut UnixStream, request: u64, command: Command) -> ResultPayload {
    exchange(
        stream,
        Message::Command {
            request_id: RequestId::new(request).unwrap(),
            operation_id: OperationId::new(uuid::Uuid::new_v4().to_string()).unwrap(),
            command,
        },
    )
    .await
}

#[tokio::test]
async fn background_lifecycle_returns_only_after_ready_and_release() {
    let fixture = Fixture::new();
    fixture.start().await;
    let (before, pid) = fixture.ready().await;
    // SAFETY: getsid only observes the authenticated test daemon's process identity.
    assert_eq!(
        unsafe { libc::getsid(pid) },
        pid,
        "background daemon must detach from the caller session"
    );
    assert_eq!(
        std::fs::metadata(&fixture.root)
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o755
    );
    for file in [
        &fixture.database,
        &fixture.socket.with_extension("sock.log"),
    ] {
        assert_eq!(
            std::fs::metadata(file).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }
    let result = fixture.run("restart").await;
    assert!(result.status.success(), "{result:?}");
    let response: ResultPayload = serde_json::from_slice(&result.stdout).unwrap();
    let ResultPayload::RestartAccepted {
        target_instance_id, ..
    } = response
    else {
        panic!("{response:?}");
    };
    // No readiness retry here: restart has already promised readiness.
    let (_, after, _) = hello(UnixStream::connect(&fixture.socket).await.unwrap()).await;
    assert_eq!(after, target_instance_id);
    assert_ne!(before, after);
    assert!(fixture.run("stop").await.status.success());
    assert!(
        !fixture.socket.exists(),
        "stop must wait for socket cleanup"
    );
    fixture.start().await;
    assert!(fixture.run("stop").await.status.success());
}

#[tokio::test]
async fn foreground_and_relative_paths_are_supported() {
    let fixture = Fixture::new();
    let mut child = fixture.command("start").arg("-f").spawn().unwrap();
    fixture.ready().await;
    assert!(child.try_wait().unwrap().is_none());
    assert!(fixture.run("stop").await.status.success());
    assert!(child.wait().await.unwrap().success());
    let mut command = tokio::process::Command::new(BINARY);
    command
        .args(["start", "--socket", "peer.sock", "--db", "data.db"])
        .env("XDG_CONFIG_HOME", fixture.root.join("config"))
        .current_dir(&fixture.root)
        .kill_on_drop(true);
    assert!(output(command).await.status.success());
    assert!(fixture.run("status").await.status.success());
}

#[tokio::test]
async fn startup_failure_returns_child_diagnostic_and_keeps_log() {
    let fixture = Fixture::new();
    std::fs::write(&fixture.database, b"not a SQLite database").unwrap();
    let result = fixture.run("start").await;
    assert!(!result.status.success());
    let error = String::from_utf8_lossy(&result.stderr);
    assert!(error.contains("not a database"), "{error}");
    assert!(error.contains("log:"), "{error}");
    assert!(!fixture.socket.exists());
    assert_eq!(
        std::fs::read(&fixture.database).unwrap(),
        b"not a SQLite database"
    );
}

#[tokio::test]
async fn concurrent_start_cannot_report_someone_elses_instance_as_its_own() {
    let fixture = Fixture::new();
    let (left, right) = tokio::join!(fixture.run("start"), fixture.run("start"));
    assert_ne!(
        left.status.success(),
        right.status.success(),
        "{left:?}\n{right:?}"
    );
    assert!(fixture.run("status").await.status.success());
}

#[tokio::test]
async fn another_socket_cannot_open_the_same_database() {
    let owner = Fixture::new();
    let mut second = Fixture::new();
    second.database = owner.database.clone();
    owner.start().await;
    let result = second.run("start").await;
    assert!(
        !result.status.success(),
        "a database needs exclusive host ownership"
    );
    assert!(String::from_utf8_lossy(&result.stderr).contains("another cued instance owns"));
    assert!(owner.run("status").await.status.success());
}

#[tokio::test]
async fn stop_waits_for_term_resistant_owned_work_to_be_reaped() {
    let fixture = Fixture::new();
    fixture.start().await;
    let (mut stream, _, _) = hello(UnixStream::connect(&fixture.socket).await.unwrap()).await;
    let marker = fixture.root.join("owned.pid");
    let scope = Scope::new(
        AbsolutePath::new(&fixture.root).unwrap(),
        BTreeMap::from([(
            EnvKey::new("PATH").unwrap(),
            EnvValue::new("/usr/bin:/bin").unwrap(),
        )]),
        FileModeMask::new(0o022).unwrap(),
    );
    let hash = scope.compute_hash();
    command(
        &mut stream,
        2,
        Command::PutScope {
            scope: Box::new(scope),
        },
    )
    .await;
    let spec = ExecutionSpec::new(
        hash,
        ExecutionPlan::run(
            Pipeline::simple(Process::new(
                Argv::new(
                    "/bin/sh",
                    vec![
                        "-c".into(),
                        "trap '' TERM; echo $$ > owned.pid; while :; do sleep 1; done".into(),
                    ],
                )
                .unwrap(),
            )),
            IoMode::Captured,
        ),
    )
    .unwrap();
    command(
        &mut stream,
        3,
        Command::SubmitExecution {
            spec: Box::new(spec),
        },
    )
    .await;
    tokio::time::timeout(Duration::from_secs(5), async {
        while !marker.exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let pid: i32 = std::fs::read_to_string(marker)
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    assert!(pid > 1);
    let result = fixture.run("stop").await;
    assert!(result.status.success(), "{result:?}");
    assert!(!fixture.socket.exists());
    // SAFETY: signal 0 only observes the fixture's recorded process.
    assert_eq!(
        unsafe { libc::kill(pid, 0) },
        -1,
        "stop returned while an owned process was alive"
    );
    assert_eq!(
        std::io::Error::last_os_error().raw_os_error(),
        Some(libc::ESRCH)
    );
    fixture.start().await; // Drained work must not leave a recovery-blocking attempt.
}
