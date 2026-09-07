#![cfg(all(feature = "cue-client", feature = "cue-daemon"))]

use std::path::PathBuf;
use std::process::{Output, Stdio};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const DAEMON: &str = env!("CARGO_BIN_EXE_cued");
const CLIENT: &str = env!("CARGO_BIN_EXE_cue-client");
const CUE: &str = env!("CARGO_BIN_EXE_cue");

struct Fixture {
    root: PathBuf,
    socket: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let root = PathBuf::from("/tmp").join(format!(
            "cue-cli-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&root).unwrap();
        Self {
            socket: root.join("peer.sock"),
            root,
        }
    }

    async fn run(&self, program: &str, args: &[&str]) -> Output {
        let mut command = tokio::process::Command::new(program);
        command
            .args(args)
            .current_dir(&self.root)
            .env_clear()
            .env("PATH", "/usr/bin:/bin")
            .env("CUE_SOCKET", &self.socket)
            .kill_on_drop(true);
        tokio::time::timeout(Duration::from_secs(20), command.output())
            .await
            .unwrap()
            .unwrap()
    }

    async fn start(&self) {
        // Explicit paths work even in service/container environments without HOME.
        let output = self.run(DAEMON, &["start", "--db", "test.db"]).await;
        assert!(output.status.success(), "{output:?}");
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let status = std::process::Command::new(DAEMON)
            .args(["stop", "--socket"])
            .arg(&self.socket)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
        if status.is_ok_and(|status| status.success()) {
            let _ = std::fs::remove_dir_all(&self.root);
        }
    }
}

#[tokio::test]
async fn errors_and_retained_output_are_visible_through_installed_command_shapes() {
    let fixture = Fixture::new();
    fixture.start().await;
    let missing = fixture
        .run(CLIENT, &["exec", "cue-command-that-does-not-exist"])
        .await;
    assert!(!missing.status.success());
    assert!(
        String::from_utf8_lossy(&missing.stderr).contains("could not start"),
        "{missing:?}"
    );

    std::fs::write(fixture.root.join("bad.cue"), "cd missing-directory\n").unwrap();
    let builtin = fixture.run(CUE, &["run", "bad.cue"]).await;
    assert!(!builtin.status.success());
    assert!(
        String::from_utf8_lossy(&builtin.stderr).contains("builtin failed"),
        "{builtin:?}"
    );

    let large = fixture
        .run(
            CLIENT,
            &["exec", ":run(pty=false) /usr/bin/printf %1048640s x"],
        )
        .await;
    assert!(large.status.success(), "{large:?}");
    assert_eq!(large.stdout.len(), 1024 * 1024);
    assert!(
        String::from_utf8_lossy(&large.stderr).contains("truncated; first retained byte is 64")
    );
    let reread = fixture.run(CLIENT, &["out", "E3/S1"]).await;
    assert!(reread.status.success());
    assert_eq!(reread.stdout, large.stdout);
    assert!(String::from_utf8_lossy(&reread.stderr).contains("truncated"));

    let normal = fixture.run(CUE, &["client", "exec", "printf hello"]).await;
    assert!(normal.status.success());
    assert_eq!(normal.stdout, b"hello");
    assert!(
        normal.stderr.is_empty(),
        "successful output must not be polluted"
    );
}

#[tokio::test]
async fn missing_daemon_has_an_actionable_start_command() {
    let fixture = Fixture::new();
    let output = fixture.run(CLIENT, &["list"]).await;
    assert!(!output.status.success());
    let error = String::from_utf8_lossy(&output.stderr);
    assert!(error.contains("cued start --socket"), "{error}");
    assert!(error.contains(fixture.socket.to_str().unwrap()), "{error}");
}
