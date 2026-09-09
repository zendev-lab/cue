#![cfg(all(feature = "cue-client", feature = "cue-daemon"))]

use std::path::PathBuf;
use std::process::{Output, Stdio};
use std::time::Duration;

const DAEMON: &str = env!("CARGO_BIN_EXE_cued");
const CLIENT: &str = env!("CARGO_BIN_EXE_cue-client");
const CUE: &str = env!("CARGO_BIN_EXE_cue");

struct Fixture {
    root: PathBuf,
    socket: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let root = PathBuf::from("/tmp").join(format!("cue-cli-{}", uuid::Uuid::new_v4()));
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

impl Fixture {
    async fn start_resources(&self, config: &str) {
        std::fs::write(self.root.join("daemon.toml"), config).unwrap();
        let output = self
            .run(
                DAEMON,
                &["start", "--db", "test.db", "--config", "daemon.toml"],
            )
            .await;
        assert!(output.status.success(), "{output:?}");
    }
    fn spawn_source(&self, source: &str, need: &str) -> tokio::process::Child {
        tokio::process::Command::new(CUE)
            .args(["client", "exec", "--need", need, "--", source])
            .current_dir(&self.root)
            .env_clear()
            .env("PATH", "/usr/bin:/bin")
            .env("CUE_SOCKET", &self.socket)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .unwrap()
    }
    async fn resource_state(&self, id: &str, state: &str) -> serde_json::Value {
        tokio::time::timeout(Duration::from_secs(12), async {
            loop {
                let out = self.run(CUE, &["resources", "--json"]).await;
                assert!(out.status.success(), "{out:?}");
                let value: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
                if let Some(row) = value["executions"].as_array().unwrap().iter().find(|r| {
                    r["execution"].as_u64() == id.strip_prefix('E').and_then(|v| v.parse().ok())
                        && r["status"] == state
                }) {
                    return row.clone();
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("{id} did not reach {state}"))
    }
}
const STATIC_RESOURCES: &str = "[[resources.providers]]\nid='slots'\nkind='static'\n[resources.providers.capacity]\nworker='2'\n";

#[tokio::test]
async fn resource_admission_holds_across_steps_and_skips_an_unsatisfied_head() {
    let f = Fixture::new();
    f.start_resources(STATIC_RESOURCES).await;
    let owner = f.spawn_source("sleep 2 -> sleep 2", "worker=1");
    f.resource_state("E1", "allocated").await;
    let waiting = f.spawn_source("printf second", "worker=2");
    f.resource_state("E2", "waiting").await;
    let smaller = f
        .run(
            CUE,
            &[
                "client", "exec", "--need", "worker=1", "--", "printf", "third",
            ],
        )
        .await;
    assert!(smaller.status.success(), "{smaller:?}");
    assert_eq!(smaller.stdout, b"third");
    f.resource_state("E1", "allocated").await;
    f.resource_state("E2", "waiting").await;
    let normal = f.run(CLIENT, &["exec", "printf unrestricted"]).await;
    assert_eq!(normal.stdout, b"unrestricted");
    assert!(owner.wait_with_output().await.unwrap().status.success());
    let out = waiting.wait_with_output().await.unwrap();
    assert!(out.status.success(), "{out:?}");
    assert_eq!(out.stdout, b"second");
    f.resource_state("E1", "released").await;
    f.resource_state("E2", "released").await;
}

#[tokio::test]
async fn queued_execution_can_be_cancelled_after_its_client_disconnects() {
    let f = Fixture::new();
    f.start_resources(STATIC_RESOURCES).await;
    let owner = f.spawn_source("sleep 30", "worker=2");
    f.resource_state("E1", "allocated").await;
    let mut queued = f.spawn_source("touch must-not-run", "worker=1");
    f.resource_state("E2", "waiting").await;
    queued.kill().await.unwrap();
    assert!(f.run(CLIENT, &["cancel", "E2"]).await.status.success());
    f.resource_state("E2", "released").await;
    assert!(!f.root.join("must-not-run").exists());
    assert!(f.run(CLIENT, &["kill", "E1"]).await.status.success());
    let out = owner.wait_with_output().await.unwrap();
    assert!(!out.status.success());
    f.resource_state("E1", "released").await;
}

#[tokio::test]
async fn resource_configuration_survives_restart_and_invalid_requests_do_not_submit() {
    let f = Fixture::new();
    f.start_resources(STATIC_RESOURCES).await;
    let before = f.run(CLIENT, &["list"]).await;
    for args in [
        vec!["client", "exec", "--need", "worker=0", "--", "true"],
        vec!["client", "exec", "--need", "worker=1GiB", "--", "true"],
        vec!["client", "exec", "--need", "missing=1", "--", "true"],
        vec![
            "client", "exec", "--need", "worker=1", "--need", "worker=1", "--", "true",
        ],
    ] {
        let out = f.run(CUE, &args).await;
        assert!(!out.status.success(), "{out:?}");
    }
    assert_eq!(before.stdout, f.run(CLIENT, &["list"]).await.stdout);
    assert!(f.run(DAEMON, &["restart"]).await.status.success());
    std::fs::write(f.root.join("task.cue"), "printf held |> cat\n").unwrap();
    let out = f.run(CUE, &["run", "task.cue", "--need", "worker=2"]).await;
    assert!(out.status.success(), "{out:?}");
    assert_eq!(out.stdout, b"held");
    f.resource_state("E1", "released").await;
    let view = f.run(CLIENT, &["show", "E1"]).await;
    let value: serde_json::Value = serde_json::from_slice(&view.stdout).unwrap();
    assert_eq!(value["resources"]["status"], "released");
}

#[tokio::test]
async fn nvidia_probe_is_optional_and_parallel_pipeline_steps_share_the_device_override() {
    let f = Fixture::new();
    std::fs::write(
        f.root.join("probe.sh"),
        "printf 'GPU-b, 100, 30\\nGPU-a, 100, 80\\n'\n",
    )
    .unwrap();
    f.start_resources(
        "[[resources.providers]]\nid='nvidia'\nkind='nvidia'\nargv=['/bin/sh','probe.sh']\n",
    )
    .await;
    let output=f.run(CUE,&["client","exec","--need","gpu_mem=24MiB","--","env set CUDA_VISIBLE_DEVICES=wrong -> printenv CUDA_VISIBLE_DEVICES |> cat ||| CUDA_VISIBLE_DEVICES=wrong printenv CUDA_VISIBLE_DEVICES"]).await;
    assert!(output.status.success(), "{output:?}");
    let text = String::from_utf8(output.stdout).unwrap();
    assert_eq!(text.matches("GPU-a").count(), 2, "{text}");
    assert!(!text.contains("wrong"));
    let row = f.resource_state("E1", "released").await;
    assert_eq!(
        row["attempts"][0]["grant"]["devices"],
        serde_json::json!(["GPU-a"])
    );
    std::fs::remove_file(f.root.join("probe.sh")).unwrap();
    let normal = f.run(CLIENT, &["exec", "printf no-gpu-needed"]).await;
    assert!(normal.status.success());
    assert_eq!(normal.stdout, b"no-gpu-needed");
    let gpu = f
        .run(CUE, &["client", "exec", "--need", "gpu=1", "--", "true"])
        .await;
    assert!(!gpu.status.success());
}
