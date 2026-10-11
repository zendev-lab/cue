//! Local background startup and observable lifecycle completion.
use std::fs::File;
use std::io::{Read as _, Seek as _, SeekFrom};
use std::os::unix::process::CommandExt as _;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use anyhow::{Context as _, Result, bail};

use crate::{dirs, host};

pub(crate) const COMPLETION_TIMEOUT: Duration = Duration::from_secs(15);

pub(crate) struct SpawnedDaemon {
    child: Child,
    log: PathBuf,
    log_offset: u64,
}

pub(crate) async fn start(socket: &Path, database: &Path, config: Option<&Path>) -> Result<()> {
    let instance = uuid::Uuid::new_v4().to_string();
    let mut spawned = spawn_daemon(socket, database, &instance, config)?;
    let pid = spawned.child.id();
    let result = wait_ready_inner(
        socket,
        &instance,
        Some(&mut spawned.child),
        COMPLETION_TIMEOUT,
    )
    .await;
    if let Err(error) = result {
        let detail = log_tail(&spawned.log, spawned.log_offset).unwrap_or_default();
        bail!(
            "background cued did not start successfully (pid {pid}): {error:#}\nlog: {}{}",
            spawned.log.display(),
            if detail.is_empty() {
                String::new()
            } else {
                format!("\n{detail}")
            },
        );
    }
    println!(
        "started {} (pid {pid}, IPC v5 ready)\nlog: {}",
        socket.display(),
        spawned.log.display()
    );
    Ok(())
}

pub(crate) fn spawn_daemon(
    socket: &Path,
    database: &Path,
    instance: &str,
    config: Option<&Path>,
) -> Result<SpawnedDaemon> {
    let log = host::sidecar(socket, ".log");
    let output = dirs::open_log_file(&log)?;
    let log_offset = output.metadata()?.len();
    let executable = std::env::current_exe().context("resolve current cued executable")?;
    let mut command = Command::new(executable);
    command
        .arg("start")
        .arg("--fg")
        .arg("--socket")
        .arg(socket)
        .arg("--db")
        .arg(database)
        .env("CUE_DAEMON_INSTANCE_ID", instance)
        .stdin(Stdio::null())
        .stdout(output.try_clone()?)
        .stderr(output);
    if let Some(config) = config {
        command.arg("--config").arg(config);
    }
    // SAFETY: the post-fork hook only calls the async-signal-safe setsid syscall.
    // A new session and redirected descriptors detach the daemon from the shell.
    unsafe {
        command.pre_exec(|| {
            if libc::setsid() == -1 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let child = command
        .spawn()
        .with_context(|| format!("spawn background cued; log: {}", log.display()))?;
    Ok(SpawnedDaemon {
        child,
        log,
        log_offset,
    })
}

pub(crate) async fn wait_ready(socket: &Path, instance: &str) -> Result<()> {
    wait_ready_inner(socket, instance, None, COMPLETION_TIMEOUT)
        .await
        .with_context(|| {
            format!(
                "replacement daemon is not confirmed ready; inspect {}",
                host::sidecar(socket, ".log").display()
            )
        })
}

async fn wait_ready_inner(
    socket: &Path,
    instance: &str,
    mut child: Option<&mut Child>,
    timeout: Duration,
) -> Result<()> {
    let mut last_probe = String::from("not yet probed");
    let result = tokio::time::timeout(timeout, async {
        loop {
            if let Some(child) = child.as_deref_mut()
                && let Some(status) = child.try_wait().context("check background cued exit")?
            {
                bail!("background process exited with {status}")
            }
            match host::probe(socket).await {
                Ok(actual) if actual == instance => {
                    if let Some(child) = child.as_deref_mut()
                        && let Some(status) = child.try_wait().context("check ready daemon exit")?
                    {
                        bail!("background process exited with {status}")
                    }
                    return Ok(());
                }
                Ok(actual) => last_probe = format!("another daemon instance {actual} is listening"),
                Err(error) => last_probe = format!("{error:#}"),
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await;
    match result {
        Ok(result) => result,
        Err(_) => bail!(
            "timed out after {} seconds waiting for instance {instance} at {}; the process may still be starting; last probe: {last_probe}",
            timeout.as_secs_f64(),
            socket.display(),
        ),
    }
}

fn log_tail(path: &Path, offset: u64) -> Result<String> {
    let mut file = File::open(path)?;
    let start = offset.max(file.metadata()?.len().saturating_sub(16 * 1024));
    file.seek(SeekFrom::Start(start))?;
    let mut bytes = Vec::new();
    file.take(16 * 1024).read_to_end(&mut bytes)?;
    Ok(String::from_utf8_lossy(&bytes).trim().to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn readiness_timeout_is_failure_not_background_success() {
        let socket =
            PathBuf::from("/tmp").join(format!("cued-no-listener-{}", uuid::Uuid::new_v4()));
        let error = wait_ready_inner(&socket, "expected", None, Duration::from_millis(50))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("timed out"));
    }
}
