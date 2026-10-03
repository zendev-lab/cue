use anyhow::{Context, Result, bail};
use std::process::Stdio;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// A provider is an isolated process group. Bound time and output, including inherited pipes.
pub async fn run(argv: &[String], input: Vec<u8>, timeout_ms: u64) -> Result<Vec<u8>> {
    let (program, args) = argv.split_first().context("empty provider argv")?;
    let mut command = tokio::process::Command::new(program);
    command
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    command.process_group(0);
    let mut child = command.spawn().context("spawn resource provider")?;
    let pid = child.id().context("provider has no process ID")?;
    struct Group(u32);
    impl Drop for Group {
        fn drop(&mut self) {
            unsafe {
                libc::kill(-(self.0 as i32), libc::SIGKILL);
            }
        }
    }
    let _group = Group(pid);
    let mut stdin = child.stdin.take().context("provider stdin")?;
    let stdout = child.stdout.take().context("provider stdout")?;
    let stderr = child.stderr.take().context("provider stderr")?;
    let result = tokio::time::timeout(Duration::from_millis(timeout_ms), async {
        let write = async {
            stdin.write_all(&input).await?;
            drop(stdin);
            Ok::<_, std::io::Error>(())
        };
        let read = async {
            let mut out = Vec::new();
            stdout.take(1_048_577).read_to_end(&mut out).await?;
            Ok::<_, std::io::Error>(out)
        };
        let err = async {
            let mut out = Vec::new();
            stderr.take(65_537).read_to_end(&mut out).await?;
            Ok::<_, std::io::Error>(out)
        };
        let (_, out, err, status) = tokio::try_join!(write, read, err, child.wait())?;
        if out.len() > 1_048_576 || err.len() > 65_536 {
            bail!("provider output limit exceeded")
        }
        if !status.success() {
            bail!(
                "provider exited {status}: {}",
                String::from_utf8_lossy(&err)
            )
        }
        Ok(out)
    })
    .await;
    match result {
        Ok(result) => result,
        Err(_) => {
            let _ = child.kill().await;
            bail!("provider timed out after {timeout_ms} ms")
        }
    }
}
