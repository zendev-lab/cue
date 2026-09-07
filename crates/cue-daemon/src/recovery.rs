//! Explicit, protocol-independent local shutdown. No legacy wire types or PID files.
use std::io;
use std::os::unix::fs::FileTypeExt as _;
use std::path::Path;
use std::time::Duration;

use anyhow::{Context as _, Result, bail};

use crate::host::connect_control;

pub(crate) async fn stop(socket: &Path) -> Result<()> {
    match std::fs::symlink_metadata(socket) {
        Ok(metadata) if !metadata.file_type().is_socket() => {
            bail!(
                "refusing to signal a peer through non-socket path {}",
                socket.display()
            )
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            println!("not running {}", socket.display());
            return Ok(());
        }
        Err(error) => return Err(error).context("inspect daemon socket"),
        Ok(_) => {}
    }
    let stream = match connect_control(socket).await {
        Ok(stream) => stream,
        Err(error)
            if error
                .downcast_ref::<io::Error>()
                .is_some_and(|error| error.kind() == io::ErrorKind::ConnectionRefused) =>
        {
            println!("not running {} (stale socket)", socket.display());
            return Ok(());
        }
        Err(error) => return Err(error),
    };
    let credentials = stream.peer_cred().context("read socket peer credentials")?;
    // SAFETY: geteuid has no preconditions and does not access Rust memory.
    let uid = unsafe { libc::geteuid() };
    let pid = validate_peer(credentials.uid(), credentials.pid(), uid)?;
    // Keep the connection alive until after signalling; never use a PID file or
    // scan/kill processes by name. --force explicitly targets this socket peer.
    // SAFETY: pid is a positive, non-self process ID authenticated by the kernel.
    if unsafe { libc::kill(pid, libc::SIGTERM) } != 0 {
        let error = io::Error::last_os_error();
        if error.raw_os_error() != Some(libc::ESRCH) {
            return Err(error).context("send SIGTERM to socket peer");
        }
    }
    drop(stream);
    println!("sent SIGTERM to socket peer {pid}; waiting for exit");
    tokio::time::timeout(Duration::from_secs(5), async {
        while process_exists(pid)? {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await
    .with_context(|| format!("socket peer {pid} did not exit within 5 seconds after SIGTERM; stop is not confirmed; no SIGKILL was sent"))??;
    // A supervisor may have immediately replaced the process. Do not claim the
    // endpoint is stopped or signal the replacement without a new request.
    match connect_control(socket).await {
        Ok(_) => bail!(
            "socket peer {pid} exited but {} is listening again; stop the supervising service before restarting manually",
            socket.display()
        ),
        Err(error)
            if error.downcast_ref::<io::Error>().is_some_and(|error| {
                matches!(
                    error.kind(),
                    io::ErrorKind::NotFound | io::ErrorKind::ConnectionRefused
                )
            }) => {}
        Err(error) => return Err(error).context("peer exited but could not verify socket release"),
    }
    println!("stopped {} (peer {pid} exited)", socket.display());
    Ok(())
}

fn validate_peer(uid: u32, pid: Option<i32>, current_uid: u32) -> Result<i32> {
    if uid != current_uid {
        bail!("refusing to signal a socket peer owned by uid {uid} (current uid {current_uid})")
    }
    let pid = pid.context("this platform does not expose the socket peer PID; stop the daemon through its service manager")?;
    if pid <= 1 || pid == std::process::id() as i32 {
        bail!("refusing to signal invalid socket peer PID {pid}")
    }
    Ok(pid)
}

fn process_exists(pid: i32) -> Result<bool> {
    // SAFETY: signal 0 only checks the existence/permission of a positive PID.
    if unsafe { libc::kill(pid, 0) } == 0 {
        return Ok(true);
    }
    let error = io::Error::last_os_error();
    match error.raw_os_error() {
        Some(libc::ESRCH) => Ok(false),
        Some(libc::EPERM) => Ok(true),
        _ => Err(error).context("check socket peer exit"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recovery_rejects_unverified_or_dangerous_signal_targets() {
        assert!(validate_peer(10, Some(42), 11).is_err());
        for pid in [
            None,
            Some(-1),
            Some(0),
            Some(1),
            Some(std::process::id() as i32),
        ] {
            assert!(validate_peer(10, pid, 10).is_err());
        }
        assert_eq!(validate_peer(10, Some(42), 10).unwrap(), 42);
    }
}
