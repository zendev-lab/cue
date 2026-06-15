use std::ffi::CString;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use cue_core::command::{ModeParams, ParamValue};
use tracing::{debug, warn};

use crate::dirs;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum SandboxMode {
    Overlay,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum SandboxUpper {
    Directory(PathBuf),
    Tmpfs,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SandboxConfig {
    pub mode: SandboxMode,
    pub upper: Option<SandboxUpper>,
}

#[derive(Clone, Debug)]
pub(crate) struct PreparedSandbox {
    lower_dir: PathBuf,
    mount_dir: PathBuf,
    _cleanup: Option<Arc<SandboxCleanup>>,
}

impl PreparedSandbox {
    pub fn cwd_for(&self, original_cwd: &Path) -> PathBuf {
        match original_cwd.strip_prefix(&self.lower_dir) {
            Ok(relative) if relative.as_os_str().is_empty() => self.mount_dir.clone(),
            Ok(relative) => self.mount_dir.join(relative),
            Err(_) => original_cwd.to_path_buf(),
        }
    }
}

#[derive(Debug)]
struct SandboxCleanup {
    mount_dir: PathBuf,
    _upper_dir: PathBuf,
    work_dir: PathBuf,
    tmpfs_upper_mount: Option<PathBuf>,
    root_dir: PathBuf,
}

impl Drop for SandboxCleanup {
    fn drop(&mut self) {
        if let Err(error) = unmount(&self.mount_dir) {
            warn!(path = %self.mount_dir.display(), err = %error, "sandbox: failed to unmount overlay");
        }
        if let Some(path) = self.tmpfs_upper_mount.as_ref()
            && let Err(error) = unmount(path)
        {
            warn!(path = %path.display(), err = %error, "sandbox: failed to unmount tmpfs upperdir");
        }
        if let Err(error) = std::fs::remove_dir_all(&self.work_dir)
            && error.kind() != std::io::ErrorKind::NotFound
        {
            warn!(path = %self.work_dir.display(), err = %error, "sandbox: failed to remove sandbox workdir");
        }
        if let Err(error) = std::fs::remove_dir_all(&self.root_dir)
            && error.kind() != std::io::ErrorKind::NotFound
        {
            warn!(path = %self.root_dir.display(), err = %error, "sandbox: failed to remove sandbox root");
        }
    }
}

impl SandboxConfig {
    pub fn from_params(params: &ModeParams) -> Result<Option<Self>, String> {
        let mode = match params.get("sandbox") {
            None => return Ok(None),
            Some(ParamValue::Str(value)) if value == "overlay" => SandboxMode::Overlay,
            Some(ParamValue::Str(value)) => {
                return Err(format!(
                    "unsupported sandbox `{value}`; supported value: overlay"
                ));
            }
            Some(ParamValue::Bool(_)) => return Err("sandbox expects a string value".into()),
        };

        let upper = match params.get("sandbox.upper") {
            None => None,
            Some(ParamValue::Str(value)) if value == "tmpfs" => Some(SandboxUpper::Tmpfs),
            Some(ParamValue::Str(value)) => Some(SandboxUpper::Directory(PathBuf::from(value))),
            Some(ParamValue::Bool(_)) => return Err("sandbox.upper expects a string value".into()),
        };

        Ok(Some(Self { mode, upper }))
    }
}

pub(crate) fn prepare(
    job_id: cue_core::JobId,
    config: &SandboxConfig,
    lower_dir: &Path,
) -> Result<PreparedSandbox> {
    match config.mode {
        SandboxMode::Overlay => prepare_overlay(job_id, config, lower_dir),
    }
}

#[cfg(target_os = "linux")]
fn prepare_overlay(
    job_id: cue_core::JobId,
    config: &SandboxConfig,
    lower_dir: &Path,
) -> Result<PreparedSandbox> {
    let lower_dir = std::fs::canonicalize(lower_dir)
        .with_context(|| format!("canonicalize sandbox lowerdir {}", lower_dir.display()))?;
    if !lower_dir.is_dir() {
        bail!(
            "sandbox lowerdir {} is not a directory",
            lower_dir.display()
        );
    }

    let root_dir = sandbox_root(job_id)?;
    let mount_dir = root_dir.join("merged");
    let default_upper_dir = root_dir.join("upper");
    let default_work_dir = root_dir.join("work");
    let tmpfs_dir = root_dir.join("tmpfs");
    std::fs::create_dir_all(&mount_dir)
        .with_context(|| format!("create sandbox mount dir {}", mount_dir.display()))?;

    let (upper_dir, work_dir, tmpfs_upper_mount) = match config.upper.as_ref() {
        Some(SandboxUpper::Directory(path)) => {
            std::fs::create_dir_all(path)
                .with_context(|| format!("create sandbox upperdir {}", path.display()))?;
            let upper_dir = std::fs::canonicalize(path)
                .with_context(|| format!("canonicalize sandbox upperdir {}", path.display()))?;
            let work_dir = upper_dir
                .parent()
                .unwrap_or_else(|| Path::new("/tmp"))
                .join(format!(".cue-shell-work-{job_id}"));
            if let Err(error) = std::fs::remove_dir_all(&work_dir)
                && error.kind() != std::io::ErrorKind::NotFound
            {
                return Err(error).with_context(|| {
                    format!("remove stale sandbox workdir {}", work_dir.display())
                });
            }
            (upper_dir, work_dir, None)
        }
        Some(SandboxUpper::Tmpfs) => {
            std::fs::create_dir_all(&tmpfs_dir)
                .with_context(|| format!("create sandbox tmpfs dir {}", tmpfs_dir.display()))?;
            mount_tmpfs(&tmpfs_dir)
                .with_context(|| format!("mount tmpfs sandbox dir {}", tmpfs_dir.display()))?;
            let upper_dir = tmpfs_dir.join("upper");
            let work_dir = tmpfs_dir.join("work");
            std::fs::create_dir_all(&upper_dir).with_context(|| {
                format!("create sandbox tmpfs upperdir {}", upper_dir.display())
            })?;
            std::fs::create_dir_all(&work_dir)
                .with_context(|| format!("create sandbox tmpfs workdir {}", work_dir.display()))?;
            (upper_dir, work_dir, Some(tmpfs_dir))
        }
        None => {
            std::fs::create_dir_all(&default_upper_dir).with_context(|| {
                format!("create sandbox upperdir {}", default_upper_dir.display())
            })?;
            (default_upper_dir, default_work_dir, None)
        }
    };
    std::fs::create_dir_all(&work_dir)
        .with_context(|| format!("create sandbox work dir {}", work_dir.display()))?;

    if let Err(error) =
        mount_overlay(&lower_dir, &upper_dir, &work_dir, &mount_dir).with_context(|| {
            format!(
                "mount overlay sandbox lowerdir={} upperdir={} workdir={} merged={}",
                lower_dir.display(),
                upper_dir.display(),
                work_dir.display(),
                mount_dir.display()
            )
        })
    {
        cleanup_failed_mount(&root_dir, &work_dir, tmpfs_upper_mount.as_deref());
        return Err(error);
    }

    debug!(
        %job_id,
        lower = %lower_dir.display(),
        upper = %upper_dir.display(),
        work = %work_dir.display(),
        merged = %mount_dir.display(),
        tmpfs_upper = tmpfs_upper_mount.is_some(),
        "sandbox: overlay prepared"
    );

    Ok(PreparedSandbox {
        lower_dir,
        mount_dir: mount_dir.clone(),
        _cleanup: Some(Arc::new(SandboxCleanup {
            mount_dir,
            _upper_dir: upper_dir,
            work_dir,
            tmpfs_upper_mount,
            root_dir,
        })),
    })
}

#[cfg(not(target_os = "linux"))]
fn prepare_overlay(
    _job_id: cue_core::JobId,
    _config: &SandboxConfig,
    _lower_dir: &Path,
) -> Result<PreparedSandbox> {
    bail!("overlay sandbox is only supported on Linux")
}

fn sandbox_root(job_id: cue_core::JobId) -> Result<PathBuf> {
    let dir = dirs::runtime_sandbox_dir().join(job_id.to_string());
    if let Err(error) = std::fs::remove_dir_all(&dir)
        && error.kind() != std::io::ErrorKind::NotFound
    {
        return Err(error).with_context(|| format!("remove stale sandbox dir {}", dir.display()));
    }
    std::fs::create_dir_all(&dir)
        .with_context(|| format!("create sandbox dir {}", dir.display()))?;
    Ok(dir)
}

#[cfg(target_os = "linux")]
fn mount_overlay(
    lower_dir: &Path,
    upper_dir: &Path,
    work_dir: &Path,
    mount_dir: &Path,
) -> Result<()> {
    let data = CString::new(format!(
        "lowerdir={},upperdir={},workdir={}",
        lower_dir.display(),
        upper_dir.display(),
        work_dir.display()
    ))?;
    mount(
        Some("overlay"),
        mount_dir,
        Some("overlay"),
        0,
        Some(data.as_c_str()),
    )
}

#[cfg(target_os = "linux")]
fn mount_tmpfs(target: &Path) -> Result<()> {
    mount(Some("tmpfs"), target, Some("tmpfs"), 0, Some(c"mode=700"))
}

#[cfg(target_os = "linux")]
fn mount(
    source: Option<&str>,
    target: &Path,
    fstype: Option<&str>,
    flags: libc::c_ulong,
    data: Option<&std::ffi::CStr>,
) -> Result<()> {
    use std::os::unix::ffi::OsStrExt;

    let source = source.map(CString::new).transpose()?;
    let target = CString::new(target.as_os_str().as_bytes())?;
    let fstype = fstype.map(CString::new).transpose()?;
    let rc = unsafe {
        libc::mount(
            source
                .as_ref()
                .map_or(std::ptr::null(), |value| value.as_ptr()),
            target.as_ptr(),
            fstype
                .as_ref()
                .map_or(std::ptr::null(), |value| value.as_ptr()),
            flags,
            data.map_or(std::ptr::null(), |value| value.as_ptr().cast()),
        )
    };
    if rc == -1 {
        return Err(std::io::Error::last_os_error()).context("mount syscall failed");
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn cleanup_failed_mount(root_dir: &Path, work_dir: &Path, tmpfs_upper_mount: Option<&Path>) {
    if let Some(path) = tmpfs_upper_mount
        && let Err(error) = unmount(path)
    {
        warn!(path = %path.display(), err = %error, "sandbox: failed to clean up tmpfs upperdir after mount error");
    }
    if let Err(error) = std::fs::remove_dir_all(work_dir)
        && error.kind() != std::io::ErrorKind::NotFound
    {
        warn!(path = %work_dir.display(), err = %error, "sandbox: failed to clean up workdir after mount error");
    }
    if let Err(error) = std::fs::remove_dir_all(root_dir)
        && error.kind() != std::io::ErrorKind::NotFound
    {
        warn!(path = %root_dir.display(), err = %error, "sandbox: failed to clean up root after mount error");
    }
}

fn unmount(path: &Path) -> Result<()> {
    let status = Command::new("umount")
        .arg(path)
        .status()
        .with_context(|| format!("spawn umount {}", path.display()))?;
    if !status.success() {
        bail!("umount {} exited with {status}", path.display());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn parses_overlay_sandbox_params() {
        let mut params = ModeParams {
            params: BTreeMap::new(),
        };
        params.insert("sandbox", ParamValue::Str("overlay".into()));
        params.insert("sandbox.upper", ParamValue::Str("tmpfs".into()));

        let config = SandboxConfig::from_params(&params)
            .expect("parse sandbox params")
            .expect("sandbox enabled");
        assert_eq!(config.mode, SandboxMode::Overlay);
        assert_eq!(config.upper, Some(SandboxUpper::Tmpfs));
    }

    #[test]
    fn rejects_unknown_sandbox_mode() {
        let mut params = ModeParams {
            params: BTreeMap::new(),
        };
        params.insert("sandbox", ParamValue::Str("docker".into()));

        let error = SandboxConfig::from_params(&params).expect_err("unknown mode should fail");
        assert!(error.contains("unsupported sandbox"));
    }

    #[test]
    fn rewrites_cwd_relative_to_overlay_lowerdir() {
        let prepared = PreparedSandbox {
            lower_dir: PathBuf::from("/repo"),
            mount_dir: PathBuf::from("/merged"),
            _cleanup: None,
        };

        assert_eq!(
            prepared.cwd_for(Path::new("/repo")),
            PathBuf::from("/merged")
        );
        assert_eq!(
            prepared.cwd_for(Path::new("/repo/subdir")),
            PathBuf::from("/merged/subdir")
        );
        assert_eq!(
            prepared.cwd_for(Path::new("/other")),
            PathBuf::from("/other")
        );
    }
}
