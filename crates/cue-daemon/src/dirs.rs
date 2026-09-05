use std::ffi::OsString;
use std::fs::{File, OpenOptions, Permissions};
use std::io;
use std::os::unix::fs::{OpenOptionsExt as _, PermissionsExt as _};
use std::path::{Path, PathBuf};

use anyhow::{Context as _, Result, bail};

const PRIVATE_DIR_MODE: u32 = 0o700;
const PRIVATE_FILE_MODE: u32 = 0o600;

pub fn socket_path() -> PathBuf {
    let root = non_empty(std::env::var_os("XDG_RUNTIME_DIR"))
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    root.join("cue/cued.sock")
}

pub fn data_dir() -> Result<PathBuf> {
    if let Some(root) = non_empty(std::env::var_os("XDG_DATA_HOME")) {
        return Ok(PathBuf::from(root).join("cue"));
    }
    let home = non_empty(std::env::var_os("HOME"))
        .ok_or_else(|| anyhow::anyhow!("HOME or XDG_DATA_HOME is required"))?;
    Ok(PathBuf::from(home).join(".local/share/cue"))
}

pub fn database_path() -> Result<PathBuf> {
    Ok(data_dir()?.join("cued-v4.db"))
}

#[cfg(test)]
pub fn legacy_database_path() -> Result<PathBuf> {
    Ok(data_dir()?.join("cued.db"))
}

pub fn ensure_private_parent(path: &Path) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("{} has no parent", path.display()))?;
    std::fs::create_dir_all(parent)
        .with_context(|| format!("create directory {}", parent.display()))?;
    reject_symlink(parent)?;
    std::fs::set_permissions(parent, Permissions::from_mode(PRIVATE_DIR_MODE))
        .with_context(|| format!("secure directory {}", parent.display()))
}

pub fn create_private_file(path: &Path) -> Result<File> {
    ensure_private_parent(path)?;
    if path.exists() {
        reject_symlink(path)?;
    }
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(PRIVATE_FILE_MODE)
        .open(path)
        .with_context(|| format!("open private file {}", path.display()))?;
    file.set_permissions(Permissions::from_mode(PRIVATE_FILE_MODE))?;
    Ok(file)
}

pub fn archive_legacy_database(now_ms: i64) -> Result<Option<PathBuf>> {
    archive_legacy_database_in(&data_dir()?, now_ms)
}

fn archive_legacy_database_in(root: &Path, now_ms: i64) -> Result<Option<PathBuf>> {
    let source = root.join("cued.db");
    if !archive_source_exists(&source)? {
        return Ok(None);
    }
    let archive = root.join(format!("cued-v3-{now_ms}.db.archive"));
    reject_archive_target(&archive)?;

    let sidecars = ["-wal", "-shm"]
        .into_iter()
        .map(|suffix| {
            let source = with_suffix(&source, suffix);
            let archive = with_suffix(&archive, suffix);
            Ok(archive_source_exists(&source)?.then_some((source, archive)))
        })
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    for (_, target) in &sidecars {
        reject_archive_target(target)?;
    }

    std::fs::rename(&source, &archive).with_context(|| {
        format!(
            "archive legacy database {} as {}",
            source.display(),
            archive.display()
        )
    })?;
    for (index, (sidecar, archived_sidecar)) in sidecars.iter().enumerate() {
        if let Err(error) = std::fs::rename(sidecar, archived_sidecar) {
            for (moved_source, moved_archive) in sidecars[..index].iter().rev() {
                let _ = std::fs::rename(moved_archive, moved_source);
            }
            let _ = std::fs::rename(&archive, &source);
            return Err(error).with_context(|| {
                format!(
                    "archive legacy database sidecar {} as {}",
                    sidecar.display(),
                    archived_sidecar.display()
                )
            });
        }
    }

    std::fs::set_permissions(&archive, Permissions::from_mode(0o400))?;
    for (_, archived_sidecar) in &sidecars {
        std::fs::set_permissions(archived_sidecar, Permissions::from_mode(0o400))?;
    }
    Ok(Some(archive))
}

fn archive_source_exists(path: &Path) -> Result<bool> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            bail!("refusing to archive symlink {}", path.display())
        }
        Ok(metadata) if !metadata.is_file() => {
            bail!("refusing to archive non-file {}", path.display())
        }
        Ok(_) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}

fn reject_archive_target(path: &Path) -> Result<()> {
    match std::fs::symlink_metadata(path) {
        Ok(_) => bail!("refusing to replace archive {}", path.display()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn with_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(suffix);
    value.into()
}

fn reject_symlink(path: &Path) -> Result<()> {
    if std::fs::symlink_metadata(path)?.file_type().is_symlink() {
        bail!("refusing to use symlinked path {}", path.display())
    }
    Ok(())
}

fn non_empty(value: Option<OsString>) -> Option<OsString> {
    value.filter(|value| !value.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_paths_use_distinct_v4_database() {
        assert_eq!(database_path().unwrap().file_name().unwrap(), "cued-v4.db");
        assert_eq!(
            legacy_database_path().unwrap().file_name().unwrap(),
            "cued.db"
        );
        assert_eq!(socket_path().file_name().unwrap(), "cued.sock");
    }

    #[test]
    fn archives_legacy_database_and_sidecars_without_importing_them() {
        let root = PathBuf::from("/tmp").join(format!("cue-archive-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&root).unwrap();
        let legacy = root.join("cued.db");
        let wal = with_suffix(&legacy, "-wal");
        std::fs::write(&legacy, b"v3 database").unwrap();
        std::fs::write(&wal, b"v3 wal").unwrap();

        let archive = archive_legacy_database_in(&root, 42)
            .unwrap()
            .expect("legacy database must be archived");
        assert_eq!(archive.file_name().unwrap(), "cued-v3-42.db.archive");
        assert_eq!(std::fs::read(&archive).unwrap(), b"v3 database");
        assert_eq!(
            std::fs::read(with_suffix(&archive, "-wal")).unwrap(),
            b"v3 wal"
        );
        assert_eq!(
            std::fs::metadata(&archive).unwrap().permissions().mode() & 0o777,
            0o400
        );
        assert!(!legacy.exists());

        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn archive_collision_leaves_legacy_database_and_sidecars_untouched() {
        let root = PathBuf::from("/tmp").join(format!("cue-archive-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&root).unwrap();
        let legacy = root.join("cued.db");
        let wal = with_suffix(&legacy, "-wal");
        let archive = root.join("cued-v3-42.db.archive");
        std::fs::write(&legacy, b"v3 database").unwrap();
        std::fs::write(&wal, b"v3 wal").unwrap();
        std::fs::write(with_suffix(&archive, "-wal"), b"collision").unwrap();

        let error = archive_legacy_database_in(&root, 42).unwrap_err();
        assert!(error.to_string().contains("refusing to replace archive"));
        assert_eq!(std::fs::read(&legacy).unwrap(), b"v3 database");
        assert_eq!(std::fs::read(&wal).unwrap(), b"v3 wal");
        assert!(!archive.exists());

        std::fs::remove_dir_all(root).unwrap();
    }
}
