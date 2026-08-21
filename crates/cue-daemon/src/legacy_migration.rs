//! One-time, fail-safe migration from the pre-v3 `cue-shell` XDG layout.
//!
//! The legacy source remains untouched. A SQLite backup plus copied output,
//! state, and config files form a read-only archive. Runtime state imports only
//! scopes and sessions; jobs, chains, scripts, and crons never become live v3
//! state.

use std::ffi::OsString;
use std::fs::{self, File, OpenOptions, Permissions};
use std::io::{self, Write};
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use rusqlite::{Connection, OpenFlags, backup::Backup};

const LEGACY_APP_DIR: &str = "cue-shell";
const ARCHIVE_NAME: &str = "cue-shell-v18";
const IMPORT_MARKER: &str = "cue-shell-v18.imported";
const MAX_LEGACY_SCHEMA_VERSION: u32 = 18;

#[derive(Debug, Clone)]
struct LegacyLayout {
    runtime: PathBuf,
    data: PathBuf,
    state: PathBuf,
    config: PathBuf,
    archive_root: PathBuf,
    marker: PathBuf,
    new_config: PathBuf,
}

pub(crate) fn prepare() -> Result<()> {
    let layout = layout_from_env(
        std::env::var_os("XDG_RUNTIME_DIR"),
        std::env::var_os("XDG_DATA_HOME"),
        std::env::var_os("XDG_STATE_HOME"),
        std::env::var_os("XDG_CONFIG_HOME"),
        std::env::var_os("HOME"),
        std::env::temp_dir(),
        crate::dirs::data_dir()?,
    )?;
    prepare_layout(&layout)
}

pub(crate) fn import_context(destination: &Connection) -> Result<()> {
    let layout = layout_from_env(
        std::env::var_os("XDG_RUNTIME_DIR"),
        std::env::var_os("XDG_DATA_HOME"),
        std::env::var_os("XDG_STATE_HOME"),
        std::env::var_os("XDG_CONFIG_HOME"),
        std::env::var_os("HOME"),
        std::env::temp_dir(),
        crate::dirs::data_dir()?,
    )?;
    import_layout_context(destination, &layout)
}

fn layout_from_env(
    xdg_runtime: Option<OsString>,
    xdg_data: Option<OsString>,
    xdg_state: Option<OsString>,
    xdg_config: Option<OsString>,
    home: Option<OsString>,
    temp: PathBuf,
    new_data: PathBuf,
) -> Result<LegacyLayout> {
    let home = home.filter(|value| !value.is_empty()).map(PathBuf::from);
    let base = |override_value: Option<OsString>, fallback: &str| -> Result<PathBuf> {
        if let Some(value) = override_value.filter(|value| !value.is_empty()) {
            Ok(PathBuf::from(value))
        } else {
            let Some(home) = home.as_ref() else {
                bail!("HOME is not set while resolving legacy Cue paths")
            };
            Ok(home.join(fallback))
        }
    };
    let runtime = xdg_runtime
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .unwrap_or(temp)
        .join(LEGACY_APP_DIR);
    let archive_parent = new_data.join("legacy");
    Ok(LegacyLayout {
        runtime,
        data: base(xdg_data, ".local/share")?.join(LEGACY_APP_DIR),
        state: base(xdg_state, ".local/state")?.join(LEGACY_APP_DIR),
        config: base(xdg_config, ".config")?.join(LEGACY_APP_DIR),
        archive_root: archive_parent.join(ARCHIVE_NAME),
        marker: archive_parent.join(IMPORT_MARKER),
        new_config: crate::dirs::config_dir()?,
    })
}

fn prepare_layout(layout: &LegacyLayout) -> Result<()> {
    if layout.marker.exists() {
        return Ok(());
    }
    if layout.archive_root.exists() {
        copy_config_if_absent(layout)?;
        return Ok(());
    }
    ensure_legacy_daemon_stopped(&layout.runtime)?;

    let roots = [&layout.data, &layout.state, &layout.config];
    let present = roots
        .iter()
        .map(|path| path.try_exists())
        .collect::<io::Result<Vec<_>>>()?;
    if !present.iter().any(|present| *present) {
        write_marker(&layout.marker, "no legacy layout found\n")?;
        return Ok(());
    }
    for (root, present) in roots.iter().zip(&present) {
        if *present {
            reject_symlink_tree(root)?;
        }
    }

    let archive_parent = layout
        .archive_root
        .parent()
        .context("legacy archive has no parent")?;
    crate::dirs::ensure_private_dir(archive_parent)?;
    let staging = archive_parent.join(format!(".{ARCHIVE_NAME}.staging-{}", std::process::id()));
    if staging.exists() {
        bail!(
            "legacy migration staging path already exists: {}",
            staging.display()
        );
    }
    crate::dirs::ensure_private_dir(&staging)?;

    let prepared = (|| -> Result<()> {
        if layout.data.exists() {
            let target = staging.join("data");
            crate::dirs::ensure_private_dir(&target)?;
            let legacy_db = layout.data.join("cued.db");
            if legacy_db.exists() {
                backup_legacy_db(&legacy_db, &target.join("cued.db"))?;
            }
            copy_tree_except_database(&layout.data, &target)?;
        }
        copy_tree_if_present(&layout.state, &staging.join("state"))?;
        copy_tree_if_present(&layout.config, &staging.join("config"))?;
        fs::rename(&staging, &layout.archive_root).with_context(|| {
            format!(
                "publish legacy archive {} -> {}",
                staging.display(),
                layout.archive_root.display()
            )
        })?;
        make_read_only(&layout.archive_root)?;
        copy_config_if_absent(layout)?;
        Ok(())
    })();
    if prepared.is_err() && staging.exists() {
        let _ = fs::remove_dir_all(&staging);
    }
    prepared
}

fn import_layout_context(destination: &Connection, layout: &LegacyLayout) -> Result<()> {
    if layout.marker.exists() {
        return Ok(());
    }
    if !layout.archive_root.exists() {
        return Ok(());
    }
    let source_path = layout.archive_root.join("data/cued.db");
    if !source_path.exists() {
        write_marker(&layout.marker, "legacy archive had no database\n")?;
        return Ok(());
    }
    let source = Connection::open_with_flags(&source_path, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .with_context(|| format!("open archived database {}", source_path.display()))?;
    validate_legacy_schema(&source)?;

    drop(source);
    destination.execute(
        "ATTACH DATABASE ?1 AS legacy",
        [source_path.to_string_lossy().as_ref()],
    )?;
    destination.execute_batch("BEGIN IMMEDIATE")?;
    let imported = destination
        .execute_batch(
            "INSERT OR IGNORE INTO scopes (hash, parent, delta_json, snap_json)
             SELECT hash, parent, delta_json, snap_json FROM legacy.scopes;
             INSERT OR IGNORE INTO scope_head (id, hash)
             SELECT id, hash FROM legacy.scope_head;
             INSERT OR IGNORE INTO sessions
                 (id, name, scope_hash, pty_default, wrapper_enabled, created_at_ms, updated_at_ms)
             SELECT id, name, scope_hash, pty_default, wrapper_enabled, created_at_ms, updated_at_ms
             FROM legacy.sessions;",
        )
        .context("copy archived context rows");
    match imported {
        Ok(()) => destination.execute_batch("COMMIT")?,
        Err(error) => {
            let _ = destination.execute_batch("ROLLBACK");
            let _ = destination.execute_batch("DETACH DATABASE legacy");
            return Err(error).context("import archived scopes and sessions");
        }
    }
    destination.execute_batch("DETACH DATABASE legacy")?;
    write_marker(&layout.marker, "scopes and sessions imported\n")
}

fn ensure_legacy_daemon_stopped(runtime: &Path) -> Result<()> {
    reject_symlink_if_present(runtime)?;
    let socket = runtime.join("cued.sock");
    reject_symlink_if_present(&socket)?;
    if socket.exists() && UnixStream::connect(&socket).is_ok() {
        bail!(
            "legacy cued is still accepting connections at {}; stop it before upgrading",
            socket.display()
        );
    }
    let pid_path = {
        let mut path = socket.as_os_str().to_os_string();
        path.push(".cued.pid");
        PathBuf::from(path)
    };
    reject_symlink_if_present(&pid_path)?;
    if let Ok(text) = fs::read_to_string(&pid_path)
        && let Ok(pid) = text.trim().parse::<i32>()
        && pid > 0
        && unsafe { libc::kill(pid, 0) } == 0
    {
        bail!("legacy cued process {pid} is still running; stop it before upgrading");
    }
    Ok(())
}

fn backup_legacy_db(source_path: &Path, destination_path: &Path) -> Result<()> {
    reject_symlink_if_present(source_path)?;
    let source = Connection::open_with_flags(source_path, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .with_context(|| format!("open legacy database {}", source_path.display()))?;
    validate_legacy_schema(&source)?;
    let mut destination = Connection::open(destination_path)
        .with_context(|| format!("create legacy backup {}", destination_path.display()))?;
    Backup::new(&source, &mut destination)?
        .run_to_completion(128, Duration::from_millis(5), None)
        .context("back up legacy SQLite database")?;
    crate::dirs::secure_private_file(destination_path)?;
    Ok(())
}

fn validate_legacy_schema(connection: &Connection) -> Result<()> {
    let version: u32 = connection.pragma_query_value(None, "user_version", |row| row.get(0))?;
    if version > MAX_LEGACY_SCHEMA_VERSION {
        bail!(
            "legacy database schema v{version} is newer than supported archive schema v{MAX_LEGACY_SCHEMA_VERSION}"
        );
    }
    for table in ["scopes", "scope_head", "sessions"] {
        let exists: bool = connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1)",
            [table],
            |row| row.get(0),
        )?;
        if !exists {
            bail!("legacy database is missing required table {table}");
        }
    }
    Ok(())
}

fn copy_tree_if_present(source: &Path, destination: &Path) -> Result<()> {
    if !source.exists() {
        return Ok(());
    }
    crate::dirs::ensure_private_dir(destination)?;
    copy_tree(source, destination, false)
}

fn copy_tree_except_database(source: &Path, destination: &Path) -> Result<()> {
    copy_tree(source, destination, true)
}

fn copy_tree(source: &Path, destination: &Path, skip_database: bool) -> Result<()> {
    for entry in fs::read_dir(source).with_context(|| format!("read {}", source.display()))? {
        let entry = entry?;
        let name = entry.file_name();
        if skip_database
            && matches!(
                name.to_str(),
                Some("cued.db" | "cued.db-wal" | "cued.db-shm")
            )
        {
            continue;
        }
        let metadata = entry.file_type()?;
        if metadata.is_symlink() {
            bail!("refusing to archive symlink {}", entry.path().display());
        }
        let target = destination.join(&name);
        if metadata.is_dir() {
            crate::dirs::ensure_private_dir(&target)?;
            copy_tree(&entry.path(), &target, false)?;
        } else if metadata.is_file() {
            copy_file(&entry.path(), &target)?;
        } else {
            bail!(
                "refusing to archive special file {}",
                entry.path().display()
            );
        }
    }
    Ok(())
}

fn copy_file(source: &Path, destination: &Path) -> Result<()> {
    reject_symlink_if_present(source)?;
    let mut input = File::open(source)?;
    let mut output = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(destination)?;
    io::copy(&mut input, &mut output)?;
    output.sync_all()?;
    Ok(())
}

fn copy_config_if_absent(layout: &LegacyLayout) -> Result<()> {
    let archived = layout.archive_root.join("config/daemon.toml");
    if !archived.exists() {
        return Ok(());
    }
    let destination = layout.new_config.join("daemon.toml");
    if destination.exists() {
        return Ok(());
    }
    crate::dirs::ensure_private_dir(&layout.new_config)?;
    copy_file(&archived, &destination).context("copy legacy daemon config")
}

fn reject_symlink_tree(path: &Path) -> Result<()> {
    reject_symlink_if_present(path)?;
    if !path.exists() {
        return Ok(());
    }
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let metadata = entry.file_type()?;
        if metadata.is_symlink() {
            bail!("refusing to migrate symlink {}", entry.path().display());
        }
        if metadata.is_dir() {
            reject_symlink_tree(&entry.path())?;
        }
    }
    Ok(())
}

fn reject_symlink_if_present(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            bail!("refusing to migrate symlinked path {}", path.display())
        }
        Ok(_) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn make_read_only(path: &Path) -> Result<()> {
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            make_read_only(&entry.path())?;
        } else {
            fs::set_permissions(entry.path(), Permissions::from_mode(0o400))?;
        }
    }
    fs::set_permissions(path, Permissions::from_mode(0o500))?;
    Ok(())
}

fn write_marker(path: &Path, contents: &str) -> Result<()> {
    let parent = path.parent().context("legacy marker has no parent")?;
    crate::dirs::ensure_private_dir(parent)?;
    if path.exists() {
        return Ok(());
    }
    let temporary = parent.join(format!(".{IMPORT_MARKER}.{}", std::process::id()));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(&temporary)?;
    file.write_all(contents.as_bytes())?;
    file.sync_all()?;
    fs::rename(&temporary, path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_root(name: &str) -> PathBuf {
        let root = std::env::temp_dir().join(format!(
            "cue-legacy-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        fs::create_dir_all(&root).expect("create test root");
        root
    }

    fn test_layout(root: &Path) -> LegacyLayout {
        LegacyLayout {
            runtime: root.join("runtime/cue-shell"),
            data: root.join("data/cue-shell"),
            state: root.join("state/cue-shell"),
            config: root.join("config/cue-shell"),
            archive_root: root.join("new-data/cue/legacy/cue-shell-v18"),
            marker: root.join("new-data/cue/legacy/cue-shell-v18.imported"),
            new_config: root.join("new-config/cue"),
        }
    }

    fn create_legacy_db(path: &Path, dangling_session: bool) {
        fs::create_dir_all(path.parent().expect("database parent")).expect("create data dir");
        let connection = Connection::open(path).expect("open legacy db");
        connection
            .execute_batch(
                "CREATE TABLE scopes (
                    hash BLOB PRIMARY KEY,
                    parent BLOB,
                    delta_json TEXT,
                    snap_json TEXT
                 );
                 CREATE TABLE scope_head (id INTEGER PRIMARY KEY, hash BLOB NOT NULL);
                 CREATE TABLE sessions (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL UNIQUE,
                    scope_hash BLOB,
                    pty_default INTEGER,
                    wrapper_enabled INTEGER,
                    created_at_ms INTEGER NOT NULL,
                    updated_at_ms INTEGER NOT NULL
                 );
                 CREATE TABLE jobs_history (id TEXT PRIMARY KEY);
                 CREATE TABLE crons (id TEXT PRIMARY KEY);
                 INSERT INTO jobs_history VALUES ('J9');
                 INSERT INTO crons VALUES ('C9');
                 PRAGMA user_version = 18;",
            )
            .expect("create legacy schema");
        let scope = vec![7_u8; 32];
        connection
            .execute(
                "INSERT INTO scopes (hash, parent, delta_json, snap_json) VALUES (?1, NULL, NULL, NULL)",
                [&scope],
            )
            .expect("insert volatile scope");
        connection
            .execute("INSERT INTO scope_head VALUES (0, ?1)", [&scope])
            .expect("insert scope head");
        let session_scope = if dangling_session {
            vec![8_u8; 32]
        } else {
            scope
        };
        connection
            .execute(
                "INSERT INTO sessions VALUES ('s1', 'build', ?1, 0, 1, 1, 2)",
                [&session_scope],
            )
            .expect("insert session");
    }

    fn cleanup(root: &Path) {
        fn writable(path: &Path) {
            let Ok(metadata) = fs::symlink_metadata(path) else {
                return;
            };
            if metadata.is_dir() {
                let _ = fs::set_permissions(path, Permissions::from_mode(0o700));
                if let Ok(entries) = fs::read_dir(path) {
                    for entry in entries.flatten() {
                        writable(&entry.path());
                    }
                }
            } else {
                let _ = fs::set_permissions(path, Permissions::from_mode(0o600));
            }
        }
        writable(root);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn archives_v18_and_imports_only_context_idempotently() {
        let root = test_root("normal");
        let layout = test_layout(&root);
        create_legacy_db(&layout.data.join("cued.db"), false);
        fs::create_dir_all(layout.data.join("output")).expect("create output");
        fs::write(layout.data.join("output/J9.log"), b"legacy output").expect("write output");
        fs::create_dir_all(&layout.config).expect("create config");
        fs::write(layout.config.join("daemon.toml"), b"[process]\n").expect("write config");

        prepare_layout(&layout).expect("prepare archive");
        assert!(
            layout.data.join("cued.db").exists(),
            "source remains untouched"
        );
        assert_eq!(
            fs::read(layout.archive_root.join("data/output/J9.log")).expect("archived output"),
            b"legacy output"
        );
        assert_eq!(
            fs::read(layout.new_config.join("daemon.toml")).expect("migrated config"),
            b"[process]\n"
        );
        assert_eq!(
            fs::metadata(&layout.archive_root)
                .expect("archive metadata")
                .permissions()
                .mode()
                & 0o777,
            0o500
        );

        let destination = crate::storage::open_db(Path::new(":memory:")).expect("new db");
        import_layout_context(&destination, &layout).expect("import context");
        import_layout_context(&destination, &layout).expect("repeat import");
        let scopes: u32 = destination
            .query_row("SELECT COUNT(*) FROM scopes", [], |row| row.get(0))
            .expect("scope count");
        let sessions: u32 = destination
            .query_row("SELECT COUNT(*) FROM sessions", [], |row| row.get(0))
            .expect("session count");
        let crons: u32 = destination
            .query_row("SELECT COUNT(*) FROM crons", [], |row| row.get(0))
            .expect("cron count");
        assert_eq!((scopes, sessions, crons), (1, 1, 0));
        let volatile: Option<String> = destination
            .query_row("SELECT snap_json FROM scopes", [], |row| row.get(0))
            .expect("volatile scope");
        assert!(volatile.is_none());
        cleanup(&root);
    }

    #[test]
    fn resumes_import_from_a_published_archive_without_a_marker() {
        let root = test_root("resume");
        let layout = test_layout(&root);
        fs::create_dir_all(layout.archive_root.join("data")).expect("create archive");
        create_legacy_db(&layout.archive_root.join("data/cued.db"), false);
        let destination = crate::storage::open_db(Path::new(":memory:")).expect("new db");

        import_layout_context(&destination, &layout).expect("resume import");

        assert!(layout.marker.exists());
        cleanup(&root);
    }

    #[test]
    fn corrupted_database_leaves_source_untouched_and_no_archive() {
        let root = test_root("corrupt");
        let layout = test_layout(&root);
        fs::create_dir_all(&layout.data).expect("create data");
        fs::write(layout.data.join("cued.db"), b"not sqlite").expect("write bad db");

        let error = prepare_layout(&layout).expect_err("corrupt database must fail");

        assert!(format!("{error:#}").contains("not a database"));
        assert_eq!(
            fs::read(layout.data.join("cued.db")).expect("source"),
            b"not sqlite"
        );
        assert!(!layout.archive_root.exists());
        cleanup(&root);
    }

    #[test]
    fn rejects_symlinks_in_legacy_data() {
        use std::os::unix::fs::symlink;

        let root = test_root("symlink");
        let layout = test_layout(&root);
        fs::create_dir_all(&layout.data).expect("create data");
        fs::write(root.join("outside"), b"secret").expect("outside");
        symlink(root.join("outside"), layout.data.join("output")).expect("create symlink");

        let error = prepare_layout(&layout).expect_err("symlink must fail");

        assert!(format!("{error:#}").contains("refusing to migrate symlink"));
        assert!(!layout.archive_root.exists());
        cleanup(&root);
    }

    #[test]
    fn failed_context_import_rolls_back_and_keeps_archive_retryable() {
        let root = test_root("rollback");
        let layout = test_layout(&root);
        fs::create_dir_all(layout.archive_root.join("data")).expect("create archive");
        create_legacy_db(&layout.archive_root.join("data/cued.db"), true);
        let destination = crate::storage::open_db(Path::new(":memory:")).expect("new db");

        import_layout_context(&destination, &layout).expect_err("foreign key must fail");

        let scopes: u32 = destination
            .query_row("SELECT COUNT(*) FROM scopes", [], |row| row.get(0))
            .expect("scope count");
        assert_eq!(scopes, 0);
        assert!(!layout.marker.exists());
        cleanup(&root);
    }
}
