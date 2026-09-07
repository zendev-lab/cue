use std::ffi::OsString;
use std::fs::{File, OpenOptions, Permissions};
use std::io;
use std::os::unix::fs::{FileTypeExt as _, OpenOptionsExt as _, PermissionsExt as _};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context as _, Result, bail};
use cue_protocol::{
    ClientId, Command, Hello, Message, OperationId, PROTOCOL_VERSION, Query, RequestId,
    ResponsePayload, ResultPayload, encode_message,
};
use rusqlite::Connection;
use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _};
use tokio::net::{UnixListener, UnixStream};

use crate::dirs;
use crate::service::{DaemonService, LifecycleSignal, serve_stream};

enum HostCommand {
    Help,
    Version,
    Start {
        socket: PathBuf,
        database: PathBuf,
        foreground: bool,
    },
    GatewayStdio {
        socket: PathBuf,
    },
    Status {
        socket: PathBuf,
    },
    Stop {
        socket: PathBuf,
        force: bool,
    },
    Restart {
        socket: PathBuf,
    },
}

pub fn run_cli() -> Result<i32> {
    match parse(std::env::args_os())? {
        HostCommand::Help => {
            print_help();
            Ok(0)
        }
        HostCommand::Version => {
            println!("cued {}", crate::version());
            Ok(0)
        }
        command => {
            tracing_subscriber::fmt()
                .with_env_filter(
                    tracing_subscriber::EnvFilter::try_from_default_env()
                        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
                )
                .with_writer(std::io::stderr)
                .init();
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .context("build daemon runtime")?;
            runtime.block_on(run(command))
        }
    }
}

async fn run(command: HostCommand) -> Result<i32> {
    match command {
        HostCommand::Start {
            socket,
            database,
            foreground,
        } => {
            if foreground {
                serve(socket, database).await?;
            } else {
                crate::startup::start(&socket, &database).await?;
            }
            Ok(0)
        }
        HostCommand::GatewayStdio { socket } => {
            relay_stdio(socket).await?;
            Ok(0)
        }
        HostCommand::Status { socket } => {
            let (code, message) = status(&socket).await;
            println!("{message}");
            Ok(code)
        }
        HostCommand::Stop { socket, force } => {
            if force {
                crate::recovery::stop(&socket).await?;
            } else {
                control(&socket, Command::Shutdown).await?;
                wait_stopped(&socket).await?;
                println!("stopped {}", socket.display());
            }
            Ok(0)
        }
        HostCommand::Restart { socket } => {
            let response = control(&socket, Command::Restart).await?;
            let ResultPayload::RestartAccepted {
                target_instance_id, ..
            } = &response
            else {
                bail!("daemon returned an unexpected Restart response: {response:?}")
            };
            crate::startup::wait_ready(&socket, target_instance_id).await?;
            println!("{}", serde_json::to_string(&response)?);
            Ok(0)
        }
        HostCommand::Help | HostCommand::Version => unreachable!(),
    }
}

async fn serve(socket: PathBuf, database: PathBuf) -> Result<()> {
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .context("install SIGTERM handler")?;
    dirs::ensure_private_parent(&socket)?;
    dirs::ensure_private_parent(&database)?;
    let lock_path = sidecar(&socket, ".lock");
    let instance_lock = InstanceLock::acquire(&lock_path)?;
    prepare_socket(&socket).await?;
    if dirs::database_path().is_ok_and(|default| database == default)
        && let Some(archive) = dirs::archive_legacy_database(now_ms())?
    {
        tracing::warn!(path = %archive.display(), "archived IPC v3 database without importing incompatible semantics");
    }
    let database_file = dirs::create_private_file(&database)?;
    drop(database_file);
    // SQLite manages its own locks on the database inode. Keep host ownership
    // on a separate file so it cannot conflict with the SQLite VFS locks.
    let database_lock =
        InstanceLock::acquire(&sidecar(&std::fs::canonicalize(&database)?, ".lock"))?;
    let connection = Connection::open(&database)
        .with_context(|| format!("open v4 database {}", database.display()))?;
    let store = cue_store_sqlite::Store::from_connection(connection)?;
    let service = DaemonService::from_store(store)?;
    service.recover().await?;

    let listener = UnixListener::bind(&socket)
        .with_context(|| format!("bind daemon socket {}", socket.display()))?;
    std::fs::set_permissions(&socket, Permissions::from_mode(0o600))?;
    let socket_guard = SocketGuard(socket.clone());
    let mut lifecycle = service.subscribe_lifecycle();
    let mut connections = tokio::task::JoinSet::new();
    tracing::info!(socket = %socket.display(), database = %database.display(), "IPC v4 daemon ready");

    let signal = loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (stream, _) = accepted.context("accept IPC v4 connection")?;
                let service = service.clone();
                connections.spawn(async move {
                    if let Err(error) = serve_stream(service, stream).await {
                        tracing::warn!(%error, "IPC v4 connection closed with error");
                    }
                });
            }
            completed = connections.join_next(), if !connections.is_empty() => {
                if let Some(Err(error)) = completed {
                    tracing::warn!(%error, "IPC v4 connection task failed");
                }
            }
            signal = lifecycle.recv() => {
                match signal {
                    Ok(signal) => break signal,
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                        tracing::warn!(skipped, "lifecycle receiver lagged");
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        break LifecycleSignal::Shutdown;
                    }
                }
            }
            _ = terminate.recv() => break LifecycleSignal::Shutdown,
            result = tokio::signal::ctrl_c() => {
                result.context("install Ctrl-C handler")?;
                break LifecycleSignal::Shutdown;
            }
        }
    };

    drop(listener);
    service.drain().await?;
    while tokio::time::timeout(
        std::time::Duration::from_millis(100),
        connections.join_next(),
    )
    .await
    .ok()
    .flatten()
    .is_some()
    {}
    connections.abort_all();
    while connections.join_next().await.is_some() {}
    drop(socket_guard);
    drop(service);
    drop(database_lock);
    drop(instance_lock);

    if let LifecycleSignal::Restart {
        target_instance_id, ..
    } = signal
    {
        crate::startup::spawn_daemon(&socket, &database, &target_instance_id)?;
    }
    Ok(())
}

async fn prepare_socket(path: &Path) -> Result<()> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            bail!("refusing to replace symlinked socket {}", path.display())
        }
        Ok(metadata) if !metadata.file_type().is_socket() => {
            bail!("refusing to replace non-socket path {}", path.display())
        }
        Ok(_) => match UnixStream::connect(path).await {
            Ok(_) => bail!("cued is already listening at {}", path.display()),
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::ConnectionRefused | io::ErrorKind::NotFound
                ) =>
            {
                std::fs::remove_file(path)
                    .with_context(|| format!("remove stale socket {}", path.display()))?;
                Ok(())
            }
            Err(error) => Err(error).with_context(|| format!("probe socket {}", path.display())),
        },
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

async fn relay_stdio(socket: PathBuf) -> Result<()> {
    let stream = UnixStream::connect(&socket)
        .await
        .with_context(|| format!("connect to {}", socket.display()))?;
    let (mut socket_read, mut socket_write) = stream.into_split();
    let mut stdin = tokio::io::stdin();
    let mut stdout = tokio::io::stdout();
    let client_to_daemon = async {
        tokio::io::copy(&mut stdin, &mut socket_write).await?;
        socket_write.shutdown().await
    };
    let daemon_to_client = async {
        tokio::io::copy(&mut socket_read, &mut stdout).await?;
        stdout.flush().await
    };
    tokio::try_join!(client_to_daemon, daemon_to_client)?;
    Ok(())
}

const CONTROL_TIMEOUT: Duration = Duration::from_secs(2);

pub(crate) async fn connect_control(socket: &Path) -> Result<UnixStream> {
    tokio::time::timeout(CONTROL_TIMEOUT, UnixStream::connect(socket))
        .await
        .context("timed out connecting to daemon")?
        .with_context(|| format!("connect to {}", socket.display()))
}

fn not_listening(error: &anyhow::Error) -> bool {
    error.downcast_ref::<io::Error>().is_some_and(|error| {
        matches!(
            error.kind(),
            io::ErrorKind::NotFound | io::ErrorKind::ConnectionRefused
        )
    })
}

fn recovery_hint(socket: &Path) -> String {
    let quoted = format!("'{}'", socket.to_string_lossy().replace('\'', "'\"'\"'"));
    format!(
        "A daemon from before the upgrade may still be running.\n\
         Stop it without IPC, then start the installed version:\n  \
         cued stop --force --socket {quoted}\n  \
         cued start --socket {quoted}\n\
         start runs in the background; preserve any custom --db setting (use --fg for a supervisor).\n\
         First default v4 start archives the legacy database without importing old sessions."
    )
}

async fn control_hello(stream: &mut UnixStream, socket: &Path) -> Result<String> {
    tokio::time::timeout(CONTROL_TIMEOUT, hello(stream))
        .await
        .context("IPC v4 handshake timed out")
        .and_then(|result| result)
        .map_err(|error| {
            anyhow::anyhow!(
                "IPC v4 handshake failed at {}: {error:#}\n{}",
                socket.display(),
                recovery_hint(socket)
            )
        })
}

async fn status(socket: &Path) -> (i32, String) {
    match connect_control(socket).await {
        Ok(mut stream) => match control_hello(&mut stream, socket).await {
            Ok(_) => (0, format!("running {} (IPC v4)", socket.display())),
            Err(error) => (
                1,
                format!(
                    "listening {} but IPC v4 unavailable: {error:#}",
                    socket.display()
                ),
            ),
        },
        Err(error) if not_listening(&error) => (1, format!("not running {}", socket.display())),
        Err(error) => (1, format!("cannot determine daemon status: {error:#}")),
    }
}

pub(crate) async fn probe(socket: &Path) -> Result<String> {
    let mut stream = connect_control(socket).await?;
    control_hello(&mut stream, socket).await
}

async fn control(socket: &Path, command: Command) -> Result<ResultPayload> {
    let mut stream = match connect_control(socket).await {
        Ok(stream) => stream,
        Err(error) if not_listening(&error) && matches!(command, Command::Shutdown) => {
            println!("not running {}", socket.display());
            return Ok(ResultPayload::Ack);
        }
        Err(error) => return Err(error),
    };
    control_hello(&mut stream, socket).await?;
    tokio::time::timeout(CONTROL_TIMEOUT, send_control(&mut stream, command))
        .await
        .context("daemon control response timed out; command outcome is unknown")?
}

async fn send_control(stream: &mut UnixStream, command: Command) -> Result<ResultPayload> {
    let request_id = RequestId::new(2)?;
    write_message(
        stream,
        &Message::Command {
            request_id,
            operation_id: OperationId::new(format!("cued-control:{}", uuid::Uuid::new_v4()))?,
            command,
        },
    )
    .await
    .context("send daemon control command; command outcome is unknown")?;
    match read_message(stream)
        .await
        .context("read daemon control response; command outcome is unknown")?
    {
        Message::Response {
            request_id: actual,
            payload: ResponsePayload::Ok(result),
        } if actual == request_id => Ok(result),
        Message::Response {
            payload: ResponsePayload::Error(error),
            ..
        } => bail!(
            "daemon rejected control command: {:?}: {}",
            error.code,
            error.message
        ),
        message => bail!("unexpected daemon control response: {message:?}"),
    }
}

async fn hello<S>(stream: &mut S) -> Result<String>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let request_id = RequestId::new(1)?;
    write_message(
        stream,
        &Message::Query {
            request_id,
            query: Query::Hello(Hello {
                protocol_version: PROTOCOL_VERSION,
                client_id: ClientId::new(format!(
                    "cued-control:{}:{}",
                    std::process::id(),
                    uuid::Uuid::new_v4()
                ))?,
            }),
        },
    )
    .await?;
    match read_message(stream).await? {
        Message::Response {
            request_id: actual,
            payload:
                ResponsePayload::Ok(ResultPayload::Hello {
                    protocol_version: PROTOCOL_VERSION,
                    instance_id,
                    ..
                }),
        } if actual == request_id => Ok(instance_id),
        message => bail!("unexpected IPC v4 Hello response: {message:?}"),
    }
}

async fn write_message<W>(writer: &mut W, message: &Message) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    writer.write_all(&encode_message(message)?).await?;
    writer.flush().await?;
    Ok(())
}

async fn read_message<R>(reader: &mut R) -> Result<Message>
where
    R: AsyncRead + Unpin,
{
    let mut header = [0u8; 4];
    reader.read_exact(&mut header).await?;
    let length = u32::from_be_bytes(header) as usize;
    if length > cue_protocol::MAX_MESSAGE_SIZE {
        bail!("daemon message exceeds IPC v4 limit")
    }
    let mut frame = Vec::with_capacity(4 + length);
    frame.extend_from_slice(&header);
    frame.resize(4 + length, 0);
    reader.read_exact(&mut frame[4..]).await?;
    Ok(cue_protocol::decode_message(&frame)?)
}

struct InstanceLock(File);

impl InstanceLock {
    fn acquire(path: &Path) -> Result<Self> {
        let file = dirs::create_private_file(path)?;
        Self::from_file(file, path)
    }

    fn from_file(file: File, path: &Path) -> Result<Self> {
        // SAFETY: flock operates on this owned descriptor for the lifetime of
        // InstanceLock and does not access memory.
        let result = unsafe {
            libc::flock(
                std::os::fd::AsRawFd::as_raw_fd(&file),
                libc::LOCK_EX | libc::LOCK_NB,
            )
        };
        if result != 0 {
            return Err(io::Error::last_os_error())
                .with_context(|| format!("another cued instance owns {}", path.display()));
        }
        Ok(Self(file))
    }
}

impl Drop for InstanceLock {
    fn drop(&mut self) {
        // SAFETY: the descriptor remains valid until this Drop completes.
        unsafe {
            libc::flock(std::os::fd::AsRawFd::as_raw_fd(&self.0), libc::LOCK_UN);
        }
    }
}

async fn wait_stopped(socket: &Path) -> Result<()> {
    tokio::time::timeout(crate::startup::COMPLETION_TIMEOUT, async {
        loop {
            let released = match OpenOptions::new().read(true).write(true)
                .custom_flags(libc::O_NOFOLLOW).open(sidecar(socket, ".lock")) {
                Ok(file) => match InstanceLock::from_file(file, &sidecar(socket, ".lock")) {
                    Ok(lock) => {
                        // Keep ownership fenced while checking the endpoint. The
                        // listener closes before drain, so EOF alone is not enough.
                        let absent = socket_is_absent(socket).await?;
                        drop(lock);
                        absent
                    }
                    Err(error) if error.downcast_ref::<io::Error>().is_some_and(|error| error.kind() == io::ErrorKind::WouldBlock) => false,
                    Err(error) => return Err(error),
                },
                Err(error) if error.kind() == io::ErrorKind::NotFound => socket_is_absent(socket).await?,
                Err(error) => return Err(error).context("inspect daemon ownership lock"),
            };
            if released { return Ok(()); }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }).await.with_context(|| format!("shutdown accepted, but {} did not release ownership within 15 seconds; stop is not confirmed", socket.display()))?
}

async fn socket_is_absent(socket: &Path) -> Result<bool> {
    match connect_control(socket).await {
        Ok(_) => Ok(false),
        Err(error) if not_listening(&error) => Ok(true),
        Err(error) => Err(error),
    }
}

struct SocketGuard(PathBuf);

impl Drop for SocketGuard {
    fn drop(&mut self) {
        if std::fs::symlink_metadata(&self.0).is_ok_and(|metadata| metadata.file_type().is_socket())
        {
            let _ = std::fs::remove_file(&self.0);
        }
    }
}

fn parse(args: impl IntoIterator<Item = OsString>) -> Result<HostCommand> {
    let mut args = args.into_iter();
    let _program = args.next();
    let first = args.next().unwrap_or_else(|| OsString::from("start"));
    let mut command = first
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("cued command must be valid UTF-8"))?;
    let mut options = args.collect::<Vec<_>>();
    if matches!(command, "--fg" | "-f" | "--socket" | "--db") {
        options.insert(0, first.clone());
        command = "start";
    }
    if matches!(command, "help" | "-h" | "--help") {
        return no_args(options.into_iter(), HostCommand::Help);
    }
    if matches!(command, "version" | "-V" | "--version") {
        return no_args(options.into_iter(), HostCommand::Version);
    }
    if !matches!(
        command,
        "start" | "gateway-stdio" | "status" | "stop" | "restart"
    ) {
        bail!("unknown cued command `{command}`")
    }
    if options.len() == 1 && matches!(options[0].to_str(), Some("--help" | "-h")) {
        return Ok(HostCommand::Help);
    }
    let mut socket = std::env::var_os("CUE_SOCKET")
        .map(PathBuf::from)
        .unwrap_or_else(dirs::socket_path);
    let mut database = None;
    let mut foreground = false;
    let mut force = false;
    let mut options = options.into_iter();
    while let Some(option) = options.next() {
        match option.to_str() {
            Some("--socket") => {
                socket = PathBuf::from(
                    options
                        .next()
                        .ok_or_else(|| anyhow::anyhow!("--socket expects a path"))?,
                )
            }
            Some("--db") if command == "start" => {
                database = Some(PathBuf::from(
                    options
                        .next()
                        .ok_or_else(|| anyhow::anyhow!("--db expects a path"))?,
                ))
            }
            Some("--fg" | "-f") if command == "start" && !foreground => foreground = true,
            Some("--force") if command == "stop" && !force => force = true,
            Some(value) => bail!("unknown or repeated {command} option `{value}`"),
            None => bail!("cued options must be valid UTF-8"),
        }
    }
    let socket = std::path::absolute(socket).context("resolve daemon socket path")?;
    Ok(match command {
        "start" => HostCommand::Start {
            socket,
            database: std::path::absolute(match database {
                Some(path) => path,
                None => dirs::database_path()?,
            })
            .context("resolve daemon database path")?,
            foreground,
        },
        "gateway-stdio" => HostCommand::GatewayStdio { socket },
        "status" => HostCommand::Status { socket },
        "stop" => HostCommand::Stop { socket, force },
        "restart" => HostCommand::Restart { socket },
        _ => unreachable!(),
    })
}

fn no_args(mut args: impl Iterator<Item = OsString>, command: HostCommand) -> Result<HostCommand> {
    if args.next().is_some() {
        bail!("command does not accept extra arguments")
    }
    Ok(command)
}

pub(crate) fn sidecar(path: &Path, suffix: &str) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(suffix);
    PathBuf::from(value)
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}

fn print_help() {
    println!(
        "cued {}\n\nUsage:\n  cued start [--fg|-f] [--socket PATH] [--db PATH]\n  cued status|restart [--socket PATH]\n  cued stop [--force] [--socket PATH]\n  cued gateway-stdio [--socket PATH]\n  cued --version\n\nstart runs in the background and returns after IPC v4 readiness.\n  --fg/-f runs in the foreground for terminals and service managers.\n  stop waits for shutdown; restart waits for the requested successor to be ready.\n  Background logs are appended to <socket>.log.\n\nThe daemon serves only strict IPC v4 and uses a fresh v4 SQLite database.\n  stop --force sends SIGTERM to the same-user socket peer without IPC and waits for exit; it never sends SIGKILL.",
        crate::version()
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parser_contains_no_v3_lifecycle_or_policy_commands() {
        assert!(parse([OsString::from("cued"), OsString::from("start")]).is_ok());
        assert!(parse([OsString::from("cued"), OsString::from("eval")]).is_err());
        assert!(parse([OsString::from("cued"), OsString::from("cron")]).is_err());
    }

    #[test]
    fn force_is_only_a_stop_option_and_socket_values_are_not_flags() {
        for args in [
            vec!["cued", "stop", "--force", "--socket", "custom.sock"],
            vec!["cued", "stop", "--socket", "custom.sock", "--force"],
        ] {
            assert!(
                matches!(parse(args.into_iter().map(OsString::from)).unwrap(), HostCommand::Stop { force: true, socket } if socket == std::path::absolute("custom.sock").unwrap())
            );
        }
        assert!(
            matches!(parse(["cued", "stop", "--socket", "--force"].map(OsString::from)).unwrap(), HostCommand::Stop { force: false, socket } if socket == std::path::absolute("--force").unwrap())
        );
        for args in [
            vec!["cued", "restart", "--force"],
            vec!["cued", "stop", "--force", "--force"],
            vec!["cued", "stop", "--socket"],
        ] {
            assert!(parse(args.into_iter().map(OsString::from)).is_err());
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn unix_host_serves_v4_control_and_removes_its_socket_on_shutdown() {
        let root = PathBuf::from("/tmp").join(format!("cued-host-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&root).unwrap();
        let socket = root.join("cued.sock");
        let database = root.join("cued-v4.db");
        let mut serving = tokio::spawn(serve(socket.clone(), database.clone()));

        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if probe(&socket).await.is_ok() {
                    break;
                }
                if serving.is_finished() {
                    let outcome = (&mut serving).await;
                    panic!("daemon exited before binding its socket: {outcome:?}");
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("daemon must bind its Unix socket");

        assert!(matches!(
            control(&socket, Command::Shutdown).await.unwrap(),
            ResultPayload::Ack
        ));
        tokio::time::timeout(std::time::Duration::from_secs(5), serving)
            .await
            .expect("daemon must stop after Shutdown")
            .expect("daemon task must not panic")
            .expect("daemon must stop cleanly");
        assert!(!socket.exists());
        assert!(database.exists());
        std::fs::remove_dir_all(root).unwrap();
    }
}
