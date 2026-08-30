use std::ffi::{OsStr, OsString};
use std::fs::{File, OpenOptions, Permissions};
use std::io;
use std::os::unix::fs::{FileTypeExt as _, OpenOptionsExt as _, PermissionsExt as _};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{SystemTime, UNIX_EPOCH};

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
    Start { socket: PathBuf, database: PathBuf },
    GatewayStdio { socket: PathBuf },
    Status { socket: PathBuf },
    Stop { socket: PathBuf },
    Restart { socket: PathBuf },
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
        HostCommand::Start { socket, database } => {
            serve(socket, database).await?;
            Ok(0)
        }
        HostCommand::GatewayStdio { socket } => {
            relay_stdio(socket).await?;
            Ok(0)
        }
        HostCommand::Status { socket } => match probe(&socket).await {
            Ok(()) => {
                println!("running {}", socket.display());
                Ok(0)
            }
            Err(error) => {
                println!("not running {} ({error})", socket.display());
                Ok(1)
            }
        },
        HostCommand::Stop { socket } => {
            control(&socket, Command::Shutdown).await?;
            Ok(0)
        }
        HostCommand::Restart { socket } => {
            let response = control(&socket, Command::Restart).await?;
            println!("{}", serde_json::to_string(&response)?);
            Ok(0)
        }
        HostCommand::Help | HostCommand::Version => unreachable!(),
    }
}

async fn serve(socket: PathBuf, database: PathBuf) -> Result<()> {
    dirs::ensure_private_parent(&socket)?;
    dirs::ensure_private_parent(&database)?;
    let lock_path = sidecar(&socket, ".lock");
    let instance_lock = InstanceLock::acquire(&lock_path)?;
    prepare_socket(&socket).await?;
    if database == dirs::database_path()?
        && let Some(archive) = dirs::archive_legacy_database(now_ms())?
    {
        tracing::warn!(path = %archive.display(), "archived IPC v3 database without importing incompatible semantics");
    }
    let database_file = dirs::create_private_file(&database)?;
    drop(database_file);
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
            result = tokio::signal::ctrl_c() => {
                result.context("install Ctrl-C handler")?;
                break LifecycleSignal::Shutdown;
            }
        }
    };

    drop(listener);
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
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
    drop(socket_guard);
    drop(instance_lock);

    if let LifecycleSignal::Restart {
        target_instance_id, ..
    } = signal
    {
        spawn_successor(&socket, &database, &target_instance_id)?;
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

async fn probe(socket: &Path) -> Result<()> {
    let mut stream = UnixStream::connect(socket)
        .await
        .with_context(|| format!("connect to {}", socket.display()))?;
    hello(&mut stream).await
}

async fn control(socket: &Path, command: Command) -> Result<ResultPayload> {
    let mut stream = UnixStream::connect(socket)
        .await
        .with_context(|| format!("connect to {}", socket.display()))?;
    hello(&mut stream).await?;
    let request_id = RequestId::new(2)?;
    write_message(
        &mut stream,
        &Message::Command {
            request_id,
            operation_id: OperationId::new(format!("cued-control:{}", uuid::Uuid::new_v4()))?,
            command,
        },
    )
    .await?;
    match read_message(&mut stream).await? {
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

async fn hello<S>(stream: &mut S) -> Result<()>
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
                    ..
                }),
        } if actual == request_id => Ok(()),
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

fn spawn_successor(socket: &Path, database: &Path, instance_id: &str) -> Result<()> {
    let executable = std::env::current_exe().context("resolve current cued executable")?;
    std::process::Command::new(executable)
        .arg("start")
        .arg("--socket")
        .arg(socket)
        .arg("--db")
        .arg(database)
        .env("CUE_DAEMON_INSTANCE_ID", instance_id)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .spawn()
        .context("spawn replacement cued")?;
    Ok(())
}

struct InstanceLock(File);

impl InstanceLock {
    fn acquire(path: &Path) -> Result<Self> {
        dirs::ensure_private_parent(path)?;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .open(path)?;
        // SAFETY: flock operates on this owned descriptor for the lifetime of
        // InstanceLock and does not access memory.
        let result = unsafe {
            libc::flock(
                std::os::fd::AsRawFd::as_raw_fd(&file),
                libc::LOCK_EX | libc::LOCK_NB,
            )
        };
        if result != 0 {
            bail!("another cued instance owns {}", path.display())
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
    let first = args.next();
    let command = first.as_deref().and_then(OsStr::to_str).unwrap_or("start");
    if first.is_some() && first.as_deref().and_then(OsStr::to_str).is_none() {
        bail!("cued command must be valid UTF-8")
    }
    match command {
        "help" | "-h" | "--help" => no_args(args, HostCommand::Help),
        "version" | "-V" | "--version" => no_args(args, HostCommand::Version),
        "start" => {
            let (socket, database) = paths(args, true)?;
            Ok(HostCommand::Start { socket, database })
        }
        "gateway-stdio" => {
            let (socket, _) = paths(args, false)?;
            Ok(HostCommand::GatewayStdio { socket })
        }
        "status" => {
            let (socket, _) = paths(args, false)?;
            Ok(HostCommand::Status { socket })
        }
        "stop" => {
            let (socket, _) = paths(args, false)?;
            Ok(HostCommand::Stop { socket })
        }
        "restart" => {
            let (socket, _) = paths(args, false)?;
            Ok(HostCommand::Restart { socket })
        }
        other => bail!("unknown cued command `{other}`"),
    }
}

fn paths(
    args: impl IntoIterator<Item = OsString>,
    allow_database: bool,
) -> Result<(PathBuf, PathBuf)> {
    let mut args = args.into_iter();
    let mut socket = std::env::var_os("CUE_SOCKET")
        .map(PathBuf::from)
        .unwrap_or_else(dirs::socket_path);
    let mut database = dirs::database_path()?;
    while let Some(option) = args.next() {
        match option.to_str() {
            Some("--socket") => {
                socket = PathBuf::from(
                    args.next()
                        .ok_or_else(|| anyhow::anyhow!("--socket expects a path"))?,
                );
            }
            Some("--db") if allow_database => {
                database = PathBuf::from(
                    args.next()
                        .ok_or_else(|| anyhow::anyhow!("--db expects a path"))?,
                );
            }
            Some(value) => bail!("unknown cued option `{value}`"),
            None => bail!("cued options must be valid UTF-8"),
        }
    }
    Ok((socket, database))
}

fn no_args(mut args: impl Iterator<Item = OsString>, command: HostCommand) -> Result<HostCommand> {
    if args.next().is_some() {
        bail!("command does not accept extra arguments")
    }
    Ok(command)
}

fn sidecar(path: &Path, suffix: &str) -> PathBuf {
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
        "cued {}\n\nUsage:\n  cued start [--socket PATH] [--db PATH]\n  cued status|stop|restart [--socket PATH]\n  cued gateway-stdio [--socket PATH]\n  cued --version\n\nThe daemon serves only strict IPC v4 and uses a fresh v4 SQLite database.",
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
