use std::io::{Read as _, Write as _};
use std::os::fd::{AsRawFd as _, FromRawFd as _, OwnedFd, RawFd};
use std::os::unix::process::ExitStatusExt as _;
use std::process::{ExitStatus, Stdio};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use cue_core::vnext::{CancelMode, IoMode, OutputStream, PipeLink, Process, Scope};
use tokio::io::{AsyncRead, AsyncReadExt as _};
use tokio::process::{Child, Command};
use tokio::sync::{mpsc, oneshot};

use crate::provider::{RunControlCommand, RunControlRequest};
use crate::{
    OutputStore, ProcessSpawner, RunControl, RunExit, RuntimeError, RuntimeErrorKind,
    RuntimeFuture, SpawnContext, SpawnRequest, SpawnedRun, TerminalSize,
};

const CONTROL_CAPACITY: usize = 32;
const IO_BUFFER_SIZE: usize = 8 * 1024;
const CHILD_POLL_INTERVAL: Duration = Duration::from_millis(20);
const GRACEFUL_TERMINATION_TIMEOUT: Duration = Duration::from_secs(5);
const OUTPUT_DRAIN_TIMEOUT: Duration = Duration::from_secs(2);
const DEFAULT_TERMINAL_COLUMNS: u16 = 80;
const DEFAULT_TERMINAL_ROWS: u16 = 24;

pub struct LocalProcessSpawner {
    output: Arc<dyn OutputStore>,
    initial_terminal_size: TerminalSize,
}

impl LocalProcessSpawner {
    pub fn new(output: Arc<dyn OutputStore>) -> Self {
        Self {
            output,
            initial_terminal_size: TerminalSize {
                columns: DEFAULT_TERMINAL_COLUMNS,
                rows: DEFAULT_TERMINAL_ROWS,
            },
        }
    }

    pub fn with_terminal_size(
        output: Arc<dyn OutputStore>,
        initial_terminal_size: TerminalSize,
    ) -> Self {
        Self {
            output,
            initial_terminal_size,
        }
    }
}

impl ProcessSpawner for LocalProcessSpawner {
    fn spawn(
        &self,
        request: SpawnRequest,
        context: SpawnContext,
    ) -> RuntimeFuture<Result<SpawnedRun, RuntimeError>> {
        let output = self.output.clone();
        let terminal_size = self.initial_terminal_size;
        Box::pin(async move {
            match request.io {
                IoMode::Captured => spawn_captured(request, context, output).await,
                IoMode::Pty => spawn_pty(request, context, output, terminal_size).await,
            }
        })
    }
}

async fn spawn_captured(
    request: SpawnRequest,
    context: SpawnContext,
    output: Arc<dyn OutputStore>,
) -> Result<SpawnedRun, RuntimeError> {
    let SpawnedPipeline { children, readers } = spawn_pipeline(&request, &context, None).await?;
    let (failure_tx, failure_rx) = mpsc::channel(1);
    let readers = spawn_readers(request.step, readers, output, failure_tx);
    let (control, control_rx) = control_channel(IoMode::Captured);
    let (complete_tx, complete_rx) = oneshot::channel();
    tokio::spawn(async move {
        let exit = supervise(children, control_rx, None, failure_rx).await;
        let exit = combine_reader_results(exit, readers).await;
        let _ = complete_tx.send(exit);
    });
    Ok(SpawnedRun::new(control, complete_rx))
}

async fn spawn_pty(
    request: SpawnRequest,
    context: SpawnContext,
    output: Arc<dyn OutputStore>,
    terminal_size: TerminalSize,
) -> Result<SpawnedRun, RuntimeError> {
    let pair = open_pty()?;
    set_terminal_size(pair.master.as_raw_fd(), terminal_size)?;
    let master = std::fs::File::from(pair.master);
    set_nonblocking(master.as_raw_fd())?;
    let reader = master
        .try_clone()
        .map_err(|error| io_error("clone PTY reader", error))?;
    let writer = master
        .try_clone()
        .map_err(|error| io_error("clone PTY writer", error))?;
    let reader = tokio::io::unix::AsyncFd::new(reader)
        .map_err(|error| io_error("register PTY reader", error))?;
    let writer = Arc::new(
        tokio::io::unix::AsyncFd::new(writer)
            .map_err(|error| io_error("register PTY writer", error))?,
    );
    let slave = std::fs::File::from(pair.slave);
    let SpawnedPipeline {
        mut children,
        readers: unexpected_readers,
    } = spawn_pipeline(&request, &context, Some(&slave)).await?;
    if !unexpected_readers.is_empty() {
        terminate_children(&mut children, CancelMode::Force);
        wait_children(&mut children).await;
        return Err(RuntimeError::infrastructure(
            "PTY pipeline exposed a second terminal-facing output",
        ));
    }
    drop(slave);
    drop(master);

    let (failure_tx, failure_rx) = mpsc::channel(1);
    let pty_reader = tokio::spawn(async move {
        let result = pump_pty(request.step, reader, output).await;
        if let Err(error) = &result {
            let _ = failure_tx.send(error.clone()).await;
        }
        result
    });
    let (control, control_rx) = control_channel(IoMode::Pty);
    let (complete_tx, complete_rx) = oneshot::channel();
    tokio::spawn(async move {
        let exit = supervise(children, control_rx, Some(writer), failure_rx).await;
        let mut pty_reader = pty_reader;
        let exit = match tokio::time::timeout(OUTPUT_DRAIN_TIMEOUT, &mut pty_reader).await {
            Ok(Ok(Ok(()))) => exit,
            Ok(Ok(Err(error))) => RunExit::InfrastructureFailure(error.to_string()),
            Ok(Err(error)) => RunExit::InfrastructureFailure(error.to_string()),
            Err(_) => {
                pty_reader.abort();
                RunExit::InfrastructureFailure("PTY output did not close after process exit".into())
            }
        };
        let _ = complete_tx.send(exit);
    });
    Ok(SpawnedRun::new(control, complete_rx))
}

fn control_channel(mode: IoMode) -> (RunControl, mpsc::Receiver<RunControlCommand>) {
    let (sender, receiver) = mpsc::channel(CONTROL_CAPACITY);
    (RunControl { mode, sender }, receiver)
}

struct SpawnedPipeline {
    children: Vec<Child>,
    readers: Vec<(OutputStream, Box<dyn AsyncRead + Unpin + Send>)>,
}

async fn spawn_pipeline(
    request: &SpawnRequest,
    context: &SpawnContext,
    terminal: Option<&std::fs::File>,
) -> Result<SpawnedPipeline, RuntimeError> {
    let processes = request.pipeline.processes().collect::<Vec<_>>();
    let mut children = Vec::with_capacity(processes.len());
    let mut readers: Vec<(OutputStream, Box<dyn AsyncRead + Unpin + Send>)> = Vec::new();
    let mut next_stdin: Option<std::fs::File> = None;

    for (index, process) in processes.iter().enumerate() {
        let mut command = configured_command(
            process,
            &request.scope,
            context,
            terminal,
            terminal.is_some() && index == 0,
        )?;
        if index == 0 {
            command.stdin(match terminal {
                Some(terminal) => Stdio::from(clone_file(terminal, "clone PTY stdin")?),
                None => Stdio::null(),
            });
        } else {
            command.stdin(Stdio::from(next_stdin.take().ok_or_else(|| {
                RuntimeError::infrastructure("pipeline successor has no stdin link")
            })?));
        }

        let link = request.pipeline.rest().get(index).map(|link| link.link());
        match (link, terminal) {
            (Some(PipeLink::StdoutToStdin), _) => {
                let (read, write) = create_pipe()?;
                command.stdout(Stdio::from(write));
                next_stdin = Some(read);
                configure_unlinked_stream(
                    &mut command,
                    OutputStream::Stderr,
                    terminal,
                    &mut readers,
                )?;
            }
            (Some(PipeLink::StderrToStdin), _) => {
                let (read, write) = create_pipe()?;
                command.stderr(Stdio::from(write));
                next_stdin = Some(read);
                configure_unlinked_stream(
                    &mut command,
                    OutputStream::Stdout,
                    terminal,
                    &mut readers,
                )?;
            }
            (Some(PipeLink::StdoutAndStderrToStdin), _) => {
                let (read, write) = create_pipe()?;
                command.stdout(Stdio::from(clone_file(&write, "clone combined pipe")?));
                command.stderr(Stdio::from(write));
                next_stdin = Some(read);
            }
            (None, _) => {
                configure_unlinked_stream(
                    &mut command,
                    OutputStream::Stdout,
                    terminal,
                    &mut readers,
                )?;
                configure_unlinked_stream(
                    &mut command,
                    OutputStream::Stderr,
                    terminal,
                    &mut readers,
                )?;
            }
        }

        let mut child = match command.spawn() {
            Ok(child) => child,
            Err(error) => {
                terminate_children(&mut children, CancelMode::Force);
                wait_children(&mut children).await;
                return Err(RuntimeError::new(
                    RuntimeErrorKind::Infrastructure,
                    format!(
                        "spawn {} for {}: {error}",
                        process.argv().program(),
                        request.step
                    ),
                ));
            }
        };
        if terminal.is_none() {
            if let Some(stdout) = child.stdout.take() {
                readers.push((OutputStream::Stdout, Box::new(stdout)));
            }
            if let Some(stderr) = child.stderr.take() {
                readers.push((OutputStream::Stderr, Box::new(stderr)));
            }
        }
        children.push(child);
    }
    Ok(SpawnedPipeline { children, readers })
}

fn configure_unlinked_stream(
    command: &mut Command,
    stream: OutputStream,
    terminal: Option<&std::fs::File>,
    _readers: &mut Vec<(OutputStream, Box<dyn AsyncRead + Unpin + Send>)>,
) -> Result<(), RuntimeError> {
    let stdio = match terminal {
        Some(terminal) => Stdio::from(clone_file(terminal, "clone PTY output")?),
        None => Stdio::piped(),
    };
    match stream {
        OutputStream::Stdout => {
            command.stdout(stdio);
        }
        OutputStream::Stderr => {
            command.stderr(stdio);
        }
        OutputStream::Terminal => {
            return Err(RuntimeError::new(
                RuntimeErrorKind::InvalidInput,
                "terminal is not a captured process stream",
            ));
        }
    }
    Ok(())
}

fn configured_command(
    process: &Process,
    scope: &Scope,
    context: &SpawnContext,
    terminal: Option<&std::fs::File>,
    terminal_leader: bool,
) -> Result<Command, RuntimeError> {
    let mut command = Command::new(process.argv().program());
    command
        .args(process.argv().arguments())
        .current_dir(context.cwd.as_path())
        .env_clear()
        .kill_on_drop(true);
    for (key, value) in process.effective_env(scope.env()) {
        command.env(key.as_str(), value.as_str());
    }
    let umask = scope.umask().get() as libc::mode_t;
    let terminal_fd = terminal.map(|file| file.as_raw_fd());
    if terminal_leader && terminal_fd.is_none() {
        return Err(RuntimeError::new(
            RuntimeErrorKind::InvalidInput,
            "PTY leader requires a terminal descriptor",
        ));
    }
    // SAFETY: the closure only calls async-signal-safe libc functions between
    // fork and exec. Every captured fd is valid in the child at this point.
    unsafe {
        command.pre_exec(move || {
            libc::umask(umask);
            if terminal_leader {
                if libc::setsid() == -1 {
                    return Err(std::io::Error::last_os_error());
                }
                let Some(fd) = terminal_fd else {
                    return Err(std::io::Error::from_raw_os_error(libc::EINVAL));
                };
                #[cfg(target_os = "macos")]
                let request = libc::TIOCSCTTY.into();
                #[cfg(not(target_os = "macos"))]
                let request = libc::TIOCSCTTY;
                if libc::ioctl(fd, request, 0) == -1 {
                    return Err(std::io::Error::last_os_error());
                }
            } else if libc::setpgid(0, 0) == -1 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    Ok(command)
}

fn spawn_readers(
    step: cue_core::StepId,
    readers: Vec<(OutputStream, Box<dyn AsyncRead + Unpin + Send>)>,
    output: Arc<dyn OutputStore>,
    failure: mpsc::Sender<RuntimeError>,
) -> Vec<tokio::task::JoinHandle<Result<(), RuntimeError>>> {
    readers
        .into_iter()
        .map(|(stream, reader)| {
            let output = output.clone();
            let failure = failure.clone();
            tokio::spawn(async move {
                let result = pump_reader(step, stream, reader, output).await;
                if let Err(error) = &result {
                    let _ = failure.send(error.clone()).await;
                }
                result
            })
        })
        .collect()
}

async fn pump_reader(
    step: cue_core::StepId,
    stream: OutputStream,
    mut reader: Box<dyn AsyncRead + Unpin + Send>,
    output: Arc<dyn OutputStore>,
) -> Result<(), RuntimeError> {
    let mut buffer = vec![0; IO_BUFFER_SIZE];
    loop {
        let read = reader
            .read(&mut buffer)
            .await
            .map_err(|error| io_error("read captured output", error))?;
        if read == 0 {
            return Ok(());
        }
        output.append(step, stream, &buffer[..read])?;
    }
}

async fn pump_pty(
    step: cue_core::StepId,
    reader: tokio::io::unix::AsyncFd<std::fs::File>,
    output: Arc<dyn OutputStore>,
) -> Result<(), RuntimeError> {
    let mut buffer = vec![0; IO_BUFFER_SIZE];
    loop {
        let mut ready = reader
            .readable()
            .await
            .map_err(|error| io_error("wait for PTY output", error))?;
        match ready.try_io(|inner| {
            let mut file = inner.get_ref();
            file.read(&mut buffer)
        }) {
            Ok(Ok(0)) => return Ok(()),
            Ok(Ok(read)) => {
                output.append(step, OutputStream::Terminal, &buffer[..read])?;
            }
            Ok(Err(error)) if error.raw_os_error() == Some(libc::EIO) => return Ok(()),
            Ok(Err(error)) => return Err(io_error("read PTY output", error)),
            Err(_would_block) => continue,
        }
    }
}

async fn combine_reader_results(
    exit: RunExit,
    readers: Vec<tokio::task::JoinHandle<Result<(), RuntimeError>>>,
) -> RunExit {
    for mut reader in readers {
        match tokio::time::timeout(OUTPUT_DRAIN_TIMEOUT, &mut reader).await {
            Ok(Ok(Ok(()))) => {}
            Ok(Ok(Err(error))) => {
                return RunExit::InfrastructureFailure(error.to_string());
            }
            Ok(Err(error)) => {
                return RunExit::InfrastructureFailure(error.to_string());
            }
            Err(_) => {
                reader.abort();
                return RunExit::InfrastructureFailure(
                    "captured output did not close after process exit".into(),
                );
            }
        }
    }
    exit
}

async fn supervise(
    mut children: Vec<Child>,
    mut control: mpsc::Receiver<RunControlCommand>,
    terminal: Option<Arc<tokio::io::unix::AsyncFd<std::fs::File>>>,
    mut reader_failures: mpsc::Receiver<RuntimeError>,
) -> RunExit {
    let mut statuses = vec![None; children.len()];
    let mut cancelled = false;
    let mut graceful_deadline = None;
    let mut reader_channel_open = true;
    loop {
        if let Err(error) = collect_statuses(&mut children, &mut statuses) {
            terminate_children(&mut children, CancelMode::Force);
            wait_children(&mut children).await;
            return RunExit::InfrastructureFailure(error.to_string());
        }
        if statuses.iter().all(Option::is_some) {
            return classify_exit(&statuses, cancelled);
        }
        if graceful_deadline.is_some_and(|deadline| tokio::time::Instant::now() >= deadline) {
            terminate_children(&mut children, CancelMode::Force);
            graceful_deadline = None;
        }

        tokio::select! {
            failure = reader_failures.recv(), if reader_channel_open => {
                match failure {
                    Some(error) => {
                        terminate_children(&mut children, CancelMode::Force);
                        wait_children(&mut children).await;
                        return RunExit::InfrastructureFailure(error.to_string());
                    }
                    None => reader_channel_open = false,
                }
            }
            command = control.recv() => {
                let Some(command) = command else {
                    tokio::time::sleep(CHILD_POLL_INTERVAL).await;
                    continue;
                };
                let result = match command.request {
                    RunControlRequest::Terminate(mode) => {
                        cancelled = true;
                        terminate_children(&mut children, mode);
                        if mode == CancelMode::Graceful {
                            graceful_deadline = Some(tokio::time::Instant::now() + GRACEFUL_TERMINATION_TIMEOUT);
                        }
                        Ok(())
                    }
                    RunControlRequest::Input(data) => match &terminal {
                        Some(terminal) => write_pty(terminal, &data).await,
                        None => Err(RuntimeError::new(RuntimeErrorKind::Unsupported, "captured run has no input")),
                    },
                    RunControlRequest::Resize(size) => match &terminal {
                        Some(terminal) => set_terminal_size(terminal.get_ref().as_raw_fd(), size),
                        None => Err(RuntimeError::new(RuntimeErrorKind::Unsupported, "captured run has no terminal")),
                    },
                };
                let _ = command.reply.send(result);
            }
            () = tokio::time::sleep(CHILD_POLL_INTERVAL) => {}
        }
    }
}

fn collect_statuses(
    children: &mut [Child],
    statuses: &mut [Option<ExitStatus>],
) -> Result<(), RuntimeError> {
    for (child, status) in children.iter_mut().zip(statuses) {
        if status.is_some() {
            continue;
        }
        if child_exit_pending_without_reaping(child)
            .map_err(|error| io_error("poll child status", error))?
        {
            let pid = child
                .id()
                .and_then(|pid| i32::try_from(pid).ok())
                .ok_or_else(|| RuntimeError::infrastructure("exited child lost its pid"))?;
            // SAFETY: waitid(WNOWAIT) proves the direct child still owns this
            // pid while remaining group members are terminated.
            unsafe { libc::kill(-pid, libc::SIGKILL) };
            *status = child
                .try_wait()
                .map_err(|error| io_error("reap child", error))?;
            if status.is_none() {
                return Err(RuntimeError::infrastructure(
                    "waitid reported exit but child could not be reaped",
                ));
            }
        }
    }
    Ok(())
}

fn child_exit_pending_without_reaping(child: &Child) -> std::io::Result<bool> {
    let pid = child
        .id()
        .and_then(|pid| libc::pid_t::try_from(pid).ok())
        .ok_or_else(|| std::io::Error::from_raw_os_error(libc::ECHILD))?;
    let mut info = std::mem::MaybeUninit::<libc::siginfo_t>::zeroed();
    loop {
        // SAFETY: info points to writable siginfo storage. WNOWAIT observes
        // the owned direct child without releasing its pid for reuse.
        let result = unsafe {
            libc::waitid(
                libc::P_PID,
                pid as libc::id_t,
                info.as_mut_ptr(),
                libc::WEXITED | libc::WNOHANG | libc::WNOWAIT,
            )
        };
        if result == 0 {
            // SAFETY: waitid initialized siginfo on success.
            return Ok(unsafe { info.assume_init().si_pid() } != 0);
        }
        let error = std::io::Error::last_os_error();
        if error.raw_os_error() != Some(libc::EINTR) {
            return Err(error);
        }
    }
}

fn classify_exit(statuses: &[Option<ExitStatus>], cancelled: bool) -> RunExit {
    let Some(status) = statuses.last().and_then(Option::as_ref) else {
        return RunExit::InfrastructureFailure("pipeline had no process status".into());
    };
    if status.success() {
        RunExit::Success
    } else if let Some(code) = status.code() {
        RunExit::ExitCode(code)
    } else {
        let signal = status.signal().unwrap_or(0);
        if cancelled && matches!(signal, libc::SIGTERM | libc::SIGKILL) {
            RunExit::Cancelled
        } else {
            RunExit::Signalled { signal }
        }
    }
}

fn terminate_children(children: &mut [Child], mode: CancelMode) {
    let signal = match mode {
        CancelMode::Graceful => libc::SIGTERM,
        CancelMode::Force => libc::SIGKILL,
    };
    for child in children {
        if let Some(pid) = child.id()
            && let Ok(pid) = i32::try_from(pid)
        {
            // SAFETY: a negative pid targets the process group created in
            // pre_exec. Failure falls back to the direct child below.
            if unsafe { libc::kill(-pid, signal) } == 0 {
                continue;
            }
            // SAFETY: pid identifies the direct child owned by this runner.
            unsafe { libc::kill(pid, signal) };
        }
        if mode == CancelMode::Force {
            let _ = child.start_kill();
        }
    }
}

async fn wait_children(children: &mut [Child]) {
    for child in children {
        let _ = child.wait().await;
    }
}

async fn write_pty(
    terminal: &tokio::io::unix::AsyncFd<std::fs::File>,
    mut data: &[u8],
) -> Result<(), RuntimeError> {
    while !data.is_empty() {
        let mut ready = terminal
            .writable()
            .await
            .map_err(|error| io_error("wait for PTY input", error))?;
        match ready.try_io(|inner| {
            let mut file = inner.get_ref();
            file.write(data)
        }) {
            Ok(Ok(0)) => {
                return Err(RuntimeError::infrastructure(
                    "PTY input made no write progress",
                ));
            }
            Ok(Ok(written)) => data = &data[written..],
            Ok(Err(error)) => return Err(io_error("write PTY input", error)),
            Err(_would_block) => continue,
        }
    }
    Ok(())
}

struct PtyPair {
    master: OwnedFd,
    slave: OwnedFd,
}

fn open_pty() -> Result<PtyPair, RuntimeError> {
    // SAFETY: posix_openpt returns a new owned descriptor on success.
    let master_fd = unsafe { libc::posix_openpt(libc::O_RDWR | libc::O_NOCTTY | libc::O_CLOEXEC) };
    if master_fd == -1 {
        return Err(io_error("posix_openpt", std::io::Error::last_os_error()));
    }
    // SAFETY: master_fd is newly allocated and uniquely owned.
    let master = unsafe { OwnedFd::from_raw_fd(master_fd) };
    // SAFETY: grantpt and unlockpt operate on the valid master descriptor.
    if unsafe { libc::grantpt(master_fd) } == -1 || unsafe { libc::unlockpt(master_fd) } == -1 {
        return Err(io_error("initialize PTY", std::io::Error::last_os_error()));
    }
    let slave_name = {
        static PTY_NAME_LOCK: Mutex<()> = Mutex::new(());
        let _guard = PTY_NAME_LOCK
            .lock()
            .map_err(|_| RuntimeError::infrastructure("PTY name lock poisoned"))?;
        // SAFETY: ptsname returns a process-global buffer guarded until copied.
        let name = unsafe { libc::ptsname(master_fd) };
        if name.is_null() {
            return Err(io_error("ptsname", std::io::Error::last_os_error()));
        }
        // SAFETY: ptsname returned a valid NUL-terminated path.
        unsafe { std::ffi::CStr::from_ptr(name) }.to_owned()
    };
    // SAFETY: slave_name is NUL-terminated and open returns a new descriptor.
    let slave_fd = unsafe {
        libc::open(
            slave_name.as_ptr(),
            libc::O_RDWR | libc::O_NOCTTY | libc::O_CLOEXEC,
        )
    };
    if slave_fd == -1 {
        return Err(io_error("open PTY slave", std::io::Error::last_os_error()));
    }
    // SAFETY: slave_fd is newly allocated and uniquely owned.
    let slave = unsafe { OwnedFd::from_raw_fd(slave_fd) };
    Ok(PtyPair { master, slave })
}

fn set_terminal_size(fd: RawFd, size: TerminalSize) -> Result<(), RuntimeError> {
    let size = libc::winsize {
        ws_row: size.rows,
        ws_col: size.columns,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    // SAFETY: fd is an open PTY descriptor and points to a winsize value.
    if unsafe { libc::ioctl(fd, libc::TIOCSWINSZ, &size) } == -1 {
        return Err(io_error("resize PTY", std::io::Error::last_os_error()));
    }
    Ok(())
}

fn set_nonblocking(fd: RawFd) -> Result<(), RuntimeError> {
    // SAFETY: fcntl reads flags for an open descriptor.
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    if flags == -1 {
        return Err(io_error(
            "read descriptor flags",
            std::io::Error::last_os_error(),
        ));
    }
    // SAFETY: fcntl updates flags for the same open descriptor.
    if unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) } == -1 {
        return Err(io_error(
            "set descriptor nonblocking",
            std::io::Error::last_os_error(),
        ));
    }
    Ok(())
}

fn create_pipe() -> Result<(std::fs::File, std::fs::File), RuntimeError> {
    let mut fds = [0; 2];
    // SAFETY: pipe initializes two fresh descriptors on success.
    if unsafe { libc::pipe(fds.as_mut_ptr()) } == -1 {
        return Err(io_error(
            "create pipeline pipe",
            std::io::Error::last_os_error(),
        ));
    }
    // SAFETY: both descriptors are newly allocated and uniquely owned.
    let read = unsafe { OwnedFd::from_raw_fd(fds[0]) };
    // SAFETY: both descriptors are newly allocated and uniquely owned.
    let write = unsafe { OwnedFd::from_raw_fd(fds[1]) };
    for fd in [read.as_raw_fd(), write.as_raw_fd()] {
        // SAFETY: fcntl operates on an open descriptor.
        if unsafe { libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC) } == -1 {
            return Err(io_error(
                "mark pipeline pipe close-on-exec",
                std::io::Error::last_os_error(),
            ));
        }
    }
    Ok((std::fs::File::from(read), std::fs::File::from(write)))
}

fn clone_file(file: &std::fs::File, action: &'static str) -> Result<std::fs::File, RuntimeError> {
    file.try_clone().map_err(|error| io_error(action, error))
}

fn io_error(action: &'static str, error: impl std::fmt::Display) -> RuntimeError {
    RuntimeError::new(
        RuntimeErrorKind::Infrastructure,
        format!("{action}: {error}"),
    )
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use cue_core::vnext::{AbsolutePath, Argv, FileModeMask, PipeContinuation, Pipeline, Process};
    use cue_core::{ExecutionId, StepId};

    use super::*;
    use crate::{MemoryOutputStore, OutputAppend, OutputSlice};

    struct FailingOutputStore;

    impl OutputStore for FailingOutputStore {
        fn append(
            &self,
            _step: StepId,
            _stream: OutputStream,
            _data: &[u8],
        ) -> Result<OutputAppend, RuntimeError> {
            Err(RuntimeError::infrastructure("output unavailable"))
        }

        fn read(
            &self,
            _step: StepId,
            _stream: OutputStream,
            _offset: u64,
            _maximum: usize,
        ) -> Result<OutputSlice, RuntimeError> {
            unreachable!()
        }

        fn tail(
            &self,
            _step: StepId,
            _stream: OutputStream,
            _maximum: usize,
        ) -> Result<OutputSlice, RuntimeError> {
            unreachable!()
        }
    }

    fn scope() -> Scope {
        Scope::new(
            AbsolutePath::new(std::env::current_dir().unwrap()).unwrap(),
            BTreeMap::new(),
            FileModeMask::new(0o022).unwrap(),
        )
    }

    fn step() -> StepId {
        StepId {
            execution: ExecutionId(1),
            index: 1,
        }
    }

    fn process(program: &str, arguments: &[&str]) -> Process {
        Process::new(
            Argv::new(
                program,
                arguments.iter().map(|argument| (*argument).to_owned()),
            )
            .unwrap(),
        )
    }

    #[tokio::test]
    async fn captured_pipeline_preserves_links_and_captures_final_output() {
        let output = Arc::new(MemoryOutputStore::new(1024));
        let spawner = LocalProcessSpawner::new(output.clone());
        let pipeline = Pipeline::new(
            process("/usr/bin/printf", &["hello"]),
            vec![PipeContinuation::new(
                PipeLink::StdoutToStdin,
                process("/usr/bin/wc", &["-c"]),
            )],
        );
        let run = spawner
            .spawn(
                SpawnRequest {
                    step: step(),
                    pipeline,
                    io: IoMode::Captured,
                    scope: scope(),
                },
                SpawnContext::local(&scope()),
            )
            .await
            .unwrap();
        assert_eq!(run.wait().await, RunExit::Success);
        let stdout = output
            .tail(step(), OutputStream::Stdout, 1024)
            .unwrap()
            .data;
        assert_eq!(String::from_utf8(stdout).unwrap().trim(), "5");
    }

    #[tokio::test]
    async fn pty_run_has_one_terminal_stream_and_explicit_control() {
        let output = Arc::new(MemoryOutputStore::new(1024));
        let spawner = LocalProcessSpawner::new(output.clone());
        let run = spawner
            .spawn(
                SpawnRequest {
                    step: step(),
                    pipeline: Pipeline::simple(process(
                        "/bin/sh",
                        &["-c", "read line; /bin/stty size; printf '%s\\n' \"$line\""],
                    )),
                    io: IoMode::Pty,
                    scope: scope(),
                },
                SpawnContext::local(&scope()),
            )
            .await
            .unwrap();
        run.control
            .resize(TerminalSize::new(100, 30).unwrap())
            .await
            .unwrap();
        run.control.input(b"pty-ok\n".to_vec()).await.unwrap();
        assert_eq!(run.wait().await, RunExit::Success);
        let terminal = output
            .tail(step(), OutputStream::Terminal, 1024)
            .unwrap()
            .data;
        assert!(String::from_utf8_lossy(&terminal).contains("pty-ok"));
        assert!(String::from_utf8_lossy(&terminal).contains("30 100"));
        assert!(
            output
                .tail(step(), OutputStream::Stdout, 1024)
                .unwrap()
                .data
                .is_empty()
        );
    }

    #[tokio::test]
    async fn force_termination_is_reported_as_cancelled() {
        let output = Arc::new(MemoryOutputStore::new(1024));
        let spawner = LocalProcessSpawner::new(output);
        let run = spawner
            .spawn(
                SpawnRequest {
                    step: step(),
                    pipeline: Pipeline::simple(process("/bin/sleep", &["10"])),
                    io: IoMode::Captured,
                    scope: scope(),
                },
                SpawnContext::local(&scope()),
            )
            .await
            .unwrap();
        run.control.terminate(CancelMode::Force).await.unwrap();
        assert_eq!(run.wait().await, RunExit::Cancelled);
    }

    #[tokio::test]
    async fn output_failure_terminates_a_pty_writer_instead_of_deadlocking() {
        let spawner = LocalProcessSpawner::new(Arc::new(FailingOutputStore));
        let run = spawner
            .spawn(
                SpawnRequest {
                    step: step(),
                    pipeline: Pipeline::simple(process("/usr/bin/yes", &[])),
                    io: IoMode::Pty,
                    scope: scope(),
                },
                SpawnContext::local(&scope()),
            )
            .await
            .unwrap();
        let exit = tokio::time::timeout(Duration::from_secs(2), run.wait())
            .await
            .expect("output failure must terminate the writer");
        assert!(
            matches!(exit, RunExit::InfrastructureFailure(message) if message.contains("output unavailable"))
        );
    }
    #[test]
    fn cancellation_does_not_overwrite_a_natural_exit() {
        assert_eq!(
            classify_exit(&[Some(ExitStatus::from_raw(0))], true),
            RunExit::Success
        );
        assert_eq!(
            classify_exit(&[Some(ExitStatus::from_raw(7 << 8))], true),
            RunExit::ExitCode(7)
        );
        assert_eq!(
            classify_exit(&[Some(ExitStatus::from_raw(libc::SIGKILL))], true),
            RunExit::Cancelled
        );
        assert_eq!(
            classify_exit(&[Some(ExitStatus::from_raw(libc::SIGSEGV))], true),
            RunExit::Signalled {
                signal: libc::SIGSEGV
            }
        );
    }
}
