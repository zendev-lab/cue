//! ProcessManager actor — OS child process lifecycle.
//!
//! Spawns real child processes via `tokio::process::Command`, reads their
//! stdout/stderr into a [`RingBuffer`], writes a persistent log file, and
//! publishes output chunks + state-change events.

use std::collections::{BTreeMap, HashMap};
use std::io::{Read, Write};
use std::os::fd::{AsRawFd, FromRawFd};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use tokio::io::unix::AsyncFd;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot, watch};
use tracing::{debug, error, info, warn};

use cue_core::ipc::{
    EventPayload, ForegroundAttachmentInfo, ForegroundRole, MAX_FOREGROUND_INPUT_BYTES,
    Stream as OutputStream,
};
use cue_core::job::{EXIT_CODE_UNAVAILABLE, JobStatus};
use cue_core::pipeline::{JobPlan, command_prefers_foreground};
use cue_core::process_status::exit_code_from_status;
use cue_core::scope::EnvSnapshot;
use cue_core::{EventChannel, JobId};

use super::{
    ActorSystem, ForegroundRoleUpdate, OutputSnapshot, ProcessJobOptions, ProcessMgrMsg,
    SchedulerMsg, ScopeStoreMsg, StderrSnapshot,
    publish_session_event as publish_actor_session_event,
    publish_session_event_except as publish_actor_session_event_except,
    send_gateway_event as send_actor_gateway_event,
};
use crate::ring_buffer::RingBuffer;
use crate::runtime_env::effective_snapshot;
use crate::word_expansion::expand_command_line;

// ── Per-child bookkeeping ──

struct ProcessEntry {
    job_id: JobId,
    /// Named session that owns this process, or `None` for legacy anonymous jobs.
    session_id: Option<String>,
    status: JobStatus,
    /// Handle for the background reader/waiter task.
    reader_handle: tokio::task::JoinHandle<()>,
    /// Send on this channel to request a kill.
    kill_tx: mpsc::Sender<()>,
    /// Shared ring buffer holding the latest output bytes for live-tail queries.
    ring_buffer: Arc<Mutex<RingBuffer>>,
    /// Separate stderr ring buffer.  `None` in PTY mode (streams are merged).
    stderr_ring: Option<Arc<Mutex<RingBuffer>>>,
    /// Bounded per-job stdin writer. The process manager retains lifecycle and
    /// controller authority; the task owns only the ordered write mechanism.
    input: Option<JobInputWriter>,
    /// PTY master fd used for resize ioctls.
    resize: Option<Arc<std::fs::File>>,
    /// Shared foreground observer set and exclusive controller lease.
    foreground: Arc<Mutex<ForegroundState>>,
}

/// Runtime-only attachment state for one job's foreground stream.
///
/// The controller is always also present in `observers`. `closed` fences late
/// attach attempts while the reader task is publishing terminal events.
#[derive(Debug, Default)]
struct ForegroundState {
    /// Client id to the epoch of its current attachment.
    observers: BTreeMap<u64, u64>,
    controller: Option<u64>,
    /// Independent PTY input generation for the current controller. Attachment
    /// epochs identify event streams; this generation fences queued input and
    /// changes on every release/reclaim, even within one attachment.
    controller_generation: Option<u64>,
    /// Last epoch allocated for this job. Zero is reserved for legacy IPC.
    last_attachment_id: u64,
    closed: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ForegroundRecipient {
    client_id: u64,
    attachment_id: u64,
}

#[derive(Debug, PartialEq, Eq)]
struct ForegroundAttachOutcome {
    role: ForegroundRole,
    attachment_id: u64,
    /// Present only when this attach acquired the free controller lease. The
    /// new caller is deliberately absent; legacy `FgAttach` clients do not know
    /// the `FgControlChanged` variant.
    control_recipients: Option<Vec<ForegroundRecipient>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ForegroundAttachError {
    Closed,
    AlreadyAttached,
    ControlHeld,
    AttachmentIdExhausted,
}

impl ForegroundState {
    fn role(&self, client_id: u64) -> Option<ForegroundRole> {
        self.observers
            .contains_key(&client_id)
            .then_some(if self.controller == Some(client_id) {
                ForegroundRole::Controller
            } else {
                ForegroundRole::Observer
            })
    }

    fn control_available(&self) -> bool {
        !self.closed && self.controller.is_none()
    }

    fn recipients(&self) -> Vec<ForegroundRecipient> {
        self.observers
            .iter()
            .map(|(&client_id, &attachment_id)| ForegroundRecipient {
                client_id,
                attachment_id,
            })
            .collect()
    }

    fn attach(
        &mut self,
        client_id: u64,
        requested_role: ForegroundRole,
    ) -> Result<ForegroundAttachOutcome, ForegroundAttachError> {
        if self.closed {
            return Err(ForegroundAttachError::Closed);
        }
        if self.observers.contains_key(&client_id) {
            return Err(ForegroundAttachError::AlreadyAttached);
        }
        if requested_role == ForegroundRole::Controller && self.controller.is_some() {
            return Err(ForegroundAttachError::ControlHeld);
        }

        let attachment_id = self
            .last_attachment_id
            .checked_add(1)
            .ok_or(ForegroundAttachError::AttachmentIdExhausted)?;
        let control_recipients =
            (requested_role == ForegroundRole::Controller).then(|| self.recipients());

        self.last_attachment_id = attachment_id;
        self.observers.insert(client_id, attachment_id);
        if requested_role == ForegroundRole::Controller {
            self.controller = Some(client_id);
        }

        Ok(ForegroundAttachOutcome {
            role: self
                .role(client_id)
                .expect("new foreground attachment must be observable"),
            attachment_id,
            control_recipients,
        })
    }

    fn detach(&mut self, client_id: u64) -> Option<(u64, Option<Vec<ForegroundRecipient>>)> {
        let attachment_id = self.observers.remove(&client_id)?;
        let released_control = self.controller == Some(client_id);
        if released_control {
            self.controller = None;
            self.controller_generation = None;
        }
        Some((attachment_id, released_control.then(|| self.recipients())))
    }
}

enum JobInputSink {
    Pty(AsyncFd<std::fs::File>),
    Pipe(tokio::process::ChildStdin),
}

const DEFAULT_PTY_COLS: u16 = 80;
const DEFAULT_PTY_ROWS: u16 = 24;
const JOB_INPUT_QUEUE_CAP: usize = 16;
/// At most 512 KiB of 8 KiB chunks may wait per pipeline before readers apply
/// backpressure to the child process pipes.
const PIPELINE_CHUNK_CAP: usize = 64;

static NEXT_INPUT_WRITER_INCARNATION: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum JobInputKind {
    Pty,
    Pipe,
}

struct JobInputItem {
    data: Vec<u8>,
    generation: Option<u64>,
}

#[derive(Clone, Copy, Debug)]
struct ActiveInput {
    generation: u64,
    written: usize,
    total: usize,
}

#[derive(Debug)]
struct InputFenceInner {
    generation: u64,
    settled_generation: u64,
    active: Option<ActiveInput>,
}

struct InputFence {
    inner: Mutex<InputFenceInner>,
    poisoned: AtomicBool,
    changed: watch::Sender<u64>,
}

impl InputFence {
    fn new() -> Self {
        let generation = 1;
        let (changed, _) = watch::channel(generation);
        Self {
            inner: Mutex::new(InputFenceInner {
                generation,
                settled_generation: generation,
                active: None,
            }),
            poisoned: AtomicBool::new(false),
            changed,
        }
    }

    fn subscribe(&self) -> watch::Receiver<u64> {
        self.changed.subscribe()
    }

    fn is_poisoned(&self) -> bool {
        self.poisoned.load(Ordering::Acquire)
    }

    fn lock_inner(&self) -> std::sync::MutexGuard<'_, InputFenceInner> {
        match self.inner.lock() {
            Ok(inner) => inner,
            Err(error) => {
                let mut inner = error.into_inner();
                self.poison_locked(&mut inner);
                inner
            }
        }
    }

    fn controller_available(&self) -> bool {
        if self.is_poisoned() {
            return false;
        }
        let inner = self.lock_inner();
        !self.is_poisoned() && inner.settled_generation == inner.generation
    }

    fn is_settled_generation(&self, generation: u64) -> bool {
        if self.is_poisoned() {
            return false;
        }
        let inner = self.lock_inner();
        !self.is_poisoned()
            && inner.generation == generation
            && inner.settled_generation == generation
    }

    fn start_controller_generation(&self) -> Result<u64, InputEnqueueError> {
        if self.is_poisoned() {
            return Err(InputEnqueueError::Poisoned);
        }
        let mut inner = self.lock_inner();
        if self.is_poisoned() {
            return Err(InputEnqueueError::Poisoned);
        }
        if inner.settled_generation != inner.generation {
            return Err(InputEnqueueError::FencePending);
        }
        let generation = inner
            .generation
            .checked_add(1)
            .ok_or(InputEnqueueError::GenerationExhausted)?;
        inner.generation = generation;
        // There is no prior controller while this method is called and the
        // preceding revoke is settled, so this generation is immediately safe.
        inner.settled_generation = generation;
        self.changed.send_replace(generation);
        Ok(generation)
    }

    fn revoke_controller_generation(
        &self,
        expected_generation: u64,
    ) -> Result<InputFenceAdvance, InputEnqueueError> {
        if self.is_poisoned() {
            return Err(InputEnqueueError::Poisoned);
        }
        let mut inner = self.lock_inner();
        if self.is_poisoned() {
            return Err(InputEnqueueError::Poisoned);
        }
        if inner.generation != expected_generation {
            return Err(InputEnqueueError::StaleGeneration);
        }
        let generation = inner
            .generation
            .checked_add(1)
            .ok_or(InputEnqueueError::GenerationExhausted)?;
        inner.generation = generation;
        let pending = inner.active.is_some_and(|active| {
            active.generation == expected_generation && active.written < active.total
        });
        if !pending {
            inner.settled_generation = generation;
        }
        self.changed.send_replace(generation);
        Ok(InputFenceAdvance { settled: !pending })
    }

    fn poison_locked(&self, inner: &mut InputFenceInner) {
        let was_poisoned = self.poisoned.swap(true, Ordering::AcqRel);
        if !was_poisoned {
            if let Some(generation) = inner.generation.checked_add(1) {
                inner.generation = generation;
            }
            self.changed.send_replace(inner.generation);
        }
        inner.active = None;
    }

    fn poison(&self) {
        let mut inner = self.lock_inner();
        if !self.is_poisoned() {
            self.poison_locked(&mut inner);
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct InputFenceAdvance {
    settled: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum InputEnqueueError {
    TooLarge { actual: usize },
    Full,
    Closed,
    Poisoned,
    FencePending,
    StaleGeneration,
    GenerationExhausted,
}

impl std::fmt::Display for InputEnqueueError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TooLarge { actual } => write!(
                formatter,
                "input is {actual} bytes, exceeding the {MAX_FOREGROUND_INPUT_BYTES}-byte limit"
            ),
            Self::Full => write!(
                formatter,
                "job input queue is full ({JOB_INPUT_QUEUE_CAP} items)"
            ),
            Self::Closed => formatter.write_str("job input writer is closed"),
            Self::Poisoned => formatter.write_str("job input writer failed"),
            Self::FencePending => {
                formatter.write_str("foreground input lease transition is still settling")
            }
            Self::StaleGeneration => {
                formatter.write_str("foreground input belongs to a stale controller generation")
            }
            Self::GenerationExhausted => {
                formatter.write_str("foreground input generation space is exhausted")
            }
        }
    }
}

struct AbortTaskOnDrop(tokio::task::JoinHandle<()>);

impl Drop for AbortTaskOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

struct InputWriterTaskExitGuard {
    job_id: JobId,
    writer_incarnation: u64,
    fence: Arc<InputFence>,
    failures: mpsc::UnboundedSender<InputWriterFailure>,
    armed: bool,
}

impl InputWriterTaskExitGuard {
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for InputWriterTaskExitGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        self.fence.poison();
        let _ = self.failures.send(InputWriterFailure {
            job_id: self.job_id,
            writer_incarnation: self.writer_incarnation,
            reason: "input writer task terminated unexpectedly".into(),
        });
    }
}

struct InputWriterFailure {
    job_id: JobId,
    writer_incarnation: u64,
    reason: String,
}

struct JobInputWriter {
    kind: JobInputKind,
    incarnation: u64,
    sender: mpsc::Sender<JobInputItem>,
    fence: Arc<InputFence>,
    _task: AbortTaskOnDrop,
}

impl JobInputWriter {
    fn spawn(
        job_id: JobId,
        sink: JobInputSink,
        process_mgr: mpsc::Sender<ProcessMgrMsg>,
        failures: mpsc::UnboundedSender<InputWriterFailure>,
    ) -> Self {
        let kind = match &sink {
            JobInputSink::Pty(_) => JobInputKind::Pty,
            JobInputSink::Pipe(_) => JobInputKind::Pipe,
        };
        let incarnation = NEXT_INPUT_WRITER_INCARNATION.fetch_add(1, Ordering::Relaxed);
        let fence = Arc::new(InputFence::new());
        let (sender, receiver) = mpsc::channel(JOB_INPUT_QUEUE_CAP);
        let task_fence = fence.clone();
        let task = tokio::spawn(async move {
            let mut exit_guard = InputWriterTaskExitGuard {
                job_id,
                writer_incarnation: incarnation,
                fence: task_fence.clone(),
                failures: failures.clone(),
                armed: true,
            };
            job_input_writer_task(
                job_id,
                incarnation,
                sink,
                receiver,
                task_fence,
                process_mgr,
                failures,
            )
            .await;
            exit_guard.disarm();
        });
        Self {
            kind,
            incarnation,
            sender,
            fence,
            _task: AbortTaskOnDrop(task),
        }
    }

    fn is_pty(&self) -> bool {
        self.kind == JobInputKind::Pty
    }

    fn is_poisoned(&self) -> bool {
        self.fence.is_poisoned()
    }

    fn controller_available(&self) -> bool {
        if self.sender.is_closed() {
            self.fence.poison();
            return false;
        }
        self.is_pty() && self.fence.controller_available()
    }

    fn start_controller_generation(&self) -> Result<u64, InputEnqueueError> {
        if !self.is_pty() {
            return Err(InputEnqueueError::Closed);
        }
        if self.sender.is_closed() {
            self.fence.poison();
            return Err(InputEnqueueError::Closed);
        }
        self.fence.start_controller_generation()
    }

    fn revoke_controller_generation(
        &self,
        generation: u64,
    ) -> Result<InputFenceAdvance, InputEnqueueError> {
        self.fence.revoke_controller_generation(generation)
    }

    fn try_enqueue(&self, data: Vec<u8>, generation: Option<u64>) -> Result<(), InputEnqueueError> {
        if data.len() > MAX_FOREGROUND_INPUT_BYTES {
            return Err(InputEnqueueError::TooLarge { actual: data.len() });
        }
        if self.is_poisoned() {
            return Err(InputEnqueueError::Poisoned);
        }

        let fence_guard = self.fence.lock_inner();
        if self.is_poisoned() {
            return Err(InputEnqueueError::Poisoned);
        }
        if self.is_pty() {
            let expected_generation = generation.ok_or(InputEnqueueError::StaleGeneration)?;
            if fence_guard.generation != expected_generation {
                return Err(InputEnqueueError::StaleGeneration);
            }
        }

        let item = JobInputItem { data, generation };
        match self.sender.try_send(item) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(_)) => Err(InputEnqueueError::Full),
            Err(mpsc::error::TrySendError::Closed(_)) => {
                drop(fence_guard);
                self.fence.poison();
                Err(InputEnqueueError::Closed)
            }
        }
    }
}

// ── Actor entry point ──

struct NativePipelineSpawn {
    children: Vec<tokio::process::Child>,
    input: Option<JobInputSink>,
    stdout_sources: Vec<tokio::process::ChildStdout>,
    stderr_sources: Vec<tokio::process::ChildStderr>,
}

struct NativePipelineOptions<'a> {
    cwd_override: Option<&'a Path>,
    sandbox: Option<&'a crate::sandbox::PreparedSandbox>,
    wrapper_enabled: bool,
    capture_stdin: bool,
    sys: &'a ActorSystem,
}

#[derive(Clone, Copy, Debug)]
enum PipelineStreamKind {
    Stdout,
    Stderr,
}

enum PipelineReaderMsg {
    Chunk {
        kind: PipelineStreamKind,
        data: Vec<u8>,
    },
    Closed,
}

#[derive(Clone, Copy)]
enum LogStream {
    Stdout,
    Stderr,
}

impl LogStream {
    fn label(self) -> &'static str {
        match self {
            Self::Stdout => "stdout",
            Self::Stderr => "stderr",
        }
    }
}

enum JobLocalBuiltin {
    Cd { path: String },
    EnvSet { assignments: Vec<String> },
}

#[derive(Clone)]
struct ProcessTaskRuntime {
    sys: ActorSystem,
    foreground: Arc<Mutex<ForegroundState>>,
    direct_output_client: Option<u64>,
    session_id: Option<String>,
    cleanup_tx: mpsc::Sender<JobId>,
}

struct PtyReaderTask {
    job_id: JobId,
    child: tokio::process::Child,
    sandbox: Option<crate::sandbox::PreparedSandbox>,
    reader: AsyncFd<std::fs::File>,
    log_file: Option<std::fs::File>,
    kill_rx: mpsc::Receiver<()>,
    ring: Arc<Mutex<RingBuffer>>,
    runtime: ProcessTaskRuntime,
}

struct PipelineReaderTask {
    job_id: JobId,
    children: Vec<tokio::process::Child>,
    sandbox: Option<crate::sandbox::PreparedSandbox>,
    stdout_sources: Vec<tokio::process::ChildStdout>,
    stderr_sources: Vec<tokio::process::ChildStderr>,
    log_file: Option<std::fs::File>,
    stderr_log: Option<std::fs::File>,
    kill_rx: mpsc::Receiver<()>,
    ring: Arc<Mutex<RingBuffer>>,
    stderr_ring: Arc<Mutex<RingBuffer>>,
    runtime: ProcessTaskRuntime,
}

struct LogicalJobTask {
    job_id: JobId,
    plan: JobPlan,
    snapshot: EnvSnapshot,
    cwd_override: Option<std::path::PathBuf>,
    sandbox: Option<crate::sandbox::PreparedSandbox>,
    log_file: Option<std::fs::File>,
    stderr_log: Option<std::fs::File>,
    kill_rx: mpsc::Receiver<()>,
    wrapper_enabled: bool,
    capture_stdin: bool,
    ring: Arc<Mutex<RingBuffer>>,
    stderr_ring: Arc<Mutex<RingBuffer>>,
    runtime: ProcessTaskRuntime,
}

#[derive(Clone, Copy)]
struct StreamingOptions {
    wrapper_enabled: bool,
    capture_stdin: bool,
}

struct StreamingContext<'a> {
    job_id: JobId,
    snapshot: &'a mut EnvSnapshot,
    sandbox: Option<&'a crate::sandbox::PreparedSandbox>,
    kill_rx: &'a mut mpsc::Receiver<()>,
    was_killed: &'a mut bool,
    options: StreamingOptions,
    sys: &'a ActorSystem,
    ring: &'a Arc<Mutex<RingBuffer>>,
    stderr_ring: &'a Arc<Mutex<RingBuffer>>,
    log_file: &'a Arc<Mutex<Option<std::fs::File>>>,
    stderr_log: &'a Arc<Mutex<Option<std::fs::File>>>,
    direct_output_client: Option<u64>,
    session_id: Option<&'a str>,
}

fn foreground_job_for_client(
    children: &HashMap<u32, ProcessEntry>,
    client_id: u64,
) -> Option<JobId> {
    children.values().find_map(|entry| {
        entry
            .foreground
            .lock()
            .unwrap()
            .observers
            .contains_key(&client_id)
            .then_some(entry.job_id)
    })
}

/// Register an observer and capture the retained PTY output under the same
/// foreground→ring lock order used by the PTY reader. That lock order is the
/// cut between snapshot bytes and later live `FgOutput` events.
fn attach_foreground(
    entry: &ProcessEntry,
    client_id: u64,
    requested_role: ForegroundRole,
) -> Result<(ForegroundAttachmentInfo, Option<Vec<ForegroundRecipient>>), String> {
    if entry.status != JobStatus::Running {
        return Err(format!("job {} is not running", entry.job_id));
    }
    if entry.resize.is_none() {
        return Err(format!(
            "job {} does not support foreground attach",
            entry.job_id
        ));
    }
    let input = entry
        .input
        .as_ref()
        .filter(|input| input.is_pty())
        .ok_or_else(|| format!("job {} foreground input is unavailable", entry.job_id))?;
    if input.is_poisoned() {
        return Err(format!("job {} foreground input failed", entry.job_id));
    }

    let mut foreground = entry.foreground.lock().unwrap();
    let outcome = foreground
        .attach(client_id, requested_role)
        .map_err(|error| match error {
            ForegroundAttachError::Closed => {
                format!("job {} foreground is closed", entry.job_id)
            }
            ForegroundAttachError::AlreadyAttached => {
                format!("client is already foreground-attached to {}", entry.job_id)
            }
            ForegroundAttachError::ControlHeld => {
                format!("job {} foreground control is already held", entry.job_id)
            }
            ForegroundAttachError::AttachmentIdExhausted => format!(
                "job {} foreground attachment id space is exhausted",
                entry.job_id
            ),
        })?;
    if requested_role == ForegroundRole::Controller {
        match input.start_controller_generation() {
            Ok(generation) => foreground.controller_generation = Some(generation),
            Err(error) => {
                foreground.detach(client_id);
                return Err(format!(
                    "job {} foreground control is unavailable: {error}",
                    entry.job_id
                ));
            }
        }
    }
    let control_available = foreground.control_available() && input.controller_available();
    let (snapshot, snapshot_truncated) = entry
        .ring_buffer
        .lock()
        .unwrap()
        .tail_with_truncation(crate::ring_buffer::DEFAULT_CAPACITY);

    Ok((
        ForegroundAttachmentInfo {
            id: entry.job_id.to_string(),
            attachment_id: outcome.attachment_id,
            role: outcome.role,
            control_available,
            snapshot,
            snapshot_truncated,
        },
        outcome.control_recipients,
    ))
}

fn claim_foreground_control(
    entry: &ProcessEntry,
    client_id: u64,
) -> (
    Result<ForegroundRoleUpdate, String>,
    Option<Vec<ForegroundRecipient>>,
) {
    let Some(input) = entry.input.as_ref().filter(|input| input.is_pty()) else {
        return (
            Err(format!(
                "job {} foreground input is unavailable",
                entry.job_id
            )),
            None,
        );
    };
    if input.is_poisoned() {
        return (
            Err(format!("job {} foreground input failed", entry.job_id)),
            None,
        );
    }
    let mut foreground = entry.foreground.lock().unwrap();
    let Some(&attachment_id) = foreground.observers.get(&client_id) else {
        return (Err("no foreground job observed".to_string()), None);
    };
    if foreground.closed {
        return (Err("no foreground job observed".to_string()), None);
    }
    if foreground
        .controller
        .is_some_and(|controller| controller != client_id)
    {
        return (
            Err(format!(
                "job {} foreground control is already held",
                entry.job_id
            )),
            None,
        );
    }
    if foreground.controller == Some(client_id) {
        return (
            Ok(ForegroundRoleUpdate {
                id: entry.job_id.to_string(),
                attachment_id,
                role: ForegroundRole::Controller,
                control_available: false,
            }),
            None,
        );
    }
    let generation = match input.start_controller_generation() {
        Ok(generation) => generation,
        Err(error) => {
            return (
                Err(format!(
                    "job {} foreground control is unavailable: {error}",
                    entry.job_id
                )),
                None,
            );
        }
    };
    foreground.controller = Some(client_id);
    foreground.controller_generation = Some(generation);
    let recipients = foreground.recipients();
    (
        Ok(ForegroundRoleUpdate {
            id: entry.job_id.to_string(),
            attachment_id,
            role: ForegroundRole::Controller,
            control_available: false,
        }),
        Some(recipients),
    )
}

fn release_foreground_control(
    entry: &ProcessEntry,
    client_id: u64,
) -> (
    Result<ForegroundRoleUpdate, String>,
    Option<Vec<ForegroundRecipient>>,
) {
    let Some(input) = entry.input.as_ref().filter(|input| input.is_pty()) else {
        return (
            Err(format!(
                "job {} foreground input is unavailable",
                entry.job_id
            )),
            None,
        );
    };
    let mut foreground = entry.foreground.lock().unwrap();
    let Some(&attachment_id) = foreground.observers.get(&client_id) else {
        return (Err("no foreground job observed".to_string()), None);
    };
    if foreground.closed {
        return (Err("no foreground job observed".to_string()), None);
    }
    let released = foreground.controller == Some(client_id);
    let fence = if released {
        let Some(generation) = foreground.controller_generation else {
            return (
                Err(format!(
                    "job {} foreground controller generation is missing",
                    entry.job_id
                )),
                None,
            );
        };
        match input.revoke_controller_generation(generation) {
            Ok(fence) => Some(fence),
            Err(error) => {
                return (
                    Err(format!(
                        "job {} foreground control release failed: {error}",
                        entry.job_id
                    )),
                    None,
                );
            }
        }
    } else {
        None
    };
    if released {
        foreground.controller = None;
        foreground.controller_generation = None;
    }
    let control_available = foreground.control_available()
        && input.controller_available()
        && fence.is_none_or(|fence| fence.settled);
    let recipients = (released && control_available).then(|| foreground.recipients());
    (
        Ok(ForegroundRoleUpdate {
            id: entry.job_id.to_string(),
            attachment_id,
            role: ForegroundRole::Observer,
            control_available,
        }),
        recipients,
    )
}

fn record_pty_output(
    ring: &Arc<Mutex<RingBuffer>>,
    foreground: &Arc<Mutex<ForegroundState>>,
    data: &[u8],
) -> Vec<ForegroundRecipient> {
    let foreground = foreground.lock().unwrap();
    ring.lock().unwrap().push(data);
    if foreground.closed {
        Vec::new()
    } else {
        foreground.recipients()
    }
}

#[cfg(test)]
fn job_input_kind_allows_client(
    requires_controller: bool,
    controller: Option<u64>,
    client_id: u64,
) -> bool {
    !requires_controller || controller == Some(client_id)
}

struct DetachedForeground {
    job_id: JobId,
    attachment_id: u64,
    session_id: Option<String>,
    control_recipients: Option<Vec<ForegroundRecipient>>,
}

struct FailedForeground {
    recipients: Vec<ForegroundRecipient>,
    job_id: JobId,
    session_id: Option<String>,
    reason: String,
}

enum InputRejection {
    None,
    Detached(DetachedForeground),
    Failed(FailedForeground),
}

fn detach_foreground_entry(
    entry: &ProcessEntry,
    client_id: u64,
) -> Result<Option<DetachedForeground>, String> {
    let mut foreground = entry.foreground.lock().unwrap();
    let Some(&attachment_id) = foreground.observers.get(&client_id) else {
        return Ok(None);
    };
    let released_control = foreground.controller == Some(client_id);
    let fence = if released_control {
        let generation = foreground.controller_generation.ok_or_else(|| {
            format!(
                "job {} foreground controller generation is missing",
                entry.job_id
            )
        })?;
        let input = entry
            .input
            .as_ref()
            .filter(|input| input.is_pty())
            .ok_or_else(|| format!("job {} foreground input is unavailable", entry.job_id))?;
        Some(
            input
                .revoke_controller_generation(generation)
                .map_err(|error| {
                    format!(
                        "job {} foreground input fence failed: {error}",
                        entry.job_id
                    )
                })?,
        )
    } else {
        None
    };

    foreground.observers.remove(&client_id);
    if released_control {
        foreground.controller = None;
        foreground.controller_generation = None;
    }
    let control_available = released_control
        && fence.is_some_and(|fence| fence.settled)
        && entry
            .input
            .as_ref()
            .is_some_and(JobInputWriter::controller_available);
    let control_recipients = control_available.then(|| foreground.recipients());
    Ok(Some(DetachedForeground {
        job_id: entry.job_id,
        attachment_id,
        session_id: entry.session_id.clone(),
        control_recipients,
    }))
}

async fn emit_detached_foreground(
    sys: &ActorSystem,
    client_id: u64,
    detached: DetachedForeground,
    reason: &str,
) {
    send_actor_gateway_event(
        "process_mgr",
        sys,
        client_id,
        EventPayload::FgExited {
            id: detached.job_id.to_string(),
            attachment_id: detached.attachment_id,
            reason: reason.to_string(),
        },
        detached.session_id.clone(),
    )
    .await;
    if let Some(recipients) = detached.control_recipients {
        emit_fg_control_changed(
            sys,
            recipients,
            detached.job_id,
            true,
            detached.session_id.as_deref(),
        )
        .await;
    }
}

fn close_foreground_state(foreground: &Arc<Mutex<ForegroundState>>) -> Vec<ForegroundRecipient> {
    let mut foreground = foreground.lock().unwrap();
    if foreground.closed {
        return Vec::new();
    }
    foreground.closed = true;
    foreground.controller = None;
    foreground.controller_generation = None;
    std::mem::take(&mut foreground.observers)
        .into_iter()
        .map(|(client_id, attachment_id)| ForegroundRecipient {
            client_id,
            attachment_id,
        })
        .collect()
}

fn request_input_failure_kill(entry: &ProcessEntry) {
    match entry.kill_tx.try_send(()) {
        Ok(()) => warn!(
            job_id = %entry.job_id,
            "process_mgr: terminating job after stdin writer failure"
        ),
        Err(mpsc::error::TrySendError::Full(_)) => debug!(
            job_id = %entry.job_id,
            "process_mgr: job kill already pending after stdin writer failure"
        ),
        Err(mpsc::error::TrySendError::Closed(_)) => debug!(
            job_id = %entry.job_id,
            "process_mgr: job already exiting after stdin writer failure"
        ),
    }
}

fn reject_controller_input(
    entry: &ProcessEntry,
    client_id: u64,
    operation: &str,
) -> InputRejection {
    match detach_foreground_entry(entry, client_id) {
        Ok(Some(detached)) => InputRejection::Detached(detached),
        Ok(None) => InputRejection::None,
        Err(error) => {
            let recipients = close_foreground_state(&entry.foreground);
            request_input_failure_kill(entry);
            InputRejection::Failed(FailedForeground {
                recipients,
                job_id: entry.job_id,
                session_id: entry.session_id.clone(),
                reason: format!("{operation} failed closed: {error}"),
            })
        }
    }
}

async fn emit_input_rejection(
    sys: &ActorSystem,
    client_id: u64,
    rejection: InputRejection,
    detach_reason: &str,
) {
    match rejection {
        InputRejection::None => {}
        InputRejection::Detached(detached) => {
            emit_detached_foreground(sys, client_id, detached, detach_reason).await;
        }
        InputRejection::Failed(failed) => {
            emit_fg_exit_recipients(
                sys,
                failed.recipients,
                failed.job_id,
                &failed.reason,
                failed.session_id.as_deref(),
            )
            .await;
        }
    }
}

enum JobInputDispatchError {
    Unauthorized(String),
    Enqueue(InputEnqueueError),
}

impl std::fmt::Display for JobInputDispatchError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unauthorized(message) => formatter.write_str(message),
            Self::Enqueue(error) => error.fmt(formatter),
        }
    }
}

fn try_enqueue_job_input(
    entry: &ProcessEntry,
    client_id: u64,
    data: Vec<u8>,
) -> Result<(), JobInputDispatchError> {
    let input = entry.input.as_ref().ok_or_else(|| {
        JobInputDispatchError::Unauthorized(format!("job {} does not accept stdin", entry.job_id))
    })?;
    if input.is_pty() {
        let foreground = entry.foreground.lock().unwrap();
        if foreground.controller != Some(client_id) {
            return Err(JobInputDispatchError::Unauthorized(format!(
                "client does not control foreground job {}",
                entry.job_id
            )));
        }
        let generation = foreground.controller_generation.ok_or_else(|| {
            JobInputDispatchError::Unauthorized(format!(
                "job {} foreground controller generation is missing",
                entry.job_id
            ))
        })?;
        // A successful try_send is the ACK linearization point: it means the
        // daemon accepted this item into the bounded per-job queue, not that
        // the child process consumed it. Controller fences cancel old,
        // not-yet-written generations before a later controller may proceed.
        input
            .try_enqueue(data, Some(generation))
            .map_err(JobInputDispatchError::Enqueue)
    } else {
        input
            .try_enqueue(data, None)
            .map_err(JobInputDispatchError::Enqueue)
    }
}

/// Spawn the ProcessManager actor task.
pub(super) fn spawn(mut rx: mpsc::Receiver<ProcessMgrMsg>, sys: ActorSystem) {
    tokio::spawn(async move {
        debug!("process_mgr: started");

        let mut children: HashMap<u32, ProcessEntry> = HashMap::new();

        // Internal channel for reader tasks to request cleanup.
        let (cleanup_tx, mut cleanup_rx) = mpsc::channel::<JobId>(super::ACTOR_CHANNEL_CAP);
        // Writer failures must bypass the bounded actor mailbox: a saturated
        // client/control queue may not suppress fail-closed PTY teardown.
        let (input_failure_tx, mut input_failure_rx) =
            mpsc::unbounded_channel::<InputWriterFailure>();

        loop {
            tokio::select! {
                biased;

                Some(InputWriterFailure {
                    job_id,
                    writer_incarnation,
                    reason,
                }) = input_failure_rx.recv() => {
                    let failure = children.get(&job_id.0).and_then(|entry| {
                        let input = entry.input.as_ref()?;
                        (input.incarnation == writer_incarnation).then(|| {
                            let recipients = close_foreground_state(&entry.foreground);
                            request_input_failure_kill(entry);
                            (
                                recipients,
                                entry.session_id.clone(),
                                format!("foreground input failed closed: {reason}"),
                            )
                        })
                    });
                    if let Some((recipients, session_id, reason)) = failure {
                        emit_fg_exit_recipients(
                            &sys,
                            recipients,
                            job_id,
                            &reason,
                            session_id.as_deref(),
                        )
                        .await;
                    }
                }

                // Reader task finished; remove the stale entry.
                Some(job_id) = cleanup_rx.recv() => {
                    debug!(%job_id, "process_mgr: cleaning up finished child");
                    children.remove(&job_id.0);
                }

                msg = rx.recv() => {
                    let Some(msg) = msg else { break; };
                    match msg {
                ProcessMgrMsg::SpawnJob {
                    job_id,
                    plan,
                    scope_hash,
                    options,
                } => {
                    info!(%job_id, plan = %plan, %scope_hash, "process_mgr: spawn");

                    // 1. Query ScopeStore for the environment snapshot.
                    let snapshot = {
                        let (tx, rx) = oneshot::channel();
                        if sys
                            .scope_store
                            .send(ScopeStoreMsg::GetScope {
                                hash: scope_hash,
                                reply: tx,
                            })
                            .await
                            .is_err()
                        {
                            error!(%job_id, "process_mgr: scope_store channel closed");
                            // Fail the job instead of continuing with the daemon environment.
                            fail_pending_spawn(&sys, job_id, options.session_id.as_deref()).await;
                            continue;
                        }
                        match rx.await {
                            Ok(Ok(Some(scope))) => match scope.snapshot {
                                Some(snapshot) => snapshot,
                                None => {
                                    error!(%job_id, %scope_hash, "process_mgr: scope has no snapshot");
                                    fail_pending_spawn(&sys, job_id, options.session_id.as_deref())
                                        .await;
                                    continue;
                                }
                            },
                            Ok(Ok(None)) => {
                                // Scope resolution failed, so the job cannot safely inherit env.
                                error!(%job_id, %scope_hash, "process_mgr: scope not found");
                                fail_pending_spawn(&sys, job_id, options.session_id.as_deref())
                                    .await;
                                continue;
                            }
                            Ok(Err(error)) => {
                                error!(%job_id, %scope_hash, "process_mgr: scope lookup failed: {error}");
                                fail_pending_spawn(&sys, job_id, options.session_id.as_deref())
                                    .await;
                                continue;
                            }
                            Err(_) => {
                                // Scope resolution failed, so the job cannot safely inherit env.
                                error!(%job_id, "process_mgr: scope_store reply dropped");
                                fail_pending_spawn(&sys, job_id, options.session_id.as_deref())
                                    .await;
                                continue;
                            }
                        }
                    };

                    let effective_snapshot = effective_snapshot(&snapshot);
                    let effective_options = effective_process_options(&options, &effective_snapshot);
                    let cwd = effective_cwd(&effective_snapshot, effective_options.cwd_override.as_deref());
                    if !cwd.is_dir() {
                        error!(
                            %job_id,
                            cwd = %cwd.display(),
                            "process_mgr: invalid cwd for job spawn"
                        );
                        emit_state_change(
                            &sys,
                            job_id,
                            JobStatus::Pending,
                            JobStatus::Failed,
                            effective_options.session_id.as_deref(),
                        )
                        .await;
                        emit_job_finished(&sys, job_id, EXIT_CODE_UNAVAILABLE).await;
                        continue;
                    }

                    clear_job_logs(job_id).await;

                    let entry = spawn_job_plan(
                        job_id,
                        &plan,
                        &effective_snapshot,
                        &effective_options,
                        sys.clone(),
                        cleanup_tx.clone(),
                        input_failure_tx.clone(),
                    )
                    .await;

                    match entry {
                        Ok(entry) => {
                            emit_state_change(
                                &sys,
                                job_id,
                                JobStatus::Pending,
                                JobStatus::Running,
                                effective_options.session_id.as_deref(),
                            )
                            .await;
                            children.insert(job_id.0, entry);
                        }
                        Err(()) => {
                            emit_state_change(
                                &sys,
                                job_id,
                                JobStatus::Pending,
                                JobStatus::Failed,
                                effective_options.session_id.as_deref(),
                            )
                            .await;
                            emit_job_finished(&sys, job_id, EXIT_CODE_UNAVAILABLE).await;
                        }
                    }
                }

                ProcessMgrMsg::KillJob { job_id, reply } => {
                    info!(%job_id, "process_mgr: kill requested");
                    let Some(entry) = children.remove(&job_id.0) else {
                        let _ = reply.send(Err(format!("job {job_id} not found")));
                        continue;
                    };
                    let ProcessEntry {
                        status,
                        reader_handle,
                        kill_tx,
                        ..
                    } = entry;
                    if !status.is_terminal() && kill_tx.send(()).await.is_err() {
                        debug!(%job_id, "process_mgr: kill channel already closed; waiting for reader exit");
                    }
                    // Do not block the process-manager actor while the child exits.
                    // The scheduler receives the acknowledgement only after the
                    // reader/waiter task has reaped every child in the job.
                    tokio::spawn(async move {
                        let result = match tokio::time::timeout(
                            std::time::Duration::from_secs(10),
                            reader_handle,
                        )
                        .await
                        {
                            Ok(Ok(())) => Ok(()),
                            Ok(Err(error)) => Err(format!(
                                "job {job_id} process waiter failed: {error}"
                            )),
                            Err(_) => Err(format!(
                                "timed out waiting for job {job_id} to stop"
                            )),
                        };
                        let _ = reply.send(result);
                    });
                }

                // Expose ring-buffer contents for live-tail queries.
                ProcessMgrMsg::GetOutput { job_id, tail_bytes, reply } => {
                    let result = children.get(&job_id.0).map(|entry| {
                        let (data, truncated) = entry
                            .ring_buffer
                            .lock()
                            .unwrap()
                            .tail_with_truncation(tail_bytes);
                        OutputSnapshot { data, truncated }
                    });
                    let _ = reply.send(result);
                }

                ProcessMgrMsg::GetStderr { job_id, tail_bytes, reply } => {
                    let result = children.get(&job_id.0).map(|entry| match &entry.stderr_ring {
                        Some(ring) => {
                            let (data, truncated) =
                                ring.lock().unwrap().tail_with_truncation(tail_bytes);
                            StderrSnapshot {
                                pty_merged: false,
                                data,
                                truncated,
                            }
                        }
                        None => StderrSnapshot {
                            pty_merged: true,
                            data: Vec::new(),
                            truncated: false,
                        },
                    });
                    let _ = reply.send(result);
                }

                ProcessMgrMsg::SendJobInput { client_id, job_id, data, reply } => {
                    let (handled, rejection) = match children.get(&job_id.0) {
                        Some(entry) => match try_enqueue_job_input(entry, client_id, data) {
                            Ok(()) => (Ok(()), InputRejection::None),
                            Err(error) => {
                                let rejection = if matches!(
                                    error,
                                    JobInputDispatchError::Enqueue(_)
                                ) && entry
                                    .input
                                    .as_ref()
                                    .is_some_and(JobInputWriter::is_pty)
                                {
                                    reject_controller_input(entry, client_id, "job input rejection")
                                } else {
                                    InputRejection::None
                                };
                                (
                                    Err(format!("failed to enqueue job input: {error}")),
                                    rejection,
                                )
                            }
                        },
                        None => (
                            Err(format!("job {job_id} does not accept stdin")),
                            InputRejection::None,
                        ),
                    };
                    let _ = reply.send(handled);
                    emit_input_rejection(
                        &sys,
                        client_id,
                        rejection,
                        "job input rejected; controller detached",
                    )
                    .await;
                }

                ProcessMgrMsg::AttachFg { client_id, job_id, role, reply } => {
                    let current_job = foreground_job_for_client(&children, client_id);
                    let (result, control_recipients, session_id) =
                        if current_job.is_some_and(|current| current != job_id) {
                            (
                                Err(format!(
                                    "client is already foreground-attached to {}",
                                    current_job.expect("checked above")
                                )),
                                None,
                                None,
                            )
                        } else if let Some(entry) = children.get(&job_id.0) {
                            let session_id = entry.session_id.clone();
                            match attach_foreground(entry, client_id, role) {
                                Ok((info, recipients)) => (Ok(info), recipients, session_id),
                                Err(error) => (Err(error), None, session_id),
                            }
                        } else {
                            (Err(format!("job {job_id} not found")), None, None)
                        };
                    // Daemons predating shared foreground mode delivered the
                    // retained snapshot as an FgOutput event after the
                    // FgAttached response. Keep that one legacy event for the
                    // controller entry point: current clients reject epoch 0
                    // for a non-zero attachment, while old clients recover
                    // their history instead of starting from an empty screen.
                    let legacy_snapshot = if role == ForegroundRole::Controller {
                        result.as_ref().ok().map(|info| info.snapshot.clone())
                    } else {
                        None
                    }
                    .filter(|snapshot| !snapshot.is_empty());
                    let _ = reply.send(result);
                    if let Some(snapshot) = legacy_snapshot {
                        send_actor_gateway_event(
                            "process_mgr",
                            &sys,
                            client_id,
                            EventPayload::FgOutput {
                                id: job_id.to_string(),
                                attachment_id: 0,
                                data: snapshot,
                            },
                            session_id.clone(),
                        )
                        .await;
                    }
                    if let Some(recipients) = control_recipients {
                        emit_fg_control_changed(
                            &sys,
                            recipients,
                            job_id,
                            false,
                            session_id.as_deref(),
                        )
                        .await;
                    }
                }

                ProcessMgrMsg::ClaimFgControl { client_id, reply } => {
                    let Some(job_id) = foreground_job_for_client(&children, client_id) else {
                        let _ = reply.send(Err("no foreground job observed".to_string()));
                        continue;
                    };
                    let entry = children
                        .get(&job_id.0)
                        .expect("foreground lookup returned a live job");
                    let session_id = entry.session_id.clone();
                    let (result, recipients) = claim_foreground_control(entry, client_id);
                    let _ = reply.send(result);
                    if let Some(recipients) = recipients {
                        emit_fg_control_changed(
                            &sys,
                            recipients,
                            job_id,
                            false,
                            session_id.as_deref(),
                        )
                        .await;
                    }
                }

                ProcessMgrMsg::ReleaseFgControl { client_id, reply } => {
                    let Some(job_id) = foreground_job_for_client(&children, client_id) else {
                        let _ = reply.send(Err("no foreground job observed".to_string()));
                        continue;
                    };
                    let entry = children
                        .get(&job_id.0)
                        .expect("foreground lookup returned a live job");
                    let session_id = entry.session_id.clone();
                    let (result, recipients) = release_foreground_control(entry, client_id);
                    let _ = reply.send(result);
                    if let Some(recipients) = recipients {
                        emit_fg_control_changed(
                            &sys,
                            recipients,
                            job_id,
                            true,
                            session_id.as_deref(),
                        )
                        .await;
                    }
                }

                ProcessMgrMsg::DetachFg { client_id, reason, reply } => {
                    let mut detached_jobs = Vec::new();
                    let mut failed_fences = Vec::new();
                    for entry in children.values() {
                        match detach_foreground_entry(entry, client_id) {
                            Ok(Some(detached)) => detached_jobs.push(detached),
                            Ok(None) => {}
                            Err(error) => {
                                request_input_failure_kill(entry);
                                failed_fences.push((
                                    entry.foreground.clone(),
                                    entry.job_id,
                                    entry.session_id.clone(),
                                    format!("foreground detach failed closed: {error}"),
                                ));
                            }
                        }
                    }
                    for detached in detached_jobs {
                        emit_detached_foreground(&sys, client_id, detached, &reason).await;
                    }
                    for (foreground, job_id, session_id, failure_reason) in failed_fences {
                        emit_fg_exit(
                            &sys,
                            &foreground,
                            job_id,
                            &failure_reason,
                            session_id.as_deref(),
                        )
                        .await;
                    }
                    if let Some(reply) = reply {
                        let _ = reply.send(());
                    }
                }

                ProcessMgrMsg::FgInput { client_id, data, reply } => {
                    let entry = children
                        .values()
                        .find(|entry| {
                            entry.foreground.lock().unwrap().controller == Some(client_id)
                        });
                    let (handled, rejection) = if let Some(entry) = entry {
                        match try_enqueue_job_input(entry, client_id, data) {
                            Ok(()) => (Ok(()), InputRejection::None),
                            Err(error) => {
                                let rejection = if matches!(
                                    error,
                                    JobInputDispatchError::Enqueue(_)
                                ) {
                                    reject_controller_input(
                                        entry,
                                        client_id,
                                        "foreground input rejection",
                                    )
                                } else {
                                    InputRejection::None
                                };
                                (
                                    Err(format!("failed to enqueue fg input: {error}")),
                                    rejection,
                                )
                            }
                        }
                    } else {
                        (
                            Err("no foreground session attached".to_string()),
                            InputRejection::None,
                        )
                    };
                    let _ = reply.send(handled);
                    emit_input_rejection(
                        &sys,
                        client_id,
                        rejection,
                        "foreground input rejected; controller detached",
                    )
                    .await;
                }

                ProcessMgrMsg::FgResize { client_id, cols, rows, reply } => {
                    let resize = children
                        .values()
                        .find(|entry| {
                            entry.foreground.lock().unwrap().controller == Some(client_id)
                        })
                        .map(|entry| entry.resize.clone());
                    let _ = reply.send(if let Some(Some(resize)) = resize {
                        set_winsize(resize.as_raw_fd(), cols, rows)
                            .map_err(|error| format!("failed to resize fg session: {error}"))
                    } else {
                        Err("no foreground session attached".into())
                    });
                }

                ProcessMgrMsg::InputWriterFenceSettled {
                    job_id,
                    writer_incarnation,
                    generation,
                } => {
                    let settled = children.get(&job_id.0).and_then(|entry| {
                        let input = entry.input.as_ref()?;
                        if input.incarnation != writer_incarnation
                            || !input.fence.is_settled_generation(generation)
                        {
                            return None;
                        }
                        let foreground = entry.foreground.lock().unwrap();
                        (foreground.controller.is_none() && !foreground.closed)
                            .then(|| (foreground.recipients(), entry.session_id.clone()))
                    });
                    if let Some((recipients, session_id)) = settled {
                        emit_fg_control_changed(
                            &sys,
                            recipients,
                            job_id,
                            true,
                            session_id.as_deref(),
                        )
                        .await;
                    }
                }

                ProcessMgrMsg::Shutdown => {
                    debug!("process_mgr: shutting down — killing all children");
                    for entry in children.values() {
                        if !entry.status.is_terminal() {
                            match entry.kill_tx.try_send(()) {
                                Ok(()) => {
                                    debug!(job_id = %entry.job_id, "process_mgr: shutdown kill requested");
                                }
                                Err(mpsc::error::TrySendError::Full(_)) => {
                                    debug!(job_id = %entry.job_id, "process_mgr: shutdown kill already pending");
                                }
                                Err(mpsc::error::TrySendError::Closed(_)) => {
                                    debug!(job_id = %entry.job_id, "process_mgr: shutdown kill channel closed");
                                }
                            }
                        }
                    }
                    // Give children a moment to exit.
                    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                    break;
                }
                    }
                }

            }
        }

        debug!("process_mgr: stopped");
    });
}

// ── Helpers ──

fn set_nonblocking(fd: std::os::fd::RawFd) -> std::io::Result<()> {
    // SAFETY: fcntl operates on a valid fd owned by this process.
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    if flags == -1 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: fcntl operates on a valid fd owned by this process.
    let rc = unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) };
    if rc == -1 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

fn set_winsize(fd: std::os::fd::RawFd, cols: u16, rows: u16) -> std::io::Result<()> {
    let winsize = libc::winsize {
        ws_row: rows,
        ws_col: cols,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    // SAFETY: ioctl operates on a valid tty/pty fd and a properly initialized winsize.
    let rc = unsafe { libc::ioctl(fd, libc::TIOCSWINSZ, &winsize) };
    if rc == -1 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

async fn read_pty(fd: &AsyncFd<std::fs::File>, buf: &mut [u8]) -> std::io::Result<usize> {
    loop {
        let mut guard = fd.readable().await?;
        match guard.try_io(|inner| inner.get_ref().read(buf)) {
            Ok(result) => return result,
            Err(_would_block) => continue,
        }
    }
}

enum PtyItemOutcome {
    Written { settled_generation: Option<u64> },
    Discarded { settled_generation: Option<u64> },
    Failed(String),
}

fn settle_idle_fence(fence: &InputFence) -> Option<u64> {
    let mut inner = fence.lock_inner();
    if fence.is_poisoned() {
        return None;
    }
    if inner.active.is_none() && inner.settled_generation != inner.generation {
        inner.settled_generation = inner.generation;
        Some(inner.generation)
    } else {
        None
    }
}

fn cancel_active_pty_item(fence: &InputFence, generation: u64) -> PtyItemOutcome {
    let mut inner = fence.lock_inner();
    if fence.is_poisoned() {
        return PtyItemOutcome::Failed("PTY input fence is poisoned".into());
    }
    if inner.generation == generation {
        return PtyItemOutcome::Discarded {
            settled_generation: None,
        };
    }
    let Some(active) = inner.active.take() else {
        let settled_generation = (inner.settled_generation != inner.generation).then(|| {
            inner.settled_generation = inner.generation;
            inner.generation
        });
        return PtyItemOutcome::Discarded { settled_generation };
    };
    if active.written == active.total {
        let settled_generation = (inner.settled_generation != inner.generation).then(|| {
            inner.settled_generation = inner.generation;
            inner.generation
        });
        return PtyItemOutcome::Written { settled_generation };
    }
    if active.written == 0 {
        let settled_generation = (inner.settled_generation != inner.generation).then(|| {
            inner.settled_generation = inner.generation;
            inner.generation
        });
        return PtyItemOutcome::Discarded { settled_generation };
    }

    let reason = format!(
        "controller generation changed after delivering {} of {} input bytes",
        active.written, active.total
    );
    fence.poison_locked(&mut inner);
    PtyItemOutcome::Failed(reason)
}

fn poison_active_pty_item(fence: &InputFence, detail: &str) -> PtyItemOutcome {
    let mut inner = fence.lock_inner();
    let active = inner.active.take();
    let (written, total) = active.map_or((0, 0), |active| (active.written, active.total));
    let reason = format!("{detail}; delivered {written} of {total} input bytes");
    fence.poison_locked(&mut inner);
    PtyItemOutcome::Failed(reason)
}

async fn write_pty_item(
    fd: &AsyncFd<std::fs::File>,
    item: &JobInputItem,
    fence: &InputFence,
    fence_changes: &mut watch::Receiver<u64>,
) -> PtyItemOutcome {
    let Some(generation) = item.generation else {
        return PtyItemOutcome::Failed("PTY input item has no controller generation".into());
    };
    if item.data.is_empty() {
        return PtyItemOutcome::Written {
            settled_generation: None,
        };
    }

    {
        let mut inner = fence.lock_inner();
        if fence.is_poisoned() {
            return PtyItemOutcome::Failed("PTY input writer is poisoned".into());
        }
        if inner.generation != generation {
            let settled_generation = (inner.settled_generation != inner.generation).then(|| {
                inner.settled_generation = inner.generation;
                inner.generation
            });
            return PtyItemOutcome::Discarded { settled_generation };
        }
        inner.active = Some(ActiveInput {
            generation,
            written: 0,
            total: item.data.len(),
        });
    }

    loop {
        tokio::select! {
            biased;

            changed = fence_changes.changed() => {
                if changed.is_err() {
                    return poison_active_pty_item(fence, "PTY input fence closed");
                }
                let outcome = cancel_active_pty_item(fence, generation);
                if !matches!(
                    outcome,
                    PtyItemOutcome::Discarded {
                        settled_generation: None,
                    }
                ) {
                    return outcome;
                }
            }

            writable = fd.writable() => {
                let mut guard = match writable {
                    Ok(guard) => guard,
                    Err(error) => {
                        return poison_active_pty_item(
                            fence,
                            &format!("wait for PTY writable failed: {error}"),
                        );
                    }
                };
                let mut inner = fence.lock_inner();
                if fence.is_poisoned() {
                    return PtyItemOutcome::Failed("PTY input writer is poisoned".into());
                }
                if inner.generation != generation {
                    drop(inner);
                    return cancel_active_pty_item(fence, generation);
                }
                let written = inner
                    .active
                    .as_ref()
                    .map(|active| active.written)
                    .unwrap_or(0);
                match guard.try_io(|inner_fd| {
                    inner_fd.get_ref().write(&item.data[written..])
                }) {
                    Ok(Ok(0)) => {
                        fence.poison_locked(&mut inner);
                        return PtyItemOutcome::Failed(format!(
                            "PTY write returned 0 bytes; delivered {written} of {} input bytes",
                            item.data.len()
                        ));
                    }
                    Ok(Ok(count)) => {
                        let active = inner
                            .active
                            .as_mut()
                            .expect("PTY item remains active while its generation is current");
                        active.written += count;
                        if active.written == active.total {
                            inner.active = None;
                            return PtyItemOutcome::Written {
                                settled_generation: None,
                            };
                        }
                    }
                    Ok(Err(error)) => {
                        fence.poison_locked(&mut inner);
                        return PtyItemOutcome::Failed(format!(
                            "PTY write failed: {error}; delivered {written} of {} input bytes",
                            item.data.len()
                        ));
                    }
                    Err(_would_block) => {}
                }
            }
        }
    }
}

fn notify_input_writer_failure(
    failures: &mpsc::UnboundedSender<InputWriterFailure>,
    job_id: JobId,
    writer_incarnation: u64,
    reason: String,
) {
    let _ = failures.send(InputWriterFailure {
        job_id,
        writer_incarnation,
        reason,
    });
}

async fn notify_input_fence_settled(
    process_mgr: &mpsc::Sender<ProcessMgrMsg>,
    job_id: JobId,
    writer_incarnation: u64,
    generation: u64,
) {
    let _ = process_mgr
        .send(ProcessMgrMsg::InputWriterFenceSettled {
            job_id,
            writer_incarnation,
            generation,
        })
        .await;
}

async fn job_input_writer_task(
    job_id: JobId,
    writer_incarnation: u64,
    mut sink: JobInputSink,
    mut receiver: mpsc::Receiver<JobInputItem>,
    fence: Arc<InputFence>,
    process_mgr: mpsc::Sender<ProcessMgrMsg>,
    failures: mpsc::UnboundedSender<InputWriterFailure>,
) {
    match &mut sink {
        JobInputSink::Pty(fd) => {
            let mut fence_changes = fence.subscribe();
            loop {
                tokio::select! {
                    biased;

                    changed = fence_changes.changed() => {
                        if changed.is_err() {
                            return;
                        }
                        if let Some(generation) = settle_idle_fence(&fence) {
                            notify_input_fence_settled(
                                &process_mgr,
                                job_id,
                                writer_incarnation,
                                generation,
                            )
                            .await;
                        }
                    }

                    item = receiver.recv() => {
                        let Some(item) = item else {
                            return;
                        };
                        match write_pty_item(fd, &item, &fence, &mut fence_changes).await {
                            PtyItemOutcome::Written { settled_generation }
                            | PtyItemOutcome::Discarded { settled_generation } => {
                                if let Some(generation) = settled_generation {
                                    notify_input_fence_settled(
                                        &process_mgr,
                                        job_id,
                                        writer_incarnation,
                                        generation,
                                    )
                                    .await;
                                }
                            }
                            PtyItemOutcome::Failed(reason) => {
                                if !fence.is_poisoned() {
                                    let mut inner = fence.lock_inner();
                                    fence.poison_locked(&mut inner);
                                }
                                notify_input_writer_failure(
                                    &failures,
                                    job_id,
                                    writer_incarnation,
                                    reason,
                                );
                                return;
                            }
                        }
                    }
                }
            }
        }
        JobInputSink::Pipe(stdin) => {
            while let Some(item) = receiver.recv().await {
                let mut written = 0;
                while written < item.data.len() {
                    match stdin.write(&item.data[written..]).await {
                        Ok(0) => {
                            {
                                let mut inner = fence.lock_inner();
                                fence.poison_locked(&mut inner);
                            }
                            notify_input_writer_failure(
                                &failures,
                                job_id,
                                writer_incarnation,
                                format!(
                                    "stdin write returned 0 bytes; delivered {written} of {} input bytes",
                                    item.data.len()
                                ),
                            );
                            return;
                        }
                        Ok(count) => written += count,
                        Err(error) => {
                            {
                                let mut inner = fence.lock_inner();
                                fence.poison_locked(&mut inner);
                            }
                            notify_input_writer_failure(
                                &failures,
                                job_id,
                                writer_incarnation,
                                format!(
                                    "stdin write failed: {error}; delivered {written} of {} input bytes",
                                    item.data.len()
                                ),
                            );
                            return;
                        }
                    }
                }
                if let Err(error) = stdin.flush().await {
                    {
                        let mut inner = fence.lock_inner();
                        fence.poison_locked(&mut inner);
                    }
                    notify_input_writer_failure(
                        &failures,
                        job_id,
                        writer_incarnation,
                        format!(
                            "stdin flush failed after delivering {} input bytes: {error}",
                            item.data.len()
                        ),
                    );
                    return;
                }
            }
        }
    }
}

#[derive(Clone)]
struct ExpandedSegment {
    command_line: Vec<String>,
    program: String,
    args: Vec<String>,
    pipe_to_next: Option<cue_core::pipeline::PipeOp>,
}

fn expand_pipeline_segments(
    job_id: JobId,
    pipeline: &cue_core::pipeline::Pipeline,
    snapshot: &EnvSnapshot,
) -> Result<Vec<ExpandedSegment>, ()> {
    let mut expanded = Vec::with_capacity(pipeline.segments.len());
    for segment in &pipeline.segments {
        let command_line = expand_command_line(&segment.command, Some(snapshot));
        let Some(program) = command_line
            .first()
            .cloned()
            .filter(|word| !word.is_empty())
        else {
            error!(
                %job_id,
                pipeline = ?segment.command,
                "process_mgr: command is empty"
            );
            return Err(());
        };
        let args = command_line.get(1..).unwrap_or(&[]).to_vec();
        expanded.push(ExpandedSegment {
            command_line,
            program,
            args,
            pipe_to_next: segment.pipe_to_next,
        });
    }
    if expanded.is_empty() {
        error!(%job_id, "process_mgr: pipeline is empty");
        return Err(());
    }
    Ok(expanded)
}

fn configure_command(
    cmd: &mut tokio::process::Command,
    snapshot: &EnvSnapshot,
    cwd_override: Option<&Path>,
    sandbox: Option<&crate::sandbox::PreparedSandbox>,
) {
    let cwd = effective_cwd_path(snapshot, cwd_override, sandbox);
    cmd.env_clear();
    cmd.envs(snapshot.env.iter());
    cmd.env("PWD", &cwd);
    cmd.current_dir(cwd);
    cmd.kill_on_drop(true);
}

fn effective_process_options(
    options: &ProcessJobOptions,
    _snapshot: &EnvSnapshot,
) -> ProcessJobOptions {
    options.clone()
}

fn effective_cwd<'a>(snapshot: &'a EnvSnapshot, cwd_override: Option<&'a Path>) -> &'a Path {
    cwd_override.unwrap_or(&snapshot.cwd)
}

fn effective_cwd_path(
    snapshot: &EnvSnapshot,
    cwd_override: Option<&Path>,
    sandbox: Option<&crate::sandbox::PreparedSandbox>,
) -> PathBuf {
    let cwd = effective_cwd(snapshot, cwd_override);
    sandbox.map_or_else(|| cwd.to_path_buf(), |sandbox| sandbox.cwd_for(cwd))
}

fn log_spawn_failure(
    job_id: JobId,
    program: &str,
    args: &[String],
    snapshot: &EnvSnapshot,
    cwd_override: Option<&Path>,
    error: &std::io::Error,
) {
    error!(
        %job_id,
        program,
        args = ?args,
        cwd = %effective_cwd(snapshot, cwd_override).display(),
        path = ?snapshot.env.get("PATH").cloned(),
        err = %error,
        "process_mgr: spawn failed"
    );
}

fn pipeline_has_job_local_builtin(pipeline: &cue_core::pipeline::Pipeline) -> bool {
    pipeline.segments.len() == 1
        && detect_job_local_builtin(&pipeline.segments[0].command).is_some()
}

fn detect_job_local_builtin(words: &[String]) -> Option<JobLocalBuiltin> {
    let command = words.first()?.as_str();
    match command {
        "cd" => Some(JobLocalBuiltin::Cd {
            path: words.get(1).cloned().unwrap_or_else(|| "~".into()),
        }),
        "env" if words.get(1).map(String::as_str) == Some("set") => Some(JobLocalBuiltin::EnvSet {
            assignments: words.get(2..).unwrap_or(&[]).to_vec(),
        }),
        _ => None,
    }
}

async fn spawn_job_plan(
    job_id: JobId,
    plan: &JobPlan,
    snapshot: &EnvSnapshot,
    options: &ProcessJobOptions,
    sys: ActorSystem,
    cleanup_tx: mpsc::Sender<JobId>,
    input_failure_tx: mpsc::UnboundedSender<InputWriterFailure>,
) -> Result<ProcessEntry, ()> {
    match plan {
        JobPlan::Pipeline(pipeline) if pipeline_has_job_local_builtin(pipeline) => {
            spawn_logical_job(
                job_id,
                plan.clone(),
                snapshot.clone(),
                options,
                sys,
                cleanup_tx,
            )
            .await
        }
        JobPlan::Pipeline(pipeline) if pipeline.segments.len() == 1 && options.pty_enabled => {
            spawn_single_pty_job(
                job_id,
                pipeline,
                snapshot,
                options,
                sys,
                cleanup_tx,
                input_failure_tx,
            )
            .await
        }
        // Single-segment without PTY → spawn with pipes.
        JobPlan::Pipeline(pipeline) if pipeline.segments.len() == 1 => {
            spawn_single_pipe_job(job_id, pipeline, snapshot, options, sys, cleanup_tx).await
        }
        JobPlan::Pipeline(pipeline) => {
            spawn_native_pipeline_job(
                job_id,
                pipeline,
                snapshot,
                options,
                sys,
                cleanup_tx,
                input_failure_tx,
            )
            .await
        }
        JobPlan::And { .. } | JobPlan::Or { .. } => {
            spawn_logical_job(
                job_id,
                plan.clone(),
                snapshot.clone(),
                options,
                sys,
                cleanup_tx,
            )
            .await
        }
    }
}

fn prepare_job_sandbox(
    job_id: JobId,
    snapshot: &EnvSnapshot,
    options: &ProcessJobOptions,
    sys: &ActorSystem,
) -> Result<Option<crate::sandbox::PreparedSandbox>, String> {
    let Some(config) = options.sandbox.as_ref() else {
        return Ok(None);
    };
    let lower_dir = effective_cwd(snapshot, options.cwd_override.as_deref());
    crate::sandbox::prepare(
        job_id,
        config,
        lower_dir,
        &crate::sandbox::SandboxDefaults {
            upper_root: sys.config.sandbox.default_upper_root.clone(),
            min_free_ratio: sys.config.sandbox.min_free_ratio,
        },
    )
    .map(Some)
    .map_err(|error| {
        let message = format!("sandbox setup failed: {error:#}");
        error!(%job_id, err = %message, "process_mgr: sandbox setup failed");
        message
    })
}

async fn prepare_job_sandbox_or_emit(
    job_id: JobId,
    snapshot: &EnvSnapshot,
    options: &ProcessJobOptions,
    sys: &ActorSystem,
) -> Result<Option<crate::sandbox::PreparedSandbox>, ()> {
    match prepare_job_sandbox(job_id, snapshot, options, sys) {
        Ok(sandbox) => Ok(sandbox),
        Err(message) => {
            emit_spawn_setup_stderr(
                sys,
                job_id,
                &message,
                options.direct_output_client,
                options.session_id.as_deref(),
            )
            .await;
            Err(())
        }
    }
}

async fn emit_spawn_setup_stderr(
    sys: &ActorSystem,
    job_id: JobId,
    message: &str,
    direct_output_client: Option<u64>,
    session_id: Option<&str>,
) {
    let line = format!("{message}\n");
    let stderr_log = Arc::new(Mutex::new(open_stderr_log(job_id).await));
    write_log(job_id, LogStream::Stderr, &stderr_log, line.as_bytes()).await;
    emit_output(
        sys,
        job_id,
        OutputStream::Stderr,
        line.as_bytes(),
        direct_output_client,
        session_id,
    )
    .await;
    emit_output_eof(sys, job_id, direct_output_client, session_id).await;
}

/// Spawn a single-segment job with pipes (stdout/stderr piped, no PTY).
/// Used when `pty=false` is specified — the child cannot detect a terminal.
async fn spawn_single_pipe_job(
    job_id: JobId,
    pipeline: &cue_core::pipeline::Pipeline,
    snapshot: &EnvSnapshot,
    options: &ProcessJobOptions,
    sys: ActorSystem,
    cleanup_tx: mpsc::Sender<JobId>,
) -> Result<ProcessEntry, ()> {
    use tokio::io::AsyncReadExt;

    let segments = expand_pipeline_segments(job_id, pipeline, snapshot)?;
    let segment = &segments[0];
    let (program, args) = wrap_segment_if_enabled(&sys, options.wrapper_enabled, segment);
    let sandbox = prepare_job_sandbox_or_emit(job_id, snapshot, options, &sys).await?;

    let mut cmd = tokio::process::Command::new(&program);
    if !args.is_empty() {
        cmd.args(&args);
    }
    configure_command(
        &mut cmd,
        snapshot,
        options.cwd_override.as_deref(),
        sandbox.as_ref(),
    );
    cmd.stdin(Stdio::null());
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());

    let mut child = cmd.spawn().map_err(|error| {
        log_spawn_failure(
            job_id,
            &program,
            &args,
            snapshot,
            options.cwd_override.as_deref(),
            &error,
        );
    })?;
    info!(%job_id, pid = ?child.id(), "process_mgr: pipe child spawned");

    let Some(mut stdout) = child.stdout.take() else {
        error!(%job_id, "process_mgr: spawned pipe child without stdout pipe");
        request_child_kill(job_id, &mut child, "missing stdout pipe");
        wait_for_child(job_id, &mut child, "after missing stdout pipe").await;
        return Err(());
    };
    let Some(mut stderr) = child.stderr.take() else {
        error!(%job_id, "process_mgr: spawned pipe child without stderr pipe");
        request_child_kill(job_id, &mut child, "missing stderr pipe");
        wait_for_child(job_id, &mut child, "after missing stderr pipe").await;
        return Err(());
    };

    let ring_buffer = Arc::new(Mutex::new(RingBuffer::default()));
    let stderr_ring = Arc::new(Mutex::new(RingBuffer::default()));
    let foreground = Arc::new(Mutex::new(ForegroundState::default()));
    let sys_clone = sys.clone();
    let ring_clone = ring_buffer.clone();
    let stderr_clone = stderr_ring.clone();
    let foreground_clone = foreground.clone();
    let cleanup_tx_clone = cleanup_tx.clone();
    let direct_output_client = options.direct_output_client;
    let session_id = options.session_id.clone();
    let entry_session_id = session_id.clone();
    let (kill_tx, mut kill_rx) = mpsc::channel::<()>(1);

    // Read stdout and stderr concurrently, wait for exit.
    let log_file = open_output_log(job_id).await;
    let reader_handle = tokio::spawn(async move {
        let _sandbox = sandbox;
        let log = Arc::new(Mutex::new(log_file));
        let log_clone = log.clone();
        let sys_emit = sys_clone.clone();
        let sys_stderr_emit = sys_clone.clone();
        let stdout_session_id = session_id.clone();
        let stderr_session_id = session_id.clone();

        let stdout_task = tokio::spawn(async move {
            let mut buf = vec![0u8; 8192];
            loop {
                match stdout.read(&mut buf).await {
                    Ok(0) => break,
                    Ok(n) => {
                        let chunk = buf[..n].to_vec();
                        ring_clone.lock().unwrap().push(&chunk);
                        write_log(job_id, LogStream::Stdout, &log_clone, &chunk).await;
                        emit_output(
                            &sys_emit,
                            job_id,
                            OutputStream::Stdout,
                            &chunk,
                            direct_output_client,
                            stdout_session_id.as_deref(),
                        )
                        .await;
                    }
                    Err(error) => {
                        warn!(%job_id, err = %error, stream = "stdout", "process_mgr: pipe read failed");
                        break;
                    }
                }
            }
        });

        let stderr_log = open_stderr_log(job_id).await;
        let stderr_log = Arc::new(Mutex::new(stderr_log));
        let stderr_task = tokio::spawn(async move {
            let mut buf = vec![0u8; 8192];
            loop {
                match stderr.read(&mut buf).await {
                    Ok(0) => break,
                    Ok(n) => {
                        let chunk = buf[..n].to_vec();
                        stderr_clone.lock().unwrap().push(&chunk);
                        write_log(job_id, LogStream::Stderr, &stderr_log, &chunk).await;
                        emit_output(
                            &sys_stderr_emit,
                            job_id,
                            OutputStream::Stderr,
                            &chunk,
                            direct_output_client,
                            stderr_session_id.as_deref(),
                        )
                        .await;
                    }
                    Err(error) => {
                        warn!(%job_id, err = %error, stream = "stderr", "process_mgr: pipe read failed");
                        break;
                    }
                }
            }
        });

        let (exit_code, was_killed) = tokio::select! {
            status = child.wait() => {
                let code = match status {
                    Ok(status) => exit_code_from_status(status, EXIT_CODE_UNAVAILABLE),
                    Err(error) => {
                        error!(%job_id, err = %error, "process_mgr: pipe child wait failed");
                        EXIT_CODE_UNAVAILABLE
                    }
                };
                (code, false)
            }
            _ = kill_rx.recv() => {
                request_child_kill(job_id, &mut child, "pipe kill requested");
                let code = wait_for_child(job_id, &mut child, "after pipe kill").await;
                (code, true)
            }
        };

        if let Err(error) = stdout_task.await {
            error!(%job_id, err = %error, stream = "stdout", "process_mgr: pipe reader task failed");
        }
        if let Err(error) = stderr_task.await {
            error!(%job_id, err = %error, stream = "stderr", "process_mgr: pipe reader task failed");
        }
        info!(%job_id, exit_code, "process_mgr: pipe child exited");

        emit_output_eof(
            &sys_clone,
            job_id,
            direct_output_client,
            session_id.as_deref(),
        )
        .await;

        let (new_state, reported_exit_code, fg_reason) = if was_killed {
            (
                JobStatus::Killed,
                EXIT_CODE_UNAVAILABLE,
                "killed".to_string(),
            )
        } else if exit_code == 0 {
            (JobStatus::Done, exit_code, format!("exit {exit_code}"))
        } else {
            (JobStatus::Failed, exit_code, format!("exit {exit_code}"))
        };
        emit_state_change(
            &sys_clone,
            job_id,
            JobStatus::Running,
            new_state,
            session_id.as_deref(),
        )
        .await;
        emit_fg_exit(
            &sys_clone,
            &foreground_clone,
            job_id,
            &fg_reason,
            session_id.as_deref(),
        )
        .await;
        emit_job_finished(&sys_clone, job_id, reported_exit_code).await;
        notify_cleanup(&cleanup_tx_clone, job_id).await;
    });

    Ok(ProcessEntry {
        job_id,
        session_id: entry_session_id,
        status: JobStatus::Running,
        reader_handle,
        kill_tx,
        ring_buffer,
        stderr_ring: Some(stderr_ring),
        input: None,
        resize: None,
        foreground,
    })
}

async fn spawn_single_pty_job(
    job_id: JobId,
    pipeline: &cue_core::pipeline::Pipeline,
    snapshot: &EnvSnapshot,
    options: &ProcessJobOptions,
    sys: ActorSystem,
    cleanup_tx: mpsc::Sender<JobId>,
    input_failure_tx: mpsc::UnboundedSender<InputWriterFailure>,
) -> Result<ProcessEntry, ()> {
    let segments = expand_pipeline_segments(job_id, pipeline, snapshot)?;
    let segment = &segments[0];
    let (program, args) = wrap_segment_if_enabled(&sys, options.wrapper_enabled, segment);
    let sandbox = prepare_job_sandbox_or_emit(job_id, snapshot, options, &sys).await?;

    let mut cmd = tokio::process::Command::new(&program);
    if !args.is_empty() {
        cmd.args(&args);
    }
    configure_command(
        &mut cmd,
        snapshot,
        options.cwd_override.as_deref(),
        sandbox.as_ref(),
    );

    let pty_pair = crate::pty::open_pty().map_err(|error| {
        error!(%job_id, err = %error, "process_mgr: open pty failed");
    })?;
    let master_file = std::fs::File::from(pty_pair.master);
    let slave = pty_pair.slave;
    if let Err(error) = set_nonblocking(master_file.as_raw_fd()) {
        error!(%job_id, err = %error, "process_mgr: set pty nonblocking failed");
        return Err(());
    }
    if let Err(error) = set_winsize(slave.as_raw_fd(), DEFAULT_PTY_COLS, DEFAULT_PTY_ROWS) {
        warn!(%job_id, err = %error, "process_mgr: set initial pty size failed");
    }
    let reader_file = master_file.try_clone().map_err(|error| {
        error!(%job_id, err = %error, "process_mgr: clone pty reader failed");
    })?;
    let input_file = master_file.try_clone().map_err(|error| {
        error!(%job_id, err = %error, "process_mgr: clone pty input failed");
    })?;
    let resize_file = Arc::new(master_file.try_clone().map_err(|error| {
        error!(%job_id, err = %error, "process_mgr: clone pty resize failed");
    })?);

    let slave_fd = slave.as_raw_fd();
    let master_fd = master_file.as_raw_fd();
    // SAFETY: the child process is single-threaded after fork here; the closure
    // only performs async-signal-safe libc calls on valid inherited fds.
    unsafe {
        cmd.pre_exec(move || {
            if libc::setsid() == -1 {
                return Err(std::io::Error::last_os_error());
            }
            #[cfg(target_os = "macos")]
            let tiocsctty = libc::TIOCSCTTY.into();
            #[cfg(not(target_os = "macos"))]
            let tiocsctty = libc::TIOCSCTTY;
            if libc::ioctl(slave_fd, tiocsctty, 0) == -1 {
                return Err(std::io::Error::last_os_error());
            }
            for target in [libc::STDIN_FILENO, libc::STDOUT_FILENO, libc::STDERR_FILENO] {
                if libc::dup2(slave_fd, target) == -1 {
                    return Err(std::io::Error::last_os_error());
                }
            }
            if slave_fd > libc::STDERR_FILENO {
                libc::close(slave_fd);
            }
            if master_fd > libc::STDERR_FILENO {
                libc::close(master_fd);
            }
            Ok(())
        });
    }
    cmd.stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());

    let mut child = cmd.spawn().map_err(|error| {
        log_spawn_failure(
            job_id,
            &program,
            &args,
            snapshot,
            options.cwd_override.as_deref(),
            &error,
        )
    })?;
    drop(slave);
    drop(master_file);

    info!(%job_id, pid = ?child.id(), "process_mgr: child spawned");

    let log_file = open_output_log(job_id).await;
    let input = match AsyncFd::new(input_file) {
        Ok(file) => file,
        Err(error) => {
            error!(%job_id, err = %error, "process_mgr: async pty input failed");
            request_child_kill(job_id, &mut child, "async pty input setup failed");
            wait_for_child(job_id, &mut child, "after async pty input setup failure").await;
            return Err(());
        }
    };
    let reader = match AsyncFd::new(reader_file) {
        Ok(file) => file,
        Err(error) => {
            error!(%job_id, err = %error, "process_mgr: async pty reader failed");
            request_child_kill(job_id, &mut child, "async pty reader setup failed");
            wait_for_child(job_id, &mut child, "after async pty reader setup failure").await;
            return Err(());
        }
    };

    let (kill_tx, kill_rx) = mpsc::channel::<()>(1);
    let ring_buffer = Arc::new(Mutex::new(RingBuffer::default()));
    let foreground = Arc::new(Mutex::new(ForegroundState::default()));
    let direct_output_client = options.direct_output_client;
    let reader_handle = tokio::spawn(reader_task(PtyReaderTask {
        job_id,
        child,
        sandbox,
        reader,
        log_file,
        kill_rx,
        ring: ring_buffer.clone(),
        runtime: ProcessTaskRuntime {
            sys: sys.clone(),
            foreground: foreground.clone(),
            direct_output_client,
            session_id: options.session_id.clone(),
            cleanup_tx: cleanup_tx.clone(),
        },
    }));

    Ok(ProcessEntry {
        job_id,
        session_id: options.session_id.clone(),
        status: JobStatus::Running,
        reader_handle,
        kill_tx,
        ring_buffer,
        stderr_ring: None,
        input: Some(JobInputWriter::spawn(
            job_id,
            JobInputSink::Pty(input),
            sys.process_mgr.clone(),
            input_failure_tx,
        )),
        resize: Some(resize_file),
        foreground,
    })
}

fn wrap_segment_if_enabled(
    sys: &ActorSystem,
    wrapper_enabled: bool,
    segment: &ExpandedSegment,
) -> (String, Vec<String>) {
    let program = segment.program.clone();
    let args = segment.args.clone();
    if !wrapper_enabled {
        return (program, args);
    }

    let wrapper = &sys.config.wrapper;
    let is_foreground = command_prefers_foreground(&segment.command_line);
    if !wrapper.should_wrap(&program, is_foreground, Some(true)) {
        return (program, args);
    }

    let mut wrapped_args = Vec::with_capacity(1 + args.len());
    wrapped_args.push(program);
    wrapped_args.extend(args);
    (wrapper.binary.clone(), wrapped_args)
}

async fn spawn_native_pipeline_job(
    job_id: JobId,
    pipeline: &cue_core::pipeline::Pipeline,
    snapshot: &EnvSnapshot,
    options: &ProcessJobOptions,
    sys: ActorSystem,
    cleanup_tx: mpsc::Sender<JobId>,
    input_failure_tx: mpsc::UnboundedSender<InputWriterFailure>,
) -> Result<ProcessEntry, ()> {
    let segments = expand_pipeline_segments(job_id, pipeline, snapshot)?;
    let sandbox = prepare_job_sandbox_or_emit(job_id, snapshot, options, &sys).await?;
    let NativePipelineSpawn {
        children,
        input,
        stdout_sources,
        stderr_sources,
    } = spawn_native_pipeline(
        job_id,
        &segments,
        snapshot,
        NativePipelineOptions {
            cwd_override: options.cwd_override.as_deref(),
            sandbox: sandbox.as_ref(),
            wrapper_enabled: options.wrapper_enabled,
            capture_stdin: options.pty_enabled,
            sys: &sys,
        },
    )?;

    let pids: Vec<u32> = children
        .iter()
        .filter_map(tokio::process::Child::id)
        .collect();
    info!(%job_id, ?pids, "process_mgr: native pipeline spawned");

    let log_file = open_output_log(job_id).await;
    let stderr_log = open_stderr_log(job_id).await;
    let (kill_tx, kill_rx) = mpsc::channel::<()>(1);
    let ring_buffer = Arc::new(Mutex::new(RingBuffer::default()));
    let stderr_ring = Arc::new(Mutex::new(RingBuffer::default()));
    let foreground = Arc::new(Mutex::new(ForegroundState::default()));
    let direct_output_client = options.direct_output_client;
    let reader_handle = tokio::spawn(pipeline_reader_task(PipelineReaderTask {
        job_id,
        children,
        sandbox,
        stdout_sources,
        stderr_sources,
        log_file,
        stderr_log,
        kill_rx,
        ring: ring_buffer.clone(),
        stderr_ring: stderr_ring.clone(),
        runtime: ProcessTaskRuntime {
            sys: sys.clone(),
            foreground: foreground.clone(),
            direct_output_client,
            session_id: options.session_id.clone(),
            cleanup_tx: cleanup_tx.clone(),
        },
    }));
    let input = input.map(|input| {
        JobInputWriter::spawn(job_id, input, sys.process_mgr.clone(), input_failure_tx)
    });

    Ok(ProcessEntry {
        job_id,
        session_id: options.session_id.clone(),
        status: JobStatus::Running,
        reader_handle,
        kill_tx,
        ring_buffer,
        stderr_ring: Some(stderr_ring),
        input,
        resize: None,
        foreground,
    })
}

async fn spawn_logical_job(
    job_id: JobId,
    plan: JobPlan,
    snapshot: EnvSnapshot,
    options: &ProcessJobOptions,
    sys: ActorSystem,
    cleanup_tx: mpsc::Sender<JobId>,
) -> Result<ProcessEntry, ()> {
    let sandbox = prepare_job_sandbox_or_emit(job_id, &snapshot, options, &sys).await?;
    let log_file = open_output_log(job_id).await;
    let stderr_log = open_stderr_log(job_id).await;
    let (kill_tx, kill_rx) = mpsc::channel::<()>(1);
    let ring_buffer = Arc::new(Mutex::new(RingBuffer::default()));
    let stderr_ring = Arc::new(Mutex::new(RingBuffer::default()));
    let foreground = Arc::new(Mutex::new(ForegroundState::default()));
    let direct_output_client = options.direct_output_client;
    let reader_handle = tokio::spawn(logical_job_task(LogicalJobTask {
        job_id,
        plan,
        snapshot,
        cwd_override: options.cwd_override.clone(),
        sandbox,
        log_file,
        stderr_log,
        kill_rx,
        wrapper_enabled: options.wrapper_enabled,
        capture_stdin: options.pty_enabled,
        ring: ring_buffer.clone(),
        stderr_ring: stderr_ring.clone(),
        runtime: ProcessTaskRuntime {
            sys: sys.clone(),
            foreground: foreground.clone(),
            direct_output_client,
            session_id: options.session_id.clone(),
            cleanup_tx: cleanup_tx.clone(),
        },
    }));

    Ok(ProcessEntry {
        job_id,
        session_id: options.session_id.clone(),
        status: JobStatus::Running,
        reader_handle,
        kill_tx,
        ring_buffer,
        stderr_ring: Some(stderr_ring),
        input: None,
        resize: None,
        foreground,
    })
}

fn spawn_native_pipeline(
    job_id: JobId,
    segments: &[ExpandedSegment],
    snapshot: &EnvSnapshot,
    options: NativePipelineOptions<'_>,
) -> Result<NativePipelineSpawn, ()> {
    let mut children = Vec::with_capacity(segments.len());
    let mut stdout_sources = Vec::new();
    let mut stderr_sources = Vec::new();
    let mut input = None;
    let mut next_stdin = None;

    for (idx, segment) in segments.iter().enumerate() {
        let (program, args) =
            wrap_segment_if_enabled(options.sys, options.wrapper_enabled, segment);
        let mut cmd = tokio::process::Command::new(&program);
        if !args.is_empty() {
            cmd.args(&args);
        }
        configure_command(&mut cmd, snapshot, options.cwd_override, options.sandbox);

        if idx == 0 {
            if options.capture_stdin {
                cmd.stdin(Stdio::piped());
            } else {
                cmd.stdin(Stdio::null());
            }
        } else if let Some(stdin) = next_stdin.take() {
            cmd.stdin(Stdio::from(stdin));
        } else {
            error!(%job_id, segment = idx, "process_mgr: missing pipeline stdin");
            return Err(());
        }

        match segment.pipe_to_next {
            Some(cue_core::pipeline::PipeOp::Stdout) => {
                let (read_end, write_end) = create_pipe().map_err(|error| {
                    error!(%job_id, segment = idx, err = %error, "process_mgr: create stdout pipe failed");
                })?;
                cmd.stdout(Stdio::from(write_end));
                cmd.stderr(Stdio::piped());
                next_stdin = Some(read_end);
            }
            Some(cue_core::pipeline::PipeOp::StdoutStderr) => {
                let (read_end, write_end) = create_pipe().map_err(|error| {
                    error!(%job_id, segment = idx, err = %error, "process_mgr: create stdout+stderr pipe failed");
                })?;
                let stderr_write = write_end.try_clone().map_err(|error| {
                    error!(%job_id, segment = idx, err = %error, "process_mgr: clone combined pipe failed");
                })?;
                cmd.stdout(Stdio::from(write_end));
                cmd.stderr(Stdio::from(stderr_write));
                next_stdin = Some(read_end);
            }
            Some(cue_core::pipeline::PipeOp::StderrOnly) => {
                let (read_end, write_end) = create_pipe().map_err(|error| {
                    error!(%job_id, segment = idx, err = %error, "process_mgr: create stderr-only pipe failed");
                })?;
                cmd.stdout(Stdio::piped());
                cmd.stderr(Stdio::from(write_end));
                next_stdin = Some(read_end);
            }
            None => {
                cmd.stdout(Stdio::piped());
                cmd.stderr(Stdio::piped());
            }
        }

        let mut child = cmd.spawn().map_err(|error| {
            log_spawn_failure(
                job_id,
                &program,
                &args,
                snapshot,
                options.cwd_override,
                &error,
            );
        })?;
        if idx == 0 && options.capture_stdin {
            input = child.stdin.take().map(JobInputSink::Pipe);
        }
        if let Some(stdout) = child.stdout.take() {
            stdout_sources.push(stdout);
        }
        if let Some(stderr) = child.stderr.take() {
            stderr_sources.push(stderr);
        }
        children.push(child);
    }

    Ok(NativePipelineSpawn {
        children,
        input,
        stdout_sources,
        stderr_sources,
    })
}

fn create_pipe() -> std::io::Result<(std::fs::File, std::fs::File)> {
    let mut fds = [0; 2];
    // SAFETY: `pipe` initializes two owned fds on success.
    let rc = unsafe { libc::pipe(fds.as_mut_ptr()) };
    if rc == -1 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: the returned fds are fresh and uniquely owned here.
    Ok(unsafe {
        (
            std::fs::File::from_raw_fd(fds[0]),
            std::fs::File::from_raw_fd(fds[1]),
        )
    })
}

/// Open (or create) the append-only log file for a job's output.
///
/// Runs on the blocking thread pool so filesystem syscalls do not stall the
/// Tokio runtime thread.
async fn open_output_log(job_id: JobId) -> Option<std::fs::File> {
    match tokio::task::spawn_blocking(move || {
        let dir = match crate::dirs::output_dir() {
            Ok(dir) => dir,
            Err(error) => {
                error!(%job_id, err = %error, "process_mgr: cannot resolve output dir");
                return None;
            }
        };
        if let Err(e) = crate::dirs::ensure_private_dir(&dir) {
            error!(%job_id, err = %e, "process_mgr: cannot create output dir");
            return None;
        }
        let path = dir.join(format!("{job_id}.log"));
        match crate::dirs::open_private_append(&path) {
            Ok(f) => Some(f),
            Err(e) => {
                error!(%job_id, path = %path.display(), err = %e, "process_mgr: open log file");
                None
            }
        }
    })
    .await
    {
        Ok(file) => file,
        Err(error) => {
            error!(%job_id, err = %error, "process_mgr: output log task failed");
            None
        }
    }
}

async fn open_stderr_log(job_id: JobId) -> Option<std::fs::File> {
    match tokio::task::spawn_blocking(move || {
        let dir = match crate::dirs::output_dir() {
            Ok(dir) => dir,
            Err(error) => {
                error!(%job_id, err = %error, "process_mgr: cannot resolve output dir");
                return None;
            }
        };
        if let Err(e) = crate::dirs::ensure_private_dir(&dir) {
            error!(%job_id, err = %e, "process_mgr: cannot create output dir");
            return None;
        }
        let path = dir.join(format!("{job_id}.stderr"));
        match crate::dirs::open_private_append(&path) {
            Ok(f) => Some(f),
            Err(e) => {
                error!(%job_id, path = %path.display(), err = %e, "process_mgr: open stderr log");
                None
            }
        }
    })
    .await
    {
        Ok(file) => file,
        Err(error) => {
            error!(%job_id, err = %error, "process_mgr: stderr log task failed");
            None
        }
    }
}

async fn clear_job_logs(job_id: JobId) {
    if let Err(error) = tokio::task::spawn_blocking(move || {
        let dir = match crate::dirs::output_dir() {
            Ok(dir) => dir,
            Err(error) => {
                warn!(%job_id, err = %error, "process_mgr: cannot resolve output dir for cleanup");
                return;
            }
        };
        for suffix in [".log", ".stderr"] {
            let path = dir.join(format!("{job_id}{suffix}"));
            if let Err(error) = std::fs::remove_file(&path)
                && error.kind() != std::io::ErrorKind::NotFound
            {
                warn!(
                    %job_id,
                    path = %path.display(),
                    err = %error,
                    "process_mgr: failed to remove stale output log"
                );
            }
        }
    })
    .await
    {
        warn!(%job_id, err = %error, "process_mgr: stale output log cleanup task failed");
    }
}

/// Background task that reads PTY output, populates the ring buffer,
/// writes to the log file, emits events, and waits for the child to exit.
async fn reader_task(task: PtyReaderTask) {
    let PtyReaderTask {
        job_id,
        mut child,
        sandbox,
        reader,
        log_file,
        mut kill_rx,
        ring,
        runtime,
    } = task;

    // Wrap the log file so it can be shared with `spawn_blocking`.
    let _sandbox = sandbox;
    let log_file = Arc::new(Mutex::new(log_file));
    let mut pty_buf = vec![0u8; 8192];
    let mut pty_done = false;

    loop {
        tokio::select! {
            // Kill signal from the main actor loop.
            _ = kill_rx.recv() => {
                info!(%job_id, "process_mgr: sending SIGTERM");
                request_child_kill(job_id, &mut child, "kill requested");

                // Wait up to 10 s for graceful exit, then SIGKILL (kill_on_drop).
                let timeout = tokio::time::sleep(std::time::Duration::from_secs(10));
                tokio::select! {
                    status = child.wait() => {
                        let code = match status {
                            Ok(status) => exit_code_from_status(status, EXIT_CODE_UNAVAILABLE),
                            Err(error) => {
                                error!(%job_id, err = %error, "process_mgr: wait after kill failed");
                                EXIT_CODE_UNAVAILABLE
                            }
                        };
                        debug!(%job_id, code, "process_mgr: child exited after SIGTERM");
                    }
                    () = timeout => {
                        warn!(%job_id, "process_mgr: child did not exit in 10 s — dropping (SIGKILL)");
                        // child is dropped here → kill_on_drop sends SIGKILL
                        drop(child);
                    }
                }

                emit_state_change(
                    &runtime.sys,
                    job_id,
                    JobStatus::Running,
                    JobStatus::Killed,
                    runtime.session_id.as_deref(),
                )
                .await;
                emit_fg_exit(
                    &runtime.sys,
                    &runtime.foreground,
                    job_id,
                    "killed",
                    runtime.session_id.as_deref(),
                )
                .await;
                emit_job_finished(&runtime.sys, job_id, EXIT_CODE_UNAVAILABLE).await;
                // Tell the main loop to remove our entry.
                notify_cleanup(&runtime.cleanup_tx, job_id).await;
                return;
            }

            result = read_pty(&reader, &mut pty_buf), if !pty_done => {
                match result {
                    Ok(0) => { pty_done = true; }
                    Ok(n) => {
                        let chunk = &pty_buf[..n];
                        let foreground_recipients =
                            record_pty_output(&ring, &runtime.foreground, chunk);
                        write_log(job_id, LogStream::Stdout, &log_file, chunk).await;
                        emit_output(
                            &runtime.sys,
                            job_id,
                            OutputStream::Stdout,
                            chunk,
                            runtime.direct_output_client,
                            runtime.session_id.as_deref(),
                        )
                        .await;
                        emit_fg_output(
                            &runtime.sys,
                            foreground_recipients,
                            job_id,
                            chunk,
                            runtime.session_id.as_deref(),
                        )
                        .await;
                    }
                    Err(e) => {
                        if e.raw_os_error() == Some(libc::EIO) {
                            pty_done = true;
                        } else {
                            debug!(%job_id, err = %e, "process_mgr: pty read error");
                            pty_done = true;
                        }
                    }
                }
            }
        }

        if pty_done {
            break;
        }
    }

    // Wait for exit status while still honoring late kill requests.
    let (exit_code, was_killed) = tokio::select! {
        status = child.wait() => {
            let code = match status {
                Ok(status) => exit_code_from_status(status, EXIT_CODE_UNAVAILABLE),
                Err(e) => {
                    error!(%job_id, err = %e, "process_mgr: wait failed");
                    EXIT_CODE_UNAVAILABLE
                }
            };
            (code, false)
        }
        _ = kill_rx.recv() => {
            request_child_kill(job_id, &mut child, "late kill requested");
            let code = wait_for_child(job_id, &mut child, "after late kill").await;
            (code, true)
        }
    };

    let ring_len = ring.lock().unwrap().len();
    info!(%job_id, exit_code, bytes = ring_len, "process_mgr: child exited");

    emit_output_eof(
        &runtime.sys,
        job_id,
        runtime.direct_output_client,
        runtime.session_id.as_deref(),
    )
    .await;

    if was_killed {
        emit_state_change(
            &runtime.sys,
            job_id,
            JobStatus::Running,
            JobStatus::Killed,
            runtime.session_id.as_deref(),
        )
        .await;
        emit_fg_exit(
            &runtime.sys,
            &runtime.foreground,
            job_id,
            "killed",
            runtime.session_id.as_deref(),
        )
        .await;
        emit_job_finished(&runtime.sys, job_id, EXIT_CODE_UNAVAILABLE).await;
    } else {
        // Determine final state.
        let new_state = if exit_code == 0 {
            JobStatus::Done
        } else {
            JobStatus::Failed
        };

        emit_state_change(
            &runtime.sys,
            job_id,
            JobStatus::Running,
            new_state,
            runtime.session_id.as_deref(),
        )
        .await;
        let reason = if exit_code == 0 {
            "done".to_string()
        } else {
            format!("exit {exit_code}")
        };
        emit_fg_exit(
            &runtime.sys,
            &runtime.foreground,
            job_id,
            &reason,
            runtime.session_id.as_deref(),
        )
        .await;

        emit_job_finished(&runtime.sys, job_id, exit_code).await;
    }

    // Tell the main loop to remove our entry.
    notify_cleanup(&runtime.cleanup_tx, job_id).await;
}

async fn pipeline_reader_task(task: PipelineReaderTask) {
    let PipelineReaderTask {
        job_id,
        mut children,
        sandbox,
        stdout_sources,
        stderr_sources,
        log_file,
        stderr_log,
        mut kill_rx,
        ring,
        stderr_ring,
        runtime,
    } = task;

    let _sandbox = sandbox;
    let log_file = Arc::new(Mutex::new(log_file));
    let stderr_log = Arc::new(Mutex::new(stderr_log));
    let (chunk_tx, mut chunk_rx) = mpsc::channel(PIPELINE_CHUNK_CAP);
    let mut active_readers = 0usize;

    for stdout in stdout_sources {
        active_readers += 1;
        spawn_pipeline_stream_reader(job_id, stdout, PipelineStreamKind::Stdout, chunk_tx.clone());
    }
    for stderr in stderr_sources {
        active_readers += 1;
        spawn_pipeline_stream_reader(job_id, stderr, PipelineStreamKind::Stderr, chunk_tx.clone());
    }
    drop(chunk_tx);

    let mut was_killed = false;
    while active_readers > 0 {
        tokio::select! {
            _ = kill_rx.recv(), if !was_killed => {
                was_killed = true;
                info!(%job_id, "process_mgr: killing native pipeline");
                terminate_children(job_id, &mut children).await;
            }
            Some(msg) = chunk_rx.recv() => {
                match msg {
                    PipelineReaderMsg::Chunk { kind: PipelineStreamKind::Stdout, data } => {
                        ring.lock().unwrap().push(&data);
                        write_log(job_id, LogStream::Stdout, &log_file, &data).await;
                        emit_output(
                            &runtime.sys,
                            job_id,
                            OutputStream::Stdout,
                            &data,
                            runtime.direct_output_client,
                            runtime.session_id.as_deref(),
                        )
                        .await;
                    }
                    PipelineReaderMsg::Chunk { kind: PipelineStreamKind::Stderr, data } => {
                        stderr_ring.lock().unwrap().push(&data);
                        write_log(job_id, LogStream::Stderr, &stderr_log, &data).await;
                        emit_output(
                            &runtime.sys,
                            job_id,
                            OutputStream::Stderr,
                            &data,
                            runtime.direct_output_client,
                            runtime.session_id.as_deref(),
                        )
                        .await;
                    }
                    PipelineReaderMsg::Closed => {
                        active_readers = active_readers.saturating_sub(1);
                    }
                }
            }
            else => break,
        }
    }

    let exit_code = if was_killed {
        wait_for_children(&mut children).await
    } else {
        tokio::select! {
            _ = kill_rx.recv() => {
                was_killed = true;
                terminate_children(job_id, &mut children).await;
                wait_for_children(&mut children).await
            }
            code = wait_for_children(&mut children) => code,
        }
    };

    let stdout_len = ring.lock().unwrap().len();
    let stderr_len = stderr_ring.lock().unwrap().len();
    info!(%job_id, exit_code, stdout_bytes = stdout_len, stderr_bytes = stderr_len, "process_mgr: native pipeline exited");

    emit_output_eof(
        &runtime.sys,
        job_id,
        runtime.direct_output_client,
        runtime.session_id.as_deref(),
    )
    .await;

    if was_killed {
        emit_state_change(
            &runtime.sys,
            job_id,
            JobStatus::Running,
            JobStatus::Killed,
            runtime.session_id.as_deref(),
        )
        .await;
        emit_fg_exit(
            &runtime.sys,
            &runtime.foreground,
            job_id,
            "killed",
            runtime.session_id.as_deref(),
        )
        .await;
        emit_job_finished(&runtime.sys, job_id, EXIT_CODE_UNAVAILABLE).await;
    } else {
        let new_state = if exit_code == 0 {
            JobStatus::Done
        } else {
            JobStatus::Failed
        };
        emit_state_change(
            &runtime.sys,
            job_id,
            JobStatus::Running,
            new_state,
            runtime.session_id.as_deref(),
        )
        .await;
        let reason = if exit_code == 0 {
            "done".to_string()
        } else {
            format!("exit {exit_code}")
        };
        emit_fg_exit(
            &runtime.sys,
            &runtime.foreground,
            job_id,
            &reason,
            runtime.session_id.as_deref(),
        )
        .await;
        emit_job_finished(&runtime.sys, job_id, exit_code).await;
    }

    notify_cleanup(&runtime.cleanup_tx, job_id).await;
}

async fn logical_job_task(task: LogicalJobTask) {
    let LogicalJobTask {
        job_id,
        plan,
        snapshot,
        cwd_override,
        sandbox,
        log_file,
        stderr_log,
        mut kill_rx,
        wrapper_enabled,
        capture_stdin,
        ring,
        stderr_ring,
        runtime,
    } = task;

    let log_file = Arc::new(Mutex::new(log_file));
    let stderr_log = Arc::new(Mutex::new(stderr_log));
    let mut was_killed = false;
    let mut local_snapshot = snapshot;
    if let Some(cwd) = cwd_override.as_ref() {
        local_snapshot.cwd = cwd.clone();
    }
    let mut streaming = StreamingContext {
        job_id,
        snapshot: &mut local_snapshot,
        sandbox: sandbox.as_ref(),
        kill_rx: &mut kill_rx,
        was_killed: &mut was_killed,
        options: StreamingOptions {
            wrapper_enabled,
            capture_stdin,
        },
        sys: &runtime.sys,
        ring: &ring,
        stderr_ring: &stderr_ring,
        log_file: &log_file,
        stderr_log: &stderr_log,
        direct_output_client: runtime.direct_output_client,
        session_id: runtime.session_id.as_deref(),
    };
    let exit_code = run_job_plan_streaming(&plan, &mut streaming).await;

    emit_output_eof(
        &runtime.sys,
        job_id,
        runtime.direct_output_client,
        runtime.session_id.as_deref(),
    )
    .await;

    if was_killed {
        emit_state_change(
            &runtime.sys,
            job_id,
            JobStatus::Running,
            JobStatus::Killed,
            runtime.session_id.as_deref(),
        )
        .await;
        emit_fg_exit(
            &runtime.sys,
            &runtime.foreground,
            job_id,
            "killed",
            runtime.session_id.as_deref(),
        )
        .await;
        emit_job_finished(&runtime.sys, job_id, EXIT_CODE_UNAVAILABLE).await;
    } else {
        let new_state = if exit_code == 0 {
            JobStatus::Done
        } else {
            JobStatus::Failed
        };
        emit_state_change(
            &runtime.sys,
            job_id,
            JobStatus::Running,
            new_state,
            runtime.session_id.as_deref(),
        )
        .await;
        let reason = if exit_code == 0 {
            "done".to_string()
        } else {
            format!("exit {exit_code}")
        };
        emit_fg_exit(
            &runtime.sys,
            &runtime.foreground,
            job_id,
            &reason,
            runtime.session_id.as_deref(),
        )
        .await;
        emit_job_finished(&runtime.sys, job_id, exit_code).await;
    }

    notify_cleanup(&runtime.cleanup_tx, job_id).await;
}

async fn run_job_plan_streaming(plan: &JobPlan, context: &mut StreamingContext<'_>) -> i32 {
    if *context.was_killed {
        return EXIT_CODE_UNAVAILABLE;
    }
    match plan {
        JobPlan::Pipeline(pipeline) => run_pipeline_streaming(pipeline, context).await,
        JobPlan::And { left, right } => {
            let code = Box::pin(run_job_plan_streaming(left, context)).await;
            if code == 0 && !*context.was_killed {
                Box::pin(run_job_plan_streaming(right, context)).await
            } else {
                code
            }
        }
        JobPlan::Or { left, right } => {
            let code = Box::pin(run_job_plan_streaming(left, context)).await;
            if code != 0 && !*context.was_killed {
                Box::pin(run_job_plan_streaming(right, context)).await
            } else {
                code
            }
        }
    }
}

async fn run_pipeline_streaming(
    pipeline: &cue_core::pipeline::Pipeline,
    context: &mut StreamingContext<'_>,
) -> i32 {
    if let Some(code) = run_job_local_builtin(
        context.job_id,
        pipeline,
        context.snapshot,
        context.stderr_ring,
        context.stderr_log,
    )
    .await
    {
        return code;
    }

    let segments = match expand_pipeline_segments(context.job_id, pipeline, context.snapshot) {
        Ok(segments) => segments,
        Err(()) => return EXIT_CODE_UNAVAILABLE,
    };
    let mut spawn = match spawn_native_pipeline(
        context.job_id,
        &segments,
        context.snapshot,
        NativePipelineOptions {
            cwd_override: None,
            sandbox: context.sandbox,
            wrapper_enabled: context.options.wrapper_enabled,
            capture_stdin: context.options.capture_stdin,
            sys: context.sys,
        },
    ) {
        Ok(spawn) => spawn,
        Err(()) => return EXIT_CODE_UNAVAILABLE,
    };

    let (chunk_tx, mut chunk_rx) = mpsc::channel(PIPELINE_CHUNK_CAP);
    let mut active_readers = 0usize;

    for stdout in spawn.stdout_sources.drain(..) {
        active_readers += 1;
        spawn_pipeline_stream_reader(
            context.job_id,
            stdout,
            PipelineStreamKind::Stdout,
            chunk_tx.clone(),
        );
    }
    for stderr in spawn.stderr_sources.drain(..) {
        active_readers += 1;
        spawn_pipeline_stream_reader(
            context.job_id,
            stderr,
            PipelineStreamKind::Stderr,
            chunk_tx.clone(),
        );
    }
    drop(chunk_tx);

    while active_readers > 0 {
        tokio::select! {
            _ = context.kill_rx.recv(), if !*context.was_killed => {
                *context.was_killed = true;
                terminate_children(context.job_id, &mut spawn.children).await;
            }
            Some(msg) = chunk_rx.recv() => {
                match msg {
                    PipelineReaderMsg::Chunk { kind: PipelineStreamKind::Stdout, data } => {
                        context.ring.lock().unwrap().push(&data);
                        write_log(context.job_id, LogStream::Stdout, context.log_file, &data).await;
                        emit_output(
                            context.sys,
                            context.job_id,
                            OutputStream::Stdout,
                            &data,
                            context.direct_output_client,
                            context.session_id,
                        )
                        .await;
                    }
                    PipelineReaderMsg::Chunk { kind: PipelineStreamKind::Stderr, data } => {
                        context.stderr_ring.lock().unwrap().push(&data);
                        write_log(context.job_id, LogStream::Stderr, context.stderr_log, &data).await;
                        emit_output(
                            context.sys,
                            context.job_id,
                            OutputStream::Stderr,
                            &data,
                            context.direct_output_client,
                            context.session_id,
                        )
                        .await;
                    }
                    PipelineReaderMsg::Closed => {
                        active_readers = active_readers.saturating_sub(1);
                    }
                }
            }
            else => break,
        }
    }

    if *context.was_killed {
        wait_for_children(&mut spawn.children).await;
        EXIT_CODE_UNAVAILABLE
    } else {
        tokio::select! {
            _ = context.kill_rx.recv() => {
                *context.was_killed = true;
                terminate_children(context.job_id, &mut spawn.children).await;
                wait_for_children(&mut spawn.children).await;
                EXIT_CODE_UNAVAILABLE
            }
            code = wait_for_children(&mut spawn.children) => code,
        }
    }
}

async fn run_job_local_builtin(
    job_id: JobId,
    pipeline: &cue_core::pipeline::Pipeline,
    snapshot: &mut EnvSnapshot,
    stderr_ring: &Arc<Mutex<RingBuffer>>,
    stderr_log: &Arc<Mutex<Option<std::fs::File>>>,
) -> Option<i32> {
    if pipeline.segments.len() != 1 {
        return None;
    }
    let segment = &pipeline.segments[0];
    if segment.pipe_to_next.is_some() {
        return None;
    }

    let expanded = expand_command_line(&segment.command, Some(snapshot));
    match detect_job_local_builtin(&expanded)? {
        JobLocalBuiltin::Cd { path } => {
            if expanded.len() > 2 {
                write_job_local_stderr(
                    job_id,
                    stderr_ring,
                    stderr_log,
                    b"cd: too many arguments\n",
                )
                .await;
                return Some(1);
            }
            match resolve_job_local_cd_target(snapshot, &path) {
                Ok(cwd) => {
                    snapshot.cwd = cwd;
                    Some(0)
                }
                Err(message) => {
                    let line = format!("{message}\n");
                    write_job_local_stderr(job_id, stderr_ring, stderr_log, line.as_bytes()).await;
                    Some(1)
                }
            }
        }
        JobLocalBuiltin::EnvSet { assignments } => {
            if assignments.is_empty() {
                write_job_local_stderr(
                    job_id,
                    stderr_ring,
                    stderr_log,
                    b"env set: expected KEY=VALUE\n",
                )
                .await;
                return Some(1);
            }
            for assignment in assignments {
                let Some((key, value)) = assignment.split_once('=') else {
                    let line = format!("env set: expected KEY=VALUE, got `{assignment}`\n");
                    write_job_local_stderr(job_id, stderr_ring, stderr_log, line.as_bytes()).await;
                    return Some(1);
                };
                if key.is_empty() {
                    write_job_local_stderr(
                        job_id,
                        stderr_ring,
                        stderr_log,
                        b"env set: empty variable name\n",
                    )
                    .await;
                    return Some(1);
                }
                snapshot.env.insert(key.to_string(), value.to_string());
            }
            Some(0)
        }
    }
}

fn resolve_job_local_cd_target(
    snapshot: &EnvSnapshot,
    path: &str,
) -> Result<std::path::PathBuf, String> {
    let requested = std::path::PathBuf::from(path);
    let target = if requested.is_absolute() {
        requested
    } else {
        snapshot.cwd.join(requested)
    };
    let resolved = std::fs::canonicalize(&target)
        .map_err(|error| format!("cd: {}: {error}", target.display()))?;
    if !resolved.is_dir() {
        return Err(format!("cd: {}: not a directory", resolved.display()));
    }
    Ok(resolved)
}

async fn write_job_local_stderr(
    job_id: JobId,
    stderr_ring: &Arc<Mutex<RingBuffer>>,
    stderr_log: &Arc<Mutex<Option<std::fs::File>>>,
    data: &[u8],
) {
    stderr_ring.lock().unwrap().push(data);
    write_log(job_id, LogStream::Stderr, stderr_log, data).await;
    debug!(%job_id, bytes = data.len(), "process_mgr: job-local builtin stderr");
}

fn spawn_pipeline_stream_reader<R>(
    job_id: JobId,
    mut reader: R,
    kind: PipelineStreamKind,
    tx: mpsc::Sender<PipelineReaderMsg>,
) where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        let mut buf = vec![0u8; 8192];
        loop {
            match reader.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => {
                    if tx
                        .send(PipelineReaderMsg::Chunk {
                            kind,
                            data: buf[..n].to_vec(),
                        })
                        .await
                        .is_err()
                    {
                        debug!(
                            %job_id,
                            stream = ?kind,
                            "process_mgr: pipeline reader receiver closed"
                        );
                        return;
                    }
                }
                Err(error) => {
                    debug!(err = %error, "process_mgr: pipeline stream read error");
                    break;
                }
            }
        }
        if tx.send(PipelineReaderMsg::Closed).await.is_err() {
            debug!(
                %job_id,
                stream = ?kind,
                "process_mgr: pipeline reader receiver closed before EOF"
            );
        }
    });
}

async fn notify_cleanup(cleanup_tx: &mpsc::Sender<JobId>, job_id: JobId) {
    if cleanup_tx.send(job_id).await.is_err() {
        debug!(%job_id, "process_mgr: cleanup channel closed");
    }
}

fn request_child_kill(job_id: JobId, child: &mut tokio::process::Child, reason: &str) {
    if let Err(error) = child.start_kill() {
        warn!(
            %job_id,
            pid = ?child.id(),
            %reason,
            err = %error,
            "process_mgr: child kill request failed"
        );
    }
}

async fn wait_for_child(job_id: JobId, child: &mut tokio::process::Child, reason: &str) -> i32 {
    match child.wait().await {
        Ok(status) => exit_code_from_status(status, EXIT_CODE_UNAVAILABLE),
        Err(error) => {
            error!(
                %job_id,
                %reason,
                err = %error,
                "process_mgr: child wait failed"
            );
            EXIT_CODE_UNAVAILABLE
        }
    }
}

async fn terminate_children(job_id: JobId, children: &mut [tokio::process::Child]) {
    for child in children.iter_mut() {
        request_child_kill(job_id, child, "pipeline kill requested");
    }
}

async fn wait_for_children(children: &mut [tokio::process::Child]) -> i32 {
    let mut exit_code = EXIT_CODE_UNAVAILABLE;
    let last_idx = children.len().saturating_sub(1);
    for (idx, child) in children.iter_mut().enumerate() {
        match child.wait().await {
            Ok(status) => {
                if idx == last_idx {
                    exit_code = exit_code_from_status(status, EXIT_CODE_UNAVAILABLE);
                }
            }
            Err(error) => {
                error!(err = %error, "process_mgr: child wait failed");
                if idx == last_idx {
                    exit_code = EXIT_CODE_UNAVAILABLE;
                }
            }
        }
    }
    exit_code
}

async fn fail_pending_spawn(sys: &ActorSystem, job_id: JobId, session_id: Option<&str>) {
    emit_state_change(
        sys,
        job_id,
        JobStatus::Pending,
        JobStatus::Failed,
        session_id,
    )
    .await;
    emit_job_finished(sys, job_id, EXIT_CODE_UNAVAILABLE).await;
}

async fn emit_job_finished(sys: &ActorSystem, job_id: JobId, exit_code: i32) {
    if sys
        .scheduler
        .send(SchedulerMsg::JobFinished { job_id, exit_code })
        .await
        .is_err()
    {
        warn!(%job_id, exit_code, "process_mgr: scheduler channel closed while reporting job completion");
    }
}

/// Emit a `JobStateChanged` event.
async fn emit_state_change(
    sys: &ActorSystem,
    job_id: JobId,
    old_state: JobStatus,
    new_state: JobStatus,
    session_id: Option<&str>,
) {
    publish_actor_session_event(
        "process_mgr",
        &sys.event_bus,
        EventChannel::Jobs,
        EventPayload::JobStateChanged {
            job_id: job_id.to_string(),
            old_state,
            new_state,
            end_scope: None,
            chain_id: None,
            chain_index: None,
        },
        session_id.map(str::to_owned),
    )
    .await;
}

/// Emit an output event without losing non-UTF-8 bytes.
async fn emit_output(
    sys: &ActorSystem,
    job_id: JobId,
    stream: OutputStream,
    data: &[u8],
    direct_output_client: Option<u64>,
    session_id: Option<&str>,
) {
    let payload = match std::str::from_utf8(data) {
        Ok(text) => EventPayload::OutputChunk {
            id: job_id.to_string(),
            stream,
            data: text.to_string(),
        },
        Err(_) => EventPayload::OutputChunkBinary {
            id: job_id.to_string(),
            stream,
            base64: BASE64_STANDARD.encode(data),
        },
    };
    if let Some(client_id) = direct_output_client {
        send_actor_gateway_event(
            "process_mgr",
            sys,
            client_id,
            payload.clone(),
            session_id.map(str::to_owned),
        )
        .await;
    }
    publish_output_event(sys, job_id, payload, direct_output_client, session_id).await;
}

async fn emit_output_eof(
    sys: &ActorSystem,
    job_id: JobId,
    direct_output_client: Option<u64>,
    session_id: Option<&str>,
) {
    let payload = EventPayload::OutputEof {
        id: job_id.to_string(),
    };
    if let Some(client_id) = direct_output_client {
        send_actor_gateway_event(
            "process_mgr",
            sys,
            client_id,
            payload.clone(),
            session_id.map(str::to_owned),
        )
        .await;
    }
    publish_output_event(sys, job_id, payload, direct_output_client, session_id).await;
}

async fn publish_output_event(
    sys: &ActorSystem,
    job_id: JobId,
    payload: EventPayload,
    excluded_client_id: Option<u64>,
    session_id: Option<&str>,
) {
    if let Some(excluded_client_id) = excluded_client_id {
        publish_actor_session_event_except(
            "process_mgr",
            &sys.event_bus,
            EventChannel::Output(job_id),
            payload,
            session_id.map(str::to_owned),
            excluded_client_id,
        )
        .await;
    } else {
        publish_actor_session_event(
            "process_mgr",
            &sys.event_bus,
            EventChannel::Output(job_id),
            payload,
            session_id.map(str::to_owned),
        )
        .await;
    }
}

async fn emit_fg_output(
    sys: &ActorSystem,
    recipients: Vec<ForegroundRecipient>,
    job_id: JobId,
    data: &[u8],
    session_id: Option<&str>,
) {
    for recipient in recipients {
        send_actor_gateway_event(
            "process_mgr",
            sys,
            recipient.client_id,
            EventPayload::FgOutput {
                id: job_id.to_string(),
                attachment_id: recipient.attachment_id,
                data: data.to_vec(),
            },
            session_id.map(str::to_owned),
        )
        .await;
    }
}

async fn emit_fg_control_changed(
    sys: &ActorSystem,
    recipients: Vec<ForegroundRecipient>,
    job_id: JobId,
    control_available: bool,
    session_id: Option<&str>,
) {
    for recipient in recipients {
        send_actor_gateway_event(
            "process_mgr",
            sys,
            recipient.client_id,
            EventPayload::FgControlChanged {
                id: job_id.to_string(),
                attachment_id: recipient.attachment_id,
                control_available,
            },
            session_id.map(str::to_owned),
        )
        .await;
    }
}

async fn emit_fg_exit(
    sys: &ActorSystem,
    foreground: &Arc<Mutex<ForegroundState>>,
    job_id: JobId,
    reason: &str,
    session_id: Option<&str>,
) {
    let recipients = close_foreground_state(foreground);
    emit_fg_exit_recipients(sys, recipients, job_id, reason, session_id).await;
}

async fn emit_fg_exit_recipients(
    sys: &ActorSystem,
    recipients: Vec<ForegroundRecipient>,
    job_id: JobId,
    reason: &str,
    session_id: Option<&str>,
) {
    for recipient in recipients {
        send_actor_gateway_event(
            "process_mgr",
            sys,
            recipient.client_id,
            EventPayload::FgExited {
                id: job_id.to_string(),
                attachment_id: recipient.attachment_id,
                reason: reason.to_string(),
            },
            session_id.map(str::to_owned),
        )
        .await;
    }
}

/// Write a chunk to the log file.
///
/// Offloaded to the blocking thread pool so the async reader task never stalls
/// the Tokio runtime with synchronous I/O.
async fn write_log(
    job_id: JobId,
    stream: LogStream,
    file: &Arc<Mutex<Option<std::fs::File>>>,
    data: &[u8],
) {
    let file = file.clone();
    let data = data.to_vec();
    let result = tokio::task::spawn_blocking(move || -> std::io::Result<()> {
        let mut guard = file
            .lock()
            .map_err(|_| std::io::Error::other("process log file lock poisoned"))?;
        let Some(f) = guard.as_mut() else {
            return Ok(());
        };
        if let Err(error) = f.write_all(&data) {
            *guard = None;
            return Err(error);
        }
        Ok(())
    })
    .await;

    match result {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            warn!(
                %job_id,
                stream = stream.label(),
                err = %error,
                "process_mgr: failed to write output log"
            );
        }
        Err(error) => {
            error!(
                %job_id,
                stream = stream.label(),
                err = %error,
                "process_mgr: output log writer task failed"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::super::GatewayMsg;
    use super::*;

    static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn make_temp_dir() -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "cue-process-mgr-test-{}-{}",
            std::process::id(),
            TEMP_COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&dir).expect("create temp dir");
        dir
    }

    fn snapshot() -> EnvSnapshot {
        EnvSnapshot {
            env: BTreeMap::from([
                ("HOME".into(), "/tmp/cue-home".into()),
                ("USER".into(), "tester".into()),
            ]),
            cwd: PathBuf::from("/tmp/work"),
        }
    }

    fn process_options() -> ProcessJobOptions {
        ProcessJobOptions {
            cwd_override: None,
            sandbox: None,
            wrapper_enabled: false,
            pty_enabled: true,
            direct_output_client: None,
            session_id: None,
        }
    }

    #[test]
    fn effective_process_options_fall_back_to_submitted_options() {
        let mut options = process_options();
        options.pty_enabled = false;
        options.session_id = Some("SS-options".into());
        options.sandbox = Some(crate::sandbox::SandboxConfig {
            mode: crate::sandbox::SandboxMode::Overlay,
            upper: Some(crate::sandbox::SandboxUpper::Directory(PathBuf::from(
                "/tmp/cue-upper",
            ))),
        });

        let effective = effective_process_options(&options, &snapshot());

        assert!(!effective.pty_enabled);
        assert_eq!(effective.sandbox, options.sandbox);
        assert_eq!(effective.session_id, options.session_id);
    }

    #[test]
    fn configure_command_sets_pwd_to_effective_cwd() {
        let mut snapshot = snapshot();
        snapshot.env.insert("PWD".into(), "/stale".into());
        let cwd = std::env::temp_dir();
        let mut cmd = tokio::process::Command::new("pwd");

        configure_command(&mut cmd, &snapshot, Some(&cwd), None);

        assert_eq!(cmd.as_std().get_current_dir(), Some(cwd.as_path()));
        let pwd = cmd
            .as_std()
            .get_envs()
            .find_map(|(key, value)| (key == "PWD").then_some(value))
            .flatten();
        assert_eq!(pwd, Some(cwd.as_os_str()));
    }

    #[tokio::test]
    async fn sandbox_setup_failure_is_emitted_to_stderr_output() {
        let (gateway_tx, _gateway_rx) = mpsc::channel(super::super::ACTOR_CHANNEL_CAP);
        let (scheduler_tx, _scheduler_rx) = mpsc::channel(super::super::ACTOR_CHANNEL_CAP);
        let (process_tx, _process_rx) = mpsc::channel(super::super::ACTOR_CHANNEL_CAP);
        let (scope_tx, _scope_rx) = mpsc::channel(super::super::ACTOR_CHANNEL_CAP);
        let (event_tx, mut event_rx) = mpsc::channel(super::super::ACTOR_CHANNEL_CAP);
        let sys = ActorSystem {
            gateway: gateway_tx,
            scheduler: scheduler_tx,
            process_mgr: process_tx,
            scope_store: scope_tx,
            event_bus: event_tx,
            config: crate::config::Config::default(),
            resources: std::sync::Arc::new(crate::resource::ProviderRegistry::empty()),
        };
        let cwd = make_temp_dir();
        let mut snapshot = snapshot();
        snapshot.cwd = cwd.clone();
        let mut options = process_options();
        options.session_id = Some("SS-sandbox".into());
        options.sandbox = Some(crate::sandbox::SandboxConfig {
            mode: crate::sandbox::SandboxMode::Overlay,
            upper: Some(crate::sandbox::SandboxUpper::Directory(PathBuf::from(
                "/tmp/cue:bad-upper",
            ))),
        });

        let result = spawn_single_pipe_job(
            JobId(404),
            &cue_core::pipeline::Pipeline {
                segments: vec![cue_core::pipeline::PipeSegment {
                    command: vec!["echo".into(), "unreachable".into()],
                    pipe_to_next: None,
                }],
            },
            &snapshot,
            &options,
            sys,
            mpsc::channel(1).0,
        )
        .await;

        assert!(result.is_err());
        let event = tokio::time::timeout(std::time::Duration::from_secs(2), event_rx.recv())
            .await
            .expect("stderr event timeout")
            .expect("stderr event");
        match event {
            super::super::EventBusMsg::PublishSession {
                payload: EventPayload::OutputChunk { id, stream, data },
                session_id,
                ..
            } => {
                assert_eq!(session_id.as_deref(), Some("SS-sandbox"));
                assert_eq!(id, "J404");
                assert_eq!(stream, OutputStream::Stderr);
                assert!(data.contains("sandbox setup failed"));
                assert!(
                    data.contains("unsupported character")
                        || data.contains("only supported on Linux"),
                    "missing sandbox setup cause in stderr: {data}"
                );
            }
            _ => panic!("expected stderr output chunk"),
        }

        std::fs::remove_dir_all(cwd).expect("remove temp dir");
    }

    #[tokio::test]
    async fn emit_output_preserves_non_utf8_as_binary_event() {
        let (gateway_tx, _gateway_rx) = mpsc::channel(super::super::ACTOR_CHANNEL_CAP);
        let (scheduler_tx, _scheduler_rx) = mpsc::channel(super::super::ACTOR_CHANNEL_CAP);
        let (process_tx, _process_rx) = mpsc::channel(super::super::ACTOR_CHANNEL_CAP);
        let (scope_tx, _scope_rx) = mpsc::channel(super::super::ACTOR_CHANNEL_CAP);
        let (event_tx, mut event_rx) = mpsc::channel(super::super::ACTOR_CHANNEL_CAP);
        let sys = ActorSystem {
            gateway: gateway_tx,
            scheduler: scheduler_tx,
            process_mgr: process_tx,
            scope_store: scope_tx,
            event_bus: event_tx,
            config: crate::config::Config::default(),
            resources: std::sync::Arc::new(crate::resource::ProviderRegistry::empty()),
        };

        emit_output(
            &sys,
            JobId(7),
            OutputStream::Stdout,
            b"\xffbin\n",
            None,
            Some("SS-output"),
        )
        .await;

        match event_rx.recv().await.expect("output event") {
            super::super::EventBusMsg::PublishSession {
                channel,
                session_id,
                payload: EventPayload::OutputChunkBinary { id, stream, base64 },
            } => {
                assert_eq!(channel, EventChannel::Output(JobId(7)));
                assert_eq!(session_id.as_deref(), Some("SS-output"));
                assert_eq!(id, "J7");
                assert_eq!(stream, OutputStream::Stdout);
                assert_eq!(
                    BASE64_STANDARD.decode(base64.as_bytes()).unwrap(),
                    b"\xffbin\n"
                );
            }
            _ => panic!("expected binary output event"),
        }
    }

    #[tokio::test]
    async fn emit_state_change_preserves_named_session_owner() {
        let (gateway_tx, _gateway_rx) = mpsc::channel(super::super::ACTOR_CHANNEL_CAP);
        let (scheduler_tx, _scheduler_rx) = mpsc::channel(super::super::ACTOR_CHANNEL_CAP);
        let (process_tx, _process_rx) = mpsc::channel(super::super::ACTOR_CHANNEL_CAP);
        let (scope_tx, _scope_rx) = mpsc::channel(super::super::ACTOR_CHANNEL_CAP);
        let (event_tx, mut event_rx) = mpsc::channel(super::super::ACTOR_CHANNEL_CAP);
        let sys = ActorSystem {
            gateway: gateway_tx,
            scheduler: scheduler_tx,
            process_mgr: process_tx,
            scope_store: scope_tx,
            event_bus: event_tx,
            config: crate::config::Config::default(),
            resources: std::sync::Arc::new(crate::resource::ProviderRegistry::empty()),
        };

        emit_state_change(
            &sys,
            JobId(9),
            JobStatus::Pending,
            JobStatus::Running,
            Some("SS-owner"),
        )
        .await;

        match event_rx.recv().await.expect("state event") {
            super::super::EventBusMsg::PublishSession {
                channel,
                session_id,
                payload:
                    EventPayload::JobStateChanged {
                        job_id, new_state, ..
                    },
            } => {
                assert_eq!(channel, EventChannel::Jobs);
                assert_eq!(session_id.as_deref(), Some("SS-owner"));
                assert_eq!(job_id, "J9");
                assert_eq!(new_state, JobStatus::Running);
            }
            _ => panic!("expected session-scoped job state event"),
        }
    }

    #[tokio::test]
    async fn emit_output_sends_direct_client_copy_and_publishes_channel_event_for_others() {
        let (gateway_tx, mut gateway_rx) = mpsc::channel(super::super::ACTOR_CHANNEL_CAP);
        let (scheduler_tx, _scheduler_rx) = mpsc::channel(super::super::ACTOR_CHANNEL_CAP);
        let (process_tx, _process_rx) = mpsc::channel(super::super::ACTOR_CHANNEL_CAP);
        let (scope_tx, _scope_rx) = mpsc::channel(super::super::ACTOR_CHANNEL_CAP);
        let (event_tx, mut event_rx) = mpsc::channel(super::super::ACTOR_CHANNEL_CAP);
        let sys = ActorSystem {
            gateway: gateway_tx,
            scheduler: scheduler_tx,
            process_mgr: process_tx,
            scope_store: scope_tx,
            event_bus: event_tx,
            config: crate::config::Config::default(),
            resources: std::sync::Arc::new(crate::resource::ProviderRegistry::empty()),
        };

        emit_output(
            &sys,
            JobId(7),
            OutputStream::Stdout,
            b"script\n",
            Some(42),
            Some("SS-script"),
        )
        .await;

        match gateway_rx.recv().await.expect("direct output") {
            GatewayMsg::SendEvent {
                client_id,
                session_id,
                payload: EventPayload::OutputChunk { id, stream, data },
            } => {
                assert_eq!(client_id, 42);
                assert_eq!(session_id.as_deref(), Some("SS-script"));
                assert_eq!(id, "J7");
                assert_eq!(stream, OutputStream::Stdout);
                assert_eq!(data, "script\n");
            }
            _ => panic!("expected direct output chunk"),
        }

        match event_rx.recv().await.expect("published output") {
            super::super::EventBusMsg::PublishSessionExcept {
                channel,
                session_id,
                excluded_client_id,
                payload: EventPayload::OutputChunk { id, data, .. },
            } => {
                assert_eq!(channel, EventChannel::Output(JobId(7)));
                assert_eq!(session_id.as_deref(), Some("SS-script"));
                assert_eq!(excluded_client_id, 42);
                assert_eq!(id, "J7");
                assert_eq!(data, "script\n");
            }
            _ => panic!("expected output chunk published to other subscribers"),
        }
    }

    #[tokio::test]
    async fn emit_output_eof_sends_direct_client_copy_and_publishes_for_others() {
        let (gateway_tx, mut gateway_rx) = mpsc::channel(super::super::ACTOR_CHANNEL_CAP);
        let (scheduler_tx, _scheduler_rx) = mpsc::channel(super::super::ACTOR_CHANNEL_CAP);
        let (process_tx, _process_rx) = mpsc::channel(super::super::ACTOR_CHANNEL_CAP);
        let (scope_tx, _scope_rx) = mpsc::channel(super::super::ACTOR_CHANNEL_CAP);
        let (event_tx, mut event_rx) = mpsc::channel(super::super::ACTOR_CHANNEL_CAP);
        let sys = ActorSystem {
            gateway: gateway_tx,
            scheduler: scheduler_tx,
            process_mgr: process_tx,
            scope_store: scope_tx,
            event_bus: event_tx,
            config: crate::config::Config::default(),
            resources: std::sync::Arc::new(crate::resource::ProviderRegistry::empty()),
        };

        emit_output_eof(&sys, JobId(7), Some(42), Some("SS-script")).await;

        match gateway_rx.recv().await.expect("direct eof") {
            GatewayMsg::SendEvent {
                client_id,
                session_id,
                payload: EventPayload::OutputEof { id },
            } => {
                assert_eq!(client_id, 42);
                assert_eq!(session_id.as_deref(), Some("SS-script"));
                assert_eq!(id, "J7");
            }
            _ => panic!("expected direct output eof"),
        }

        match event_rx.recv().await.expect("published eof") {
            super::super::EventBusMsg::PublishSessionExcept {
                channel,
                session_id,
                excluded_client_id,
                payload: EventPayload::OutputEof { id },
            } => {
                assert_eq!(channel, EventChannel::Output(JobId(7)));
                assert_eq!(session_id.as_deref(), Some("SS-script"));
                assert_eq!(excluded_client_id, 42);
                assert_eq!(id, "J7");
            }
            _ => panic!("expected output eof published to other subscribers"),
        }
    }

    #[tokio::test]
    async fn foreground_events_reach_all_observers_and_exit_closes_state() {
        let (gateway_tx, mut gateway_rx) = mpsc::channel(super::super::ACTOR_CHANNEL_CAP);
        let (scheduler_tx, _scheduler_rx) = mpsc::channel(super::super::ACTOR_CHANNEL_CAP);
        let (process_tx, _process_rx) = mpsc::channel(super::super::ACTOR_CHANNEL_CAP);
        let (scope_tx, _scope_rx) = mpsc::channel(super::super::ACTOR_CHANNEL_CAP);
        let (event_tx, _event_rx) = mpsc::channel(super::super::ACTOR_CHANNEL_CAP);
        let sys = ActorSystem {
            gateway: gateway_tx,
            scheduler: scheduler_tx,
            process_mgr: process_tx,
            scope_store: scope_tx,
            event_bus: event_tx,
            config: crate::config::Config::default(),
            resources: std::sync::Arc::new(crate::resource::ProviderRegistry::empty()),
        };
        let foreground = Arc::new(Mutex::new(ForegroundState {
            observers: BTreeMap::from([(42, 7), (43, 9)]),
            controller: Some(42),
            controller_generation: Some(1),
            last_attachment_id: 9,
            closed: false,
        }));
        let ring = Arc::new(Mutex::new(RingBuffer::default()));

        let recipients = record_pty_output(&ring, &foreground, b"prompt");
        emit_fg_output(&sys, recipients, JobId(8), b"prompt", Some("SS-fg")).await;
        let recipients = foreground.lock().unwrap().recipients();
        emit_fg_control_changed(&sys, recipients, JobId(8), false, Some("SS-fg")).await;
        emit_fg_exit(&sys, &foreground, JobId(8), "done", Some("SS-fg")).await;

        for (expected_client, expected_attachment) in [(42, 7), (43, 9)] {
            match gateway_rx.recv().await.expect("foreground output") {
                GatewayMsg::SendEvent {
                    client_id,
                    session_id,
                    payload:
                        EventPayload::FgOutput {
                            id,
                            attachment_id,
                            data,
                        },
                } => {
                    assert_eq!(client_id, expected_client);
                    assert_eq!(session_id.as_deref(), Some("SS-fg"));
                    assert_eq!(id, "J8");
                    assert_eq!(attachment_id, expected_attachment);
                    assert_eq!(data, b"prompt");
                }
                _ => panic!("expected foreground output"),
            }
        }
        for (expected_client, expected_attachment) in [(42, 7), (43, 9)] {
            match gateway_rx.recv().await.expect("foreground control state") {
                GatewayMsg::SendEvent {
                    client_id,
                    session_id,
                    payload:
                        EventPayload::FgControlChanged {
                            id,
                            attachment_id,
                            control_available,
                        },
                } => {
                    assert_eq!(client_id, expected_client);
                    assert_eq!(session_id.as_deref(), Some("SS-fg"));
                    assert_eq!(id, "J8");
                    assert_eq!(attachment_id, expected_attachment);
                    assert!(!control_available);
                }
                _ => panic!("expected foreground control state"),
            }
        }
        for (expected_client, expected_attachment) in [(42, 7), (43, 9)] {
            match gateway_rx.recv().await.expect("foreground exit") {
                GatewayMsg::SendEvent {
                    client_id,
                    session_id,
                    payload:
                        EventPayload::FgExited {
                            id,
                            attachment_id,
                            reason,
                        },
                } => {
                    assert_eq!(client_id, expected_client);
                    assert_eq!(session_id.as_deref(), Some("SS-fg"));
                    assert_eq!(id, "J8");
                    assert_eq!(attachment_id, expected_attachment);
                    assert_eq!(reason, "done");
                }
                _ => panic!("expected foreground exit"),
            }
        }
        assert_eq!(ring.lock().unwrap().as_bytes(), b"prompt");
        let foreground = foreground.lock().unwrap();
        assert!(foreground.closed);
        assert!(foreground.observers.is_empty());
        assert_eq!(foreground.controller, None);
    }

    #[tokio::test]
    async fn foreground_registration_snapshot_and_controller_transitions_are_consistent() {
        let ring_buffer = Arc::new(Mutex::new(RingBuffer::default()));
        ring_buffer.lock().unwrap().push(b"before");
        let foreground = Arc::new(Mutex::new(ForegroundState::default()));
        let (kill_tx, _kill_rx) = mpsc::channel(1);
        let pty = crate::pty::open_pty().expect("test PTY");
        let _slave = pty.slave;
        let master = std::fs::File::from(pty.master);
        set_nonblocking(master.as_raw_fd()).expect("nonblocking test PTY");
        let input = AsyncFd::new(master).expect("async test PTY");
        let (process_mgr, _process_rx) = mpsc::channel(super::super::ACTOR_CHANNEL_CAP);
        let (input_failures, _input_failure_rx) = mpsc::unbounded_channel();
        let entry = ProcessEntry {
            job_id: JobId(9),
            session_id: Some("SS-shared".into()),
            status: JobStatus::Running,
            reader_handle: tokio::spawn(async {}),
            kill_tx,
            ring_buffer: ring_buffer.clone(),
            stderr_ring: None,
            input: Some(JobInputWriter::spawn(
                JobId(9),
                JobInputSink::Pty(input),
                process_mgr,
                input_failures,
            )),
            resize: Some(Arc::new(std::fs::File::open("/dev/null").unwrap())),
            foreground: foreground.clone(),
        };

        let (first, notice) =
            attach_foreground(&entry, 42, ForegroundRole::Observer).expect("first watch");
        assert_eq!(first.attachment_id, 1);
        assert_eq!(first.role, ForegroundRole::Observer);
        assert!(first.control_available);
        assert_eq!(first.snapshot, b"before");
        assert!(!first.snapshot_truncated);
        assert!(notice.is_none());
        assert!(
            attach_foreground(&entry, 42, ForegroundRole::Observer)
                .unwrap_err()
                .contains("already foreground-attached")
        );

        let recipients = record_pty_output(&ring_buffer, &foreground, b"-after");
        assert_eq!(
            recipients,
            vec![ForegroundRecipient {
                client_id: 42,
                attachment_id: 1,
            }]
        );

        let (second, notice) =
            attach_foreground(&entry, 43, ForegroundRole::Observer).expect("second watch");
        assert_eq!(second.attachment_id, 2);
        assert_eq!(second.snapshot, b"before-after");
        assert!(notice.is_none());

        let (controller, notice) =
            attach_foreground(&entry, 44, ForegroundRole::Controller).expect("claim on attach");
        assert_eq!(controller.attachment_id, 3);
        assert_eq!(controller.role, ForegroundRole::Controller);
        assert!(!controller.control_available);
        assert_eq!(
            notice,
            Some(vec![
                ForegroundRecipient {
                    client_id: 42,
                    attachment_id: 1,
                },
                ForegroundRecipient {
                    client_id: 43,
                    attachment_id: 2,
                },
            ])
        );
        assert!(
            notice
                .as_ref()
                .is_none_or(|recipients| recipients.iter().all(|item| item.client_id != 44)),
            "legacy controller attach must not notify its own client"
        );
        assert!(attach_foreground(&entry, 45, ForegroundRole::Controller).is_err());

        let (released, notice) = release_foreground_control(&entry, 44);
        let released = released.unwrap();
        assert_eq!(released.attachment_id, 3);
        assert_eq!(released.role, ForegroundRole::Observer);
        assert_eq!(notice.as_ref().map(Vec::len), Some(3));
        let (claimed, notice) = claim_foreground_control(&entry, 43);
        let claimed = claimed.unwrap();
        assert_eq!(claimed.attachment_id, 2);
        assert_eq!(claimed.role, ForegroundRole::Controller);
        assert_eq!(notice.as_ref().map(Vec::len), Some(3));
        let (idempotent, notice) = claim_foreground_control(&entry, 43);
        let idempotent = idempotent.unwrap();
        assert_eq!(idempotent.attachment_id, 2);
        assert_eq!(idempotent.role, ForegroundRole::Controller);
        assert!(notice.is_none());

        let detached = foreground
            .lock()
            .unwrap()
            .detach(42)
            .expect("detach first epoch");
        assert_eq!(detached.0, 1);
        assert!(detached.1.is_none());
        let (reattached, notice) =
            attach_foreground(&entry, 42, ForegroundRole::Observer).expect("reattach observer");
        assert_eq!(reattached.attachment_id, 4);
        assert!(notice.is_none());

        let mut fresh = ForegroundState::default();
        let first_controller = fresh
            .attach(99, ForegroundRole::Controller)
            .expect("first legacy controller attach");
        assert_eq!(first_controller.attachment_id, 1);
        assert_eq!(first_controller.control_recipients, Some(Vec::new()));
    }

    #[test]
    fn pty_input_requires_controller_but_pipe_input_remains_compatible() {
        assert!(job_input_kind_allows_client(false, None, 42));
        assert!(job_input_kind_allows_client(false, Some(7), 42));
        assert!(!job_input_kind_allows_client(true, None, 42));
        assert!(!job_input_kind_allows_client(true, Some(7), 42));
        assert!(job_input_kind_allows_client(true, Some(42), 42));
    }

    #[tokio::test]
    async fn unexpected_input_writer_exit_survives_saturated_mailbox_and_poisoned_mutex() {
        let (main_mailbox, _main_rx) = mpsc::channel(1);
        main_mailbox
            .try_send(ProcessMgrMsg::Shutdown)
            .expect("saturate main actor mailbox");
        assert_eq!(main_mailbox.capacity(), 0);

        let fence = Arc::new(InputFence::new());
        let poisoned_fence = fence.clone();
        assert!(
            std::thread::spawn(move || {
                let _guard = poisoned_fence.inner.lock().unwrap();
                panic!("poison input fence mutex");
            })
            .join()
            .is_err()
        );
        let (failures, mut failure_rx) = mpsc::unbounded_channel();
        let exit = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            drop(InputWriterTaskExitGuard {
                job_id: JobId(9),
                writer_incarnation: 17,
                fence: fence.clone(),
                failures,
                armed: true,
            });
        }));

        assert!(exit.is_ok(), "exit fencing must never double-panic");
        assert!(fence.is_poisoned());
        let InputWriterFailure {
            job_id,
            writer_incarnation,
            reason,
        } = failure_rx
            .recv()
            .await
            .expect("writer failure notification");
        assert_eq!(job_id, JobId(9));
        assert_eq!(writer_incarnation, 17);
        assert!(reason.contains("terminated unexpectedly"), "{reason}");
    }

    #[tokio::test]
    async fn input_writer_queue_is_bounded_and_drop_aborts_its_task() {
        struct DropSignal(Option<oneshot::Sender<()>>);
        impl Drop for DropSignal {
            fn drop(&mut self) {
                if let Some(sender) = self.0.take() {
                    let _ = sender.send(());
                }
            }
        }

        let (sender, receiver) = mpsc::channel(JOB_INPUT_QUEUE_CAP);
        let fence = Arc::new(InputFence::new());
        let (started_tx, started_rx) = oneshot::channel();
        let (dropped_tx, dropped_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            let _drop_signal = DropSignal(Some(dropped_tx));
            let _ = started_tx.send(());
            std::future::pending::<()>().await;
        });
        started_rx.await.expect("writer task started");
        let writer = JobInputWriter {
            kind: JobInputKind::Pipe,
            incarnation: 7,
            sender,
            fence,
            _task: AbortTaskOnDrop(task),
        };

        assert_eq!(
            writer.try_enqueue(vec![0; MAX_FOREGROUND_INPUT_BYTES + 1], None),
            Err(InputEnqueueError::TooLarge {
                actual: MAX_FOREGROUND_INPUT_BYTES + 1,
            })
        );
        for _ in 0..JOB_INPUT_QUEUE_CAP {
            writer
                .try_enqueue(vec![b'x'], None)
                .expect("bounded queue slot");
        }
        assert_eq!(
            writer.try_enqueue(vec![b'x'], None),
            Err(InputEnqueueError::Full)
        );
        drop(receiver);
        assert_eq!(
            writer.try_enqueue(vec![b'x'], None),
            Err(InputEnqueueError::Closed)
        );

        drop(writer);
        tokio::time::timeout(std::time::Duration::from_secs(1), dropped_rx)
            .await
            .expect("writer abort timed out")
            .expect("writer drop signal");
    }

    #[tokio::test]
    async fn full_and_oversize_pty_input_synchronously_detach_with_fg_exited() {
        let (gateway_tx, mut gateway_rx) = mpsc::channel(super::super::ACTOR_CHANNEL_CAP);
        let (scheduler_tx, _scheduler_rx) = mpsc::channel(super::super::ACTOR_CHANNEL_CAP);
        let (process_tx, _process_rx) = mpsc::channel(super::super::ACTOR_CHANNEL_CAP);
        let (scope_tx, _scope_rx) = mpsc::channel(super::super::ACTOR_CHANNEL_CAP);
        let (event_tx, _event_rx) = mpsc::channel(super::super::ACTOR_CHANNEL_CAP);
        let sys = ActorSystem {
            gateway: gateway_tx,
            scheduler: scheduler_tx,
            process_mgr: process_tx,
            scope_store: scope_tx,
            event_bus: event_tx,
            config: crate::config::Config::default(),
            resources: std::sync::Arc::new(crate::resource::ProviderRegistry::empty()),
        };

        let (sender, _receiver) = mpsc::channel(JOB_INPUT_QUEUE_CAP);
        let fence = Arc::new(InputFence::new());
        let generation = fence
            .start_controller_generation()
            .expect("controller generation");
        let writer = JobInputWriter {
            kind: JobInputKind::Pty,
            incarnation: 21,
            sender,
            fence,
            _task: AbortTaskOnDrop(tokio::spawn(std::future::pending())),
        };
        let foreground = Arc::new(Mutex::new(ForegroundState {
            observers: BTreeMap::from([(42, 1)]),
            controller: Some(42),
            controller_generation: Some(generation),
            last_attachment_id: 1,
            closed: false,
        }));
        let (kill_tx, mut kill_rx) = mpsc::channel(1);
        let entry = ProcessEntry {
            job_id: JobId(9),
            session_id: Some("SS-input-limit".into()),
            status: JobStatus::Running,
            reader_handle: tokio::spawn(async {}),
            kill_tx,
            ring_buffer: Arc::new(Mutex::new(RingBuffer::default())),
            stderr_ring: None,
            input: Some(writer),
            resize: Some(Arc::new(std::fs::File::open("/dev/null").unwrap())),
            foreground: foreground.clone(),
        };

        for _ in 0..JOB_INPUT_QUEUE_CAP {
            assert!(
                try_enqueue_job_input(&entry, 42, vec![b'x']).is_ok(),
                "fill bounded foreground queue"
            );
        }
        assert!(matches!(
            try_enqueue_job_input(&entry, 42, vec![b'x']),
            Err(JobInputDispatchError::Enqueue(InputEnqueueError::Full))
        ));
        let rejection = reject_controller_input(&entry, 42, "foreground input rejection");
        assert!(matches!(rejection, InputRejection::Detached(_)));
        emit_input_rejection(
            &sys,
            42,
            rejection,
            "foreground input rejected; controller detached",
        )
        .await;
        match gateway_rx.recv().await.expect("queue-full FgExited") {
            GatewayMsg::SendEvent {
                client_id,
                payload:
                    EventPayload::FgExited {
                        id, attachment_id, ..
                    },
                ..
            } => {
                assert_eq!(client_id, 42);
                assert_eq!(id, "J9");
                assert_eq!(attachment_id, 1);
            }
            _ => panic!("expected queue-full FgExited"),
        }

        let (attached, _) = attach_foreground(&entry, 43, ForegroundRole::Controller)
            .expect("attach after settled queue-full fence");
        assert!(matches!(
            try_enqueue_job_input(&entry, 43, vec![b'x'; MAX_FOREGROUND_INPUT_BYTES + 1],),
            Err(JobInputDispatchError::Enqueue(
                InputEnqueueError::TooLarge { .. }
            ))
        ));
        let rejection = reject_controller_input(&entry, 43, "foreground input rejection");
        assert!(matches!(rejection, InputRejection::Detached(_)));
        emit_input_rejection(
            &sys,
            43,
            rejection,
            "foreground input rejected; controller detached",
        )
        .await;
        match gateway_rx.recv().await.expect("oversize FgExited") {
            GatewayMsg::SendEvent {
                client_id,
                payload:
                    EventPayload::FgExited {
                        id, attachment_id, ..
                    },
                ..
            } => {
                assert_eq!(client_id, 43);
                assert_eq!(id, "J9");
                assert_eq!(attachment_id, attached.attachment_id);
            }
            _ => panic!("expected oversize FgExited"),
        }
        assert!(matches!(
            kill_rx.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        assert!(foreground.lock().unwrap().observers.is_empty());
    }

    #[tokio::test]
    async fn closed_pty_writer_synchronously_poisons_kills_and_exits_all_observers() {
        let (gateway_tx, mut gateway_rx) = mpsc::channel(super::super::ACTOR_CHANNEL_CAP);
        let (scheduler_tx, _scheduler_rx) = mpsc::channel(super::super::ACTOR_CHANNEL_CAP);
        let (process_tx, _process_rx) = mpsc::channel(super::super::ACTOR_CHANNEL_CAP);
        let (scope_tx, _scope_rx) = mpsc::channel(super::super::ACTOR_CHANNEL_CAP);
        let (event_tx, _event_rx) = mpsc::channel(super::super::ACTOR_CHANNEL_CAP);
        let sys = ActorSystem {
            gateway: gateway_tx,
            scheduler: scheduler_tx,
            process_mgr: process_tx,
            scope_store: scope_tx,
            event_bus: event_tx,
            config: crate::config::Config::default(),
            resources: std::sync::Arc::new(crate::resource::ProviderRegistry::empty()),
        };

        let (sender, receiver) = mpsc::channel(JOB_INPUT_QUEUE_CAP);
        drop(receiver);
        let fence = Arc::new(InputFence::new());
        let generation = fence
            .start_controller_generation()
            .expect("controller generation");
        let writer = JobInputWriter {
            kind: JobInputKind::Pty,
            incarnation: 22,
            sender,
            fence: fence.clone(),
            _task: AbortTaskOnDrop(tokio::spawn(std::future::pending())),
        };
        let foreground = Arc::new(Mutex::new(ForegroundState {
            observers: BTreeMap::from([(42, 1), (43, 2)]),
            controller: Some(42),
            controller_generation: Some(generation),
            last_attachment_id: 2,
            closed: false,
        }));
        let (kill_tx, mut kill_rx) = mpsc::channel(1);
        let entry = ProcessEntry {
            job_id: JobId(9),
            session_id: Some("SS-closed-input".into()),
            status: JobStatus::Running,
            reader_handle: tokio::spawn(async {}),
            kill_tx,
            ring_buffer: Arc::new(Mutex::new(RingBuffer::default())),
            stderr_ring: None,
            input: Some(writer),
            resize: Some(Arc::new(std::fs::File::open("/dev/null").unwrap())),
            foreground: foreground.clone(),
        };

        assert!(matches!(
            try_enqueue_job_input(&entry, 42, vec![b'x']),
            Err(JobInputDispatchError::Enqueue(InputEnqueueError::Closed))
        ));
        assert!(fence.is_poisoned());
        let rejection = reject_controller_input(&entry, 42, "foreground input rejection");
        assert!(matches!(rejection, InputRejection::Failed(_)));
        emit_input_rejection(
            &sys,
            42,
            rejection,
            "foreground input rejected; controller detached",
        )
        .await;

        for (expected_client, expected_attachment) in [(42, 1), (43, 2)] {
            match gateway_rx.recv().await.expect("closed-writer FgExited") {
                GatewayMsg::SendEvent {
                    client_id,
                    payload:
                        EventPayload::FgExited {
                            id, attachment_id, ..
                        },
                    ..
                } => {
                    assert_eq!(client_id, expected_client);
                    assert_eq!(id, "J9");
                    assert_eq!(attachment_id, expected_attachment);
                }
                _ => panic!("expected closed-writer FgExited"),
            }
        }
        kill_rx.try_recv().expect("closed writer must request kill");
        let foreground = foreground.lock().unwrap();
        assert!(foreground.closed);
        assert!(foreground.observers.is_empty());
        assert_eq!(foreground.controller, None);
    }

    #[tokio::test]
    async fn stale_controller_generation_is_discarded_without_writing() {
        let pty = crate::pty::open_pty().expect("test PTY");
        let master = std::fs::File::from(pty.master);
        let mut slave = std::fs::File::from(pty.slave);
        set_nonblocking(master.as_raw_fd()).expect("nonblocking PTY master");
        set_nonblocking(slave.as_raw_fd()).expect("nonblocking PTY slave");
        let master = AsyncFd::new(master).expect("async PTY master");
        let fence = InputFence::new();
        let generation = fence
            .start_controller_generation()
            .expect("controller generation");
        assert!(
            fence
                .revoke_controller_generation(generation)
                .expect("revoke generation")
                .settled
        );
        let item = JobInputItem {
            data: b"must-not-cross-lease".to_vec(),
            generation: Some(generation),
        };
        let mut changes = fence.subscribe();

        assert!(matches!(
            write_pty_item(&master, &item, &fence, &mut changes).await,
            PtyItemOutcome::Discarded { .. }
        ));
        let mut byte = [0_u8; 1];
        let error = slave
            .read(&mut byte)
            .expect_err("stale generation must not reach the slave PTY");
        assert_eq!(error.kind(), std::io::ErrorKind::WouldBlock);
        assert!(!fence.is_poisoned());
    }

    #[test]
    fn partial_controller_fence_poisons_the_writer() {
        let fence = InputFence::new();
        let generation = fence
            .start_controller_generation()
            .expect("controller generation");
        {
            let mut inner = fence.lock_inner();
            inner.active = Some(ActiveInput {
                generation,
                written: 3,
                total: 10,
            });
        }
        assert!(
            !fence
                .revoke_controller_generation(generation)
                .expect("revoke partial generation")
                .settled
        );

        match cancel_active_pty_item(&fence, generation) {
            PtyItemOutcome::Failed(reason) => {
                assert!(reason.contains("delivering 3 of 10"), "{reason}");
            }
            _ => panic!("partial cancellation must fail closed"),
        }
        assert!(fence.is_poisoned());
        assert!(!fence.controller_available());
        assert_eq!(
            fence.start_controller_generation(),
            Err(InputEnqueueError::Poisoned)
        );
    }

    #[test]
    fn zero_byte_controller_fence_settles_before_reclaiming_a_new_generation() {
        let fence = InputFence::new();
        let first_generation = fence
            .start_controller_generation()
            .expect("first controller generation");
        {
            let mut inner = fence.lock_inner();
            inner.active = Some(ActiveInput {
                generation: first_generation,
                written: 0,
                total: 10,
            });
        }
        assert!(
            !fence
                .revoke_controller_generation(first_generation)
                .expect("revoke blocked generation")
                .settled
        );
        assert!(matches!(
            cancel_active_pty_item(&fence, first_generation),
            PtyItemOutcome::Discarded {
                settled_generation: Some(_)
            }
        ));
        assert!(fence.controller_available());

        let second_generation = fence
            .start_controller_generation()
            .expect("reclaimed controller generation");
        assert_ne!(second_generation, first_generation);
        let inner = fence.lock_inner();
        assert_eq!(inner.generation, second_generation);
        assert_eq!(inner.settled_generation, second_generation);
        assert!(inner.active.is_none());
    }

    #[test]
    fn expands_scope_words_for_jobs() {
        let expanded = expand_command_line(
            &[
                "~/bin/tool".into(),
                "~".into(),
                "$HOME".into(),
                "${USER}".into(),
                "prefix-$USER-suffix".into(),
            ],
            Some(&snapshot()),
        );

        assert_eq!(
            expanded,
            vec![
                "/tmp/cue-home/bin/tool",
                "/tmp/cue-home",
                "/tmp/cue-home",
                "tester",
                "prefix-tester-suffix",
            ]
        );
    }

    #[test]
    fn preserves_unsupported_parameter_forms() {
        let expanded = expand_command_line(
            &[
                "echo".into(),
                "${USER:-guest}".into(),
                "${BROKEN".into(),
                "$1".into(),
                "\\$USER".into(),
            ],
            Some(&snapshot()),
        );

        assert_eq!(
            expanded,
            vec!["echo", "${USER:-guest}", "${BROKEN", "$1", "$USER"]
        );
    }

    #[test]
    fn multi_segment_pipeline_expands_each_segment_independently() {
        let pipeline = cue_core::pipeline::Pipeline {
            segments: vec![
                cue_core::pipeline::PipeSegment {
                    command: vec!["printf".into(), "%s".into(), "hello world".into()],
                    pipe_to_next: Some(cue_core::pipeline::PipeOp::Stdout),
                },
                cue_core::pipeline::PipeSegment {
                    command: vec!["grep".into(), "hello world".into()],
                    pipe_to_next: None,
                },
            ],
        };

        let snapshot = snapshot();
        let segments =
            expand_pipeline_segments(JobId(7), &pipeline, &snapshot).expect("expanded segments");

        assert_eq!(segments.len(), 2);
        assert_eq!(segments[0].program, "printf");
        assert_eq!(segments[0].args, vec!["%s", "hello world"]);
        assert_eq!(segments[1].program, "grep");
        assert_eq!(segments[1].args, vec!["hello world"]);
    }

    #[test]
    fn stderr_only_pipeline_keeps_metacharacters_as_data() {
        let pipeline = cue_core::pipeline::Pipeline {
            segments: vec![
                cue_core::pipeline::PipeSegment {
                    command: vec!["producer".into(), "semi;colon".into()],
                    pipe_to_next: Some(cue_core::pipeline::PipeOp::StderrOnly),
                },
                cue_core::pipeline::PipeSegment {
                    command: vec!["consumer".into()],
                    pipe_to_next: None,
                },
            ],
        };

        let snapshot = snapshot();
        let segments =
            expand_pipeline_segments(JobId(9), &pipeline, &snapshot).expect("expanded segments");

        assert_eq!(segments[0].args, vec!["semi;colon"]);
        assert!(matches!(
            segments[0].pipe_to_next,
            Some(cue_core::pipeline::PipeOp::StderrOnly)
        ));
    }

    #[tokio::test]
    async fn spawn_job_rejects_scope_without_snapshot() {
        let (gateway_tx, _gateway_rx) = mpsc::channel(super::super::ACTOR_CHANNEL_CAP);
        let (scheduler_tx, mut scheduler_rx) = mpsc::channel(super::super::ACTOR_CHANNEL_CAP);
        let (process_tx, process_rx) = mpsc::channel(super::super::ACTOR_CHANNEL_CAP);
        let (scope_tx, mut scope_rx) = mpsc::channel(super::super::ACTOR_CHANNEL_CAP);
        let (event_tx, _event_rx) = mpsc::channel(super::super::ACTOR_CHANNEL_CAP);
        let sys = ActorSystem {
            gateway: gateway_tx,
            scheduler: scheduler_tx,
            process_mgr: process_tx.clone(),
            scope_store: scope_tx,
            event_bus: event_tx,
            config: crate::config::Config::default(),
            resources: std::sync::Arc::new(crate::resource::ProviderRegistry::empty()),
        };

        spawn(process_rx, sys);

        tokio::spawn(async move {
            while let Some(msg) = scope_rx.recv().await {
                if let ScopeStoreMsg::GetScope { hash, reply } = msg {
                    let _ = reply.send(Ok(Some(cue_core::scope::Scope {
                        hash,
                        parent: None,
                        delta: None,
                        snapshot: None,
                    })));
                }
            }
        });

        let job_id = JobId(77);
        process_tx
            .send(ProcessMgrMsg::SpawnJob {
                job_id,
                plan: JobPlan::Pipeline(cue_core::pipeline::Pipeline {
                    segments: vec![cue_core::pipeline::PipeSegment {
                        command: vec!["echo".into(), "should-not-run".into()],
                        pipe_to_next: None,
                    }],
                }),
                scope_hash: cue_core::ScopeHash([9; 32]),
                options: ProcessJobOptions {
                    cwd_override: None,
                    sandbox: None,
                    wrapper_enabled: false,
                    pty_enabled: false,
                    direct_output_client: None,
                    session_id: None,
                },
            })
            .await
            .expect("send spawn job");

        let msg = tokio::time::timeout(std::time::Duration::from_secs(1), scheduler_rx.recv())
            .await
            .expect("job failure should be reported")
            .expect("scheduler channel should stay open");
        match msg {
            SchedulerMsg::JobFinished {
                job_id: finished,
                exit_code,
            } => {
                assert_eq!(finished, job_id);
                assert_eq!(exit_code, EXIT_CODE_UNAVAILABLE);
            }
            _ => panic!("expected JobFinished"),
        }
    }

    #[tokio::test]
    async fn kill_single_pipe_job_stops_child_and_reports_finished() {
        let cwd = make_temp_dir();
        let (gateway_tx, _gateway_rx) = mpsc::channel(super::super::ACTOR_CHANNEL_CAP);
        let (scheduler_tx, mut scheduler_rx) = mpsc::channel(super::super::ACTOR_CHANNEL_CAP);
        let (process_tx, process_rx) = mpsc::channel(super::super::ACTOR_CHANNEL_CAP);
        let (scope_tx, mut scope_rx) = mpsc::channel(super::super::ACTOR_CHANNEL_CAP);
        let (event_tx, _event_rx) = mpsc::channel(super::super::ACTOR_CHANNEL_CAP);
        let sys = ActorSystem {
            gateway: gateway_tx,
            scheduler: scheduler_tx,
            process_mgr: process_tx.clone(),
            scope_store: scope_tx,
            event_bus: event_tx,
            config: crate::config::Config::default(),
            resources: std::sync::Arc::new(crate::resource::ProviderRegistry::empty()),
        };

        spawn(process_rx, sys);

        tokio::spawn({
            let cwd = cwd.clone();
            async move {
                while let Some(msg) = scope_rx.recv().await {
                    match msg {
                        ScopeStoreMsg::GetScope { hash, reply } => {
                            reply
                                .send(Ok(Some(cue_core::scope::Scope {
                                    hash,
                                    parent: None,
                                    delta: None,
                                    snapshot: Some(EnvSnapshot {
                                        env: BTreeMap::new(),
                                        cwd: cwd.clone(),
                                    }),
                                })))
                                .expect("send scope reply");
                        }
                        ScopeStoreMsg::Shutdown => break,
                        _ => {}
                    }
                }
            }
        });

        let job_id = JobId(78);
        process_tx
            .send(ProcessMgrMsg::SpawnJob {
                job_id,
                plan: JobPlan::Pipeline(cue_core::pipeline::Pipeline {
                    segments: vec![cue_core::pipeline::PipeSegment {
                        command: vec!["/bin/sleep".into(), "30".into()],
                        pipe_to_next: None,
                    }],
                }),
                scope_hash: cue_core::ScopeHash([8; 32]),
                options: ProcessJobOptions {
                    cwd_override: None,
                    sandbox: None,
                    wrapper_enabled: false,
                    pty_enabled: false,
                    direct_output_client: None,
                    session_id: None,
                },
            })
            .await
            .expect("send spawn job");

        let (reply_tx, reply_rx) = oneshot::channel();
        process_tx
            .send(ProcessMgrMsg::KillJob {
                job_id,
                reply: reply_tx,
            })
            .await
            .expect("send kill job");
        let kill_result = tokio::time::timeout(std::time::Duration::from_secs(2), reply_rx)
            .await
            .expect("kill reply")
            .expect("kill reply sender");
        assert_eq!(kill_result, Ok(()));

        let msg = tokio::time::timeout(std::time::Duration::from_secs(5), scheduler_rx.recv())
            .await
            .expect("job finished after kill")
            .expect("scheduler channel should stay open");
        match msg {
            SchedulerMsg::JobFinished {
                job_id: finished,
                exit_code,
            } => {
                assert_eq!(finished, job_id);
                assert_eq!(exit_code, EXIT_CODE_UNAVAILABLE);
            }
            _ => panic!("expected JobFinished"),
        }

        process_tx
            .send(ProcessMgrMsg::Shutdown)
            .await
            .expect("send process_mgr shutdown");
        std::fs::remove_dir_all(cwd).expect("remove temp dir");
    }

    #[tokio::test]
    async fn write_log_persists_exact_output_bytes() {
        let dir = make_temp_dir();
        let path = dir.join("J42.log");
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .expect("open log file");
        let file = Arc::new(Mutex::new(Some(file)));

        write_log(JobId(42), LogStream::Stdout, &file, b"hello\n").await;
        write_log(JobId(42), LogStream::Stdout, &file, b"world").await;

        drop(file);
        assert_eq!(
            std::fs::read(&path).expect("read log file"),
            b"hello\nworld"
        );
        std::fs::remove_dir_all(dir).expect("remove temp dir");
    }
}
