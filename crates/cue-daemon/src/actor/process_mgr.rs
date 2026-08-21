//! ProcessManager actor — OS child process lifecycle.
//!
//! Spawns real child processes via `tokio::process::Command`, reads their
//! stdout/stderr into a [`RingBuffer`], writes a persistent log file, and
//! publishes output chunks + state-change events.

use std::collections::{BTreeMap, HashMap};
use std::io::{Read, Write};
use std::os::fd::{AsRawFd, FromRawFd};
use std::os::unix::process::ExitStatusExt as _;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Mutex};

use tokio::io::unix::AsyncFd;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, error, info, warn};

use cue_core::ipc::{
    EventPayload, ForegroundAttachmentInfo, ForegroundRole, Stream as OutputStream,
};
use cue_core::launch::EXIT_CODE_UNAVAILABLE;
use cue_core::pipeline::{Pipeline, command_prefers_foreground};
use cue_core::process_status::exit_code_from_status;
use cue_core::scope::EnvSnapshot;
use cue_core::spawn_adapter::{SpawnAdapterRequest, SpawnResult};
use cue_core::{EventChannel, StepId};

use super::{
    ActorSystem, ForegroundRoleUpdate, ProcessMgrMsg, ProcessSpawnAdapter, ProcessStepOptions,
    ScopeStoreMsg, publish_session_event as publish_actor_session_event,
    publish_session_event_except as publish_actor_session_event_except,
    send_gateway_event as send_actor_gateway_event,
};
use crate::ring_buffer::RingBuffer;
use crate::runtime_env::effective_snapshot;
use crate::word_expansion::{expand_command_line, expand_environment};

// ── Per-child bookkeeping ──

struct ProcessEntry {
    step_id: StepId,
    /// Named session that owns this process, or `None` for legacy anonymous jobs.
    session_id: Option<String>,
    /// Handle for the background reader/waiter task.
    reader_handle: tokio::task::JoinHandle<()>,
    /// Send on this channel to request a kill.
    kill_tx: mpsc::Sender<()>,
    /// Shared ring buffer holding the latest output bytes for live-tail queries.
    ring_buffer: Arc<Mutex<RingBuffer>>,
    /// Job stdin, either the PTY master or a pipe to the first process.
    input: Option<JobInput>,
    /// PTY master fd used for resize ioctls.
    resize: Option<Arc<std::fs::File>>,
    /// Shared foreground observer set and exclusive controller lease.
    foreground: Arc<Mutex<ForegroundState>>,
}

impl ProcessEntry {
    fn public_id(&self) -> String {
        self.step_id.to_string()
    }
}

/// Runtime-only attachment state for one step's foreground stream.
///
/// The controller is always also present in `observers`. `closed` fences late
/// attach attempts while the reader task is publishing terminal events.
#[derive(Debug, Default)]
struct ForegroundState {
    /// Client id to the epoch of its current attachment.
    observers: BTreeMap<u64, u64>,
    controller: Option<u64>,
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

    fn claim_control(&mut self, client_id: u64) -> Result<bool, ()> {
        if self.closed || !self.observers.contains_key(&client_id) {
            return Err(());
        }
        match self.controller {
            Some(controller) if controller != client_id => Err(()),
            Some(_) => Ok(false),
            None => {
                self.controller = Some(client_id);
                Ok(true)
            }
        }
    }

    fn release_control(&mut self, client_id: u64) -> Result<bool, ()> {
        if self.closed || !self.observers.contains_key(&client_id) {
            return Err(());
        }
        let released = self.controller == Some(client_id);
        if released {
            self.controller = None;
        }
        Ok(released)
    }

    fn detach(&mut self, client_id: u64) -> Option<(u64, Option<Vec<ForegroundRecipient>>)> {
        let attachment_id = self.observers.remove(&client_id)?;
        let released_control = self.controller == Some(client_id);
        if released_control {
            self.controller = None;
        }
        Some((attachment_id, released_control.then(|| self.recipients())))
    }
}

#[derive(Clone)]
enum JobInput {
    Pty(Arc<AsyncFd<std::fs::File>>),
    Pipe(Arc<tokio::sync::Mutex<tokio::process::ChildStdin>>),
}

const DEFAULT_PTY_COLS: u16 = 80;
const DEFAULT_PTY_ROWS: u16 = 24;
/// At most 512 KiB of 8 KiB chunks may wait per pipeline before readers apply
/// backpressure to the child process pipes.
const PIPELINE_CHUNK_CAP: usize = 64;

// ── Actor entry point ──

struct NativePipelineSpawn {
    children: Vec<tokio::process::Child>,
    settlements: Vec<Option<PreparedAdapterSettlement>>,
    input: Option<JobInput>,
    stdout_sources: Vec<tokio::process::ChildStdout>,
    stderr_sources: Vec<tokio::process::ChildStderr>,
}

struct NativePipelineOptions<'a> {
    cwd_override: Option<&'a Path>,
    sandbox: Option<&'a crate::sandbox::PreparedSandbox>,
    wrapper_enabled: bool,
    spawn_adapter: Option<&'a ProcessSpawnAdapter>,
    capture_stdin: bool,
    sys: &'a ActorSystem,
}

struct PrepareSpawnOptions<'a> {
    snapshot: &'a EnvSnapshot,
    cwd_override: Option<&'a Path>,
    workspace_view: Option<&'a crate::sandbox::PreparedSandbox>,
    wrapper_enabled: bool,
    spawn_adapter: Option<&'a ProcessSpawnAdapter>,
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

#[derive(Clone)]
struct ProcessTaskRuntime {
    sys: ActorSystem,
    foreground: Arc<Mutex<ForegroundState>>,
    direct_output_client: Option<u64>,
    session_id: Option<String>,
    cleanup_tx: mpsc::Sender<StepId>,
}

struct PtyReaderTask {
    step_id: StepId,
    child: tokio::process::Child,
    sandbox: Option<crate::sandbox::PreparedSandbox>,
    reader: AsyncFd<std::fs::File>,
    log_file: Option<std::fs::File>,
    kill_rx: mpsc::Receiver<()>,
    ring: Arc<Mutex<RingBuffer>>,
    settlement: Option<PreparedAdapterSettlement>,
    runtime: ProcessTaskRuntime,
}

struct PipelineReaderTask {
    step_id: StepId,
    children: Vec<tokio::process::Child>,
    settlements: Vec<Option<PreparedAdapterSettlement>>,
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

fn foreground_step_for_client(
    children: &HashMap<StepId, ProcessEntry>,
    client_id: u64,
) -> Option<StepId> {
    children.values().find_map(|entry| {
        entry
            .foreground
            .lock()
            .unwrap()
            .observers
            .contains_key(&client_id)
            .then_some(entry.step_id)
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
    let public_id = entry.public_id();
    if entry.resize.is_none() {
        return Err(format!(
            "step {public_id} does not support foreground attach"
        ));
    }

    let mut foreground = entry.foreground.lock().unwrap();
    let outcome = foreground
        .attach(client_id, requested_role)
        .map_err(|error| match error {
            ForegroundAttachError::Closed => {
                format!("step {public_id} foreground is closed")
            }
            ForegroundAttachError::AlreadyAttached => {
                format!("client is already foreground-attached to {public_id}")
            }
            ForegroundAttachError::ControlHeld => {
                format!("step {public_id} foreground control is already held")
            }
            ForegroundAttachError::AttachmentIdExhausted => {
                format!("step {public_id} foreground attachment id space is exhausted")
            }
        })?;
    let control_available = foreground.control_available();
    let (snapshot, snapshot_truncated) = entry
        .ring_buffer
        .lock()
        .unwrap()
        .tail_with_truncation(crate::ring_buffer::DEFAULT_CAPACITY);

    Ok((
        ForegroundAttachmentInfo {
            id: entry.step_id,
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
                "step {} foreground control is already held",
                entry.public_id()
            )),
            None,
        );
    }
    match foreground.claim_control(client_id) {
        Ok(false) => (
            Ok(ForegroundRoleUpdate {
                id: entry.step_id,
                attachment_id,
                role: ForegroundRole::Controller,
                control_available: false,
            }),
            None,
        ),
        Ok(true) => {
            let recipients = foreground.recipients();
            (
                Ok(ForegroundRoleUpdate {
                    id: entry.step_id,
                    attachment_id,
                    role: ForegroundRole::Controller,
                    control_available: false,
                }),
                Some(recipients),
            )
        }
        Err(()) => (Err("no foreground job observed".to_string()), None),
    }
}

fn release_foreground_control(
    entry: &ProcessEntry,
    client_id: u64,
) -> (
    Result<ForegroundRoleUpdate, String>,
    Option<Vec<ForegroundRecipient>>,
) {
    let mut foreground = entry.foreground.lock().unwrap();
    let Some(&attachment_id) = foreground.observers.get(&client_id) else {
        return (Err("no foreground job observed".to_string()), None);
    };
    if foreground.closed {
        return (Err("no foreground job observed".to_string()), None);
    }
    let released = foreground
        .release_control(client_id)
        .expect("observer presence checked above");
    let control_available = foreground.control_available();
    let recipients = released.then(|| foreground.recipients());
    (
        Ok(ForegroundRoleUpdate {
            id: entry.step_id,
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

/// Spawn the ProcessManager actor task.
pub(super) fn spawn(mut rx: mpsc::Receiver<ProcessMgrMsg>, sys: ActorSystem) {
    tokio::spawn(async move {
        debug!("process_mgr: started");

        let mut children: HashMap<StepId, ProcessEntry> = HashMap::new();

        // Internal channel for reader tasks to request cleanup.
        let (cleanup_tx, mut cleanup_rx) = mpsc::channel::<StepId>(super::ACTOR_CHANNEL_CAP);

        loop {
            tokio::select! {
                msg = rx.recv() => {
                    let Some(msg) = msg else { break; };
                    match msg {
                ProcessMgrMsg::SpawnStep {
                    step_id,
                    pipeline,
                    scope_hash,
                    options,
                } => {
                    info!(%step_id, plan = %pipeline, %scope_hash, "process_mgr: spawn");

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
                            error!(%step_id, "process_mgr: scope_store channel closed");
                            // Fail the job instead of continuing with the daemon environment.
                            fail_pending_spawn(
                                &sys,
                                step_id,
                                options.session_id.as_deref(),
                            )
                            .await;
                            continue;
                        }
                        match rx.await {
                            Ok(Ok(Some(scope))) => match scope.snapshot {
                                Some(snapshot) => snapshot,
                                None => {
                                    error!(%step_id, %scope_hash, "process_mgr: scope has no snapshot");
                                    fail_pending_spawn(
                                        &sys,
                                        step_id,
                                        options.session_id.as_deref(),
                                    )
                                        .await;
                                    continue;
                                }
                            },
                            Ok(Ok(None)) => {
                                // Scope resolution failed, so the job cannot safely inherit env.
                                error!(%step_id, %scope_hash, "process_mgr: scope not found");
                                fail_pending_spawn(
                                    &sys,
                                    step_id,
                                    options.session_id.as_deref(),
                                )
                                    .await;
                                continue;
                            }
                            Ok(Err(error)) => {
                                error!(%step_id, %scope_hash, "process_mgr: scope lookup failed: {error}");
                                fail_pending_spawn(
                                    &sys,
                                    step_id,
                                    options.session_id.as_deref(),
                                )
                                    .await;
                                continue;
                            }
                            Err(_) => {
                                // Scope resolution failed, so the job cannot safely inherit env.
                                error!(%step_id, "process_mgr: scope_store reply dropped");
                                fail_pending_spawn(
                                    &sys,
                                    step_id,
                                    options.session_id.as_deref(),
                                )
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
                            %step_id,
                            cwd = %cwd.display(),
                            "process_mgr: invalid cwd for step spawn"
                        );
                        emit_step_finished(
                            &sys,
                            step_id,
                            EXIT_CODE_UNAVAILABLE,
                        )
                        .await;
                        continue;
                    }

                    clear_step_logs(step_id).await;

                    let entry = spawn_pipeline_step(
                        step_id,
                        &pipeline,
                        &effective_snapshot,
                        &effective_options,
                        sys.clone(),
                        cleanup_tx.clone(),
                    )
                    .await;

                    match entry {
                        Ok(entry) => {
                            children.insert(step_id, entry);
                        }
                        Err(()) => {
                            emit_step_finished(
                                &sys,
                                step_id,
                                EXIT_CODE_UNAVAILABLE,
                            )
                            .await;
                        }
                    }
                }

                ProcessMgrMsg::KillStep { step_id, reply } => {
                    info!(%step_id, "process_mgr: kill requested");
                    let Some(entry) = children.remove(&step_id) else {
                        let _ = reply.send(Err(format!("step {step_id} not found")));
                        continue;
                    };
                    let ProcessEntry {
                        reader_handle,
                        kill_tx,
                        ..
                    } = entry;
                    if kill_tx.send(()).await.is_err() {
                        debug!(%step_id, "process_mgr: kill channel already closed; waiting for reader exit");
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
                                "step {step_id} process waiter failed: {error}"
                            )),
                            Err(_) => Err(format!(
                                "timed out waiting for step {step_id} to stop"
                            )),
                        };
                        let _ = reply.send(result);
                    });
                }

                ProcessMgrMsg::AttachFg {
                    client_id,
                    step_id,
                    role,
                    legacy_snapshot_event,
                    reply,
                } => {
                    let current_step = foreground_step_for_client(&children, client_id);
                    let (result, control_recipients, session_id, step_id) =
                        if current_step.is_some_and(|current| current != step_id) {
                            (
                                Err(format!(
                                    "client is already foreground-attached to {}",
                                    current_step.expect("checked above")
                                )),
                                None,
                                None,
                                None,
                            )
                        } else if let Some(entry) = children.get(&step_id) {
                            let session_id = entry.session_id.clone();
                            match attach_foreground(entry, client_id, role) {
                                Ok((info, recipients)) => {
                                    (Ok(info), recipients, session_id, Some(entry.step_id))
                                }
                                Err(error) => {
                                    (Err(error), None, session_id, Some(entry.step_id))
                                }
                            }
                        } else {
                            (Err(format!("step {step_id} not found")), None, None, None)
                        };
                    // Daemons predating shared foreground mode delivered the
                    // retained snapshot as an FgOutput event after the
                    // FgAttached response. Keep that one legacy event for the
                    // controller entry point: current clients reject epoch 0
                    // for a non-zero attachment, while old clients recover
                    // their history instead of starting from an empty screen.
                    let legacy_snapshot = if legacy_snapshot_event
                        && role == ForegroundRole::Controller
                    {
                        result.as_ref().ok().map(|info| info.snapshot.clone())
                    } else {
                        None
                    }
                    .filter(|snapshot| !snapshot.is_empty());
                    let _ = reply.send(result);
                    if let (Some(snapshot), Some(step_id)) =
                        (legacy_snapshot, step_id)
                    {
                        send_actor_gateway_event(
                            "process_mgr",
                            &sys,
                            client_id,
                            EventPayload::FgOutput {
                                id: step_id,
                                attachment_id: 0,
                                data: snapshot,
                            },
                            session_id.clone(),
                        )
                        .await;
                    }
                    if let (Some(recipients), Some(step_id)) =
                        (control_recipients, step_id)
                    {
                        emit_fg_control_changed(
                            &sys,
                            recipients,
                            step_id,
                            false,
                            session_id.as_deref(),
                        )
                        .await;
                    }
                }

                ProcessMgrMsg::ClaimFgControl { client_id, reply } => {
                    let Some(step_id) = foreground_step_for_client(&children, client_id) else {
                        let _ = reply.send(Err("no foreground job observed".to_string()));
                        continue;
                    };
                    let entry = children
                        .get(&step_id)
                        .expect("foreground lookup returned a live job");
                    let session_id = entry.session_id.clone();
                    let step_id = entry.step_id;
                    let (result, recipients) = claim_foreground_control(entry, client_id);
                    let _ = reply.send(result);
                    if let Some(recipients) = recipients {
                        emit_fg_control_changed(
                            &sys,
                            recipients,
                            step_id,
                            false,
                            session_id.as_deref(),
                        )
                        .await;
                    }
                }

                ProcessMgrMsg::ReleaseFgControl { client_id, reply } => {
                    let Some(step_id) = foreground_step_for_client(&children, client_id) else {
                        let _ = reply.send(Err("no foreground job observed".to_string()));
                        continue;
                    };
                    let entry = children
                        .get(&step_id)
                        .expect("foreground lookup returned a live job");
                    let session_id = entry.session_id.clone();
                    let step_id = entry.step_id;
                    let (result, recipients) = release_foreground_control(entry, client_id);
                    let _ = reply.send(result);
                    if let Some(recipients) = recipients {
                        emit_fg_control_changed(
                            &sys,
                            recipients,
                            step_id,
                            true,
                            session_id.as_deref(),
                        )
                        .await;
                    }
                }

                ProcessMgrMsg::DetachFg { client_id, reason, reply } => {
                    let mut detached_steps = Vec::new();
                    for entry in children.values() {
                        let mut foreground = entry.foreground.lock().unwrap();
                        if let Some((attachment_id, control_recipients)) =
                            foreground.detach(client_id)
                        {
                            detached_steps.push((
                                entry.step_id,
                                attachment_id,
                                entry.session_id.clone(),
                                control_recipients,
                            ));
                        }
                    }
                    for (step_id, attachment_id, session_id, control_recipients) in detached_steps {
                        send_actor_gateway_event(
                            "process_mgr",
                            &sys,
                            client_id,
                            EventPayload::FgExited {
                                id: step_id,
                                attachment_id,
                                reason: reason.clone(),
                            },
                            session_id.clone(),
                        )
                        .await;
                        if let Some(recipients) = control_recipients {
                            emit_fg_control_changed(
                                &sys,
                                recipients,
                                step_id,
                                true,
                                session_id.as_deref(),
                            )
                            .await;
                        }
                    }
                    if let Some(reply) = reply {
                        let _ = reply.send(());
                    }
                }

                ProcessMgrMsg::FgInput { client_id, data, reply } => {
                    let input = children
                        .values()
                        .find(|entry| {
                            entry.foreground.lock().unwrap().controller == Some(client_id)
                        })
                        .and_then(|entry| entry.input.clone());
                    let handled = if let Some(input) = input {
                        match write_step_input(&input, &data).await {
                            Ok(()) => Ok(()),
                            Err(error) => Err(format!("failed to write fg input: {error}")),
                        }
                    } else {
                        Err("no foreground session attached".to_string())
                    };
                    let _ = reply.send(handled);
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

                ProcessMgrMsg::Shutdown => {
                    debug!("process_mgr: shutting down — killing all children");
                    for entry in children.values() {
                        match entry.kill_tx.try_send(()) {
                            Ok(()) => {
                                debug!(step_id = %entry.step_id, "process_mgr: shutdown kill requested");
                            }
                            Err(mpsc::error::TrySendError::Full(_)) => {
                                debug!(step_id = %entry.step_id, "process_mgr: shutdown kill already pending");
                            }
                            Err(mpsc::error::TrySendError::Closed(_)) => {
                                debug!(step_id = %entry.step_id, "process_mgr: shutdown kill channel closed");
                            }
                        }
                    }
                    // Give children a moment to exit.
                    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                    break;
                }
                    }
                }

                // Reader task finished; remove the stale entry.
                Some(step_id) = cleanup_rx.recv() => {
                    debug!(%step_id, "process_mgr: cleaning up finished child");
                    children.remove(&step_id);
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

async fn write_pty(fd: &AsyncFd<std::fs::File>, data: &[u8]) -> std::io::Result<()> {
    let mut written = 0;
    while written < data.len() {
        let mut guard = fd.writable().await?;
        match guard.try_io(|inner| inner.get_ref().write(&data[written..])) {
            Ok(Ok(0)) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    "pty write returned 0 bytes",
                ));
            }
            Ok(Ok(n)) => written += n,
            Ok(Err(error)) => return Err(error),
            Err(_would_block) => continue,
        }
    }
    Ok(())
}

async fn write_step_input(input: &JobInput, data: &[u8]) -> std::io::Result<()> {
    match input {
        JobInput::Pty(fd) => write_pty(fd, data).await,
        JobInput::Pipe(stdin) => {
            let mut stdin = stdin.lock().await;
            stdin.write_all(data).await?;
            stdin.flush().await
        }
    }
}

#[derive(Clone)]
struct ExpandedSegment {
    command_line: Vec<String>,
    env: BTreeMap<String, String>,
    program: String,
    args: Vec<String>,
    pipe_to_next: Option<cue_core::pipeline::PipeOp>,
}

struct PreparedSpawn {
    program: String,
    args: Vec<String>,
    command: tokio::process::Command,
    settlement: Option<PreparedAdapterSettlement>,
}

struct PreparedAdapterSettlement {
    client: crate::spawn_adapter::SpawnAdapterClient,
    token: cue_core::SecretToken,
    execution_id: cue_core::ExecutionId,
    step_id: cue_core::StepId,
    segment_index: u32,
}

impl PreparedAdapterSettlement {
    async fn settle(
        &self,
        result: SpawnResult,
        diagnostic_tail: String,
        diagnostic_truncated: bool,
    ) -> Result<(), crate::spawn_adapter::SpawnAdapterError> {
        self.client
            .settle(SpawnAdapterRequest::Settle {
                token: self.token.clone(),
                execution_id: self.execution_id,
                step_id: self.step_id,
                segment_index: self.segment_index,
                result,
                diagnostic_tail,
                diagnostic_truncated,
            })
            .await
    }
}

fn expand_pipeline_segments(
    step_id: StepId,
    pipeline: &cue_core::pipeline::Pipeline,
    snapshot: &EnvSnapshot,
) -> Result<Vec<ExpandedSegment>, ()> {
    let mut expanded = Vec::with_capacity(pipeline.segments.len());
    for segment in &pipeline.segments {
        let command_line = expand_command_line(&segment.command, Some(snapshot));
        let env = expand_environment(&segment.env, Some(snapshot));
        let Some(program) = command_line
            .first()
            .cloned()
            .filter(|word| !word.is_empty())
        else {
            error!(
                %step_id,
                pipeline = ?segment.command,
                "process_mgr: command is empty"
            );
            return Err(());
        };
        let args = command_line.get(1..).unwrap_or(&[]).to_vec();
        expanded.push(ExpandedSegment {
            command_line,
            env,
            program,
            args,
            pipe_to_next: segment.pipe_to_next,
        });
    }
    if expanded.is_empty() {
        error!(%step_id, "process_mgr: pipeline is empty");
        return Err(());
    }
    Ok(expanded)
}

fn configure_command(
    cmd: &mut tokio::process::Command,
    snapshot: &EnvSnapshot,
    env: &BTreeMap<String, String>,
    cwd_override: Option<&Path>,
    sandbox: Option<&crate::sandbox::PreparedSandbox>,
) {
    let cwd = effective_cwd_path(snapshot, cwd_override, sandbox);
    cmd.env_clear();
    cmd.envs(snapshot.env.iter());
    cmd.envs(env);
    cmd.env("PWD", &cwd);
    cmd.current_dir(cwd);
    cmd.kill_on_drop(true);
}

/// Build the one authoritative command image for a process segment.
///
/// Callers still own stream and process-group wiring because PTY and pipe
/// children require different file descriptors, but argv, workspace view,
/// wrapper application, environment, cwd, and kill-on-drop semantics must all
/// pass through this function before `spawn`.
async fn prepare_spawn(
    segment: &ExpandedSegment,
    segment_index: usize,
    options: PrepareSpawnOptions<'_>,
) -> Result<PreparedSpawn, crate::spawn_adapter::SpawnAdapterError> {
    let mut program = segment.program.clone();
    let mut args = segment.args.clone();

    match options
        .sys
        .config
        .check_command_guardrail(&segment.command_line)
    {
        Some(crate::config::BlockDecision::Block(message)) => {
            return Err(crate::spawn_adapter::SpawnAdapterError::Rejected(message));
        }
        Some(crate::config::BlockDecision::Warn(message)) => {
            warn!(command = ?segment.command_line, %message, "process_mgr: command guardrail warning");
        }
        None => {}
    }

    if options.wrapper_enabled {
        let wrapper = &options.sys.config.wrapper;
        let is_foreground = command_prefers_foreground(&segment.command_line);
        if wrapper.should_wrap(&program, is_foreground, Some(true)) {
            let mut wrapped_args = Vec::with_capacity(1 + args.len());
            wrapped_args.push(program);
            wrapped_args.extend(args);
            program = wrapper.binary.clone();
            args = wrapped_args;
        }
    }

    let settlement = if let Some(adapter) = options.spawn_adapter {
        let mut argv = Vec::with_capacity(args.len() + 1);
        argv.push(program);
        argv.extend(args);
        let client = crate::spawn_adapter::SpawnAdapterClient::new(adapter.handle.clone());
        let prepared = client
            .prepare(SpawnAdapterRequest::Prepare {
                token: adapter.handle.token.clone(),
                execution_id: adapter.execution_id,
                step_id: adapter.step_id,
                segment_index: segment_index as u32,
                argv,
                cwd: effective_cwd_path(
                    options.snapshot,
                    options.cwd_override,
                    options.workspace_view,
                ),
            })
            .await?;
        program = prepared[0].clone();
        args = prepared[1..].to_vec();
        Some(PreparedAdapterSettlement {
            client,
            token: adapter.handle.token.clone(),
            execution_id: adapter.execution_id,
            step_id: adapter.step_id,
            segment_index: segment_index as u32,
        })
    } else {
        None
    };

    let mut command = tokio::process::Command::new(&program);
    command.args(&args);
    configure_command(
        &mut command,
        options.snapshot,
        &segment.env,
        options.cwd_override,
        options.workspace_view,
    );

    Ok(PreparedSpawn {
        program,
        args,
        command,
        settlement,
    })
}

#[cfg(unix)]
fn configure_process_group(cmd: &mut tokio::process::Command) {
    // SAFETY: `setpgid` is async-signal-safe, and the closure only operates on
    // the forked child before exec. A per-child process group lets cancellation
    // terminate grandchildren instead of orphaning them under PID 1.
    unsafe {
        cmd.pre_exec(|| {
            if libc::setpgid(0, 0) == -1 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
}

#[cfg(not(unix))]
fn configure_process_group(_cmd: &mut tokio::process::Command) {}

fn effective_process_options(
    options: &ProcessStepOptions,
    _snapshot: &EnvSnapshot,
) -> ProcessStepOptions {
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
    step_id: StepId,
    program: &str,
    args: &[String],
    snapshot: &EnvSnapshot,
    cwd_override: Option<&Path>,
    error: &std::io::Error,
) {
    error!(
        %step_id,
        program,
        args = ?args,
        cwd = %effective_cwd(snapshot, cwd_override).display(),
        path = ?snapshot.env.get("PATH").cloned(),
        err = %error,
        "process_mgr: spawn failed"
    );
}

async fn spawn_pipeline_step(
    step_id: StepId,
    pipeline: &Pipeline,
    snapshot: &EnvSnapshot,
    options: &ProcessStepOptions,
    sys: ActorSystem,
    cleanup_tx: mpsc::Sender<StepId>,
) -> Result<ProcessEntry, ()> {
    if pipeline.segments.len() == 1 && options.pty_enabled {
        spawn_single_pty_step(step_id, pipeline, snapshot, options, sys, cleanup_tx).await
    } else if pipeline.segments.len() == 1 {
        spawn_single_pipe_step(step_id, pipeline, snapshot, options, sys, cleanup_tx).await
    } else {
        spawn_native_pipeline_step(step_id, pipeline, snapshot, options, sys, cleanup_tx).await
    }
}

fn prepare_step_sandbox(
    step_id: StepId,
    snapshot: &EnvSnapshot,
    options: &ProcessStepOptions,
    sys: &ActorSystem,
) -> Result<Option<crate::sandbox::PreparedSandbox>, String> {
    let Some(config) = options.sandbox.as_ref() else {
        return Ok(None);
    };
    let lower_dir = effective_cwd(snapshot, options.cwd_override.as_deref());
    crate::sandbox::prepare(
        step_id,
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
        error!(%step_id, err = %message, "process_mgr: sandbox setup failed");
        message
    })
}

async fn prepare_step_sandbox_or_emit(
    step_id: StepId,
    snapshot: &EnvSnapshot,
    options: &ProcessStepOptions,
    sys: &ActorSystem,
) -> Result<Option<crate::sandbox::PreparedSandbox>, ()> {
    match prepare_step_sandbox(step_id, snapshot, options, sys) {
        Ok(sandbox) => Ok(sandbox),
        Err(message) => {
            emit_spawn_setup_stderr(
                sys,
                step_id,
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
    step_id: StepId,
    message: &str,
    direct_output_client: Option<u64>,
    session_id: Option<&str>,
) {
    let line = format!("{message}\n");
    let stderr_log = Arc::new(Mutex::new(open_stderr_log(step_id).await));
    write_log(step_id, LogStream::Stderr, &stderr_log, line.as_bytes()).await;
    emit_output(
        sys,
        step_id,
        OutputStream::Stderr,
        line.as_bytes(),
        direct_output_client,
        session_id,
    )
    .await;
}

/// Spawn a single-segment job with pipes (stdout/stderr piped, no PTY).
/// Used when `pty=false` is specified — the child cannot detect a terminal.
async fn spawn_single_pipe_step(
    step_id: StepId,
    pipeline: &cue_core::pipeline::Pipeline,
    snapshot: &EnvSnapshot,
    options: &ProcessStepOptions,
    sys: ActorSystem,
    cleanup_tx: mpsc::Sender<StepId>,
) -> Result<ProcessEntry, ()> {
    use tokio::io::AsyncReadExt;

    let segments = expand_pipeline_segments(step_id, pipeline, snapshot)?;
    let sandbox = prepare_step_sandbox_or_emit(step_id, snapshot, options, &sys).await?;
    let PreparedSpawn {
        program,
        args,
        command: mut cmd,
        settlement,
    } = prepare_spawn(
        &segments[0],
        0,
        PrepareSpawnOptions {
            snapshot,
            cwd_override: options.cwd_override.as_deref(),
            workspace_view: sandbox.as_ref(),
            wrapper_enabled: options.wrapper_enabled,
            spawn_adapter: options.spawn_adapter.as_ref(),
            sys: &sys,
        },
    )
    .await
    .map_err(|error| error!(%step_id, %error, "process_mgr: prepare spawn failed"))?;
    configure_process_group(&mut cmd);
    cmd.stdin(Stdio::null());
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());

    let mut child = match cmd.spawn() {
        Ok(child) => child,
        Err(error) => {
            log_spawn_failure(
                step_id,
                &program,
                &args,
                snapshot,
                options.cwd_override.as_deref(),
                &error,
            );
            settle_spawn_error(step_id, settlement.as_ref(), &error).await;
            return Err(());
        }
    };
    info!(%step_id, pid = ?child.id(), "process_mgr: pipe child spawned");

    let Some(mut stdout) = child.stdout.take() else {
        error!(%step_id, "process_mgr: spawned pipe child without stdout pipe");
        request_child_kill(step_id, &mut child, "missing stdout pipe");
        let (_, result) =
            wait_for_child_result(step_id, &mut child, "after missing stdout pipe").await;
        let empty_diagnostic = Arc::new(Mutex::new(RingBuffer::default()));
        settle_spawn_result(step_id, settlement.as_ref(), result, &empty_diagnostic).await;
        return Err(());
    };
    let Some(mut stderr) = child.stderr.take() else {
        error!(%step_id, "process_mgr: spawned pipe child without stderr pipe");
        request_child_kill(step_id, &mut child, "missing stderr pipe");
        let (_, result) =
            wait_for_child_result(step_id, &mut child, "after missing stderr pipe").await;
        let empty_diagnostic = Arc::new(Mutex::new(RingBuffer::default()));
        settle_spawn_result(step_id, settlement.as_ref(), result, &empty_diagnostic).await;
        return Err(());
    };

    let ring_buffer = Arc::new(Mutex::new(RingBuffer::default()));
    let stderr_ring = Arc::new(Mutex::new(RingBuffer::default()));
    let foreground = Arc::new(Mutex::new(ForegroundState::default()));
    let sys_clone = sys.clone();
    let ring_clone = ring_buffer.clone();
    let stderr_clone = stderr_ring.clone();
    let stderr_settlement = stderr_ring.clone();
    let foreground_clone = foreground.clone();
    let cleanup_tx_clone = cleanup_tx.clone();
    let direct_output_client = options.direct_output_client;
    let session_id = options.session_id.clone();
    let entry_session_id = session_id.clone();
    let (kill_tx, mut kill_rx) = mpsc::channel::<()>(1);

    // Read stdout and stderr concurrently, wait for exit.
    let log_file = open_output_log(step_id).await;
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
                        write_log(step_id, LogStream::Stdout, &log_clone, &chunk).await;
                        emit_output(
                            &sys_emit,
                            step_id,
                            OutputStream::Stdout,
                            &chunk,
                            direct_output_client,
                            stdout_session_id.as_deref(),
                        )
                        .await;
                    }
                    Err(error) => {
                        warn!(%step_id, err = %error, stream = "stdout", "process_mgr: pipe read failed");
                        break;
                    }
                }
            }
        });

        let stderr_log = open_stderr_log(step_id).await;
        let stderr_log = Arc::new(Mutex::new(stderr_log));
        let stderr_task = tokio::spawn(async move {
            let mut buf = vec![0u8; 8192];
            loop {
                match stderr.read(&mut buf).await {
                    Ok(0) => break,
                    Ok(n) => {
                        let chunk = buf[..n].to_vec();
                        stderr_clone.lock().unwrap().push(&chunk);
                        write_log(step_id, LogStream::Stderr, &stderr_log, &chunk).await;
                        emit_output(
                            &sys_stderr_emit,
                            step_id,
                            OutputStream::Stderr,
                            &chunk,
                            direct_output_client,
                            stderr_session_id.as_deref(),
                        )
                        .await;
                    }
                    Err(error) => {
                        warn!(%step_id, err = %error, stream = "stderr", "process_mgr: pipe read failed");
                        break;
                    }
                }
            }
        });

        let (exit_code, spawn_result, was_killed) = tokio::select! {
            exit = wait_for_child_exit_unreaped(&mut child) => {
                match exit {
                    Ok(()) => {
                        signal_owned_process_group(step_id, &child, "pipe child exit cleanup", true);
                    }
                    Err(error) => {
                        warn!(%step_id, err = %error, "process_mgr: cannot prove pipe process-group ownership; using direct-child fallback");
                        if let Err(kill_error) = child.start_kill() {
                            warn!(%step_id, err = %kill_error, "process_mgr: pipe child fallback kill failed");
                        }
                    }
                }
                let (code, result) = wait_for_child_result(step_id, &mut child, "after pipe child exit").await;
                (code, result, false)
            }
            _ = kill_rx.recv() => {
                request_child_kill(step_id, &mut child, "pipe kill requested");
                let (code, result) = wait_for_child_result(step_id, &mut child, "after pipe kill").await;
                (code, result, true)
            }
        };

        if let Err(error) = stdout_task.await {
            error!(%step_id, err = %error, stream = "stdout", "process_mgr: pipe reader task failed");
        }
        if let Err(error) = stderr_task.await {
            error!(%step_id, err = %error, stream = "stderr", "process_mgr: pipe reader task failed");
        }
        info!(%step_id, exit_code, "process_mgr: pipe child exited");

        let adapter_settled = settle_spawn_result(
            step_id,
            settlement.as_ref(),
            spawn_result,
            &stderr_settlement,
        )
        .await;

        let (reported_exit_code, fg_reason) = if was_killed {
            (EXIT_CODE_UNAVAILABLE, "killed".to_string())
        } else if !adapter_settled {
            (
                EXIT_CODE_UNAVAILABLE,
                "spawn adapter infrastructure failure".to_string(),
            )
        } else {
            (exit_code, format!("exit {exit_code}"))
        };
        emit_fg_exit(
            &sys_clone,
            &foreground_clone,
            step_id,
            &fg_reason,
            session_id.as_deref(),
        )
        .await;
        emit_step_finished(&sys_clone, step_id, reported_exit_code).await;
        notify_cleanup(&cleanup_tx_clone, step_id).await;
    });

    Ok(ProcessEntry {
        step_id,
        session_id: entry_session_id,
        reader_handle,
        kill_tx,
        ring_buffer,
        input: None,
        resize: None,
        foreground,
    })
}

async fn spawn_single_pty_step(
    step_id: StepId,
    pipeline: &cue_core::pipeline::Pipeline,
    snapshot: &EnvSnapshot,
    options: &ProcessStepOptions,
    sys: ActorSystem,
    cleanup_tx: mpsc::Sender<StepId>,
) -> Result<ProcessEntry, ()> {
    let segments = expand_pipeline_segments(step_id, pipeline, snapshot)?;
    let sandbox = prepare_step_sandbox_or_emit(step_id, snapshot, options, &sys).await?;
    let PreparedSpawn {
        program,
        args,
        command: mut cmd,
        settlement,
    } = prepare_spawn(
        &segments[0],
        0,
        PrepareSpawnOptions {
            snapshot,
            cwd_override: options.cwd_override.as_deref(),
            workspace_view: sandbox.as_ref(),
            wrapper_enabled: options.wrapper_enabled,
            spawn_adapter: options.spawn_adapter.as_ref(),
            sys: &sys,
        },
    )
    .await
    .map_err(|error| error!(%step_id, %error, "process_mgr: prepare spawn failed"))?;

    let pty_pair = crate::pty::open_pty().map_err(|error| {
        error!(%step_id, err = %error, "process_mgr: open pty failed");
    })?;
    let master_file = std::fs::File::from(pty_pair.master);
    let slave = pty_pair.slave;
    if let Err(error) = set_nonblocking(master_file.as_raw_fd()) {
        error!(%step_id, err = %error, "process_mgr: set pty nonblocking failed");
        return Err(());
    }
    if let Err(error) = set_winsize(slave.as_raw_fd(), DEFAULT_PTY_COLS, DEFAULT_PTY_ROWS) {
        warn!(%step_id, err = %error, "process_mgr: set initial pty size failed");
    }
    let reader_file = master_file.try_clone().map_err(|error| {
        error!(%step_id, err = %error, "process_mgr: clone pty reader failed");
    })?;
    let input_file = master_file.try_clone().map_err(|error| {
        error!(%step_id, err = %error, "process_mgr: clone pty input failed");
    })?;
    let resize_file = Arc::new(master_file.try_clone().map_err(|error| {
        error!(%step_id, err = %error, "process_mgr: clone pty resize failed");
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

    let mut child = match cmd.spawn() {
        Ok(child) => child,
        Err(error) => {
            log_spawn_failure(
                step_id,
                &program,
                &args,
                snapshot,
                options.cwd_override.as_deref(),
                &error,
            );
            settle_spawn_error(step_id, settlement.as_ref(), &error).await;
            return Err(());
        }
    };
    drop(slave);
    drop(master_file);

    info!(%step_id, pid = ?child.id(), "process_mgr: child spawned");

    let log_file = open_output_log(step_id).await;
    let input = match AsyncFd::new(input_file) {
        Ok(file) => Arc::new(file),
        Err(error) => {
            error!(%step_id, err = %error, "process_mgr: async pty input failed");
            request_child_kill(step_id, &mut child, "async pty input setup failed");
            let (_, result) =
                wait_for_child_result(step_id, &mut child, "after async pty input setup failure")
                    .await;
            let empty_diagnostic = Arc::new(Mutex::new(RingBuffer::default()));
            settle_spawn_result(step_id, settlement.as_ref(), result, &empty_diagnostic).await;
            return Err(());
        }
    };
    let reader = match AsyncFd::new(reader_file) {
        Ok(file) => file,
        Err(error) => {
            error!(%step_id, err = %error, "process_mgr: async pty reader failed");
            request_child_kill(step_id, &mut child, "async pty reader setup failed");
            let (_, result) =
                wait_for_child_result(step_id, &mut child, "after async pty reader setup failure")
                    .await;
            let empty_diagnostic = Arc::new(Mutex::new(RingBuffer::default()));
            settle_spawn_result(step_id, settlement.as_ref(), result, &empty_diagnostic).await;
            return Err(());
        }
    };

    let (kill_tx, kill_rx) = mpsc::channel::<()>(1);
    let ring_buffer = Arc::new(Mutex::new(RingBuffer::default()));
    let foreground = Arc::new(Mutex::new(ForegroundState::default()));
    let direct_output_client = options.direct_output_client;
    let reader_handle = tokio::spawn(reader_task(PtyReaderTask {
        step_id,
        child,
        sandbox,
        reader,
        log_file,
        kill_rx,
        ring: ring_buffer.clone(),
        settlement,
        runtime: ProcessTaskRuntime {
            sys: sys.clone(),
            foreground: foreground.clone(),
            direct_output_client,
            session_id: options.session_id.clone(),
            cleanup_tx: cleanup_tx.clone(),
        },
    }));

    Ok(ProcessEntry {
        step_id,
        session_id: options.session_id.clone(),
        reader_handle,
        kill_tx,
        ring_buffer,
        input: Some(JobInput::Pty(input)),
        resize: Some(resize_file),
        foreground,
    })
}

async fn spawn_native_pipeline_step(
    step_id: StepId,
    pipeline: &cue_core::pipeline::Pipeline,
    snapshot: &EnvSnapshot,
    options: &ProcessStepOptions,
    sys: ActorSystem,
    cleanup_tx: mpsc::Sender<StepId>,
) -> Result<ProcessEntry, ()> {
    let segments = expand_pipeline_segments(step_id, pipeline, snapshot)?;
    let sandbox = prepare_step_sandbox_or_emit(step_id, snapshot, options, &sys).await?;
    let NativePipelineSpawn {
        children,
        settlements,
        input,
        stdout_sources,
        stderr_sources,
    } = spawn_native_pipeline(
        step_id,
        &segments,
        snapshot,
        NativePipelineOptions {
            cwd_override: options.cwd_override.as_deref(),
            sandbox: sandbox.as_ref(),
            wrapper_enabled: options.wrapper_enabled,
            spawn_adapter: options.spawn_adapter.as_ref(),
            capture_stdin: options.pty_enabled,
            sys: &sys,
        },
    )
    .await?;

    let pids: Vec<u32> = children
        .iter()
        .filter_map(tokio::process::Child::id)
        .collect();
    info!(%step_id, ?pids, "process_mgr: native pipeline spawned");

    let log_file = open_output_log(step_id).await;
    let stderr_log = open_stderr_log(step_id).await;
    let (kill_tx, kill_rx) = mpsc::channel::<()>(1);
    let ring_buffer = Arc::new(Mutex::new(RingBuffer::default()));
    let stderr_ring = Arc::new(Mutex::new(RingBuffer::default()));
    let foreground = Arc::new(Mutex::new(ForegroundState::default()));
    let direct_output_client = options.direct_output_client;
    let reader_handle = tokio::spawn(pipeline_reader_task(PipelineReaderTask {
        step_id,
        children,
        settlements,
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

    Ok(ProcessEntry {
        step_id,
        session_id: options.session_id.clone(),
        reader_handle,
        kill_tx,
        ring_buffer,
        input,
        resize: None,
        foreground,
    })
}

async fn spawn_native_pipeline(
    step_id: StepId,
    segments: &[ExpandedSegment],
    snapshot: &EnvSnapshot,
    options: NativePipelineOptions<'_>,
) -> Result<NativePipelineSpawn, ()> {
    spawn_native_pipeline_with_hook(step_id, segments, snapshot, options, |_| Ok(())).await
}

async fn spawn_native_pipeline_with_hook(
    step_id: StepId,
    segments: &[ExpandedSegment],
    snapshot: &EnvSnapshot,
    options: NativePipelineOptions<'_>,
    mut before_segment: impl FnMut(usize) -> Result<(), ()>,
) -> Result<NativePipelineSpawn, ()> {
    let mut children = Vec::with_capacity(segments.len());
    let mut settlements = Vec::with_capacity(segments.len());
    let mut stdout_sources = Vec::new();
    let mut stderr_sources = Vec::new();
    let mut input = None;
    let mut next_stdin = None;

    for (idx, segment) in segments.iter().enumerate() {
        if before_segment(idx).is_err() {
            cleanup_partial_pipeline_spawn(step_id, children, settlements).await;
            return Err(());
        }
        let prepared = match prepare_spawn(
            segment,
            idx,
            PrepareSpawnOptions {
                snapshot,
                cwd_override: options.cwd_override,
                workspace_view: options.sandbox,
                wrapper_enabled: options.wrapper_enabled,
                spawn_adapter: options.spawn_adapter,
                sys: options.sys,
            },
        )
        .await
        {
            Ok(prepared) => prepared,
            Err(error) => {
                error!(%step_id, segment = idx, %error, "process_mgr: prepare spawn failed");
                cleanup_partial_pipeline_spawn(step_id, children, settlements).await;
                return Err(());
            }
        };
        let PreparedSpawn {
            program,
            args,
            command: mut cmd,
            settlement,
        } = prepared;
        configure_process_group(&mut cmd);

        let spawn_result = (|| -> Result<tokio::process::Child, ()> {
            if idx == 0 {
                if options.capture_stdin {
                    cmd.stdin(Stdio::piped());
                } else {
                    cmd.stdin(Stdio::null());
                }
            } else if let Some(stdin) = next_stdin.take() {
                cmd.stdin(Stdio::from(stdin));
            } else {
                error!(%step_id, segment = idx, "process_mgr: missing pipeline stdin");
                return Err(());
            }

            match segment.pipe_to_next {
                Some(cue_core::pipeline::PipeOp::Stdout) => {
                    let (read_end, write_end) = create_pipe().map_err(|error| {
                        error!(%step_id, segment = idx, err = %error, "process_mgr: create stdout pipe failed");
                    })?;
                    cmd.stdout(Stdio::from(write_end));
                    cmd.stderr(Stdio::piped());
                    next_stdin = Some(read_end);
                }
                Some(cue_core::pipeline::PipeOp::StdoutStderr) => {
                    let (read_end, write_end) = create_pipe().map_err(|error| {
                        error!(%step_id, segment = idx, err = %error, "process_mgr: create stdout+stderr pipe failed");
                    })?;
                    let stderr_write = write_end.try_clone().map_err(|error| {
                        error!(%step_id, segment = idx, err = %error, "process_mgr: clone combined pipe failed");
                    })?;
                    cmd.stdout(Stdio::from(write_end));
                    cmd.stderr(Stdio::from(stderr_write));
                    next_stdin = Some(read_end);
                }
                Some(cue_core::pipeline::PipeOp::StderrOnly) => {
                    let (read_end, write_end) = create_pipe().map_err(|error| {
                        error!(%step_id, segment = idx, err = %error, "process_mgr: create stderr-only pipe failed");
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

            cmd.spawn().map_err(|error| {
                log_spawn_failure(
                    step_id,
                    &program,
                    &args,
                    snapshot,
                    options.cwd_override,
                    &error,
                );
            })
        })();
        let mut child = match spawn_result {
            Ok(child) => child,
            Err(()) => {
                if let Some(settlement) = settlement.as_ref()
                    && let Err(error) = settlement
                        .settle(
                            SpawnResult::SpawnError {
                                message: "failed to spawn prepared command".into(),
                            },
                            String::new(),
                            false,
                        )
                        .await
                {
                    error!(%step_id, segment = idx, %error, "process_mgr: settle spawn error failed");
                }
                cleanup_partial_pipeline_spawn(step_id, children, settlements).await;
                return Err(());
            }
        };
        if idx == 0 && options.capture_stdin {
            input = child
                .stdin
                .take()
                .map(|stdin| JobInput::Pipe(Arc::new(tokio::sync::Mutex::new(stdin))));
        }
        if let Some(stdout) = child.stdout.take() {
            stdout_sources.push(stdout);
        }
        if let Some(stderr) = child.stderr.take() {
            stderr_sources.push(stderr);
        }
        children.push(child);
        settlements.push(settlement);
    }

    Ok(NativePipelineSpawn {
        children,
        settlements,
        input,
        stdout_sources,
        stderr_sources,
    })
}

async fn cleanup_partial_pipeline_spawn(
    step_id: StepId,
    mut children: Vec<tokio::process::Child>,
    settlements: Vec<Option<PreparedAdapterSettlement>>,
) {
    terminate_children(step_id, &mut children).await;
    let (_, results) = wait_for_children_results(step_id, &mut children).await;
    settle_pipeline_results(step_id, &settlements, results, None).await;
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

/// Open (or create) the append-only log file for a step's output.
///
/// Runs on the blocking thread pool so filesystem syscalls do not stall the
/// Tokio runtime thread.
async fn open_output_log(step_id: StepId) -> Option<std::fs::File> {
    match tokio::task::spawn_blocking(move || {
        let dir = match crate::dirs::output_dir() {
            Ok(dir) => dir,
            Err(error) => {
                error!(%step_id, err = %error, "process_mgr: cannot resolve output dir");
                return None;
            }
        };
        if let Err(e) = crate::dirs::ensure_private_dir(&dir) {
            error!(%step_id, err = %e, "process_mgr: cannot create output dir");
            return None;
        }
        let path = dir.join(format!("{}.log", super::process_output_stem(step_id)));
        match crate::dirs::open_private_append(&path) {
            Ok(f) => Some(f),
            Err(e) => {
                error!(%step_id, path = %path.display(), err = %e, "process_mgr: open log file");
                None
            }
        }
    })
    .await
    {
        Ok(file) => file,
        Err(error) => {
            error!(%step_id, err = %error, "process_mgr: output log task failed");
            None
        }
    }
}

async fn open_stderr_log(step_id: StepId) -> Option<std::fs::File> {
    match tokio::task::spawn_blocking(move || {
        let dir = match crate::dirs::output_dir() {
            Ok(dir) => dir,
            Err(error) => {
                error!(%step_id, err = %error, "process_mgr: cannot resolve output dir");
                return None;
            }
        };
        if let Err(e) = crate::dirs::ensure_private_dir(&dir) {
            error!(%step_id, err = %e, "process_mgr: cannot create output dir");
            return None;
        }
        let path = dir.join(format!("{}.stderr", super::process_output_stem(step_id)));
        match crate::dirs::open_private_append(&path) {
            Ok(f) => Some(f),
            Err(e) => {
                error!(%step_id, path = %path.display(), err = %e, "process_mgr: open stderr log");
                None
            }
        }
    })
    .await
    {
        Ok(file) => file,
        Err(error) => {
            error!(%step_id, err = %error, "process_mgr: stderr log task failed");
            None
        }
    }
}

async fn clear_step_logs(step_id: StepId) {
    if let Err(error) = tokio::task::spawn_blocking(move || {
        let dir = match crate::dirs::output_dir() {
            Ok(dir) => dir,
            Err(error) => {
                warn!(%step_id, err = %error, "process_mgr: cannot resolve output dir for cleanup");
                return;
            }
        };
        let stem = super::process_output_stem(step_id);
        for suffix in [".log", ".stderr"] {
            let path = dir.join(format!("{stem}{suffix}"));
            if let Err(error) = std::fs::remove_file(&path)
                && error.kind() != std::io::ErrorKind::NotFound
            {
                warn!(
                    %step_id,
                    path = %path.display(),
                    err = %error,
                    "process_mgr: failed to remove stale output log"
                );
            }
        }
    })
    .await
    {
        warn!(%step_id, err = %error, "process_mgr: stale output log cleanup task failed");
    }
}

/// Background task that reads PTY output, populates the ring buffer,
/// writes to the log file, emits events, and waits for the child to exit.
async fn reader_task(task: PtyReaderTask) {
    let PtyReaderTask {
        step_id,
        mut child,
        sandbox,
        reader,
        log_file,
        mut kill_rx,
        ring,
        settlement,
        runtime,
    } = task;

    // Wrap the log file so it can be shared with `spawn_blocking`.
    let _sandbox = sandbox;
    let log_file = Arc::new(Mutex::new(log_file));
    let mut pty_buf = vec![0u8; 8192];
    let mut pty_done = false;
    let mut child_exit_observed = false;
    let mut child_exit_poll = tokio::time::interval(std::time::Duration::from_millis(25));
    child_exit_poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            _ = child_exit_poll.tick(), if !child_exit_observed => {
                match child_exit_pending_without_reaping(&child) {
                    Ok(true) => {
                        signal_owned_process_group(
                            step_id,
                            &child,
                            "PTY child exit cleanup",
                            true,
                        );
                        child_exit_observed = true;
                    }
                    Ok(false) => {}
                    Err(error) => {
                        warn!(%step_id, err = %error, "process_mgr: failed to poll PTY child exit");
                    }
                }
            }
            // Kill signal from the main actor loop.
            _ = kill_rx.recv() => {
                info!(%step_id, "process_mgr: sending SIGKILL to step process group");
                request_child_kill(step_id, &mut child, "kill requested");

                // The group has already received SIGKILL. Keep a bounded wait
                // only for reaping the direct child and releasing its handle.
                let timeout = tokio::time::sleep(std::time::Duration::from_secs(10));
                let spawn_result = tokio::select! {
                    status = child.wait() => {
                        let result = match status {
                            Ok(status) => spawn_result_from_status(status),
                            Err(error) => {
                                error!(%step_id, err = %error, "process_mgr: wait after kill failed");
                                SpawnResult::SpawnError { message: error.to_string() }
                            }
                        };
                        debug!(%step_id, result = ?result, "process_mgr: child reaped after SIGKILL");
                        result
                    }
                    () = timeout => {
                        warn!(%step_id, "process_mgr: child was not reaped within 10 s of SIGKILL; dropping handle");
                        drop(child);
                        SpawnResult::SpawnError { message: "timed out reaping killed process".into() }
                    }
                };
                emit_fg_exit(
                    &runtime.sys,
                    &runtime.foreground,
                    step_id,
                    "killed",
                    runtime.session_id.as_deref(),
                )
                .await;
                emit_step_finished(
                    &runtime.sys,
                    step_id,
                    EXIT_CODE_UNAVAILABLE,
                )
                .await;
                let _ = settle_spawn_result(
                    step_id,
                    settlement.as_ref(),
                    spawn_result,
                    &ring,
                )
                .await;
                // Tell the main loop to remove our entry.
                notify_cleanup(&runtime.cleanup_tx, step_id).await;
                return;
            }

            result = read_pty(&reader, &mut pty_buf), if !pty_done => {
                match result {
                    Ok(0) => { pty_done = true; }
                    Ok(n) => {
                        let chunk = &pty_buf[..n];
                        let foreground_recipients =
                            record_pty_output(&ring, &runtime.foreground, chunk);
                        write_log(step_id, LogStream::Stdout, &log_file, chunk).await;
                        emit_output(
                            &runtime.sys,
                            step_id,
                            OutputStream::Stdout,
                            chunk,
                            runtime.direct_output_client,
                            runtime.session_id.as_deref(),
                        )
                        .await;
                        emit_fg_output(
                            &runtime.sys,
                            foreground_recipients,
                            step_id,
                            chunk,
                            runtime.session_id.as_deref(),
                        )
                        .await;
                    }
                    Err(e) => {
                        if e.raw_os_error() == Some(libc::EIO) {
                            pty_done = true;
                        } else {
                            debug!(%step_id, err = %e, "process_mgr: pty read error");
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

    // Wait for exit status while still honoring late kill requests. Observe
    // natural exit without reaping first so the owned session/process-group id
    // cannot be reused before descendant cleanup.
    let (exit_code, spawn_result, was_killed) = if child_exit_observed {
        let (code, result) =
            wait_for_child_result(step_id, &mut child, "after observed PTY child exit").await;
        (code, result, false)
    } else {
        tokio::select! {
            exit = wait_for_child_exit_unreaped(&mut child) => {
                match exit {
                    Ok(()) => {
                        signal_owned_process_group(
                            step_id,
                            &child,
                            "PTY child exit cleanup",
                            true,
                        );
                    }
                    Err(error) => {
                        warn!(%step_id, err = %error, "process_mgr: cannot prove PTY process-group ownership; using direct-child fallback");
                        if let Err(kill_error) = child.start_kill() {
                            warn!(%step_id, err = %kill_error, "process_mgr: PTY child fallback kill failed");
                        }
                    }
                }
                let (code, result) = wait_for_child_result(step_id, &mut child, "after PTY child exit").await;
                (code, result, false)
            }
            _ = kill_rx.recv() => {
                request_child_kill(step_id, &mut child, "late kill requested");
                let (code, result) = wait_for_child_result(step_id, &mut child, "after late kill").await;
                (code, result, true)
            }
        }
    };

    let ring_len = ring.lock().unwrap().len();
    info!(%step_id, exit_code, bytes = ring_len, "process_mgr: child exited");
    let adapter_settled =
        settle_spawn_result(step_id, settlement.as_ref(), spawn_result, &ring).await;

    if was_killed {
        emit_fg_exit(
            &runtime.sys,
            &runtime.foreground,
            step_id,
            "killed",
            runtime.session_id.as_deref(),
        )
        .await;
        emit_step_finished(&runtime.sys, step_id, EXIT_CODE_UNAVAILABLE).await;
    } else {
        let exit_code = if adapter_settled {
            exit_code
        } else {
            EXIT_CODE_UNAVAILABLE
        };
        let reason = if exit_code == 0 {
            "done".to_string()
        } else {
            format!("exit {exit_code}")
        };
        emit_fg_exit(
            &runtime.sys,
            &runtime.foreground,
            step_id,
            &reason,
            runtime.session_id.as_deref(),
        )
        .await;

        emit_step_finished(&runtime.sys, step_id, exit_code).await;
    }

    // Tell the main loop to remove our entry.
    notify_cleanup(&runtime.cleanup_tx, step_id).await;
}

async fn pipeline_reader_task(task: PipelineReaderTask) {
    let PipelineReaderTask {
        step_id,
        mut children,
        settlements,
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
        spawn_pipeline_stream_reader(
            step_id,
            stdout,
            PipelineStreamKind::Stdout,
            chunk_tx.clone(),
        );
    }
    for stderr in stderr_sources {
        active_readers += 1;
        spawn_pipeline_stream_reader(
            step_id,
            stderr,
            PipelineStreamKind::Stderr,
            chunk_tx.clone(),
        );
    }
    drop(chunk_tx);

    let mut was_killed = false;
    let mut cleaned_groups = vec![false; children.len()];
    let mut child_exit_poll = tokio::time::interval(std::time::Duration::from_millis(25));
    child_exit_poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    while active_readers > 0 {
        tokio::select! {
            _ = child_exit_poll.tick() => {
                cleanup_exited_process_groups(step_id, &children, &mut cleaned_groups);
            }
            _ = kill_rx.recv(), if !was_killed => {
                was_killed = true;
                info!(%step_id, "process_mgr: killing native pipeline");
                terminate_children(step_id, &mut children).await;
            }
            Some(msg) = chunk_rx.recv() => {
                match msg {
                    PipelineReaderMsg::Chunk { kind: PipelineStreamKind::Stdout, data } => {
                        ring.lock().unwrap().push(&data);
                        write_log(step_id, LogStream::Stdout, &log_file, &data).await;
                        emit_output(
                            &runtime.sys,
                            step_id,
                            OutputStream::Stdout,
                            &data,
                            runtime.direct_output_client,
                            runtime.session_id.as_deref(),
                        )
                        .await;
                    }
                    PipelineReaderMsg::Chunk { kind: PipelineStreamKind::Stderr, data } => {
                        stderr_ring.lock().unwrap().push(&data);
                        write_log(step_id, LogStream::Stderr, &stderr_log, &data).await;
                        emit_output(
                            &runtime.sys,
                            step_id,
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

    let (exit_code, results) = if was_killed {
        wait_for_children_results(step_id, &mut children).await
    } else {
        tokio::select! {
            _ = kill_rx.recv() => {
                was_killed = true;
                terminate_children(step_id, &mut children).await;
                wait_for_children_results(step_id, &mut children).await
            }
            result = wait_for_children_results(step_id, &mut children) => result,
        }
    };
    let adapter_settled =
        settle_pipeline_results(step_id, &settlements, results, Some(&stderr_ring)).await;
    let exit_code = if adapter_settled {
        exit_code
    } else {
        EXIT_CODE_UNAVAILABLE
    };

    let stdout_len = ring.lock().unwrap().len();
    let stderr_len = stderr_ring.lock().unwrap().len();
    info!(%step_id, exit_code, stdout_bytes = stdout_len, stderr_bytes = stderr_len, "process_mgr: native pipeline exited");

    if was_killed {
        emit_fg_exit(
            &runtime.sys,
            &runtime.foreground,
            step_id,
            "killed",
            runtime.session_id.as_deref(),
        )
        .await;
        emit_step_finished(&runtime.sys, step_id, EXIT_CODE_UNAVAILABLE).await;
    } else {
        let reason = if exit_code == 0 {
            "done".to_string()
        } else {
            format!("exit {exit_code}")
        };
        emit_fg_exit(
            &runtime.sys,
            &runtime.foreground,
            step_id,
            &reason,
            runtime.session_id.as_deref(),
        )
        .await;
        emit_step_finished(&runtime.sys, step_id, exit_code).await;
    }

    notify_cleanup(&runtime.cleanup_tx, step_id).await;
}

fn spawn_pipeline_stream_reader<R>(
    step_id: StepId,
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
                            %step_id,
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
                %step_id,
                stream = ?kind,
                "process_mgr: pipeline reader receiver closed before EOF"
            );
        }
    });
}

async fn notify_cleanup(cleanup_tx: &mpsc::Sender<StepId>, step_id: StepId) {
    if cleanup_tx.send(step_id).await.is_err() {
        debug!(%step_id, "process_mgr: cleanup channel closed");
    }
}

#[cfg(unix)]
fn child_exit_pending_without_reaping(child: &tokio::process::Child) -> std::io::Result<bool> {
    let pid = child
        .id()
        .and_then(|pid| libc::pid_t::try_from(pid).ok())
        .ok_or_else(|| std::io::Error::from_raw_os_error(libc::ECHILD))?;
    let mut info = std::mem::MaybeUninit::<libc::siginfo_t>::zeroed();
    loop {
        let result = unsafe {
            libc::waitid(
                libc::P_PID,
                pid as libc::id_t,
                info.as_mut_ptr(),
                libc::WEXITED | libc::WNOHANG | libc::WNOWAIT,
            )
        };
        if result == 0 {
            return Ok(unsafe { info.assume_init().si_pid() } != 0);
        }
        let error = std::io::Error::last_os_error();
        if error.raw_os_error() != Some(libc::EINTR) {
            return Err(error);
        }
    }
}

async fn wait_for_child_exit_unreaped(child: &mut tokio::process::Child) -> std::io::Result<()> {
    loop {
        #[cfg(unix)]
        if child_exit_pending_without_reaping(child)? {
            return Ok(());
        }
        #[cfg(not(unix))]
        if child.try_wait()?.is_some() {
            return Ok(());
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}

#[cfg(unix)]
fn signal_owned_process_group(
    step_id: StepId,
    child: &tokio::process::Child,
    reason: &str,
    leader_exited: bool,
) -> bool {
    let Some(pid) = child.id().and_then(|pid| libc::pid_t::try_from(pid).ok()) else {
        return false;
    };
    // Callers retain an unreaped direct child while signaling. Its PID cannot
    // be reused during that interval. The child became a process-group leader
    // via `setpgid(0, 0)` (or a session/group leader via `setsid()` for PTY),
    // and POSIX does not allow a process-group leader to join another group.
    // Consequently `pid` remains the unique owned PGID until this signal, even
    // after the direct child has become a zombie and `getpgid(pid)` reports
    // ESRCH on platforms such as macOS.
    let result = unsafe { libc::kill(-pid, libc::SIGKILL) };
    if result == 0 {
        return true;
    }
    let error = std::io::Error::last_os_error();
    if cfg!(target_os = "macos") && leader_exited && error.raw_os_error() == Some(libc::EPERM) {
        debug!(
            %step_id,
            pid,
            %reason,
            "process_mgr: exited process group has no remaining signalable members"
        );
        return true;
    }
    if error.raw_os_error() != Some(libc::ESRCH) {
        warn!(
            %step_id,
            pid,
            %reason,
            err = %error,
            "process_mgr: process-group signal failed"
        );
    }
    false
}

#[cfg(not(unix))]
fn signal_owned_process_group(
    _step_id: StepId,
    _child: &tokio::process::Child,
    _reason: &str,
    _leader_exited: bool,
) -> bool {
    false
}

fn cleanup_exited_process_groups(
    step_id: StepId,
    children: &[tokio::process::Child],
    cleaned: &mut [bool],
) {
    for (index, child) in children.iter().enumerate() {
        if cleaned.get(index).copied().unwrap_or(true) {
            continue;
        }
        match child_exit_pending_without_reaping(child) {
            Ok(true) => {
                signal_owned_process_group(step_id, child, "pipeline child exit cleanup", true);
                cleaned[index] = true;
            }
            Ok(false) => {}
            Err(error) => {
                warn!(
                    %step_id,
                    pid = ?child.id(),
                    err = %error,
                    "process_mgr: failed to poll pipeline child exit"
                );
            }
        }
    }
}

fn request_child_kill(step_id: StepId, child: &mut tokio::process::Child, reason: &str) {
    #[cfg(unix)]
    match child_exit_pending_without_reaping(child) {
        Ok(leader_exited) => {
            // `waitid(..., WNOWAIT)` proves that the daemon still owns an
            // unreaped direct child. Its numeric PID therefore cannot be
            // reused while we signal the process group established at spawn.
            if signal_owned_process_group(step_id, child, reason, leader_exited) {
                return;
            }
        }
        Err(error) => {
            warn!(
                %step_id,
                pid = ?child.id(),
                %reason,
                err = %error,
                "process_mgr: could not prove process-group ownership; falling back to child kill"
            );
        }
    }

    if let Err(error) = child.start_kill() {
        warn!(
            %step_id,
            pid = ?child.id(),
            %reason,
            err = %error,
            "process_mgr: child kill request failed"
        );
    }
}

async fn wait_for_child_result(
    step_id: StepId,
    child: &mut tokio::process::Child,
    reason: &str,
) -> (i32, SpawnResult) {
    match child.wait().await {
        Ok(status) => {
            let code = exit_code_from_status(status, EXIT_CODE_UNAVAILABLE);
            (code, spawn_result_from_status(status))
        }
        Err(error) => {
            error!(
                %step_id,
                %reason,
                err = %error,
                "process_mgr: child wait failed"
            );
            (
                EXIT_CODE_UNAVAILABLE,
                SpawnResult::SpawnError {
                    message: error.to_string(),
                },
            )
        }
    }
}

fn spawn_result_from_status(status: std::process::ExitStatus) -> SpawnResult {
    match status.signal() {
        Some(signal) => SpawnResult::Signaled { signal },
        None => SpawnResult::Exited {
            code: exit_code_from_status(status, EXIT_CODE_UNAVAILABLE),
        },
    }
}

async fn settle_spawn_error(
    step_id: StepId,
    settlement: Option<&PreparedAdapterSettlement>,
    error: &std::io::Error,
) {
    let Some(settlement) = settlement else {
        return;
    };
    if let Err(settle_error) = settlement
        .settle(
            SpawnResult::SpawnError {
                message: error.to_string(),
            },
            String::new(),
            false,
        )
        .await
    {
        error!(%step_id, error = %settle_error, "process_mgr: settle spawn error failed");
    }
}

async fn settle_spawn_result(
    step_id: StepId,
    settlement: Option<&PreparedAdapterSettlement>,
    result: SpawnResult,
    diagnostic: &Arc<Mutex<RingBuffer>>,
) -> bool {
    let Some(settlement) = settlement else {
        return true;
    };
    let (tail, truncated) = diagnostic
        .lock()
        .unwrap()
        .tail_with_truncation(cue_core::spawn_adapter::MAX_SPAWN_DIAGNOSTIC_BYTES);
    let diagnostic_tail = String::from_utf8_lossy(&tail).into_owned();
    match settlement.settle(result, diagnostic_tail, truncated).await {
        Ok(()) => true,
        Err(error) => {
            error!(%step_id, %error, "process_mgr: settle spawn failed");
            false
        }
    }
}

async fn terminate_children(step_id: StepId, children: &mut [tokio::process::Child]) {
    for child in children.iter_mut() {
        request_child_kill(step_id, child, "pipeline kill requested");
    }
}

async fn wait_for_children_results(
    step_id: StepId,
    children: &mut [tokio::process::Child],
) -> (i32, Vec<SpawnResult>) {
    let mut exit_code = EXIT_CODE_UNAVAILABLE;
    let mut results = Vec::with_capacity(children.len());
    let last_idx = children.len().saturating_sub(1);
    for (idx, child) in children.iter_mut().enumerate() {
        match wait_for_child_exit_unreaped(child).await {
            Ok(()) => {
                signal_owned_process_group(step_id, child, "pipeline child exit cleanup", true);
            }
            Err(error) => {
                warn!(
                    %step_id,
                    pid = ?child.id(),
                    err = %error,
                    "process_mgr: cannot prove pipeline process-group ownership; using direct-child fallback"
                );
                if let Err(kill_error) = child.start_kill() {
                    warn!(
                        %step_id,
                        pid = ?child.id(),
                        err = %kill_error,
                        "process_mgr: pipeline child fallback kill failed"
                    );
                }
            }
        }
        match child.wait().await {
            Ok(status) => {
                let code = exit_code_from_status(status, EXIT_CODE_UNAVAILABLE);
                if idx == last_idx {
                    exit_code = code;
                }
                results.push(spawn_result_from_status(status));
            }
            Err(error) => {
                error!(err = %error, "process_mgr: child wait failed");
                if idx == last_idx {
                    exit_code = EXIT_CODE_UNAVAILABLE;
                }
                results.push(SpawnResult::SpawnError {
                    message: error.to_string(),
                });
            }
        }
    }
    (exit_code, results)
}

async fn settle_pipeline_results(
    step_id: StepId,
    settlements: &[Option<PreparedAdapterSettlement>],
    results: Vec<SpawnResult>,
    diagnostic: Option<&Arc<Mutex<RingBuffer>>>,
) -> bool {
    if settlements.len() != results.len() {
        error!(
            %step_id,
            settlements = settlements.len(),
            results = results.len(),
            "process_mgr: spawn adapter settlement count mismatch"
        );
        return false;
    }

    let (diagnostic_tail, diagnostic_truncated) = diagnostic.map_or_else(
        || (String::new(), false),
        |ring| {
            let (tail, truncated) = ring
                .lock()
                .unwrap()
                .tail_with_truncation(cue_core::spawn_adapter::MAX_SPAWN_DIAGNOSTIC_BYTES);
            (String::from_utf8_lossy(&tail).into_owned(), truncated)
        },
    );
    let mut settled = true;
    for (segment_index, (settlement, result)) in settlements.iter().zip(results).enumerate() {
        let Some(settlement) = settlement else {
            continue;
        };
        if let Err(error) = settlement
            .settle(result, diagnostic_tail.clone(), diagnostic_truncated)
            .await
        {
            settled = false;
            error!(%step_id, segment = segment_index, %error, "process_mgr: settle pipeline segment failed");
        }
    }
    settled
}

async fn fail_pending_spawn(sys: &ActorSystem, step_id: StepId, _session_id: Option<&str>) {
    emit_step_finished(sys, step_id, EXIT_CODE_UNAVAILABLE).await;
}

async fn emit_step_finished(sys: &ActorSystem, step_id: cue_core::StepId, exit_code: i32) {
    if sys
        .execution
        .send(super::ExecutionCoordinatorMsg::StepFinished { step_id, exit_code })
        .await
        .is_err()
    {
        warn!(%step_id, exit_code, "process_mgr: execution coordinator channel closed while reporting step completion");
    }
}

async fn emit_output(
    sys: &ActorSystem,
    step_id: cue_core::StepId,
    stream: OutputStream,
    data: &[u8],
    direct_output_client: Option<u64>,
    session_id: Option<&str>,
) {
    let payload = EventPayload::OutputChunk {
        id: step_id,
        stream,
        data: data.to_vec(),
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
    publish_output_event(sys, payload, direct_output_client, session_id).await;
}

async fn publish_output_event(
    sys: &ActorSystem,
    payload: EventPayload,
    excluded_client_id: Option<u64>,
    session_id: Option<&str>,
) {
    let channel = EventChannel::Executions;
    if let Some(excluded_client_id) = excluded_client_id {
        publish_actor_session_event_except(
            "process_mgr",
            &sys.event_bus,
            channel,
            payload,
            session_id.map(str::to_owned),
            excluded_client_id,
        )
        .await;
    } else {
        publish_actor_session_event(
            "process_mgr",
            &sys.event_bus,
            channel,
            payload,
            session_id.map(str::to_owned),
        )
        .await;
    }
}

async fn emit_fg_output(
    sys: &ActorSystem,
    recipients: Vec<ForegroundRecipient>,
    step_id: StepId,
    data: &[u8],
    session_id: Option<&str>,
) {
    for recipient in recipients {
        send_actor_gateway_event(
            "process_mgr",
            sys,
            recipient.client_id,
            EventPayload::FgOutput {
                id: step_id,
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
    step_id: StepId,
    control_available: bool,
    session_id: Option<&str>,
) {
    for recipient in recipients {
        send_actor_gateway_event(
            "process_mgr",
            sys,
            recipient.client_id,
            EventPayload::FgControlChanged {
                id: step_id,
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
    step_id: StepId,
    reason: &str,
    session_id: Option<&str>,
) {
    let recipients = {
        let mut foreground = foreground.lock().unwrap();
        if foreground.closed {
            return;
        }
        foreground.closed = true;
        foreground.controller = None;
        std::mem::take(&mut foreground.observers)
    };
    for (client_id, attachment_id) in recipients {
        send_actor_gateway_event(
            "process_mgr",
            sys,
            client_id,
            EventPayload::FgExited {
                id: step_id,
                attachment_id,
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
    step_id: StepId,
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
                %step_id,
                stream = stream.label(),
                err = %error,
                "process_mgr: failed to write output log"
            );
        }
        Err(error) => {
            error!(
                %step_id,
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

    fn step(execution: u64) -> StepId {
        StepId {
            execution: cue_core::ExecutionId(execution),
            index: 1,
        }
    }

    fn process_options() -> ProcessStepOptions {
        ProcessStepOptions {
            cwd_override: None,
            sandbox: None,
            wrapper_enabled: false,
            pty_enabled: true,
            direct_output_client: None,
            session_id: None,
            spawn_adapter: None,
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
        let env = BTreeMap::from([
            ("PWD".into(), "/assignment-must-not-win".into()),
            ("USER".into(), "overridden".into()),
        ]);

        configure_command(&mut cmd, &snapshot, &env, Some(&cwd), None);

        assert_eq!(cmd.as_std().get_current_dir(), Some(cwd.as_path()));
        let pwd = cmd
            .as_std()
            .get_envs()
            .find_map(|(key, value)| (key == "PWD").then_some(value))
            .flatten();
        assert_eq!(pwd, Some(cwd.as_os_str()));
        let user = cmd
            .as_std()
            .get_envs()
            .find_map(|(key, value)| (key == "USER").then_some(value))
            .flatten();
        assert_eq!(user, Some(std::ffi::OsStr::new("overridden")));
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
            sessions: scheduler_tx,
            execution: mpsc::channel(1).0,
            triggers: mpsc::channel(1).0,
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
        let step_id = step(404);
        options.session_id = Some("SS-sandbox".into());
        options.sandbox = Some(crate::sandbox::SandboxConfig {
            mode: crate::sandbox::SandboxMode::Overlay,
            upper: Some(crate::sandbox::SandboxUpper::Directory(PathBuf::from(
                "/tmp/cue:bad-upper",
            ))),
        });

        let result = spawn_single_pipe_step(
            step_id,
            &cue_core::pipeline::Pipeline {
                segments: vec![cue_core::pipeline::PipeSegment {
                    env: BTreeMap::new(),
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
                assert_eq!(id, step(404));
                assert_eq!(stream, OutputStream::Stderr);
                let data = String::from_utf8(data).unwrap();
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
    async fn emit_output_preserves_non_utf8_bytes() {
        let (gateway_tx, _gateway_rx) = mpsc::channel(super::super::ACTOR_CHANNEL_CAP);
        let (scheduler_tx, _scheduler_rx) = mpsc::channel(super::super::ACTOR_CHANNEL_CAP);
        let (process_tx, _process_rx) = mpsc::channel(super::super::ACTOR_CHANNEL_CAP);
        let (scope_tx, _scope_rx) = mpsc::channel(super::super::ACTOR_CHANNEL_CAP);
        let (event_tx, mut event_rx) = mpsc::channel(super::super::ACTOR_CHANNEL_CAP);
        let sys = ActorSystem {
            gateway: gateway_tx,
            sessions: scheduler_tx,
            execution: mpsc::channel(1).0,
            triggers: mpsc::channel(1).0,
            process_mgr: process_tx,
            scope_store: scope_tx,
            event_bus: event_tx,
            config: crate::config::Config::default(),
            resources: std::sync::Arc::new(crate::resource::ProviderRegistry::empty()),
        };

        emit_output(
            &sys,
            step(7),
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
                payload: EventPayload::OutputChunk { id, stream, data },
            } => {
                assert_eq!(channel, EventChannel::Executions);
                assert_eq!(session_id.as_deref(), Some("SS-output"));
                assert_eq!(id, step(7));
                assert_eq!(stream, OutputStream::Stdout);
                assert_eq!(data, b"\xffbin\n");
            }
            _ => panic!("expected output event"),
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
            sessions: scheduler_tx,
            execution: mpsc::channel(1).0,
            triggers: mpsc::channel(1).0,
            process_mgr: process_tx,
            scope_store: scope_tx,
            event_bus: event_tx,
            config: crate::config::Config::default(),
            resources: std::sync::Arc::new(crate::resource::ProviderRegistry::empty()),
        };

        emit_output(
            &sys,
            step(7),
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
                assert_eq!(id, step(7));
                assert_eq!(stream, OutputStream::Stdout);
                assert_eq!(data, b"script\n");
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
                assert_eq!(channel, EventChannel::Executions);
                assert_eq!(session_id.as_deref(), Some("SS-script"));
                assert_eq!(excluded_client_id, 42);
                assert_eq!(id, step(7));
                assert_eq!(data, b"script\n");
            }
            _ => panic!("expected output chunk published to other subscribers"),
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
            sessions: scheduler_tx,
            execution: mpsc::channel(1).0,
            triggers: mpsc::channel(1).0,
            process_mgr: process_tx,
            scope_store: scope_tx,
            event_bus: event_tx,
            config: crate::config::Config::default(),
            resources: std::sync::Arc::new(crate::resource::ProviderRegistry::empty()),
        };
        let foreground = Arc::new(Mutex::new(ForegroundState {
            observers: BTreeMap::from([(42, 7), (43, 9)]),
            controller: Some(42),
            last_attachment_id: 9,
            closed: false,
        }));
        let ring = Arc::new(Mutex::new(RingBuffer::default()));

        let recipients = record_pty_output(&ring, &foreground, b"prompt");
        emit_fg_output(&sys, recipients, step(8), b"prompt", Some("SS-fg")).await;
        let recipients = foreground.lock().unwrap().recipients();
        emit_fg_control_changed(&sys, recipients, step(8), false, Some("SS-fg")).await;
        emit_fg_exit(&sys, &foreground, step(8), "done", Some("SS-fg")).await;

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
                    assert_eq!(id, step(8));
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
                    assert_eq!(id, step(8));
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
                    assert_eq!(id, step(8));
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
        let entry = ProcessEntry {
            step_id: step(9),
            session_id: Some("SS-shared".into()),
            reader_handle: tokio::spawn(async {}),
            kill_tx,
            ring_buffer: ring_buffer.clone(),
            input: None,
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
                    env: BTreeMap::new(),
                    command: vec!["printf".into(), "%s".into(), "hello world".into()],
                    pipe_to_next: Some(cue_core::pipeline::PipeOp::Stdout),
                },
                cue_core::pipeline::PipeSegment {
                    env: BTreeMap::new(),
                    command: vec!["grep".into(), "hello world".into()],
                    pipe_to_next: None,
                },
            ],
        };

        let snapshot = snapshot();
        let segments =
            expand_pipeline_segments(step(7), &pipeline, &snapshot).expect("expanded segments");

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
                    env: BTreeMap::new(),
                    command: vec!["producer".into(), "semi;colon".into()],
                    pipe_to_next: Some(cue_core::pipeline::PipeOp::StderrOnly),
                },
                cue_core::pipeline::PipeSegment {
                    env: BTreeMap::new(),
                    command: vec!["consumer".into()],
                    pipe_to_next: None,
                },
            ],
        };

        let snapshot = snapshot();
        let segments =
            expand_pipeline_segments(step(9), &pipeline, &snapshot).expect("expanded segments");

        assert_eq!(segments[0].args, vec!["semi;colon"]);
        assert!(matches!(
            segments[0].pipe_to_next,
            Some(cue_core::pipeline::PipeOp::StderrOnly)
        ));
    }

    #[tokio::test]
    async fn spawn_step_rejects_scope_without_snapshot() {
        let (gateway_tx, _gateway_rx) = mpsc::channel(super::super::ACTOR_CHANNEL_CAP);
        let (session_tx, _session_rx) = mpsc::channel(super::super::ACTOR_CHANNEL_CAP);
        let (execution_tx, mut execution_rx) = mpsc::channel(super::super::ACTOR_CHANNEL_CAP);
        let (process_tx, process_rx) = mpsc::channel(super::super::ACTOR_CHANNEL_CAP);
        let (scope_tx, mut scope_rx) = mpsc::channel(super::super::ACTOR_CHANNEL_CAP);
        let (event_tx, _event_rx) = mpsc::channel(super::super::ACTOR_CHANNEL_CAP);
        let sys = ActorSystem {
            gateway: gateway_tx,
            sessions: session_tx,
            execution: execution_tx,
            triggers: mpsc::channel(1).0,
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

        let step_id = StepId {
            execution: cue_core::ExecutionId(77),
            index: 1,
        };
        process_tx
            .send(ProcessMgrMsg::SpawnStep {
                step_id,
                pipeline: cue_core::pipeline::Pipeline {
                    segments: vec![cue_core::pipeline::PipeSegment {
                        env: BTreeMap::new(),
                        command: vec!["echo".into(), "should-not-run".into()],
                        pipe_to_next: None,
                    }],
                },
                scope_hash: cue_core::ScopeHash([9; 32]),
                options: Box::new(ProcessStepOptions {
                    cwd_override: None,
                    sandbox: None,
                    wrapper_enabled: false,
                    pty_enabled: false,
                    direct_output_client: None,
                    session_id: None,
                    spawn_adapter: None,
                }),
            })
            .await
            .expect("send spawn job");

        let msg = tokio::time::timeout(std::time::Duration::from_secs(1), execution_rx.recv())
            .await
            .expect("job failure should be reported")
            .expect("execution channel should stay open");
        match msg {
            super::super::ExecutionCoordinatorMsg::StepFinished {
                step_id: finished,
                exit_code,
            } => {
                assert_eq!(finished, step_id);
                assert_eq!(exit_code, EXIT_CODE_UNAVAILABLE);
            }
            _ => panic!("expected StepFinished"),
        }
    }

    #[tokio::test]
    async fn kill_single_pipe_step_stops_child_and_reports_finished() {
        let cwd = make_temp_dir();
        let (gateway_tx, _gateway_rx) = mpsc::channel(super::super::ACTOR_CHANNEL_CAP);
        let (session_tx, _session_rx) = mpsc::channel(super::super::ACTOR_CHANNEL_CAP);
        let (execution_tx, mut execution_rx) = mpsc::channel(super::super::ACTOR_CHANNEL_CAP);
        let (process_tx, process_rx) = mpsc::channel(super::super::ACTOR_CHANNEL_CAP);
        let (scope_tx, mut scope_rx) = mpsc::channel(super::super::ACTOR_CHANNEL_CAP);
        let (event_tx, _event_rx) = mpsc::channel(super::super::ACTOR_CHANNEL_CAP);
        let sys = ActorSystem {
            gateway: gateway_tx,
            sessions: session_tx,
            execution: execution_tx,
            triggers: mpsc::channel(1).0,
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

        let step_id = StepId {
            execution: cue_core::ExecutionId(78),
            index: 1,
        };
        process_tx
            .send(ProcessMgrMsg::SpawnStep {
                step_id,
                pipeline: cue_core::pipeline::Pipeline {
                    segments: vec![cue_core::pipeline::PipeSegment {
                        env: BTreeMap::new(),
                        command: vec!["/bin/sleep".into(), "30".into()],
                        pipe_to_next: None,
                    }],
                },
                scope_hash: cue_core::ScopeHash([8; 32]),
                options: Box::new(ProcessStepOptions {
                    cwd_override: None,
                    sandbox: None,
                    wrapper_enabled: false,
                    pty_enabled: false,
                    direct_output_client: None,
                    session_id: None,
                    spawn_adapter: None,
                }),
            })
            .await
            .expect("send spawn job");

        let (reply_tx, reply_rx) = oneshot::channel();
        process_tx
            .send(ProcessMgrMsg::KillStep {
                step_id,
                reply: reply_tx,
            })
            .await
            .expect("send kill job");
        let kill_result = tokio::time::timeout(std::time::Duration::from_secs(2), reply_rx)
            .await
            .expect("kill reply")
            .expect("kill reply sender");
        assert_eq!(kill_result, Ok(()));

        let msg = tokio::time::timeout(std::time::Duration::from_secs(5), execution_rx.recv())
            .await
            .expect("job finished after kill")
            .expect("execution channel should stay open");
        match msg {
            super::super::ExecutionCoordinatorMsg::StepFinished {
                step_id: finished,
                exit_code,
            } => {
                assert_eq!(finished, step_id);
                assert_eq!(exit_code, EXIT_CODE_UNAVAILABLE);
            }
            _ => panic!("expected StepFinished"),
        }

        process_tx
            .send(ProcessMgrMsg::Shutdown)
            .await
            .expect("send process_mgr shutdown");
        std::fs::remove_dir_all(cwd).expect("remove temp dir");
    }

    #[cfg(unix)]
    struct FixtureDirectory(PathBuf);

    #[cfg(unix)]
    impl Drop for FixtureDirectory {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[cfg(unix)]
    struct ProcessGroupFixtureGuard {
        groups: Vec<(libc::pid_t, libc::pid_t)>,
        owner_markers: Vec<PathBuf>,
    }

    #[cfg(unix)]
    impl Drop for ProcessGroupFixtureGuard {
        fn drop(&mut self) {
            for (&(leader, descendant), marker) in self.groups.iter().zip(&self.owner_markers) {
                let marker = marker.to_string_lossy();
                let command_matches = |pid: libc::pid_t| {
                    std::process::Command::new("/bin/ps")
                        .args(["-p", &pid.to_string(), "-o", "command="])
                        .output()
                        .ok()
                        .filter(|output| output.status.success())
                        .is_some_and(|output| {
                            String::from_utf8_lossy(&output.stdout).contains(marker.as_ref())
                        })
                };
                let leader_owned =
                    command_matches(leader) && unsafe { libc::getpgid(leader) } == leader;
                let descendant_owned =
                    command_matches(descendant) && unsafe { libc::getpgid(descendant) } == leader;
                if leader_owned || descendant_owned {
                    unsafe {
                        libc::kill(-leader, libc::SIGKILL);
                    }
                }
            }
        }
    }

    #[cfg(unix)]
    async fn read_process_group_fixture(path: &Path) -> (libc::pid_t, libc::pid_t) {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if let Ok(raw) = std::fs::read_to_string(path) {
                    let mut parts = raw.trim().split(':');
                    if let (Some(leader), Some(descendant)) = (parts.next(), parts.next())
                        && let (Ok(leader), Ok(descendant)) = (
                            leader.parse::<libc::pid_t>(),
                            descendant.parse::<libc::pid_t>(),
                        )
                    {
                        break (leader, descendant);
                    }
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("process group fixture pid file")
    }

    #[cfg(unix)]
    async fn assert_processes_gone(groups: &[(libc::pid_t, libc::pid_t)]) {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if groups.iter().all(|&(leader, descendant)| {
                    [leader, descendant].into_iter().all(|pid| {
                        let result = unsafe { libc::kill(pid, 0) };
                        result == -1
                            && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH)
                    })
                }) {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("owned direct children and descendants should terminate");
    }

    #[cfg(unix)]
    async fn assert_job_stops_descendants(
        step_id: StepId,
        scope_byte: u8,
        pty_enabled: bool,
        pipeline_tail: bool,
        request_kill: bool,
    ) {
        let cwd = make_temp_dir();
        let _cwd_guard = FixtureDirectory(cwd.clone());
        let child_pid_path = cwd.join("child.pid");
        let script_path = cwd.join("spawn-child.sh");
        let tail_pid_path = cwd.join("tail-child.pid");
        let tail_script_path = cwd.join("spawn-tail-child.sh");
        let wait_line = if request_kill { "wait" } else { "exit 0" };
        let tail_output = if !request_kill && pty_enabled {
            "printf 'pty-tail-output'"
        } else {
            ""
        };
        let redirects = if request_kill {
            ""
        } else {
            " </dev/null >/dev/null 2>&1"
        };
        std::fs::write(
            &script_path,
            format!(
                "#!/bin/sh\n/bin/sh -c 'trap : TERM; /bin/sleep 30; :' '{}' {redirects} &\nchild=$!\nprintf '%s:%s' \"$$\" \"$child\" > '{}'\n{tail_output}\n{wait_line}\n",
                script_path.display(),
                child_pid_path.display()
            ),
        )
        .expect("write descendant fixture");
        if pipeline_tail {
            std::fs::write(
                &tail_script_path,
                format!(
                    "#!/bin/sh\n/bin/sh -c 'trap : TERM; /bin/sleep 30; :' '{}' {redirects} &\nchild=$!\nprintf '%s:%s' \"$$\" \"$child\" > '{}'\n{wait_line}\n",
                    tail_script_path.display(),
                    tail_pid_path.display()
                ),
            )
            .expect("write tail descendant fixture");
        }
        let (gateway_tx, _gateway_rx) = mpsc::channel(super::super::ACTOR_CHANNEL_CAP);
        let (session_tx, _session_rx) = mpsc::channel(super::super::ACTOR_CHANNEL_CAP);
        let (execution_tx, mut execution_rx) = mpsc::channel(super::super::ACTOR_CHANNEL_CAP);
        let (process_tx, process_rx) = mpsc::channel(super::super::ACTOR_CHANNEL_CAP);
        let (scope_tx, mut scope_rx) = mpsc::channel(super::super::ACTOR_CHANNEL_CAP);
        let (event_tx, mut event_rx) = mpsc::channel(super::super::ACTOR_CHANNEL_CAP);
        let sys = ActorSystem {
            gateway: gateway_tx,
            sessions: session_tx,
            execution: execution_tx,
            triggers: mpsc::channel(1).0,
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

        let mut segments = vec![cue_core::pipeline::PipeSegment {
            env: BTreeMap::new(),
            command: vec!["/bin/sh".into(), script_path.to_string_lossy().into_owned()],
            pipe_to_next: pipeline_tail.then_some(cue_core::pipeline::PipeOp::Stdout),
        }];
        if pipeline_tail {
            segments.push(cue_core::pipeline::PipeSegment {
                env: BTreeMap::new(),
                command: vec![
                    "/bin/sh".into(),
                    tail_script_path.to_string_lossy().into_owned(),
                ],
                pipe_to_next: None,
            });
        }
        process_tx
            .send(ProcessMgrMsg::SpawnStep {
                step_id,
                pipeline: cue_core::pipeline::Pipeline { segments },
                scope_hash: cue_core::ScopeHash([scope_byte; 32]),
                options: Box::new(ProcessStepOptions {
                    cwd_override: None,
                    sandbox: None,
                    wrapper_enabled: false,
                    pty_enabled,
                    direct_output_client: None,
                    session_id: None,
                    spawn_adapter: None,
                }),
            })
            .await
            .expect("send spawn job");

        let mut groups = vec![read_process_group_fixture(&child_pid_path).await];
        if pipeline_tail {
            groups.push(read_process_group_fixture(&tail_pid_path).await);
        }
        let _guard = ProcessGroupFixtureGuard {
            groups: groups.clone(),
            owner_markers: if pipeline_tail {
                vec![script_path.clone(), tail_script_path.clone()]
            } else {
                vec![script_path.clone()]
            },
        };
        if request_kill {
            for &(leader, descendant) in &groups {
                assert_eq!(
                    unsafe { libc::getpgid(leader) },
                    leader,
                    "direct child should remain leader of its owned process group"
                );
                assert_eq!(
                    unsafe { libc::getpgid(descendant) },
                    leader,
                    "descendant should inherit the direct child's process group"
                );
            }
            if pipeline_tail {
                assert_ne!(
                    groups[0].0, groups[1].0,
                    "native pipeline segments should own distinct process groups"
                );
            }
        }

        if request_kill {
            let (reply_tx, reply_rx) = oneshot::channel();
            process_tx
                .send(ProcessMgrMsg::KillStep {
                    step_id,
                    reply: reply_tx,
                })
                .await
                .expect("send kill job");
            tokio::time::timeout(std::time::Duration::from_secs(2), reply_rx)
                .await
                .expect("kill reply")
                .expect("kill reply sender")
                .expect("kill job");
        }
        let finished = tokio::time::timeout(std::time::Duration::from_secs(5), execution_rx.recv())
            .await
            .expect("job finished after process-group cleanup")
            .expect("execution channel should stay open");
        assert!(matches!(
            finished,
            super::super::ExecutionCoordinatorMsg::StepFinished {
                step_id: StepId { execution, .. },
                ..
            } if execution == step_id.execution
        ));

        assert_processes_gone(&groups).await;
        if !request_kill && pty_enabled {
            let mut output = String::new();
            while let Ok(message) = event_rx.try_recv() {
                if let super::super::EventBusMsg::PublishSession {
                    payload: EventPayload::OutputChunk { id, data, .. },
                    ..
                } = message
                    && id == step_id
                {
                    output.push_str(&String::from_utf8_lossy(&data));
                }
            }
            assert!(
                output.contains("pty-tail-output"),
                "PTY tail output should be drained after parent exit: {output:?}"
            );
        }

        process_tx
            .send(ProcessMgrMsg::Shutdown)
            .await
            .expect("send process_mgr shutdown");
        std::fs::remove_dir_all(cwd).expect("remove temp dir");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn kill_pipe_step_stops_descendants_in_the_job_process_group() {
        assert_job_stops_descendants(step(79), 9, false, false, true).await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn kill_pty_step_stops_descendants_in_the_job_session() {
        assert_job_stops_descendants(step(80), 10, true, false, true).await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn kill_native_pipeline_stops_descendants_in_each_process_group() {
        assert_job_stops_descendants(step(81), 11, false, true, true).await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn normal_parent_exit_cleans_detached_descendant_before_reaping() {
        assert_job_stops_descendants(step(82), 12, false, false, false).await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn normal_pty_parent_exit_cleans_detached_descendant_before_reaping() {
        assert_job_stops_descendants(step(83), 13, true, false, false).await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn normal_pipeline_exit_cleans_each_segments_detached_descendant() {
        assert_job_stops_descendants(step(84), 14, false, true, false).await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn partial_pipeline_spawn_failure_cleans_started_process_groups() {
        let cwd = make_temp_dir();
        let _cwd_guard = FixtureDirectory(cwd.clone());
        let pid_path = cwd.join("partial-child.pid");
        let script_path = cwd.join("partial-child.sh");
        std::fs::write(
            &script_path,
            format!(
                "#!/bin/sh\n/bin/sh -c 'trap : TERM; /bin/sleep 30; :' '{}' &\nchild=$!\nprintf '%s:%s' \"$$\" \"$child\" > '{}'\nwait\n",
                script_path.display(),
                pid_path.display()
            ),
        )
        .expect("write partial-spawn descendant fixture");
        let pipeline = cue_core::pipeline::Pipeline {
            segments: vec![
                cue_core::pipeline::PipeSegment {
                    env: BTreeMap::new(),
                    command: vec!["/bin/sh".into(), script_path.to_string_lossy().into_owned()],
                    pipe_to_next: Some(cue_core::pipeline::PipeOp::Stdout),
                },
                cue_core::pipeline::PipeSegment {
                    env: BTreeMap::new(),
                    command: vec!["/bin/cat".into()],
                    pipe_to_next: None,
                },
            ],
        };
        let snapshot = snapshot();
        let segments =
            expand_pipeline_segments(step(85), &pipeline, &snapshot).expect("expand pipeline");
        let (gateway_tx, _gateway_rx) = mpsc::channel(super::super::ACTOR_CHANNEL_CAP);
        let (scheduler_tx, _scheduler_rx) = mpsc::channel(super::super::ACTOR_CHANNEL_CAP);
        let (process_tx, _process_rx) = mpsc::channel(super::super::ACTOR_CHANNEL_CAP);
        let (scope_tx, _scope_rx) = mpsc::channel(super::super::ACTOR_CHANNEL_CAP);
        let (event_tx, _event_rx) = mpsc::channel(super::super::ACTOR_CHANNEL_CAP);
        let sys = ActorSystem {
            gateway: gateway_tx,
            sessions: scheduler_tx,
            execution: mpsc::channel(1).0,
            triggers: mpsc::channel(1).0,
            process_mgr: process_tx,
            scope_store: scope_tx,
            event_bus: event_tx,
            config: crate::config::Config::default(),
            resources: std::sync::Arc::new(crate::resource::ProviderRegistry::empty()),
        };
        let mut observed = None;
        let result = spawn_native_pipeline_with_hook(
            step(85),
            &segments,
            &snapshot,
            NativePipelineOptions {
                cwd_override: Some(&cwd),
                sandbox: None,
                wrapper_enabled: false,
                spawn_adapter: None,
                capture_stdin: false,
                sys: &sys,
            },
            |index| {
                if index == 1 {
                    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
                    while std::time::Instant::now() < deadline {
                        if let Ok(raw) = std::fs::read_to_string(&pid_path) {
                            let mut parts = raw.trim().split(':');
                            if let (Some(leader), Some(descendant)) = (parts.next(), parts.next())
                                && let (Ok(leader), Ok(descendant)) = (
                                    leader.parse::<libc::pid_t>(),
                                    descendant.parse::<libc::pid_t>(),
                                )
                            {
                                observed = Some((leader, descendant));
                                return Err(());
                            }
                        }
                        std::thread::sleep(std::time::Duration::from_millis(10));
                    }
                    panic!(
                        "first pipeline segment did not start descendant before injected failure"
                    );
                }
                Ok(())
            },
        )
        .await;
        assert!(result.is_err(), "second segment failure should abort spawn");
        let group = observed.expect("first segment process group");
        let _guard = ProcessGroupFixtureGuard {
            groups: vec![group],
            owner_markers: vec![script_path],
        };
        assert_processes_gone(&[group]).await;
    }

    #[tokio::test]
    async fn write_log_persists_exact_output_bytes() {
        let dir = make_temp_dir();
        let path = dir.join("E42-S1.log");
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .expect("open log file");
        let file = Arc::new(Mutex::new(Some(file)));

        write_log(step(42), LogStream::Stdout, &file, b"hello\n").await;
        write_log(step(42), LogStream::Stdout, &file, b"world").await;

        drop(file);
        assert_eq!(
            std::fs::read(&path).expect("read log file"),
            b"hello\nworld"
        );
        std::fs::remove_dir_all(dir).expect("remove temp dir");
    }
}
