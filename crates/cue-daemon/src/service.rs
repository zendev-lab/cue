//! IPC v4 daemon service.
//!
//! A connection binds a `ClientId` through `Hello`; commands then use the strict
//! operation ledger while queries remain side-effect free.

use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use cue_core::{EventId, ExecutionId, ScopeHash, StepId};
use cue_core::{
    Execution, ExecutionProjection, ExecutionSnapshot, ExecutionState, Fact, FactDraft, FactEvent,
    OutputStream, RunCompletion, Scope, StepAction, StepFailure, StepState,
};
use cue_protocol::{
    AttachmentId, Capability, ClientId, Command, EventPayload, ExecutionView, Hello,
    MAX_MESSAGE_SIZE, Message, OperationId, OutputChunk, OutputRange, PROTOCOL_VERSION,
    ProtocolErrorCode, PtyRole, Query, RequestId, ResponsePayload, ResultPayload, decode_message,
    encode_message,
};
use cue_runtime::{
    Composition, ExecutionStore, LocalProcessSpawner, MemoryOutputStore, OutputAppend, OutputSlice,
    OutputStore, ProcessSpawner, ProviderBundle, ProviderId, ProviderRegistry, ProviderSpec,
    RunControl, RunExit, RuntimeAssembly, RuntimeError, RuntimeErrorKind, RuntimeFuture,
    ScopeDurability, ScopeStore, SpawnRequest, TerminalSize, canonical_port_specs,
    runtime_root_ports,
};
use cue_store_sqlite::{
    ExecutionCommandCommit, OperationCommit, OperationRecord, ScopeCommandCommit, Store, StoreError,
};

const OUTPUT_READ_LIMIT: usize = 16 * 1024 * 1024;

/// SQLite is the authoritative store; unsupported Sensitive data is rejected.
struct StoreProvider {
    durable: Mutex<Store>,
    events: tokio::sync::broadcast::Sender<FactEvent>,
    output_events: tokio::sync::broadcast::Sender<LiveOutput>,
}

#[derive(Clone)]
struct LiveOutput {
    step: StepId,
    stream: OutputStream,
    offset: u64,
    data: Vec<u8>,
}

impl StoreProvider {
    fn new(store: Store) -> Self {
        let (events, _) = tokio::sync::broadcast::channel(1024);
        let (output_events, _) = tokio::sync::broadcast::channel(1024);
        Self {
            durable: Mutex::new(store),
            events,
            output_events,
        }
    }

    fn lock_store(&self) -> Result<std::sync::MutexGuard<'_, Store>, RuntimeError> {
        self.durable.lock().map_err(|_| lock_error("SQLite store"))
    }

    fn load_scope(&self, hash: ScopeHash) -> Result<Option<Scope>, RuntimeError> {
        ScopeStore::get(self, hash)
    }
    fn load_execution(&self, id: ExecutionId) -> Result<Option<ExecutionProjection>, RuntimeError> {
        ExecutionStore::get(self, id)
    }

    fn next_execution_id(&self) -> Result<ExecutionId, RuntimeError> {
        let maximum = self
            .list(None, 1)?
            .first()
            .map(|execution| execution.snapshot.id.0)
            .unwrap_or(0);
        maximum
            .checked_add(1)
            .map(ExecutionId)
            .ok_or_else(|| RuntimeError::infrastructure("execution ID space exhausted"))
    }

    fn record_durable_operation(
        &self,
        client: &ClientId,
        operation: &OperationId,
        command: &Command,
        response: &ResponsePayload,
        completed_at_ms: i64,
    ) -> Result<OperationOutcome, RuntimeError> {
        Ok(
            match self
                .lock_store()?
                .record_operation(client, operation, command, Some(response), completed_at_ms)
                .map_err(store_error)?
            {
                OperationRecord::Inserted => OperationOutcome::Inserted(Vec::new()),
                OperationRecord::Replay {
                    response: Some(response),
                } => OperationOutcome::Replay(response),
                OperationRecord::Replay { response: None } => OperationOutcome::Expired,
                OperationRecord::Conflict { .. } => OperationOutcome::Conflict,
            },
        )
    }

    fn commit_submission(
        &self,
        client: &ClientId,
        operation: &OperationId,
        command: &Command,
        execution: &ExecutionProjection,
        facts: &[FactDraft],
        response: &ResponsePayload,
    ) -> Result<OperationOutcome, RuntimeError> {
        Ok(
            match self
                .lock_store()?
                .commit_execution_command(
                    OperationCommit {
                        client,
                        operation,
                        command,
                        response: Some(response),
                        completed_at_ms: execution.updated_at_ms,
                    },
                    execution,
                    facts,
                )
                .map_err(store_error)?
            {
                ExecutionCommandCommit::Committed { facts } => OperationOutcome::Inserted(facts),
                ExecutionCommandCommit::Replay {
                    response: Some(response),
                } => OperationOutcome::Replay(response),
                ExecutionCommandCommit::Replay { response: None } => OperationOutcome::Expired,
                ExecutionCommandCommit::Conflict { .. } => OperationOutcome::Conflict,
            },
        )
    }

    fn commit_scope_operation(
        &self,
        client: &ClientId,
        operation: &OperationId,
        command: &Command,
        scope: &Scope,
        response: &ResponsePayload,
        completed_at_ms: i64,
    ) -> Result<(OperationOutcome, ScopeDurability), RuntimeError> {
        let outcome = match self
            .lock_store()?
            .commit_scope_command(
                OperationCommit {
                    client,
                    operation,
                    command,
                    response: Some(response),
                    completed_at_ms,
                },
                scope,
            )
            .map_err(store_error)?
        {
            ScopeCommandCommit::Committed => OperationOutcome::Inserted(Vec::new()),
            ScopeCommandCommit::Replay {
                response: Some(response),
            } => OperationOutcome::Replay(response),
            ScopeCommandCommit::Replay { response: None } => OperationOutcome::Expired,
            ScopeCommandCommit::Conflict { .. } => OperationOutcome::Conflict,
        };
        Ok((outcome, ScopeDurability::Durable))
    }

    fn publish(&self, events: &[FactEvent]) {
        for event in events {
            let _ = self.events.send(event.clone());
        }
    }
}

enum OperationOutcome {
    Inserted(Vec<FactEvent>),
    Replay(ResponsePayload),
    Conflict,
    Expired,
}

impl ExecutionStore for StoreProvider {
    fn get(&self, id: ExecutionId) -> Result<Option<ExecutionProjection>, RuntimeError> {
        self.lock_store()?.get_execution(id).map_err(store_error)
    }
    fn list(
        &self,
        before: Option<ExecutionId>,
        limit: u16,
    ) -> Result<Vec<ExecutionProjection>, RuntimeError> {
        self.lock_store()?
            .list_executions(before, limit)
            .map_err(store_error)
    }
    fn commit(
        &self,
        execution: &ExecutionProjection,
        facts: &[FactDraft],
    ) -> Result<Vec<FactEvent>, RuntimeError> {
        self.lock_store()?
            .commit_execution(execution, facts)
            .map_err(store_error)
    }
    fn facts_after(
        &self,
        execution: ExecutionId,
        after: Option<EventId>,
        limit: u16,
    ) -> Result<Vec<FactEvent>, RuntimeError> {
        self.lock_store()?
            .facts_after(execution, after, limit)
            .map_err(store_error)
    }
}

impl ScopeStore for StoreProvider {
    fn put(&self, scope: &Scope, created_at_ms: i64) -> Result<ScopeDurability, RuntimeError> {
        self.lock_store()?
            .put_scope(scope, created_at_ms)
            .map_err(store_error)?;
        Ok(ScopeDurability::Durable)
    }
    fn get(&self, hash: ScopeHash) -> Result<Option<Scope>, RuntimeError> {
        self.lock_store()?.get_scope(hash).map_err(store_error)
    }
}

struct FactingOutputStore {
    output: MemoryOutputStore,
    executions: Arc<StoreProvider>,
}

impl FactingOutputStore {
    fn new(executions: Arc<StoreProvider>) -> Self {
        Self {
            output: MemoryOutputStore::default(),
            executions,
        }
    }
}

impl OutputStore for FactingOutputStore {
    fn append(
        &self,
        step: StepId,
        stream: OutputStream,
        data: &[u8],
    ) -> Result<OutputAppend, RuntimeError> {
        let (append, committed) = {
            let store = self.executions.lock_store()?;
            let mut execution = store
                .get_execution(step.execution)
                .map_err(store_error)?
                .ok_or_else(|| RuntimeError::infrastructure("output execution disappeared"))?;
            let append = self.output.append(step, stream, data)?;
            execution.updated_at_ms = now_ms().max(execution.updated_at_ms);
            let committed = store
                .commit_execution(
                    &execution,
                    &[FactDraft {
                        occurred_at_ms: execution.updated_at_ms,
                        fact: Fact::OutputAppended {
                            step,
                            stream,
                            start_offset: append.start_offset,
                            end_offset: append.end_offset,
                        },
                    }],
                )
                .map_err(store_error)?;
            (append, committed)
        };
        self.executions.publish(&committed);
        let _ = self.executions.output_events.send(LiveOutput {
            step,
            stream,
            offset: append.start_offset,
            data: data.to_vec(),
        });
        Ok(append)
    }

    fn read(
        &self,
        step: StepId,
        stream: OutputStream,
        offset: u64,
        maximum: usize,
    ) -> Result<OutputSlice, RuntimeError> {
        self.output.read(step, stream, offset, maximum)
    }

    fn tail(
        &self,
        step: StepId,
        stream: OutputStream,
        maximum: usize,
    ) -> Result<OutputSlice, RuntimeError> {
        self.output.tail(step, stream, maximum)
    }
}

fn build_runtime(
    store: Arc<StoreProvider>,
) -> Result<(Arc<RuntimeAssembly>, Arc<FactingOutputStore>), RuntimeError> {
    let output = Arc::new(FactingOutputStore::new(store.clone()));
    let spawner: Arc<dyn ProcessSpawner> = Arc::new(LocalProcessSpawner::new(output.clone()));
    let provider = ProviderId::new("cue-daemon-local")
        .map_err(|error| RuntimeError::infrastructure(error.to_string()))?;
    let mut composition = Composition::new();
    for port in canonical_port_specs() {
        composition
            .register_port(port)
            .map_err(|error| RuntimeError::infrastructure(error.to_string()))?;
    }
    composition
        .register_provider(
            ProviderSpec::new(
                provider.clone(),
                env!("CARGO_PKG_VERSION"),
                [
                    cue_runtime::RuntimePort::ExecutionStore.port_id(),
                    cue_runtime::RuntimePort::ScopeStore.port_id(),
                    cue_runtime::RuntimePort::OutputStore.port_id(),
                    cue_runtime::RuntimePort::ProcessSpawner.port_id(),
                ],
            )
            .map_err(|error| RuntimeError::infrastructure(error.to_string()))?,
        )
        .map_err(|error| RuntimeError::infrastructure(error.to_string()))?;
    let assembly = composition
        .resolve(runtime_root_ports())
        .map_err(|error| RuntimeError::infrastructure(error.to_string()))?;
    let mut registry = ProviderRegistry::default();
    registry
        .insert(
            provider,
            ProviderBundle {
                execution_store: Some(store.clone()),
                scope_store: Some(store),
                output_store: Some(output.clone()),
                process_spawner: Some(spawner),
                ..ProviderBundle::default()
            },
        )
        .map_err(|error| RuntimeError::infrastructure(error.to_string()))?;
    let runtime = RuntimeAssembly::bind(assembly, registry)
        .map_err(|error| RuntimeError::infrastructure(error.to_string()))?;
    Ok((Arc::new(runtime), output))
}

pub struct DaemonService {
    store: Arc<StoreProvider>,
    runtime: Arc<RuntimeAssembly>,
    output: Arc<FactingOutputStore>,
    tasks: tokio::sync::Mutex<BTreeMap<ExecutionId, Arc<ExecutionTask>>>,
    attachments: tokio::sync::Mutex<BTreeMap<AttachmentId, Attachment>>,
    next_attachment: AtomicU64,
    lifecycle: tokio::sync::broadcast::Sender<LifecycleSignal>,
    draining: std::sync::atomic::AtomicBool,
    lifecycle_outcomes: Mutex<BTreeMap<(String, String), LifecycleSignal>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LifecycleSignal {
    Shutdown,
    Restart {
        restart_id: String,
        target_instance_id: String,
    },
}

struct ExecutionTask {
    id: ExecutionId,
    state: tokio::sync::Mutex<TaskState>,
    controls: tokio::sync::Mutex<BTreeMap<StepId, RunControl>>,
    changed: tokio::sync::Notify,
}

struct TaskState {
    execution: Execution,
    created_at_ms: i64,
    updated_at_ms: i64,
}

struct Attachment {
    client: ClientId,
    step: StepId,
    role: PtyRole,
}

impl DaemonService {
    pub fn in_memory() -> Result<Arc<Self>, RuntimeError> {
        Self::from_store(Store::in_memory().map_err(store_error)?)
    }

    pub fn from_store(store: Store) -> Result<Arc<Self>, RuntimeError> {
        let store = Arc::new(StoreProvider::new(store));
        let (runtime, output) = build_runtime(store.clone())?;
        let (lifecycle, _) = tokio::sync::broadcast::channel(16);
        Ok(Arc::new(Self {
            store,
            runtime,
            output,
            tasks: tokio::sync::Mutex::new(BTreeMap::new()),
            attachments: tokio::sync::Mutex::new(BTreeMap::new()),
            next_attachment: AtomicU64::new(1),
            lifecycle,
            draining: std::sync::atomic::AtomicBool::new(false),
            lifecycle_outcomes: Mutex::new(BTreeMap::new()),
        }))
    }

    /// Stop accepting new submissions and drain owned attempts before releasing the host lock.
    pub async fn drain(self: &Arc<Self>) -> Result<(), RuntimeError> {
        self.draining.store(true, Ordering::Release);
        let tasks = self
            .tasks
            .lock()
            .await
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for mode in [cue_core::CancelMode::Graceful, cue_core::CancelMode::Force] {
            for task in &tasks {
                let mut state = task.state.lock().await;
                if state.execution.state().is_terminal() {
                    continue;
                }
                let mut candidate = state.execution.clone();
                let transition = candidate.cancel(mode);
                self.commit_transition(&mut state, candidate, &transition)?;
                drop(state);
                self.schedule_runtime(task.clone())?;
            }
            let drained = tokio::time::timeout(std::time::Duration::from_secs(5), async {
                for task in &tasks {
                    self.wait_execution(task.id).await?;
                }
                Ok::<(), RuntimeError>(())
            })
            .await;
            if let Ok(result) = drained {
                return result;
            }
        }
        Err(RuntimeError::new(
            RuntimeErrorKind::Conflict,
            "cannot prove all Run attempts quiescent while draining",
        ))
    }

    pub fn connection(self: &Arc<Self>) -> DaemonConnection {
        DaemonConnection {
            pending_lifecycle: None,
            service: self.clone(),
            client: None,
            watched: BTreeMap::new(),
            pending_facts: VecDeque::new(),
        }
    }

    pub fn subscribe_facts(&self) -> tokio::sync::broadcast::Receiver<FactEvent> {
        self.store.events.subscribe()
    }

    pub fn subscribe_lifecycle(&self) -> tokio::sync::broadcast::Receiver<LifecycleSignal> {
        self.lifecycle.subscribe()
    }

    fn subscribe_output(&self) -> tokio::sync::broadcast::Receiver<LiveOutput> {
        self.store.output_events.subscribe()
    }

    /// Recover unstarted/replayable work only after exclusive host ownership.
    /// Unknown physical attempts reject recovery before any facts or spawn.
    pub async fn recover(self: &Arc<Self>) -> Result<(), RuntimeError> {
        self.store
            .lock_store()?
            .recover_runtime_work()
            .map_err(store_error)?;
        let projections = self.store.list(None, u16::MAX)?;
        for projection in projections {
            if projection.state.is_terminal() {
                continue;
            }
            let execution = Execution::restore(projection.snapshot.clone()).map_err(|error| {
                RuntimeError::new(RuntimeErrorKind::Conflict, error.to_string())
            })?;
            let task = Arc::new(ExecutionTask {
                id: projection.snapshot.id,
                state: tokio::sync::Mutex::new(TaskState {
                    execution,
                    created_at_ms: projection.created_at_ms,
                    updated_at_ms: projection.updated_at_ms,
                }),
                controls: tokio::sync::Mutex::new(BTreeMap::new()),
                changed: tokio::sync::Notify::new(),
            });
            self.tasks
                .lock()
                .await
                .insert(projection.snapshot.id, task.clone());
            self.clone().drive(task).await?;
        }
        Ok(())
    }

    async fn submit(
        self: &Arc<Self>,
        client: &ClientId,
        operation: &OperationId,
        command: &Command,
        spec: cue_core::ExecutionSpec,
    ) -> Result<ResponsePayload, RuntimeError> {
        if self.store.load_scope(spec.scope())?.is_none() {
            return Err(RuntimeError::new(
                RuntimeErrorKind::NotFound,
                format!("scope {} is not available", spec.scope()),
            ));
        }
        let mut tasks = self.tasks.lock().await;
        if self.draining.load(Ordering::Acquire) {
            return Err(RuntimeError::new(
                RuntimeErrorKind::Conflict,
                "daemon is draining",
            ));
        }
        let id = self.store.next_execution_id()?;
        let now = now_ms();
        let execution = Execution::new(id, spec);
        let projection = projection(&execution, now, now);
        let response = ResponsePayload::Ok(ResultPayload::ExecutionSubmitted {
            execution: Box::new(view(&projection)),
        });
        let created = FactDraft {
            occurred_at_ms: now,
            fact: Fact::ExecutionCreated {
                id,
                scope: projection.snapshot.spec.scope(),
            },
        };
        let committed = match self.store.commit_submission(
            client,
            operation,
            command,
            &projection,
            &[created],
            &response,
        )? {
            OperationOutcome::Replay(response) => return Ok(response),
            OperationOutcome::Conflict => return Err(operation_conflict()),
            OperationOutcome::Expired => return Err(operation_expired()),
            OperationOutcome::Inserted(facts) => facts,
        };
        let task = Arc::new(ExecutionTask {
            id,
            state: tokio::sync::Mutex::new(TaskState {
                execution,
                created_at_ms: now,
                updated_at_ms: now,
            }),
            controls: tokio::sync::Mutex::new(BTreeMap::new()),
            changed: tokio::sync::Notify::new(),
        });
        tasks.insert(id, task.clone());
        drop(tasks);
        self.store.publish(&committed);
        self.clone().drive(task).await?;
        Ok(response)
    }

    async fn drive(self: Arc<Self>, task: Arc<ExecutionTask>) -> Result<(), RuntimeError> {
        {
            let mut state = task.state.lock().await;
            let mut candidate = state.execution.clone();
            let transition = candidate.advance().map_err(reducer_error)?;
            if candidate != state.execution {
                self.commit_transition(&mut state, candidate, &transition)?;
            }
        }
        task.changed.notify_waiters();
        self.schedule_runtime(task)
    }

    fn schedule_runtime(self: &Arc<Self>, task: Arc<ExecutionTask>) -> Result<(), RuntimeError> {
        let steps = self
            .store
            .lock_store()?
            .pending_runtime_steps()
            .map_err(store_error)?;
        for step in steps.into_iter().filter(|step| step.execution == task.id) {
            let service = self.clone();
            let task = task.clone();
            tokio::spawn(async move {
                if let Err(error) = service.realize(task, step).await {
                    tracing::error!(%step, %error, "runtime follow-up stopped without asserting completion");
                }
            });
        }
        Ok(())
    }

    fn realize(
        self: Arc<Self>,
        task: Arc<ExecutionTask>,
        step: StepId,
    ) -> RuntimeFuture<Result<(), RuntimeError>> {
        Box::pin(async move {
            let spawned = {
                // Serialize the physical start boundary with cancellation commits.
                let mut state = task.state.lock().await;
                let generation = self
                    .store
                    .lock_store()?
                    .claim_runtime_step(step)
                    .map_err(store_error)?;
                let Some(generation) = generation else {
                    return Ok(());
                };
                let committed = self
                    .store
                    .load_execution(step.execution)?
                    .ok_or_else(|| RuntimeError::infrastructure("claimed execution disappeared"))?;
                let latest = Execution::restore(committed.snapshot).map_err(reducer_error)?;
                let record = latest
                    .step(step)
                    .ok_or_else(|| RuntimeError::infrastructure("claimed step disappeared"))?;
                let mut spawned = None;
                match record.state() {
                    StepState::Running => {
                        if !task.controls.lock().await.contains_key(&step) {
                            let input = record.input_scope().ok_or_else(|| {
                                RuntimeError::infrastructure("active step lacks scope")
                            })?;
                            let scope = self.store.load_scope(input)?.ok_or_else(|| {
                                RuntimeError::infrastructure("committed scope disappeared")
                            })?;
                            match latest.action(step).ok_or_else(|| {
                                RuntimeError::infrastructure("committed plan leaf disappeared")
                            })? {
                                StepAction::Builtin(command) => {
                                    let result =
                                        cue_runtime::realize_builtin(step, &command, &scope)
                                            .map_err(|error| StepFailure::Builtin {
                                                message: error.to_string(),
                                            });
                                    let mut candidate = latest;
                                    let transition = candidate
                                        .complete_builtin(step, &scope, result)
                                        .map_err(reducer_error)?;
                                    self.commit_transition(&mut state, candidate, &transition)?;
                                }
                                StepAction::Run { pipeline, io } => {
                                    if !self
                                        .store
                                        .lock_store()?
                                        .begin_run_attempt(step, generation)
                                        .map_err(store_error)?
                                    {
                                        return Err(RuntimeError::new(
                                            RuntimeErrorKind::Conflict,
                                            "Run start is stale or already owned",
                                        ));
                                    }
                                    match self
                                        .runtime
                                        .spawn(SpawnRequest {
                                            step,
                                            pipeline,
                                            io,
                                            scope,
                                        })
                                        .await
                                    {
                                        Ok(run) => {
                                            task.controls
                                                .lock()
                                                .await
                                                .insert(step, run.control.clone());
                                            spawned = Some(run);
                                        }
                                        Err(error) => {
                                            let mut candidate = latest;
                                            let transition = candidate
                                                .complete_run(
                                                    step,
                                                    RunCompletion::Failed(StepFailure::Spawn {
                                                        message: error.to_string(),
                                                    }),
                                                )
                                                .map_err(reducer_error)?;
                                            self.commit_transition(
                                                &mut state,
                                                candidate,
                                                &transition,
                                            )?;
                                        }
                                    }
                                }
                            }
                        }
                    }
                    StepState::Cancelling { mode, .. } => {
                        let control = task.controls.lock().await.get(&step).cloned();
                        if let Some(control) = control {
                            // A raced natural completion is still reported by its owner.
                            let _ = control.terminate(*mode).await;
                        } else {
                            if self
                                .store
                                .lock_store()?
                                .run_attempt_started(step)
                                .map_err(store_error)?
                            {
                                return Err(RuntimeError::new(
                                    RuntimeErrorKind::Conflict,
                                    "cannot cancel an unowned physical Run attempt",
                                ));
                            }
                            let mut candidate = latest;
                            let transition =
                                candidate.complete_cancelled(step).map_err(reducer_error)?;
                            self.commit_transition(&mut state, candidate, &transition)?;
                        }
                    }
                    _ => {}
                }
                self.store
                    .lock_store()?
                    .acknowledge_runtime_step(step, generation)
                    .map_err(store_error)?;
                spawned
            };
            task.changed.notify_waiters();
            self.schedule_runtime(task.clone())?;
            if let Some(run) = spawned {
                let result = run_result(run.wait().await)?;
                let mut state = task.state.lock().await;
                let mut candidate = state.execution.clone();
                let transition = candidate
                    .complete_run(step, result)
                    .map_err(reducer_error)?;
                self.commit_transition(&mut state, candidate, &transition)?;
                task.controls.lock().await.remove(&step);
                drop(state);
                task.changed.notify_waiters();
                self.schedule_runtime(task)?;
            }
            Ok(())
        })
    }

    fn commit_transition(
        &self,
        state: &mut TaskState,
        candidate: Execution,
        transition: &cue_core::ExecutionTransition,
    ) -> Result<(), RuntimeError> {
        let latest_time = self
            .store
            .load_execution(candidate.id())?
            .map(|stored| stored.updated_at_ms)
            .unwrap_or(state.updated_at_ms);
        let timestamp = now_ms().max(state.updated_at_ms).max(latest_time);
        for scope in &transition.new_scopes {
            self.runtime.scope_store().put(scope, timestamp)?;
        }
        let facts = transition_facts(
            &state.execution.snapshot(),
            &state.execution.state(),
            &candidate,
            timestamp,
        );
        let committed = self.store.commit(
            &projection(&candidate, state.created_at_ms, timestamp),
            &facts,
        )?;
        state.execution = candidate;
        state.updated_at_ms = timestamp;
        self.store.publish(&committed);
        Ok(())
    }

    async fn wait_execution(&self, id: ExecutionId) -> Result<ExecutionProjection, RuntimeError> {
        let mut changes = self.store.events.subscribe();
        loop {
            let Some(execution) = self.store.load_execution(id)? else {
                return Err(RuntimeError::new(
                    RuntimeErrorKind::NotFound,
                    format!("execution {id} was not found"),
                ));
            };
            if execution.state.is_terminal() {
                return Ok(execution);
            }
            match changes.recv().await {
                Ok(_) | Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    return Err(RuntimeError::infrastructure(
                        "execution event stream closed",
                    ));
                }
            }
        }
    }
}

pub struct DaemonConnection {
    pending_lifecycle: Option<LifecycleSignal>,
    service: Arc<DaemonService>,
    client: Option<ClientId>,
    watched: BTreeMap<ExecutionId, Option<EventId>>,
    pending_facts: VecDeque<FactEvent>,
}

impl DaemonConnection {
    fn response_flushed(&mut self) {
        if let Some(signal) = self.pending_lifecycle.take()
            && !self.service.draining.swap(true, Ordering::AcqRel)
        {
            let _ = self.service.lifecycle.send(signal);
        }
    }

    pub async fn handle(&mut self, message: Message) -> Message {
        match message {
            Message::Query { request_id, query } => {
                let payload = self
                    .handle_query(query)
                    .await
                    .unwrap_or_else(protocol_error);
                Message::Response {
                    request_id,
                    payload,
                }
            }
            Message::Command {
                request_id,
                operation_id,
                command,
            } => {
                let payload = self
                    .handle_command(operation_id, command)
                    .await
                    .unwrap_or_else(protocol_error);
                Message::Response {
                    request_id,
                    payload,
                }
            }
            Message::Response { request_id, .. } => Message::Response {
                request_id,
                payload: ResponsePayload::error(
                    ProtocolErrorCode::InvalidRequest,
                    "clients may send only query or command messages",
                ),
            },
            Message::Event { .. } => Message::Response {
                request_id: RequestId::new(1).expect("one is a valid request ID"),
                payload: ResponsePayload::error(
                    ProtocolErrorCode::InvalidRequest,
                    "clients may not send event messages",
                ),
            },
        }
    }

    async fn handle_query(&mut self, query: Query) -> Result<ResponsePayload, RuntimeError> {
        match query {
            Query::Hello(Hello {
                protocol_version,
                client_id,
            }) => {
                if protocol_version != PROTOCOL_VERSION {
                    return Err(RuntimeError::new(
                        RuntimeErrorKind::Unsupported,
                        format!(
                            "protocol {protocol_version} is unsupported; expected {PROTOCOL_VERSION}"
                        ),
                    ));
                }
                if self
                    .client
                    .as_ref()
                    .is_some_and(|bound| bound != &client_id)
                {
                    return Err(RuntimeError::new(
                        RuntimeErrorKind::Conflict,
                        "connection is already bound to a different client identity",
                    ));
                }
                self.client = Some(client_id);
                Ok(ResponsePayload::Ok(ResultPayload::Hello {
                    protocol_version: PROTOCOL_VERSION,
                    server_version: crate::version().into(),
                    instance_id: crate::daemon_instance_id().into(),
                    capabilities: vec![
                        Capability::OperationIdempotency,
                        Capability::EventReplay,
                        Capability::SharedPty,
                        Capability::GracefulRestart,
                    ],
                }))
            }
            query => {
                self.require_client()?;
                match query {
                    Query::Ping => Ok(ResponsePayload::ack()),
                    Query::GetScope { hash } => {
                        let scope = self.service.store.load_scope(hash)?.ok_or_else(|| {
                            RuntimeError::new(
                                RuntimeErrorKind::NotFound,
                                format!("scope {hash} was not found"),
                            )
                        })?;
                        Ok(ResponsePayload::Ok(ResultPayload::Scope {
                            hash,
                            scope: Box::new(scope),
                        }))
                    }
                    Query::GetExecution { id } => {
                        let execution =
                            self.service.store.load_execution(id)?.ok_or_else(|| {
                                RuntimeError::new(
                                    RuntimeErrorKind::NotFound,
                                    format!("execution {id} was not found"),
                                )
                            })?;
                        Ok(ResponsePayload::Ok(ResultPayload::Execution {
                            execution: Box::new(view(&execution)),
                        }))
                    }
                    Query::ListExecutions { before, limit } => {
                        let executions = self.service.store.list(before, limit)?;
                        let next_before = (executions.len() == usize::from(limit))
                            .then(|| executions.last().map(|item| item.snapshot.id))
                            .flatten();
                        Ok(ResponsePayload::Ok(ResultPayload::Executions {
                            executions: executions.iter().map(view).collect(),
                            next_before,
                        }))
                    }
                    Query::WaitExecution { id } => {
                        let execution = self.service.wait_execution(id).await?;
                        Ok(ResponsePayload::Ok(ResultPayload::Execution {
                            execution: Box::new(view(&execution)),
                        }))
                    }
                    Query::TailOutput {
                        step,
                        stream,
                        max_bytes,
                    } => {
                        let maximum = usize::try_from(max_bytes)
                            .unwrap_or(OUTPUT_READ_LIMIT)
                            .min(OUTPUT_READ_LIMIT);
                        let slice = self.service.output.tail(step, stream, maximum)?;
                        Ok(ResponsePayload::Ok(ResultPayload::Output {
                            chunks: vec![OutputChunk {
                                step,
                                stream,
                                offset: slice.offset,
                                data: slice.data,
                                eof: true,
                            }],
                        }))
                    }
                    Query::ReadOutput {
                        step,
                        stdout,
                        stderr,
                        terminal,
                    } => Ok(ResponsePayload::Ok(ResultPayload::Output {
                        chunks: [
                            (OutputStream::Stdout, stdout),
                            (OutputStream::Stderr, stderr),
                            (OutputStream::Terminal, terminal),
                        ]
                        .into_iter()
                        .filter_map(|(stream, range)| {
                            self.read_output(step, stream, range).transpose()
                        })
                        .collect::<Result<Vec<_>, _>>()?,
                    })),
                    Query::Hello(_) => unreachable!("hello handled before client requirement"),
                }
            }
        }
    }

    async fn handle_command(
        &mut self,
        operation: OperationId,
        command: Command,
    ) -> Result<ResponsePayload, RuntimeError> {
        let external = matches!(
            command,
            Command::DetachPty { .. }
                | Command::ClaimPtyControl { .. }
                | Command::ReleasePtyControl { .. }
                | Command::PtyInput { .. }
                | Command::PtyResize { .. }
        );
        if !external {
            return self.handle_command_effect(operation, command).await;
        }
        let client = self.require_client()?.clone();
        match self
            .service
            .store
            .lock_store()?
            .record_operation(&client, &operation, &command, None, now_ms())
            .map_err(store_error)?
        {
            OperationRecord::Inserted => {}
            OperationRecord::Replay {
                response: Some(response),
            } => return Ok(response),
            OperationRecord::Replay { response: None } => return Err(operation_expired()),
            OperationRecord::Conflict { .. } => return Err(operation_conflict()),
        }
        let response = self
            .handle_command_effect(operation.clone(), command.clone())
            .await
            .unwrap_or_else(protocol_error);
        if !self
            .service
            .store
            .lock_store()?
            .finish_claimed_operation(OperationCommit {
                client: &client,
                operation: &operation,
                command: &command,
                response: Some(&response),
                completed_at_ms: now_ms(),
            })
            .map_err(store_error)?
        {
            return Err(RuntimeError::infrastructure(
                "external operation claim was lost",
            ));
        }
        Ok(response)
    }

    async fn handle_command_effect(
        &mut self,
        operation: OperationId,
        command: Command,
    ) -> Result<ResponsePayload, RuntimeError> {
        let client = self.require_client()?.clone();
        match &command {
            Command::PutScope { scope } => {
                let hash = scope.compute_hash();
                let durable = true;
                let response = ResponsePayload::Ok(ResultPayload::ScopeStored { hash, durable });
                let (outcome, _) = self.service.store.commit_scope_operation(
                    &client,
                    &operation,
                    &command,
                    scope,
                    &response,
                    now_ms(),
                )?;
                match outcome {
                    OperationOutcome::Inserted(_) => Ok(response),
                    OperationOutcome::Replay(response) => Ok(response),
                    OperationOutcome::Conflict => Err(operation_conflict()),
                    OperationOutcome::Expired => Err(operation_expired()),
                }
            }
            Command::SubmitExecution { spec } => {
                self.service
                    .submit(&client, &operation, &command, spec.as_ref().clone())
                    .await
            }
            Command::CancelExecution { id, mode } => {
                self.cancel(&client, &operation, &command, *id, *mode).await
            }
            Command::WatchExecution { id, after_event } => {
                let execution = self.service.store.load_execution(*id)?.ok_or_else(|| {
                    RuntimeError::new(
                        RuntimeErrorKind::NotFound,
                        format!("execution {id} was not found"),
                    )
                })?;
                let replay = self
                    .service
                    .store
                    .facts_after(*id, *after_event, u16::MAX)?;
                let latest_event = replay.last().map(|event| event.id).or(*after_event);
                let response = ResponsePayload::Ok(ResultPayload::Watching {
                    execution: Box::new(view(&execution)),
                    latest_event,
                });
                let response = self.record_plain(&client, &operation, &command, &response)?;
                self.pending_facts.extend(replay);
                self.watched.insert(*id, latest_event);
                Ok(response)
            }
            Command::UnwatchExecution { id } => {
                let response = ResponsePayload::ack();
                let response = self.record_plain(&client, &operation, &command, &response)?;
                self.watched.remove(id);
                Ok(response)
            }
            Command::AttachPty { step, replay_bytes } => {
                self.attach_pty(&client, &operation, &command, *step, *replay_bytes)
                    .await
            }
            Command::DetachPty { attachment } => {
                let mut attachments = self.service.attachments.lock().await;
                require_attachment_owner(&attachments, *attachment, &client)?;
                attachments.remove(attachment);
                drop(attachments);
                let response = ResponsePayload::ack();
                Ok(response)
            }
            Command::ClaimPtyControl { attachment } => {
                self.set_control(&client, *attachment, true).await?;
                let response = ResponsePayload::ack();
                Ok(response)
            }
            Command::ReleasePtyControl { attachment } => {
                self.set_control(&client, *attachment, false).await?;
                let response = ResponsePayload::ack();
                Ok(response)
            }
            Command::PtyInput { attachment, data } => {
                let control = self.pty_control(&client, *attachment).await?;
                control.input(data.clone()).await?;
                let response = ResponsePayload::ack();
                Ok(response)
            }
            Command::PtyResize {
                attachment,
                cols,
                rows,
            } => {
                let control = self.pty_control(&client, *attachment).await?;
                control.resize(TerminalSize::new(*cols, *rows)?).await?;
                let response = ResponsePayload::ack();
                Ok(response)
            }
            Command::Restart => {
                let restart_id = uuid::Uuid::new_v4().to_string();
                let target_instance_id = uuid::Uuid::new_v4().to_string();
                let response = ResponsePayload::Ok(ResultPayload::RestartAccepted {
                    restart_id: restart_id.clone(),
                    target_instance_id: target_instance_id.clone(),
                });
                self.record_lifecycle(
                    &client,
                    &operation,
                    &command,
                    response,
                    LifecycleSignal::Restart {
                        restart_id,
                        target_instance_id,
                    },
                )
            }
            Command::Shutdown => self.record_lifecycle(
                &client,
                &operation,
                &command,
                ResponsePayload::ack(),
                LifecycleSignal::Shutdown,
            ),
        }
    }

    fn record_lifecycle(
        &mut self,
        client: &ClientId,
        operation: &OperationId,
        command: &Command,
        response: ResponsePayload,
        signal: LifecycleSignal,
    ) -> Result<ResponsePayload, RuntimeError> {
        // Keep unflushed outcomes across connections of this host. A successor
        // must not act on replayed lifecycle commands belonging to an older host.
        let mut outcomes = self
            .service
            .lifecycle_outcomes
            .lock()
            .map_err(|_| RuntimeError::infrastructure("lifecycle outcomes lock poisoned"))?;
        let key = (client.as_str().to_owned(), operation.as_str().to_owned());
        match self.service.store.record_durable_operation(
            client,
            operation,
            command,
            &response,
            now_ms(),
        )? {
            OperationOutcome::Inserted(_) => {
                outcomes.insert(key, signal.clone());
                self.pending_lifecycle = Some(signal);
                Ok(response)
            }
            OperationOutcome::Replay(response) => {
                self.pending_lifecycle = outcomes.get(&key).cloned();
                Ok(response)
            }
            OperationOutcome::Conflict => Err(operation_conflict()),
            OperationOutcome::Expired => Err(operation_expired()),
        }
    }

    fn require_client(&self) -> Result<&ClientId, RuntimeError> {
        self.client.as_ref().ok_or_else(|| {
            RuntimeError::new(
                RuntimeErrorKind::InvalidInput,
                "Hello must bind a client identity before other messages",
            )
        })
    }

    fn accepts_fact(&mut self, fact: &FactEvent) -> bool {
        let Some(cursor) = self.watched.get_mut(&fact.fact.execution_id()) else {
            return false;
        };
        if cursor.is_some_and(|cursor| fact.id <= cursor) {
            return false;
        }
        *cursor = Some(fact.id);
        true
    }

    pub fn drain_replayed_facts(&mut self) -> impl Iterator<Item = FactEvent> + '_ {
        self.pending_facts.drain(..)
    }

    async fn pty_attachments(&self, step: StepId) -> Vec<AttachmentId> {
        let Some(client) = &self.client else {
            return Vec::new();
        };
        self.service
            .attachments
            .lock()
            .await
            .iter()
            .filter_map(|(id, attachment)| {
                (attachment.client == *client && attachment.step == step).then_some(*id)
            })
            .collect()
    }

    fn record_plain(
        &self,
        client: &ClientId,
        operation: &OperationId,
        command: &Command,
        response: &ResponsePayload,
    ) -> Result<ResponsePayload, RuntimeError> {
        let outcome = self.service.store.record_durable_operation(
            client,
            operation,
            command,
            response,
            now_ms(),
        )?;
        match outcome {
            OperationOutcome::Inserted(_) => Ok(response.clone()),
            OperationOutcome::Replay(response) => Ok(response),
            OperationOutcome::Conflict => Err(operation_conflict()),
            OperationOutcome::Expired => Err(operation_expired()),
        }
    }

    fn read_output(
        &self,
        step: StepId,
        stream: OutputStream,
        range: OutputRange,
    ) -> Result<Option<OutputChunk>, RuntimeError> {
        if range.max_bytes == 0 {
            return Ok(None);
        }
        let maximum = usize::try_from(range.max_bytes)
            .unwrap_or(OUTPUT_READ_LIMIT)
            .min(OUTPUT_READ_LIMIT);
        let slice = self
            .service
            .output
            .read(step, stream, range.offset, maximum)?;
        Ok(Some(OutputChunk {
            step,
            stream,
            offset: slice.offset,
            eof: slice.data.len() < maximum,
            data: slice.data,
        }))
    }

    async fn cancel(
        &self,
        client: &ClientId,
        operation: &OperationId,
        command: &Command,
        id: ExecutionId,
        mode: cue_core::CancelMode,
    ) -> Result<ResponsePayload, RuntimeError> {
        let task = self
            .service
            .tasks
            .lock()
            .await
            .get(&id)
            .cloned()
            .ok_or_else(|| {
                RuntimeError::new(
                    RuntimeErrorKind::NotFound,
                    format!("execution {id} has no active task"),
                )
            })?;
        let response = {
            let mut state = task.state.lock().await;
            let mut candidate = state.execution.clone();
            candidate.cancel(mode);
            let latest_time = self
                .service
                .store
                .load_execution(id)?
                .map(|stored| stored.updated_at_ms)
                .unwrap_or(state.updated_at_ms);
            let timestamp = now_ms().max(state.updated_at_ms).max(latest_time);
            let facts = transition_facts(
                &state.execution.snapshot(),
                &state.execution.state(),
                &candidate,
                timestamp,
            );
            let projection = projection(&candidate, state.created_at_ms, timestamp);
            let response = ResponsePayload::Ok(ResultPayload::Execution {
                execution: Box::new(view(&projection)),
            });
            match self.service.store.commit_submission(
                client,
                operation,
                command,
                &projection,
                &facts,
                &response,
            )? {
                OperationOutcome::Inserted(committed) => {
                    state.execution = candidate;
                    state.updated_at_ms = timestamp;
                    self.service.store.publish(&committed);
                    response
                }
                OperationOutcome::Replay(response) => return Ok(response),
                OperationOutcome::Conflict => return Err(operation_conflict()),
                OperationOutcome::Expired => return Err(operation_expired()),
            }
        };
        task.changed.notify_waiters();
        self.service.schedule_runtime(task)?;
        Ok(response)
    }

    async fn attach_pty(
        &self,
        client: &ClientId,
        operation: &OperationId,
        command: &Command,
        step: StepId,
        replay_bytes: u32,
    ) -> Result<ResponsePayload, RuntimeError> {
        let task = self
            .service
            .tasks
            .lock()
            .await
            .get(&step.execution)
            .cloned()
            .ok_or_else(|| RuntimeError::new(RuntimeErrorKind::NotFound, "PTY task not found"))?;
        if !task.controls.lock().await.contains_key(&step) {
            return Err(RuntimeError::new(
                RuntimeErrorKind::Conflict,
                format!("PTY step {step} is not running"),
            ));
        }
        let attachment =
            AttachmentId::new(self.service.next_attachment.fetch_add(1, Ordering::Relaxed))
                .map_err(|error| RuntimeError::infrastructure(error.to_string()))?;
        let snapshot =
            self.service
                .output
                .tail(step, OutputStream::Terminal, replay_bytes as usize)?;
        let response = ResponsePayload::Ok(ResultPayload::PtyAttached {
            attachment,
            step,
            role: PtyRole::Observer,
            control_available: !self.controller_exists(step).await,
            snapshot: snapshot.data,
            snapshot_truncated: snapshot.truncated,
            next_offset: snapshot.next_offset,
        });
        match self.record_plain(client, operation, command, &response)? {
            replay if replay != response => Ok(replay),
            _ => {
                self.service.attachments.lock().await.insert(
                    attachment,
                    Attachment {
                        client: client.clone(),
                        step,
                        role: PtyRole::Observer,
                    },
                );
                Ok(response)
            }
        }
    }

    async fn controller_exists(&self, step: StepId) -> bool {
        self.service
            .attachments
            .lock()
            .await
            .values()
            .any(|attachment| attachment.step == step && attachment.role == PtyRole::Controller)
    }

    async fn set_control(
        &self,
        client: &ClientId,
        id: AttachmentId,
        claim: bool,
    ) -> Result<(), RuntimeError> {
        let mut attachments = self.service.attachments.lock().await;
        let step = require_attachment_owner(&attachments, id, client)?.step;
        if claim
            && attachments.iter().any(|(other_id, attachment)| {
                *other_id != id && attachment.step == step && attachment.role == PtyRole::Controller
            })
        {
            return Err(RuntimeError::new(
                RuntimeErrorKind::Conflict,
                format!("PTY step {step} already has a controller"),
            ));
        }
        let attachment = attachments.get_mut(&id).ok_or_else(|| {
            RuntimeError::new(RuntimeErrorKind::NotFound, "PTY attachment disappeared")
        })?;
        attachment.role = if claim {
            PtyRole::Controller
        } else {
            PtyRole::Observer
        };
        Ok(())
    }

    async fn pty_control(
        &self,
        client: &ClientId,
        id: AttachmentId,
    ) -> Result<RunControl, RuntimeError> {
        let attachment = {
            let attachments = self.service.attachments.lock().await;
            let attachment = require_attachment_owner(&attachments, id, client)?;
            if attachment.role != PtyRole::Controller {
                return Err(RuntimeError::new(
                    RuntimeErrorKind::Conflict,
                    "PTY input and resize require the controller role",
                ));
            }
            attachment.step
        };
        let task = self
            .service
            .tasks
            .lock()
            .await
            .get(&attachment.execution)
            .cloned()
            .ok_or_else(|| RuntimeError::new(RuntimeErrorKind::NotFound, "PTY task not found"))?;
        let control = task
            .controls
            .lock()
            .await
            .get(&attachment)
            .cloned()
            .ok_or_else(|| RuntimeError::new(RuntimeErrorKind::Conflict, "PTY run has finished"))?;
        Ok(control)
    }
}

/// Serve one strict IPC v4 stream. Fact events are emitted only after the
/// connection successfully watches their ExecutionId.
pub async fn serve_stream<S>(service: Arc<DaemonService>, stream: S) -> Result<(), RuntimeError>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    use tokio::io::AsyncWriteExt as _;

    let (mut reader, mut writer) = tokio::io::split(stream);
    let mut connection = service.connection();
    let mut facts = service.subscribe_facts();
    let mut output = service.subscribe_output();
    let mut lifecycle = service.subscribe_lifecycle();
    loop {
        tokio::select! {
            incoming = read_wire_message(&mut reader) => {
                let Some(message) = incoming? else {
                    return Ok(());
                };
                let response = connection.handle(message).await;
                writer
                    .write_all(&encode_message(&response).map_err(|error| {
                        RuntimeError::infrastructure(format!("encode v4 response: {error}"))
                    })?)
                    .await
                    .map_err(|error| RuntimeError::infrastructure(format!("write v4 response: {error}")))?;
                writer
                    .flush()
                    .await
                    .map_err(|error| RuntimeError::infrastructure(format!("flush v4 response: {error}")))?;
                connection.response_flushed();
                for fact in connection.drain_replayed_facts() {
                    let message = Message::Event {
                        payload: EventPayload::Fact(fact),
                    };
                    writer
                        .write_all(&encode_message(&message).map_err(|error| {
                            RuntimeError::infrastructure(format!("encode replayed fact: {error}"))
                        })?)
                        .await
                        .map_err(|error| RuntimeError::infrastructure(format!("write replayed fact: {error}")))?;
                }
                writer
                    .flush()
                    .await
                    .map_err(|error| RuntimeError::infrastructure(format!("flush replayed facts: {error}")))?;
            }
            event = facts.recv() => {
                match event {
                    Ok(event) if connection.accepts_fact(&event) => {
                        let message = Message::Event {
                            payload: EventPayload::Fact(event),
                        };
                        writer
                            .write_all(&encode_message(&message).map_err(|error| {
                                RuntimeError::infrastructure(format!("encode v4 event: {error}"))
                            })?)
                            .await
                            .map_err(|error| RuntimeError::infrastructure(format!("write v4 event: {error}")))?;
                        writer
                            .flush()
                            .await
                            .map_err(|error| RuntimeError::infrastructure(format!("flush v4 event: {error}")))?;
                    }
                    Ok(_) => {}
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                        return Err(RuntimeError::infrastructure(format!(
                            "connection fell behind by {skipped} fact events"
                        )));
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => return Ok(()),
                }
            }
            event = output.recv() => {
                match event {
                    Ok(event) if event.stream == OutputStream::Terminal => {
                        for attachment in connection.pty_attachments(event.step).await {
                            let message = Message::Event {
                                payload: EventPayload::PtyOutput {
                                    attachment,
                                    offset: event.offset,
                                    data: event.data.clone(),
                                },
                            };
                            writer
                                .write_all(&encode_message(&message).map_err(|error| {
                                    RuntimeError::infrastructure(format!("encode PTY event: {error}"))
                                })?)
                                .await
                                .map_err(|error| RuntimeError::infrastructure(format!("write PTY event: {error}")))?;
                        }
                        writer
                            .flush()
                            .await
                            .map_err(|error| RuntimeError::infrastructure(format!("flush PTY event: {error}")))?;
                    }
                    Ok(_) => {}
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                        return Err(RuntimeError::infrastructure(format!(
                            "connection fell behind by {skipped} output events"
                        )));
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => return Ok(()),
                }
            }
            signal = lifecycle.recv() => {
                let reason = match signal {
                    Ok(LifecycleSignal::Shutdown) => "daemon shutting down".to_owned(),
                    Ok(LifecycleSignal::Restart { restart_id, .. }) => {
                        format!("daemon restarting ({restart_id})")
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                        format!("daemon lifecycle receiver lagged by {skipped} events")
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => return Ok(()),
                };
                let message = Message::Event {
                    payload: EventPayload::ServerDraining { reason },
                };
                writer
                    .write_all(&encode_message(&message).map_err(|error| {
                        RuntimeError::infrastructure(format!("encode draining event: {error}"))
                    })?)
                    .await
                    .map_err(|error| RuntimeError::infrastructure(format!("write draining event: {error}")))?;
                writer
                    .flush()
                    .await
                    .map_err(|error| RuntimeError::infrastructure(format!("flush draining event: {error}")))?;
                return Ok(());
            }
        }
    }
}

async fn read_wire_message<R>(reader: &mut R) -> Result<Option<Message>, RuntimeError>
where
    R: tokio::io::AsyncRead + Unpin,
{
    use tokio::io::AsyncReadExt as _;

    let mut header = [0u8; 4];
    match reader.read_exact(&mut header).await {
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(error) => {
            return Err(RuntimeError::infrastructure(format!(
                "read v4 frame header: {error}"
            )));
        }
    }
    let length = u32::from_be_bytes(header) as usize;
    if length > MAX_MESSAGE_SIZE {
        return Err(RuntimeError::new(
            RuntimeErrorKind::InvalidInput,
            format!("v4 frame length {length} exceeds {MAX_MESSAGE_SIZE}"),
        ));
    }
    let mut frame = Vec::with_capacity(4 + length);
    frame.extend_from_slice(&header);
    frame.resize(4 + length, 0);
    reader.read_exact(&mut frame[4..]).await.map_err(|error| {
        RuntimeError::new(
            RuntimeErrorKind::InvalidInput,
            format!("read v4 frame body: {error}"),
        )
    })?;
    decode_message(&frame)
        .map(Some)
        .map_err(|error| RuntimeError::new(RuntimeErrorKind::InvalidInput, error.to_string()))
}

fn transition_facts(
    before: &ExecutionSnapshot,
    before_state: &ExecutionState,
    execution: &Execution,
    occurred_at_ms: i64,
) -> Vec<FactDraft> {
    let after = execution.snapshot();
    let after_state = execution.state();
    let mut facts = Vec::new();
    for (previous, next) in before.steps.iter().zip(&after.steps) {
        if previous != next {
            facts.push(FactDraft {
                occurred_at_ms,
                fact: Fact::StepStateChanged {
                    id: next.id(),
                    previous: previous.state().clone(),
                    next: next.state().clone(),
                    input_scope: next.input_scope(),
                    output_scope: next.output_scope(),
                },
            });
        }
    }
    if before_state != &after_state {
        facts.push(FactDraft {
            occurred_at_ms,
            fact: Fact::ExecutionStateChanged {
                id: execution.id(),
                previous: before_state.clone(),
                next: after_state.clone(),
            },
        });
    }
    if !before_state.is_terminal() && after_state.is_terminal() {
        facts.push(FactDraft {
            occurred_at_ms,
            fact: Fact::ExecutionFinished {
                id: execution.id(),
                state: after_state,
            },
        });
    }
    facts
}

fn projection(
    execution: &Execution,
    created_at_ms: i64,
    updated_at_ms: i64,
) -> ExecutionProjection {
    ExecutionProjection {
        snapshot: execution.snapshot(),
        state: execution.state(),
        created_at_ms,
        updated_at_ms,
    }
}

fn view(execution: &ExecutionProjection) -> ExecutionView {
    ExecutionView {
        snapshot: execution.snapshot.clone(),
        state: execution.state.clone(),
        created_at_ms: execution.created_at_ms,
        updated_at_ms: execution.updated_at_ms,
    }
}

fn run_result(exit: RunExit) -> Result<RunCompletion, RuntimeError> {
    Ok(match exit {
        RunExit::Success => RunCompletion::Succeeded,
        RunExit::ExitCode(code) => RunCompletion::Failed(StepFailure::Exit { code }),
        RunExit::Signalled { signal } => RunCompletion::Failed(StepFailure::Signal { signal }),
        RunExit::Cancelled => RunCompletion::Cancelled,
        RunExit::SpawnFailed(message) => RunCompletion::Failed(StepFailure::Spawn { message }),
        RunExit::InfrastructureFailure(message) => {
            RunCompletion::Failed(StepFailure::Infrastructure { message })
        }
        RunExit::OwnershipLost(message) => {
            return Err(RuntimeError::new(RuntimeErrorKind::Conflict, message));
        }
    })
}

fn require_attachment_owner<'a>(
    attachments: &'a BTreeMap<AttachmentId, Attachment>,
    id: AttachmentId,
    client: &ClientId,
) -> Result<&'a Attachment, RuntimeError> {
    let attachment = attachments.get(&id).ok_or_else(|| {
        RuntimeError::new(
            RuntimeErrorKind::NotFound,
            format!("PTY attachment {} was not found", id.get()),
        )
    })?;
    if &attachment.client != client {
        return Err(RuntimeError::new(
            RuntimeErrorKind::Conflict,
            "PTY attachment belongs to another client",
        ));
    }
    Ok(attachment)
}

fn protocol_error(error: RuntimeError) -> ResponsePayload {
    let code = match error.kind {
        RuntimeErrorKind::InvalidInput => ProtocolErrorCode::InvalidRequest,
        RuntimeErrorKind::NotFound => ProtocolErrorCode::NotFound,
        RuntimeErrorKind::Conflict => ProtocolErrorCode::Conflict,
        RuntimeErrorKind::Unsupported => ProtocolErrorCode::NotSupported,
        RuntimeErrorKind::Infrastructure => ProtocolErrorCode::Internal,
    };
    ResponsePayload::error(code, error.message)
}

fn operation_conflict() -> RuntimeError {
    RuntimeError::new(
        RuntimeErrorKind::Conflict,
        "operation ID was already used for a different command",
    )
}

fn operation_expired() -> RuntimeError {
    RuntimeError::new(
        RuntimeErrorKind::Conflict,
        "operation outcome was tombstoned and cannot be replayed",
    )
}

fn reducer_error(error: impl std::fmt::Display) -> RuntimeError {
    RuntimeError::new(RuntimeErrorKind::Conflict, error.to_string())
}

fn store_error(error: StoreError) -> RuntimeError {
    let kind = if matches!(error, StoreError::SensitiveEnvironmentUnsupported) {
        RuntimeErrorKind::Unsupported
    } else {
        RuntimeErrorKind::Infrastructure
    };
    RuntimeError::new(kind, error.to_string())
}

fn lock_error(name: &str) -> RuntimeError {
    RuntimeError::infrastructure(format!("{name} lock poisoned"))
}

fn now_ms() -> i64 {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    i64::try_from(millis).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use cue_core::{
        AbsolutePath, Argv, BuiltinCommand, EnvEdit, EnvKey, EnvPatch, EnvValue, ExecutionPlan,
        ExecutionSpec, FileModeMask, IoMode, Pipeline, Process, SequenceCondition,
    };

    use super::*;

    fn scope(secret: bool) -> Scope {
        let mut environment = BTreeMap::new();
        environment.insert(
            EnvKey::new("PATH").unwrap(),
            EnvValue::new("/usr/bin:/bin").unwrap(),
        );
        if secret {
            environment.insert(
                EnvKey::new("ACCESS_TOKEN").unwrap(),
                EnvValue::new("do-not-persist").unwrap(),
            );
        }
        Scope::new(
            AbsolutePath::new(std::env::current_dir().unwrap()).unwrap(),
            environment,
            FileModeMask::new(0o022).unwrap(),
        )
    }

    fn spec(scope: ScopeHash, program: &str, arguments: &[&str]) -> ExecutionSpec {
        let process = Process::new(
            Argv::new(
                program,
                arguments.iter().map(|argument| (*argument).to_owned()),
            )
            .unwrap(),
        );
        ExecutionSpec::new(
            scope,
            ExecutionPlan::run(Pipeline::simple(process), IoMode::Captured),
        )
        .unwrap()
    }

    async fn hello(connection: &mut DaemonConnection) {
        let response = connection
            .handle(Message::Query {
                request_id: RequestId::new(1).unwrap(),
                query: Query::Hello(Hello {
                    protocol_version: PROTOCOL_VERSION,
                    client_id: ClientId::new("test-client").unwrap(),
                }),
            })
            .await;
        assert!(matches!(
            response,
            Message::Response {
                payload: ResponsePayload::Ok(ResultPayload::Hello { .. }),
                ..
            }
        ));
    }

    async fn put_scope(connection: &mut DaemonConnection, scope: Scope) -> (ScopeHash, bool) {
        let hash = scope.compute_hash();
        let response = connection
            .handle(Message::Command {
                request_id: RequestId::new(2).unwrap(),
                operation_id: OperationId::new(format!("put:{hash}")).unwrap(),
                command: Command::PutScope {
                    scope: Box::new(scope),
                },
            })
            .await;
        let Message::Response {
            payload:
                ResponsePayload::Ok(ResultPayload::ScopeStored {
                    hash: actual,
                    durable,
                }),
            ..
        } = response
        else {
            panic!("unexpected put scope response: {response:?}");
        };
        assert_eq!(actual, hash);
        (hash, durable)
    }

    async fn submit(
        connection: &mut DaemonConnection,
        request: u64,
        operation: &str,
        spec: ExecutionSpec,
    ) -> Message {
        connection
            .handle(Message::Command {
                request_id: RequestId::new(request).unwrap(),
                operation_id: OperationId::new(operation).unwrap(),
                command: Command::SubmitExecution {
                    spec: Box::new(spec),
                },
            })
            .await
    }

    fn submitted_id(message: &Message) -> ExecutionId {
        let Message::Response {
            payload: ResponsePayload::Ok(ResultPayload::ExecutionSubmitted { execution }),
            ..
        } = message
        else {
            panic!("unexpected submission response: {message:?}");
        };
        execution.snapshot.id
    }

    async fn committed_running_task(
        service: &Arc<DaemonService>,
        plan: ExecutionPlan,
    ) -> Arc<ExecutionTask> {
        let input = scope(false);
        service
            .store
            .lock_store()
            .unwrap()
            .put_scope(&input, 1)
            .unwrap();
        let mut execution = Execution::new(
            ExecutionId(1),
            ExecutionSpec::new(input.compute_hash(), plan).unwrap(),
        );
        let before = execution.snapshot();
        execution.advance().unwrap();
        let mut facts = vec![FactDraft {
            occurred_at_ms: 1,
            fact: Fact::ExecutionCreated {
                id: execution.id(),
                scope: input.compute_hash(),
            },
        }];
        facts.extend(transition_facts(
            &before,
            &ExecutionState::Pending,
            &execution,
            1,
        ));
        service
            .store
            .commit(&projection(&execution, 1, 1), &facts)
            .unwrap();
        let task = Arc::new(ExecutionTask {
            id: execution.id(),
            state: tokio::sync::Mutex::new(TaskState {
                execution,
                created_at_ms: 1,
                updated_at_ms: 1,
            }),
            controls: tokio::sync::Mutex::new(BTreeMap::new()),
            changed: tokio::sync::Notify::new(),
        });
        service.tasks.lock().await.insert(task.id, task.clone());
        task
    }

    #[tokio::test]
    async fn stale_running_delivery_obeys_latest_cancel_for_runs_and_builtins() {
        let plans = [
            spec(scope(false).compute_hash(), "/bin/echo", &["must-not-run"])
                .plan()
                .clone(),
            ExecutionPlan::builtin(BuiltinCommand::Umask(FileModeMask::new(0o077).unwrap())),
        ];
        for plan in plans {
            let service = DaemonService::in_memory().unwrap();
            let task = committed_running_task(&service, plan).await;
            let step = StepId {
                execution: task.id,
                index: 1,
            };
            {
                let mut state = task.state.lock().await;
                let mut next = state.execution.clone();
                let transition = next.cancel(cue_core::CancelMode::Force);
                service
                    .commit_transition(&mut state, next, &transition)
                    .unwrap();
            }
            service.clone().realize(task.clone(), step).await.unwrap();
            assert_eq!(
                task.state.lock().await.execution.state(),
                ExecutionState::Cancelled
            );
            assert!(
                !service
                    .store
                    .lock_store()
                    .unwrap()
                    .run_attempt_started(step)
                    .unwrap()
            );
            assert!(
                service
                    .output
                    .read(step, OutputStream::Stdout, 0, 1024)
                    .unwrap()
                    .data
                    .is_empty()
            );
        }
    }

    #[tokio::test]
    async fn duplicate_delivery_starts_one_physical_attempt() {
        let service = DaemonService::in_memory().unwrap();
        let plan = spec(scope(false).compute_hash(), "/usr/bin/printf", &["once"])
            .plan()
            .clone();
        let task = committed_running_task(&service, plan).await;
        let step = StepId {
            execution: task.id,
            index: 1,
        };
        let (first, duplicate) = tokio::join!(
            service.clone().realize(task.clone(), step),
            service.clone().realize(task.clone(), step)
        );
        first.unwrap();
        duplicate.unwrap();
        service.clone().realize(task.clone(), step).await.unwrap();
        assert_eq!(
            task.state.lock().await.execution.state(),
            ExecutionState::Succeeded
        );
        assert_eq!(
            service
                .output
                .read(step, OutputStream::Stdout, 0, 1024)
                .unwrap()
                .data,
            b"once"
        );
        assert!(
            service
                .store
                .lock_store()
                .unwrap()
                .pending_runtime_steps()
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn failed_commit_keeps_live_state_and_restart_fails_closed_for_unknown_attempt() {
        let uri = format!(
            "file:cue-fp-{}?mode=memory&cache=shared",
            uuid::Uuid::new_v4()
        );
        let store = Store::from_connection(rusqlite::Connection::open(&uri).unwrap()).unwrap();
        let injection = rusqlite::Connection::open(&uri).unwrap();
        let service = DaemonService::from_store(store).unwrap();
        let task = committed_running_task(
            &service,
            spec(scope(false).compute_hash(), "/bin/echo", &["must-not-run"])
                .plan()
                .clone(),
        )
        .await;
        let mut events = service.subscribe_facts();
        injection.execute_batch("CREATE TRIGGER reject_fact BEFORE INSERT ON facts BEGIN SELECT RAISE(ABORT, 'injected failure'); END;").unwrap();
        {
            let mut state = task.state.lock().await;
            let before = state.execution.clone();
            let mut next = before.clone();
            let transition = next.cancel(cue_core::CancelMode::Force);
            assert!(
                service
                    .commit_transition(&mut state, next, &transition)
                    .is_err()
            );
            assert_eq!(state.execution, before);
            assert_eq!(
                service
                    .store
                    .load_execution(task.id)
                    .unwrap()
                    .unwrap()
                    .snapshot,
                before.snapshot()
            );
        }
        assert!(events.try_recv().is_err());
        injection
            .execute_batch("DROP TRIGGER reject_fact;")
            .unwrap();
        let step = StepId {
            execution: task.id,
            index: 1,
        };
        {
            let store = service.store.lock_store().unwrap();
            let generation = store.claim_runtime_step(step).unwrap().unwrap();
            assert!(store.begin_run_attempt(step, generation).unwrap());
        }
        assert!(service.recover().await.is_err());
        assert_eq!(
            task.state.lock().await.execution.state(),
            ExecutionState::Running
        );
        assert!(events.try_recv().is_err());
        assert!(run_result(RunExit::OwnershipLost("lost supervisor".into())).is_err());
    }

    struct FlushGate {
        stream: tokio::io::DuplexStream,
        blocked: Arc<std::sync::atomic::AtomicBool>,
        entered: Arc<tokio::sync::Notify>,
        waker: Arc<Mutex<Option<std::task::Waker>>>,
    }

    impl tokio::io::AsyncRead for FlushGate {
        fn poll_read(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::pin::Pin::new(&mut self.stream).poll_read(cx, buf)
        }
    }
    impl tokio::io::AsyncWrite for FlushGate {
        fn poll_write(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
            data: &[u8],
        ) -> std::task::Poll<std::io::Result<usize>> {
            std::pin::Pin::new(&mut self.stream).poll_write(cx, data)
        }
        fn poll_flush(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            let mut waker = self.waker.lock().unwrap();
            if self.blocked.load(Ordering::Acquire) {
                *waker = Some(cx.waker().clone());
                self.entered.notify_one();
                return std::task::Poll::Pending;
            }
            drop(waker);
            std::pin::Pin::new(&mut self.stream).poll_flush(cx)
        }
        fn poll_shutdown(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::pin::Pin::new(&mut self.stream).poll_shutdown(cx)
        }
    }

    #[tokio::test]
    async fn lifecycle_signal_waits_for_the_actual_ack_flush() {
        use tokio::io::AsyncWriteExt as _;
        let service = DaemonService::in_memory().unwrap();
        let mut lifecycle = service.subscribe_lifecycle();
        let (mut client, stream) = tokio::io::duplex(64 * 1024);
        let blocked = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let entered = Arc::new(tokio::sync::Notify::new());
        let waker = Arc::new(Mutex::new(None));
        let server = tokio::spawn(serve_stream(
            service.clone(),
            FlushGate {
                stream,
                blocked: blocked.clone(),
                entered: entered.clone(),
                waker: waker.clone(),
            },
        ));
        client
            .write_all(
                &encode_message(&Message::Query {
                    request_id: RequestId::new(1).unwrap(),
                    query: Query::Hello(Hello {
                        protocol_version: PROTOCOL_VERSION,
                        client_id: ClientId::new("flush-client").unwrap(),
                    }),
                })
                .unwrap(),
            )
            .await
            .unwrap();
        read_wire_message(&mut client).await.unwrap().unwrap();
        blocked.store(true, Ordering::Release);
        client
            .write_all(
                &encode_message(&Message::Command {
                    request_id: RequestId::new(2).unwrap(),
                    operation_id: OperationId::new("shutdown").unwrap(),
                    command: Command::Shutdown,
                })
                .unwrap(),
            )
            .await
            .unwrap();
        entered.notified().await;
        assert!(lifecycle.try_recv().is_err());
        assert!(!service.draining.load(Ordering::Acquire));
        blocked.store(false, Ordering::Release);
        if let Some(waker) = waker.lock().unwrap().take() {
            waker.wake();
        }
        assert!(matches!(
            read_wire_message(&mut client).await.unwrap().unwrap(),
            Message::Response {
                payload: ResponsePayload::Ok(ResultPayload::Ack),
                ..
            }
        ));
        assert_eq!(lifecycle.recv().await.unwrap(), LifecycleSignal::Shutdown);
        assert!(service.draining.load(Ordering::Acquire));
        drop(client);
        server.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn lifecycle_replay_after_a_lost_ack_still_signals_once_after_flush() {
        for command in [Command::Shutdown, Command::Restart] {
            let service = DaemonService::in_memory().unwrap();
            let mut signals = service.subscribe_lifecycle();
            let mut connection = service.connection();
            hello(&mut connection).await;
            let message = Message::Command {
                request_id: RequestId::new(2).unwrap(),
                operation_id: OperationId::new("lifecycle:replay").unwrap(),
                command,
            };
            let accepted = connection.handle(message.clone()).await;
            drop(connection); // The committed outcome's response was never flushed.
            assert!(signals.try_recv().is_err());
            let mut retry = service.connection();
            hello(&mut retry).await;
            assert_eq!(retry.handle(message.clone()).await, accepted);
            assert!(signals.try_recv().is_err());
            retry.response_flushed();
            signals.try_recv().unwrap();
            assert_eq!(retry.handle(message).await, accepted);
            retry.response_flushed();
            assert!(signals.try_recv().is_err());
        }
    }

    #[tokio::test]
    async fn hello_is_required_and_binds_one_client_identity() {
        let service = DaemonService::in_memory().unwrap();
        let mut connection = service.connection();
        let before_hello = connection
            .handle(Message::Query {
                request_id: RequestId::new(1).unwrap(),
                query: Query::Ping,
            })
            .await;
        assert!(matches!(
            before_hello,
            Message::Response {
                payload: ResponsePayload::Error(_),
                ..
            }
        ));
        hello(&mut connection).await;
    }

    #[tokio::test]
    async fn strict_length_prefixed_stream_serves_v4_messages() {
        use tokio::io::AsyncWriteExt as _;

        let service = DaemonService::in_memory().unwrap();
        let (mut client, server) = tokio::io::duplex(64 * 1024);
        let serving = tokio::spawn(serve_stream(service, server));
        let hello = Message::Query {
            request_id: RequestId::new(1).unwrap(),
            query: Query::Hello(Hello {
                protocol_version: PROTOCOL_VERSION,
                client_id: ClientId::new("stream-client").unwrap(),
            }),
        };
        client
            .write_all(&encode_message(&hello).unwrap())
            .await
            .unwrap();
        let response = read_wire_message(&mut client).await.unwrap().unwrap();
        assert!(matches!(
            response,
            Message::Response {
                payload: ResponsePayload::Ok(ResultPayload::Hello { .. }),
                ..
            }
        ));
        drop(client);
        serving.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn shutdown_ack_precedes_the_server_draining_event() {
        use tokio::io::AsyncWriteExt as _;

        let service = DaemonService::in_memory().unwrap();
        let (mut client, server) = tokio::io::duplex(64 * 1024);
        let serving = tokio::spawn(serve_stream(service, server));
        let hello = Message::Query {
            request_id: RequestId::new(1).unwrap(),
            query: Query::Hello(Hello {
                protocol_version: PROTOCOL_VERSION,
                client_id: ClientId::new("lifecycle-client").unwrap(),
            }),
        };
        client
            .write_all(&encode_message(&hello).unwrap())
            .await
            .unwrap();
        let _hello_response = read_wire_message(&mut client).await.unwrap().unwrap();

        let shutdown = Message::Command {
            request_id: RequestId::new(2).unwrap(),
            operation_id: OperationId::new("shutdown:once").unwrap(),
            command: Command::Shutdown,
        };
        client
            .write_all(&encode_message(&shutdown).unwrap())
            .await
            .unwrap();
        let response = read_wire_message(&mut client).await.unwrap().unwrap();
        assert!(matches!(
            response,
            Message::Response {
                request_id,
                payload: ResponsePayload::Ok(ResultPayload::Ack),
            } if request_id == RequestId::new(2).unwrap()
        ));
        let draining = read_wire_message(&mut client).await.unwrap().unwrap();
        assert!(matches!(
            draining,
            Message::Event {
                payload: EventPayload::ServerDraining { ref reason },
            } if reason == "daemon shutting down"
        ));
        serving.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn put_submit_wait_and_read_output_use_the_v4_contract() {
        let service = DaemonService::in_memory().unwrap();
        let mut connection = service.connection();
        hello(&mut connection).await;
        let (scope, durable) = put_scope(&mut connection, scope(false)).await;
        assert!(durable);
        let submitted = submit(
            &mut connection,
            3,
            "submit:echo",
            spec(scope, "/bin/echo", &["hello"]),
        )
        .await;
        let id = submitted_id(&submitted);
        let waited = connection
            .handle(Message::Query {
                request_id: RequestId::new(4).unwrap(),
                query: Query::WaitExecution { id },
            })
            .await;
        assert!(matches!(
            waited,
            Message::Response {
                payload: ResponsePayload::Ok(ResultPayload::Execution { ref execution }),
                ..
            } if execution.state == ExecutionState::Succeeded
        ));

        let output = connection
            .handle(Message::Query {
                request_id: RequestId::new(5).unwrap(),
                query: Query::ReadOutput {
                    step: StepId {
                        execution: id,
                        index: 1,
                    },
                    stdout: OutputRange {
                        offset: 0,
                        max_bytes: 1024,
                    },
                    stderr: OutputRange {
                        offset: 0,
                        max_bytes: 0,
                    },
                    terminal: OutputRange {
                        offset: 0,
                        max_bytes: 0,
                    },
                },
            })
            .await;
        assert!(matches!(
            output,
            Message::Response {
                payload: ResponsePayload::Ok(ResultPayload::Output { ref chunks }),
                ..
            } if chunks.len() == 1 && chunks[0].data == b"hello\n"
        ));
    }

    #[tokio::test]
    async fn submit_operation_replay_does_not_allocate_a_second_execution() {
        let service = DaemonService::in_memory().unwrap();
        let mut connection = service.connection();
        hello(&mut connection).await;
        let (scope, _) = put_scope(&mut connection, scope(false)).await;
        let execution_spec = spec(scope, "/usr/bin/true", &[]);
        let first = submit(&mut connection, 3, "submit:same", execution_spec.clone()).await;
        let replay = submit(&mut connection, 4, "submit:same", execution_spec).await;
        assert_eq!(submitted_id(&first), submitted_id(&replay));

        let conflict = submit(
            &mut connection,
            5,
            "submit:same",
            spec(scope, "/usr/bin/false", &[]),
        )
        .await;
        assert!(matches!(
            conflict,
            Message::Response {
                payload: ResponsePayload::Error(ref error),
                ..
            } if error.code == ProtocolErrorCode::Conflict
        ));
    }

    #[tokio::test]
    async fn sensitive_scope_is_explicitly_rejected_without_storing_values() {
        use cue_core::Sensitivity;
        let service = DaemonService::in_memory().unwrap();
        let mut connection = service.connection();
        hello(&mut connection).await;
        let initial = scope(false);
        let mut env = initial.env().clone();
        env.insert(
            EnvKey::new("PLAIN_NAME").unwrap(),
            EnvValue::classified("classified", Sensitivity::Sensitive).unwrap(),
        );
        let scope = Scope::new(initial.cwd().clone(), env, initial.umask());
        let hash = scope.compute_hash();
        let response = connection
            .handle(Message::Command {
                request_id: RequestId::new(2).unwrap(),
                operation_id: OperationId::new("unsupported-sensitive").unwrap(),
                command: Command::PutScope {
                    scope: Box::new(scope),
                },
            })
            .await;
        assert!(
            matches!(response, Message::Response { payload: ResponsePayload::Error(error), .. } if error.code == ProtocolErrorCode::NotSupported)
        );
        assert!(service.store.load_scope(hash).unwrap().is_none());
    }

    #[tokio::test]
    async fn cd_env_and_umask_thread_scope_into_a_real_run() {
        let service = DaemonService::in_memory().unwrap();
        let mut connection = service.connection();
        hello(&mut connection).await;
        let (scope, _) = put_scope(&mut connection, scope(false)).await;
        let directory = std::env::temp_dir().join(format!("cue-v4-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&directory).unwrap();
        let env = EnvPatch::new(BTreeMap::from([(
            EnvKey::new("MODE").unwrap(),
            EnvEdit::set("release").unwrap(),
        )]));
        let inspect = ExecutionPlan::run(
            Pipeline::simple(Process::new(
                Argv::new(
                    "/bin/sh",
                    ["-c".to_owned(), "pwd; printenv MODE; umask".to_owned()],
                )
                .unwrap(),
            )),
            IoMode::Captured,
        );
        let plan = ExecutionPlan::sequence(
            ExecutionPlan::builtin(BuiltinCommand::cd(&directory).unwrap()),
            ExecutionPlan::sequence(
                ExecutionPlan::builtin(BuiltinCommand::env(env).unwrap()),
                ExecutionPlan::sequence(
                    ExecutionPlan::builtin(BuiltinCommand::umask(
                        FileModeMask::new(0o027).unwrap(),
                    )),
                    inspect,
                    SequenceCondition::Success,
                ),
                SequenceCondition::Success,
            ),
            SequenceCondition::Success,
        );
        let submitted = submit(
            &mut connection,
            3,
            "submit:builtins",
            ExecutionSpec::new(scope, plan).unwrap(),
        )
        .await;
        let id = submitted_id(&submitted);
        let projection = service.wait_execution(id).await.unwrap();
        assert_eq!(projection.state, ExecutionState::Succeeded);
        let output = service
            .output
            .tail(
                StepId {
                    execution: id,
                    index: 4,
                },
                OutputStream::Stdout,
                4096,
            )
            .unwrap();
        let output = String::from_utf8(output.data).unwrap();
        assert!(output.contains(&directory.display().to_string()));
        assert!(output.contains("release"));
        assert!(output.contains("0027"));
        std::fs::remove_dir(&directory).unwrap();
    }

    #[tokio::test]
    async fn pty_attach_claim_input_and_terminal_output_share_one_run_endpoint() {
        let service = DaemonService::in_memory().unwrap();
        let mut connection = service.connection();
        hello(&mut connection).await;
        let (scope, _) = put_scope(&mut connection, scope(false)).await;
        let process = Process::new(
            Argv::new(
                "/bin/sh",
                [
                    "-c".to_owned(),
                    "read line; printf 'got:%s\\n' \"$line\"".to_owned(),
                ],
            )
            .unwrap(),
        );
        let submitted = submit(
            &mut connection,
            3,
            "submit:pty",
            ExecutionSpec::new(
                scope,
                ExecutionPlan::run(Pipeline::simple(process), IoMode::Pty),
            )
            .unwrap(),
        )
        .await;
        let execution = submitted_id(&submitted);
        let step = StepId {
            execution,
            index: 1,
        };
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                let task = service.tasks.lock().await.get(&execution).cloned().unwrap();
                if task.controls.lock().await.contains_key(&step) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let attached = connection
            .handle(Message::Command {
                request_id: RequestId::new(4).unwrap(),
                operation_id: OperationId::new("attach:pty").unwrap(),
                command: Command::AttachPty {
                    step,
                    replay_bytes: 4096,
                },
            })
            .await;
        let Message::Response {
            payload:
                ResponsePayload::Ok(ResultPayload::PtyAttached {
                    attachment,
                    role: PtyRole::Observer,
                    ..
                }),
            ..
        } = attached
        else {
            panic!("unexpected attach response: {attached:?}");
        };
        for (request, operation, command) in [
            (5, "claim:pty", Command::ClaimPtyControl { attachment }),
            (
                6,
                "input:pty",
                Command::PtyInput {
                    attachment,
                    data: b"hello\n".to_vec(),
                },
            ),
        ] {
            let response = connection
                .handle(Message::Command {
                    request_id: RequestId::new(request).unwrap(),
                    operation_id: OperationId::new(operation).unwrap(),
                    command,
                })
                .await;
            assert!(matches!(
                response,
                Message::Response {
                    payload: ResponsePayload::Ok(ResultPayload::Ack),
                    ..
                }
            ));
        }
        let projection = service.wait_execution(execution).await.unwrap();
        assert_eq!(projection.state, ExecutionState::Succeeded);
        let replay = connection
            .handle(Message::Command {
                request_id: RequestId::new(7).unwrap(),
                operation_id: OperationId::new("input:pty").unwrap(),
                command: Command::PtyInput {
                    attachment,
                    data: b"hello\n".to_vec(),
                },
            })
            .await;
        assert!(matches!(
            replay,
            Message::Response {
                payload: ResponsePayload::Ok(ResultPayload::Ack),
                ..
            }
        ));
        let terminal = service
            .output
            .tail(step, OutputStream::Terminal, 4096)
            .unwrap();
        assert!(String::from_utf8_lossy(&terminal.data).contains("got:hello"));
    }
}
