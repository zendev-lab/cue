//! IPC v4 daemon service.
//!
//! This path is deliberately separate from the IPC v3 actor tree. A
//! connection binds a `ClientId` through `Hello`; commands then use the strict
//! operation ledger while queries remain side-effect free.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use cue_core::vnext::{
    AbsolutePath, BuiltinCommand, BuiltinSuccess, Execution, ExecutionProjection,
    ExecutionSnapshot, ExecutionState, Fact, FactDraft, FactEvent, OutputStream, ReadyStep, Scope,
    StepAction, StepFailure, StepState,
};
use cue_core::{EventId, ExecutionId, ScopeHash, StepId};
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
    ExecutionCommandCommit, OperationCommit, OperationRecord, ScopeCommandCommit, ScopePersistence,
    Store, StoreError, command_fingerprint,
};

const VOLATILE_EVENT_BASE: u64 = 1 << 63;
const OUTPUT_READ_LIMIT: usize = 16 * 1024 * 1024;

/// Store adapter that keeps secret-bearing scopes and their executions in
/// memory while delegating durable state to the fresh vNext SQLite schema.
struct StoreProvider {
    durable: Mutex<Store>,
    scopes: Mutex<HashMap<ScopeHash, Scope>>,
    volatile_executions: Mutex<BTreeMap<ExecutionId, ExecutionProjection>>,
    volatile_facts: Mutex<Vec<FactEvent>>,
    volatile_operations: Mutex<HashMap<(String, String), VolatileOperation>>,
    next_volatile_event: AtomicU64,
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

#[derive(Clone)]
struct VolatileOperation {
    fingerprint: [u8; 32],
    response: ResponsePayload,
}

impl StoreProvider {
    fn new(store: Store) -> Self {
        let (events, _) = tokio::sync::broadcast::channel(1024);
        let (output_events, _) = tokio::sync::broadcast::channel(1024);
        Self {
            durable: Mutex::new(store),
            scopes: Mutex::new(HashMap::new()),
            volatile_executions: Mutex::new(BTreeMap::new()),
            volatile_facts: Mutex::new(Vec::new()),
            volatile_operations: Mutex::new(HashMap::new()),
            next_volatile_event: AtomicU64::new(VOLATILE_EVENT_BASE),
            events,
            output_events,
        }
    }

    fn scope_is_durable(&self, hash: ScopeHash) -> Result<bool, RuntimeError> {
        self.durable
            .lock()
            .map_err(|_| lock_error("SQLite store"))?
            .get_scope(hash)
            .map(|scope| scope.is_some())
            .map_err(store_error)
    }

    fn load_scope(&self, hash: ScopeHash) -> Result<Option<Scope>, RuntimeError> {
        ScopeStore::get(self, hash)
    }

    fn load_execution(&self, id: ExecutionId) -> Result<Option<ExecutionProjection>, RuntimeError> {
        ExecutionStore::get(self, id)
    }

    fn execution_is_volatile(&self, id: ExecutionId) -> Result<bool, RuntimeError> {
        Ok(self
            .volatile_executions
            .lock()
            .map_err(|_| lock_error("volatile execution store"))?
            .contains_key(&id))
    }

    fn next_execution_id(&self) -> Result<ExecutionId, RuntimeError> {
        let maximum = self
            .list(None, u16::MAX)?
            .into_iter()
            .map(|execution| execution.snapshot.id.0)
            .max()
            .unwrap_or(0);
        maximum
            .checked_add(1)
            .map(ExecutionId)
            .ok_or_else(|| RuntimeError::infrastructure("execution ID space exhausted"))
    }

    fn record_volatile_operation(
        &self,
        client: &ClientId,
        operation: &OperationId,
        command: &Command,
        response: &ResponsePayload,
    ) -> Result<OperationOutcome, RuntimeError> {
        let fingerprint = command_fingerprint(command).map_err(store_error)?;
        let key = (client.as_str().to_owned(), operation.as_str().to_owned());
        let mut operations = self
            .volatile_operations
            .lock()
            .map_err(|_| lock_error("volatile operation store"))?;
        match operations.get(&key) {
            Some(existing) if existing.fingerprint == fingerprint => {
                Ok(OperationOutcome::Replay(existing.response.clone()))
            }
            Some(_) => Ok(OperationOutcome::Conflict),
            None => {
                operations.insert(
                    key,
                    VolatileOperation {
                        fingerprint,
                        response: response.clone(),
                    },
                );
                Ok(OperationOutcome::Inserted)
            }
        }
    }

    fn record_durable_operation(
        &self,
        client: &ClientId,
        operation: &OperationId,
        command: &Command,
        response: &ResponsePayload,
        completed_at_ms: i64,
    ) -> Result<OperationOutcome, RuntimeError> {
        let result = self
            .durable
            .lock()
            .map_err(|_| lock_error("SQLite store"))?
            .record_operation(client, operation, command, Some(response), completed_at_ms)
            .map_err(store_error)?;
        Ok(match result {
            OperationRecord::Inserted => OperationOutcome::Inserted,
            OperationRecord::Replay {
                response: Some(response),
            } => OperationOutcome::Replay(response),
            OperationRecord::Replay { response: None } => OperationOutcome::Expired,
            OperationRecord::Conflict { .. } => OperationOutcome::Conflict,
        })
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
        if !self.scope_is_durable(execution.snapshot.spec.scope())? {
            let fingerprint = command_fingerprint(command).map_err(store_error)?;
            let key = (client.as_str().to_owned(), operation.as_str().to_owned());
            let mut operations = self
                .volatile_operations
                .lock()
                .map_err(|_| lock_error("volatile operation store"))?;
            match operations.get(&key) {
                Some(existing) if existing.fingerprint == fingerprint => {
                    Ok(OperationOutcome::Replay(existing.response.clone()))
                }
                Some(_) => Ok(OperationOutcome::Conflict),
                None => {
                    self.commit(execution, facts)?;
                    operations.insert(
                        key,
                        VolatileOperation {
                            fingerprint,
                            response: response.clone(),
                        },
                    );
                    Ok(OperationOutcome::Inserted)
                }
            }
        } else {
            let result = self
                .durable
                .lock()
                .map_err(|_| lock_error("SQLite store"))?
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
                .map_err(store_error)?;
            Ok(match result {
                ExecutionCommandCommit::Committed { facts } => {
                    self.publish(&facts);
                    OperationOutcome::Inserted
                }
                ExecutionCommandCommit::Replay {
                    response: Some(response),
                } => OperationOutcome::Replay(response),
                ExecutionCommandCommit::Replay { response: None } => OperationOutcome::Expired,
                ExecutionCommandCommit::Conflict { .. } => OperationOutcome::Conflict,
            })
        }
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
        if Store::scope_persistence(scope) == ScopePersistence::Persisted {
            let result = self
                .durable
                .lock()
                .map_err(|_| lock_error("SQLite store"))?
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
                .map_err(store_error)?;
            let outcome = match result {
                ScopeCommandCommit::Committed => {
                    self.scopes
                        .lock()
                        .map_err(|_| lock_error("scope cache"))?
                        .insert(scope.compute_hash(), scope.clone());
                    OperationOutcome::Inserted
                }
                ScopeCommandCommit::Replay {
                    response: Some(response),
                } => OperationOutcome::Replay(response),
                ScopeCommandCommit::Replay { response: None } => OperationOutcome::Expired,
                ScopeCommandCommit::Conflict { .. } => OperationOutcome::Conflict,
            };
            return Ok((outcome, ScopeDurability::Durable));
        }

        let fingerprint = command_fingerprint(command).map_err(store_error)?;
        let key = (client.as_str().to_owned(), operation.as_str().to_owned());
        let mut operations = self
            .volatile_operations
            .lock()
            .map_err(|_| lock_error("volatile operation store"))?;
        let outcome = match operations.get(&key) {
            Some(existing) if existing.fingerprint == fingerprint => {
                OperationOutcome::Replay(existing.response.clone())
            }
            Some(_) => OperationOutcome::Conflict,
            None => {
                self.scopes
                    .lock()
                    .map_err(|_| lock_error("scope cache"))?
                    .insert(scope.compute_hash(), scope.clone());
                operations.insert(
                    key,
                    VolatileOperation {
                        fingerprint,
                        response: response.clone(),
                    },
                );
                OperationOutcome::Inserted
            }
        };
        Ok((outcome, ScopeDurability::Volatile))
    }

    fn next_volatile_event_id(&self) -> Result<EventId, RuntimeError> {
        let id = self.next_volatile_event.fetch_add(1, Ordering::Relaxed);
        EventId::new(id).map_err(|error| RuntimeError::infrastructure(error.to_string()))
    }

    fn publish(&self, events: &[FactEvent]) {
        for event in events {
            let _ = self.events.send(event.clone());
        }
    }
}

enum OperationOutcome {
    Inserted,
    Replay(ResponsePayload),
    Conflict,
    Expired,
}

impl ExecutionStore for StoreProvider {
    fn get(&self, id: ExecutionId) -> Result<Option<ExecutionProjection>, RuntimeError> {
        if let Some(execution) = self
            .volatile_executions
            .lock()
            .map_err(|_| lock_error("volatile execution store"))?
            .get(&id)
            .cloned()
        {
            return Ok(Some(execution));
        }
        self.durable
            .lock()
            .map_err(|_| lock_error("SQLite store"))?
            .get_execution(id)
            .map_err(store_error)
    }

    fn list(
        &self,
        before: Option<ExecutionId>,
        limit: u16,
    ) -> Result<Vec<ExecutionProjection>, RuntimeError> {
        let mut executions = self
            .durable
            .lock()
            .map_err(|_| lock_error("SQLite store"))?
            .list_executions(before, limit)
            .map_err(store_error)?;
        executions.extend(
            self.volatile_executions
                .lock()
                .map_err(|_| lock_error("volatile execution store"))?
                .values()
                .filter(|execution| before.is_none_or(|id| execution.snapshot.id < id))
                .cloned(),
        );
        executions.sort_by_key(|execution| std::cmp::Reverse(execution.snapshot.id));
        executions.truncate(usize::from(limit));
        Ok(executions)
    }

    fn commit(
        &self,
        execution: &ExecutionProjection,
        facts: &[FactDraft],
    ) -> Result<Vec<FactEvent>, RuntimeError> {
        let volatile = self.execution_is_volatile(execution.snapshot.id)?
            || !self.scope_is_durable(execution.snapshot.spec.scope())?;
        if !volatile {
            let committed = self
                .durable
                .lock()
                .map_err(|_| lock_error("SQLite store"))?
                .commit_execution(execution, facts)
                .map_err(store_error)?;
            self.publish(&committed);
            return Ok(committed);
        }
        Execution::restore(execution.snapshot.clone())
            .map_err(|error| RuntimeError::new(RuntimeErrorKind::Conflict, error.to_string()))?;
        self.volatile_executions
            .lock()
            .map_err(|_| lock_error("volatile execution store"))?
            .insert(execution.snapshot.id, execution.clone());
        let mut committed = Vec::with_capacity(facts.len());
        for draft in facts {
            committed.push(FactEvent {
                id: self.next_volatile_event_id()?,
                occurred_at_ms: draft.occurred_at_ms,
                fact: draft.fact.clone(),
            });
        }
        self.volatile_facts
            .lock()
            .map_err(|_| lock_error("volatile fact store"))?
            .extend(committed.iter().cloned());
        self.publish(&committed);
        Ok(committed)
    }

    fn facts_after(
        &self,
        execution: ExecutionId,
        after: Option<EventId>,
        limit: u16,
    ) -> Result<Vec<FactEvent>, RuntimeError> {
        if self.execution_is_volatile(execution)? {
            let cursor = after.map(EventId::get).unwrap_or(0);
            return Ok(self
                .volatile_facts
                .lock()
                .map_err(|_| lock_error("volatile fact store"))?
                .iter()
                .filter(|event| event.fact.execution_id() == execution && event.id.get() > cursor)
                .take(usize::from(limit))
                .cloned()
                .collect());
        }
        self.durable
            .lock()
            .map_err(|_| lock_error("SQLite store"))?
            .facts_after(execution, after, limit)
            .map_err(store_error)
    }
}

impl ScopeStore for StoreProvider {
    fn put(&self, scope: &Scope, created_at_ms: i64) -> Result<ScopeDurability, RuntimeError> {
        let hash = scope.compute_hash();
        self.scopes
            .lock()
            .map_err(|_| lock_error("scope cache"))?
            .insert(hash, scope.clone());
        let persistence = self
            .durable
            .lock()
            .map_err(|_| lock_error("SQLite store"))?
            .put_scope(scope, created_at_ms)
            .map_err(store_error)?;
        Ok(match persistence {
            ScopePersistence::Persisted => ScopeDurability::Durable,
            ScopePersistence::VolatileSensitiveEnvironment => ScopeDurability::Volatile,
        })
    }

    fn get(&self, hash: ScopeHash) -> Result<Option<Scope>, RuntimeError> {
        if let Some(scope) = self
            .scopes
            .lock()
            .map_err(|_| lock_error("scope cache"))?
            .get(&hash)
            .cloned()
        {
            return Ok(Some(scope));
        }
        self.durable
            .lock()
            .map_err(|_| lock_error("SQLite store"))?
            .get_scope(hash)
            .map_err(store_error)
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
        let append = self.output.append(step, stream, data)?;
        let _ = self.executions.output_events.send(LiveOutput {
            step,
            stream,
            offset: append.start_offset,
            data: data.to_vec(),
        });
        if let Some(mut execution) = self.executions.load_execution(step.execution)? {
            execution.updated_at_ms = now_ms().max(execution.updated_at_ms);
            self.executions.commit(
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
            )?;
        }
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

pub struct VnextService {
    store: Arc<StoreProvider>,
    runtime: Arc<RuntimeAssembly>,
    output: Arc<FactingOutputStore>,
    tasks: tokio::sync::Mutex<BTreeMap<ExecutionId, Arc<ExecutionTask>>>,
    attachments: tokio::sync::Mutex<BTreeMap<AttachmentId, Attachment>>,
    next_attachment: AtomicU64,
}

struct ExecutionTask {
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

impl VnextService {
    pub fn in_memory() -> Result<Arc<Self>, RuntimeError> {
        Self::from_store(Store::in_memory().map_err(store_error)?)
    }

    pub fn from_store(store: Store) -> Result<Arc<Self>, RuntimeError> {
        let store = Arc::new(StoreProvider::new(store));
        let (runtime, output) = build_runtime(store.clone())?;
        Ok(Arc::new(Self {
            store,
            runtime,
            output,
            tasks: tokio::sync::Mutex::new(BTreeMap::new()),
            attachments: tokio::sync::Mutex::new(BTreeMap::new()),
            next_attachment: AtomicU64::new(1),
        }))
    }

    pub fn connection(self: &Arc<Self>) -> VnextConnection {
        VnextConnection {
            service: self.clone(),
            client: None,
            watched: BTreeMap::new(),
            pending_facts: VecDeque::new(),
        }
    }

    pub fn subscribe_facts(&self) -> tokio::sync::broadcast::Receiver<FactEvent> {
        self.store.events.subscribe()
    }

    fn subscribe_output(&self) -> tokio::sync::broadcast::Receiver<LiveOutput> {
        self.store.output_events.subscribe()
    }

    /// Restore all non-terminal projections. Running steps are first committed
    /// as infrastructure failures, then ordinary reducer advancement resumes.
    pub async fn recover(self: &Arc<Self>) -> Result<(), RuntimeError> {
        let projections = self.store.list(None, u16::MAX)?;
        for mut projection in projections {
            if projection.state.is_terminal() {
                continue;
            }
            if let Some(recovery) = cue_runtime::recover_interrupted(
                &projection,
                now_ms().max(projection.updated_at_ms),
                "daemon restarted",
            )? {
                self.store.commit(&recovery.execution, &recovery.facts)?;
                projection = recovery.execution;
            }
            let execution = Execution::restore(projection.snapshot.clone()).map_err(|error| {
                RuntimeError::new(RuntimeErrorKind::Conflict, error.to_string())
            })?;
            let task = Arc::new(ExecutionTask {
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
        spec: cue_core::vnext::ExecutionSpec,
    ) -> Result<ResponsePayload, RuntimeError> {
        if self.store.load_scope(spec.scope())?.is_none() {
            return Err(RuntimeError::new(
                RuntimeErrorKind::NotFound,
                format!("scope {} is not available", spec.scope()),
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
        match self.store.commit_submission(
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
            OperationOutcome::Inserted => {}
        }
        let task = Arc::new(ExecutionTask {
            state: tokio::sync::Mutex::new(TaskState {
                execution,
                created_at_ms: now,
                updated_at_ms: now,
            }),
            controls: tokio::sync::Mutex::new(BTreeMap::new()),
            changed: tokio::sync::Notify::new(),
        });
        self.tasks.lock().await.insert(id, task.clone());
        self.clone().drive(task).await?;
        Ok(response)
    }

    async fn drive(self: Arc<Self>, task: Arc<ExecutionTask>) -> Result<(), RuntimeError> {
        let (ready, to_cancel) = {
            let mut state = task.state.lock().await;
            let before = state.execution.snapshot();
            let before_state = state.execution.state();
            let transition = state.execution.advance().map_err(reducer_error)?;
            for scope in &transition.new_scopes {
                self.runtime.scope_store().put(scope, now_ms())?;
            }
            for ready in &transition.ready {
                state
                    .execution
                    .mark_running(ready.id)
                    .map_err(reducer_error)?;
            }
            let timestamp = now_ms().max(state.updated_at_ms);
            let facts = transition_facts(&before, &before_state, &state.execution, timestamp);
            state.updated_at_ms = timestamp;
            self.store.commit(
                &projection(&state.execution, state.created_at_ms, state.updated_at_ms),
                &facts,
            )?;
            (transition.ready, transition.to_cancel)
        };
        self.terminate_steps(&task, &to_cancel, cue_core::vnext::CancelMode::Force)
            .await;
        task.changed.notify_waiters();
        for ready in ready {
            let service = self.clone();
            let task = task.clone();
            tokio::spawn(async move {
                if let Err(error) = service.realize(task, ready).await {
                    tracing::error!(%error, "vNext execution leaf failed");
                }
            });
        }
        Ok(())
    }

    fn realize(
        self: Arc<Self>,
        task: Arc<ExecutionTask>,
        ready: ReadyStep,
    ) -> RuntimeFuture<Result<(), RuntimeError>> {
        Box::pin(async move {
            let scope = self
                .runtime
                .scope_store()
                .get(ready.input_scope)?
                .ok_or_else(|| {
                    RuntimeError::new(
                        RuntimeErrorKind::NotFound,
                        format!("scope {} disappeared", ready.input_scope),
                    )
                })?;
            match ready.action {
                StepAction::Builtin(command) => {
                    let result = realize_builtin(&command, &scope);
                    self.complete_builtin(task, ready.id, scope, result).await
                }
                StepAction::Run { pipeline, io } => {
                    let spawned = self
                        .runtime
                        .spawn(SpawnRequest {
                            step: ready.id,
                            pipeline,
                            io,
                            scope,
                        })
                        .await;
                    match spawned {
                        Ok(spawned) => {
                            task.controls
                                .lock()
                                .await
                                .insert(ready.id, spawned.control.clone());
                            let result = run_result(spawned.wait().await);
                            task.controls.lock().await.remove(&ready.id);
                            self.complete_run(task, ready.id, result).await
                        }
                        Err(error) => {
                            self.complete_run(
                                task,
                                ready.id,
                                Err(StepFailure::Spawn {
                                    message: error.to_string(),
                                }),
                            )
                            .await
                        }
                    }
                }
            }
        })
    }

    async fn complete_run(
        self: Arc<Self>,
        task: Arc<ExecutionTask>,
        step: StepId,
        result: Result<(), StepFailure>,
    ) -> Result<(), RuntimeError> {
        let (ready, to_cancel) = {
            let mut state = task.state.lock().await;
            if !matches!(
                state.execution.step(step).map(|record| record.state()),
                Some(StepState::Running)
            ) {
                return Ok(());
            }
            let before = state.execution.snapshot();
            let before_state = state.execution.state();
            let transition = state
                .execution
                .complete_run(step, result)
                .map_err(reducer_error)?;
            self.commit_transition(&mut state, &before, &before_state, &transition)?;
            (transition.ready, transition.to_cancel)
        };
        self.terminate_steps(&task, &to_cancel, cue_core::vnext::CancelMode::Force)
            .await;
        task.changed.notify_waiters();
        for ready in ready {
            let service = self.clone();
            let task = task.clone();
            tokio::spawn(async move {
                if let Err(error) = service.realize(task, ready).await {
                    tracing::error!(%error, "vNext execution leaf failed");
                }
            });
        }
        Ok(())
    }

    async fn complete_builtin(
        self: Arc<Self>,
        task: Arc<ExecutionTask>,
        step: StepId,
        input: Scope,
        result: Result<BuiltinSuccess, StepFailure>,
    ) -> Result<(), RuntimeError> {
        let (ready, to_cancel) = {
            let mut state = task.state.lock().await;
            let before = state.execution.snapshot();
            let before_state = state.execution.state();
            let transition = state
                .execution
                .complete_builtin(step, &input, result)
                .map_err(reducer_error)?;
            self.commit_transition(&mut state, &before, &before_state, &transition)?;
            (transition.ready, transition.to_cancel)
        };
        self.terminate_steps(&task, &to_cancel, cue_core::vnext::CancelMode::Force)
            .await;
        task.changed.notify_waiters();
        for ready in ready {
            let service = self.clone();
            let task = task.clone();
            tokio::spawn(async move {
                if let Err(error) = service.realize(task, ready).await {
                    tracing::error!(%error, "vNext execution leaf failed");
                }
            });
        }
        Ok(())
    }

    fn commit_transition(
        &self,
        state: &mut TaskState,
        before: &ExecutionSnapshot,
        before_state: &ExecutionState,
        transition: &cue_core::vnext::ExecutionTransition,
    ) -> Result<(), RuntimeError> {
        let timestamp = now_ms().max(state.updated_at_ms);
        for scope in &transition.new_scopes {
            self.runtime.scope_store().put(scope, timestamp)?;
        }
        for ready in &transition.ready {
            state
                .execution
                .mark_running(ready.id)
                .map_err(reducer_error)?;
        }
        let facts = transition_facts(before, before_state, &state.execution, timestamp);
        state.updated_at_ms = timestamp;
        self.store.commit(
            &projection(&state.execution, state.created_at_ms, state.updated_at_ms),
            &facts,
        )?;
        Ok(())
    }

    async fn terminate_steps(
        &self,
        task: &ExecutionTask,
        steps: &[StepId],
        mode: cue_core::vnext::CancelMode,
    ) {
        let controls = task.controls.lock().await;
        for step in steps {
            if let Some(control) = controls.get(step) {
                let _ = control.terminate(mode).await;
            }
        }
    }

    async fn wait_execution(&self, id: ExecutionId) -> Result<ExecutionProjection, RuntimeError> {
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
            let task = self.tasks.lock().await.get(&id).cloned().ok_or_else(|| {
                RuntimeError::new(
                    RuntimeErrorKind::Conflict,
                    format!("execution {id} has no active runtime task"),
                )
            })?;
            task.changed.notified().await;
        }
    }
}

pub struct VnextConnection {
    service: Arc<VnextService>,
    client: Option<ClientId>,
    watched: BTreeMap<ExecutionId, Option<EventId>>,
    pending_facts: VecDeque<FactEvent>,
}

impl VnextConnection {
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
        let client = self.require_client()?.clone();
        match &command {
            Command::PutScope { scope } => {
                let hash = scope.compute_hash();
                let durable = Store::scope_persistence(scope) == ScopePersistence::Persisted;
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
                    OperationOutcome::Inserted => Ok(response),
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
                let response =
                    self.record_plain(&client, &operation, &command, &response, false)?;
                self.pending_facts.extend(replay);
                self.watched.insert(*id, latest_event);
                Ok(response)
            }
            Command::UnwatchExecution { id } => {
                let response = ResponsePayload::ack();
                let response =
                    self.record_plain(&client, &operation, &command, &response, false)?;
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
                self.record_plain(&client, &operation, &command, &response, false)
            }
            Command::ClaimPtyControl { attachment } => {
                self.set_control(&client, *attachment, true).await?;
                let response = ResponsePayload::ack();
                self.record_plain(&client, &operation, &command, &response, false)
            }
            Command::ReleasePtyControl { attachment } => {
                self.set_control(&client, *attachment, false).await?;
                let response = ResponsePayload::ack();
                self.record_plain(&client, &operation, &command, &response, false)
            }
            Command::PtyInput { attachment, data } => {
                let control = self.pty_control(&client, *attachment).await?;
                control.input(data.clone()).await?;
                let response = ResponsePayload::ack();
                self.record_plain(&client, &operation, &command, &response, false)
            }
            Command::PtyResize {
                attachment,
                cols,
                rows,
            } => {
                let control = self.pty_control(&client, *attachment).await?;
                control.resize(TerminalSize::new(*cols, *rows)?).await?;
                let response = ResponsePayload::ack();
                self.record_plain(&client, &operation, &command, &response, false)
            }
            Command::Restart => {
                let response = ResponsePayload::Ok(ResultPayload::RestartAccepted {
                    restart_id: uuid::Uuid::new_v4().to_string(),
                    target_instance_id: uuid::Uuid::new_v4().to_string(),
                });
                self.record_plain(&client, &operation, &command, &response, false)
            }
            Command::Shutdown => {
                let response = ResponsePayload::ack();
                self.record_plain(&client, &operation, &command, &response, false)
            }
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
        volatile: bool,
    ) -> Result<ResponsePayload, RuntimeError> {
        let outcome = if volatile {
            self.service
                .store
                .record_volatile_operation(client, operation, command, response)?
        } else {
            self.service.store.record_durable_operation(
                client,
                operation,
                command,
                response,
                now_ms(),
            )?
        };
        match outcome {
            OperationOutcome::Inserted => Ok(response.clone()),
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
        mode: cue_core::vnext::CancelMode,
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
        let (response, to_cancel) = {
            let mut state = task.state.lock().await;
            let previous = state.execution.clone();
            let before = state.execution.snapshot();
            let before_state = state.execution.state();
            let transition = state.execution.cancel(mode);
            let timestamp = now_ms().max(state.updated_at_ms);
            let facts = transition_facts(&before, &before_state, &state.execution, timestamp);
            state.updated_at_ms = timestamp;
            let projection = projection(&state.execution, state.created_at_ms, state.updated_at_ms);
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
                OperationOutcome::Inserted => (response, transition.to_cancel),
                OperationOutcome::Replay(response) => {
                    state.execution = previous;
                    return Ok(response);
                }
                OperationOutcome::Conflict => {
                    state.execution = previous;
                    return Err(operation_conflict());
                }
                OperationOutcome::Expired => {
                    state.execution = previous;
                    return Err(operation_expired());
                }
            }
        };
        self.service.terminate_steps(&task, &to_cancel, mode).await;
        task.changed.notify_waiters();
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
        match self.record_plain(client, operation, command, &response, false)? {
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
pub async fn serve_stream<S>(service: Arc<VnextService>, stream: S) -> Result<(), RuntimeError>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    use tokio::io::AsyncWriteExt as _;

    let (mut reader, mut writer) = tokio::io::split(stream);
    let mut connection = service.connection();
    let mut facts = service.subscribe_facts();
    let mut output = service.subscribe_output();
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

fn realize_builtin(command: &BuiltinCommand, scope: &Scope) -> Result<BuiltinSuccess, StepFailure> {
    match command {
        BuiltinCommand::Cd(path) => {
            let requested = path.as_path();
            let target = if requested.is_absolute() {
                requested.to_path_buf()
            } else {
                scope.cwd().as_path().join(requested)
            };
            let canonical = target
                .canonicalize()
                .map_err(|error| StepFailure::Builtin {
                    message: format!("cannot cd to {}: {error}", target.display()),
                })?;
            if !canonical.is_dir() {
                return Err(StepFailure::Builtin {
                    message: format!("cd target is not a directory: {}", canonical.display()),
                });
            }
            let cwd = AbsolutePath::new(canonical).map_err(|error| StepFailure::Builtin {
                message: error.to_string(),
            })?;
            Ok(BuiltinSuccess::Cd { cwd })
        }
        BuiltinCommand::Env(_) => Ok(BuiltinSuccess::Env),
        BuiltinCommand::Umask(_) => Ok(BuiltinSuccess::Umask),
    }
}

fn run_result(exit: RunExit) -> Result<(), StepFailure> {
    match exit {
        RunExit::Success => Ok(()),
        RunExit::ExitCode(code) => Err(StepFailure::Exit { code }),
        RunExit::Signalled { signal } => Err(StepFailure::Signal { signal }),
        RunExit::Cancelled => Err(StepFailure::Infrastructure {
            message: "runner reported cancellation before reducer cancellation".into(),
        }),
        RunExit::SpawnFailed(message) => Err(StepFailure::Spawn { message }),
        RunExit::InfrastructureFailure(message) => Err(StepFailure::Infrastructure { message }),
    }
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
    RuntimeError::new(RuntimeErrorKind::Infrastructure, error.to_string())
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

    use cue_core::vnext::{
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

    async fn hello(connection: &mut VnextConnection) {
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

    async fn put_scope(connection: &mut VnextConnection, scope: Scope) -> (ScopeHash, bool) {
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
        connection: &mut VnextConnection,
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

    #[tokio::test]
    async fn hello_is_required_and_binds_one_client_identity() {
        let service = VnextService::in_memory().unwrap();
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

        let service = VnextService::in_memory().unwrap();
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
    async fn put_submit_wait_and_read_output_use_the_v4_contract() {
        let service = VnextService::in_memory().unwrap();
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
        let service = VnextService::in_memory().unwrap();
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
    async fn sensitive_scope_and_execution_remain_available_but_volatile() {
        let service = VnextService::in_memory().unwrap();
        let mut connection = service.connection();
        hello(&mut connection).await;
        let (scope, durable) = put_scope(&mut connection, scope(true)).await;
        assert!(!durable);
        let submitted = submit(
            &mut connection,
            3,
            "submit:volatile",
            spec(scope, "/usr/bin/true", &[]),
        )
        .await;
        let id = submitted_id(&submitted);
        let projection = service.wait_execution(id).await.unwrap();
        assert_eq!(projection.state, ExecutionState::Succeeded);
        assert!(service.store.execution_is_volatile(id).unwrap());
    }

    #[tokio::test]
    async fn cd_env_and_umask_thread_scope_into_a_real_run() {
        let service = VnextService::in_memory().unwrap();
        let mut connection = service.connection();
        hello(&mut connection).await;
        let (scope, _) = put_scope(&mut connection, scope(false)).await;
        let directory = std::env::temp_dir().join(format!("cue-vnext-{}", uuid::Uuid::new_v4()));
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
        let service = VnextService::in_memory().unwrap();
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
        let terminal = service
            .output
            .tail(step, OutputStream::Terminal, 4096)
            .unwrap();
        assert!(String::from_utf8_lossy(&terminal.data).contains("got:hello"));
    }
}
