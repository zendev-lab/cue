//! Sole lifecycle owner for IPC v3 executions.

use std::collections::BTreeMap;

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use cue_core::execution::{
    Execution, ExecutionAction, ExecutionState, NodeOutcome, PlanNodeId, StepFailure, StepState,
};
use cue_core::ipc::{
    ExecutionInfo, ExecutionStepInfo, ForegroundRole, OkPayload, OutputEncoding, ResponsePayload,
    StepOutput, StreamText, error_code,
};
use cue_core::scope::EnvDelta;
use cue_core::{EventChannel, ExecutionId, JobId, ScopeHash, StepId};
use tokio::sync::{mpsc, oneshot};
use tracing::{error, warn};

use super::{
    ActorSystem, ExecutionCoordinatorMsg, GatewayMsg, ProcessJobOptions, ProcessMgrMsg,
    ProcessSpawnAdapter, ResponseTarget, ScopeStoreMsg, SessionBinding, publish_session_event,
};
use crate::storage;

const FIRST_EXECUTION_PROCESS_JOB: u32 = 0x8000_0000;
const DEFAULT_OUTPUT_TAIL: usize = cue_core::ipc::MAX_MESSAGE_SIZE / 4;

struct Waiter {
    client_id: u64,
    request_id: u32,
}

struct ExecutionRecord {
    execution: Execution,
    current_scope: ScopeHash,
    session_id: Option<String>,
    pty_default: bool,
    wrapper_default: bool,
    direct_output_client: Option<u64>,
    adapter_required: bool,
    jobs: BTreeMap<StepId, JobId>,
    waiters: Vec<Waiter>,
    finished_published: bool,
}

impl ExecutionRecord {
    fn visible_to(&self, named_session_id: Option<&str>) -> bool {
        named_session_id.is_none() || self.session_id.as_deref() == named_session_id
    }

    fn info(&self) -> ExecutionInfo {
        let steps = self
            .execution
            .nodes()
            .into_iter()
            .filter_map(|node| {
                let id = node.step_id?;
                let ExecutionAction::Pipeline(pipeline) = node.action else {
                    return None;
                };
                Some(ExecutionStepInfo {
                    id,
                    state: self
                        .execution
                        .step_state(id)
                        .cloned()
                        .expect("execution node has a state"),
                    pipeline: pipeline.to_string(),
                })
            })
            .collect();
        let mut spec = self.execution.spec().clone();
        spec.launch_context.spawn_adapter = None;
        ExecutionInfo {
            id: self.execution.id(),
            state: self.execution.state(),
            steps,
            spec,
        }
    }

    fn stored(&self) -> storage::StoredExecution {
        let mut snapshot = self.execution.snapshot();
        snapshot.spec.launch_context.spawn_adapter = None;
        storage::StoredExecution {
            snapshot,
            current_scope: self.current_scope,
            session_id: self.session_id.clone(),
            pty_default: self.pty_default,
            wrapper_default: self.wrapper_default,
            adapter_required: self.adapter_required,
        }
    }
}

struct CoordinatorState {
    next_execution: u64,
    next_process_job: u32,
    records: BTreeMap<ExecutionId, ExecutionRecord>,
    draining: bool,
    drain_waiters: Vec<oneshot::Sender<()>>,
}

impl CoordinatorState {
    fn new(next_execution: u64, records: BTreeMap<ExecutionId, ExecutionRecord>) -> Self {
        Self {
            next_execution,
            next_process_job: FIRST_EXECUTION_PROCESS_JOB,
            records,
            draining: false,
            drain_waiters: Vec::new(),
        }
    }

    fn alloc_execution(&mut self) -> Option<ExecutionId> {
        if self.next_execution > i64::MAX as u64 {
            return None;
        }
        let id = ExecutionId(self.next_execution);
        self.next_execution = self.next_execution.checked_add(1)?;
        Some(id)
    }

    fn alloc_process_job(&mut self) -> Option<JobId> {
        let id = JobId(self.next_process_job);
        self.next_process_job = self.next_process_job.checked_add(1)?;
        Some(id)
    }
}

pub(super) async fn spawn(
    mut rx: mpsc::Receiver<ExecutionCoordinatorMsg>,
    sys: ActorSystem,
    db: storage::SharedConnection,
) -> anyhow::Result<()> {
    let stored = storage::with_connection(&db, storage::load_executions).await?;
    let mut records = BTreeMap::new();
    let mut next_execution = 1;
    for stored in stored {
        next_execution = next_execution.max(stored.snapshot.id.0.saturating_add(1));
        let mut execution = Execution::restore(stored.snapshot)?;
        if stored.adapter_required && !execution.state().is_terminal() {
            execution
                .fail_nonterminal("required spawn adapter lease did not survive daemon restart");
        } else {
            execution.interrupt_running("process interrupted by daemon restart");
        }
        let id = execution.id();
        let finished_published = execution.state().is_terminal();
        let record = ExecutionRecord {
            execution,
            current_scope: stored.current_scope,
            session_id: stored.session_id,
            pty_default: stored.pty_default,
            wrapper_default: stored.wrapper_default,
            direct_output_client: None,
            adapter_required: stored.adapter_required,
            jobs: BTreeMap::new(),
            waiters: Vec::new(),
            finished_published,
        };
        persist_record(&db, &record).await?;
        records.insert(id, record);
    }
    tokio::spawn(async move {
        let mut state = CoordinatorState::new(next_execution, records);
        let mut admission_retry = tokio::time::interval(std::time::Duration::from_millis(250));
        admission_retry.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                _ = admission_retry.tick() => {
                    let ids = state.records.keys().copied().collect::<Vec<_>>();
                    for id in ids {
                        drive_execution(id, &mut state, &sys, &db).await;
                    }
                    finish_drain_if_idle(&mut state);
                }
                message = rx.recv() => {
                    let Some(message) = message else { break };
                    match message {
                        ExecutionCoordinatorMsg::Submit { client_id, request_id, spec, binding } => {
                            if state.draining {
                                send_response(&sys, client_id, request_id, ResponsePayload::err(
                                    error_code::DAEMON_DRAINING,
                                    "daemon is draining; new execution admission is closed",
                                )).await;
                            } else {
                                submit(client_id, Some(request_id), *spec, binding, &mut state, &sys, &db).await;
                            }
                        }
                        ExecutionCoordinatorMsg::SubmitTriggered { spec, binding } => {
                            if !state.draining {
                                submit(0, None, *spec, binding, &mut state, &sys, &db).await;
                            }
                        }
                        ExecutionCoordinatorMsg::Get { client_id, request_id, id, named_session_id } => {
                            let response = visible_record(&state, id, named_session_id.as_deref())
                                .map_or_else(not_found, |record| ResponsePayload::Ok(OkPayload::ExecutionInfo(Box::new(record.info()))));
                            send_response(&sys, client_id, request_id, response).await;
                        }
                        ExecutionCoordinatorMsg::List { client_id, request_id, limit, named_session_id } => {
                            let mut executions = state.records.values()
                                .filter(|record| record.visible_to(named_session_id.as_deref()))
                                .map(ExecutionRecord::info)
                                .collect::<Vec<_>>();
                            executions.sort_by_key(|info| std::cmp::Reverse(info.id));
                            if let Some(limit) = limit { executions.truncate(limit); }
                            send_response(&sys, client_id, request_id, ResponsePayload::Ok(OkPayload::ExecutionList(executions))).await;
                        }
                        ExecutionCoordinatorMsg::Wait { client_id, request_id, id, named_session_id } => {
                            match visible_record_mut(&mut state, id, named_session_id.as_deref()) {
                                Some(record) if record.execution.state().is_terminal() => {
                                    let response = ResponsePayload::Ok(OkPayload::ExecutionInfo(Box::new(record.info())));
                                    send_response(&sys, client_id, request_id, response).await;
                                }
                                Some(record) => record.waiters.push(Waiter { client_id, request_id }),
                                None => send_response(&sys, client_id, request_id, not_found()).await,
                            }
                        }
                        ExecutionCoordinatorMsg::Cancel { client_id, request_id, id, mode, named_session_id } => {
                            cancel(ResponseTarget { client_id, request_id }, id, mode, named_session_id.as_deref(), &mut state, &sys, &db).await;
                        }
                        ExecutionCoordinatorMsg::ReadOutput { client_id, request_id, id, step_id, stdout_bytes, stderr_bytes, named_session_id } => {
                            let response = read_output(&state, id, step_id, stdout_bytes, stderr_bytes, named_session_id.as_deref()).await;
                            send_response(&sys, client_id, request_id, response).await;
                        }
                        ExecutionCoordinatorMsg::AttachStep { client_id, request_id, id, role, named_session_id } => {
                            attach_step(client_id, request_id, id, role, named_session_id.as_deref(), &state, &sys).await;
                        }
                        ExecutionCoordinatorMsg::StepFinished { step_id, exit_code } => {
                            step_finished(step_id, exit_code, &mut state, &sys, &db).await;
                        }
                        ExecutionCoordinatorMsg::SessionArchiveBlocker { session_id, reply } => {
                            let blocker = state.records.values().find(|record| {
                                record.session_id.as_deref() == Some(&session_id)
                                    && !record.execution.state().is_terminal()
                            }).map(|record| format!(
                                "named session has non-terminal execution {}; wait for or cancel it before archiving",
                                record.execution.id()
                            ));
                            let _ = reply.send(blocker);
                        }
                        ExecutionCoordinatorMsg::BeginDrain { reply } => {
                            state.draining = true;
                            state.drain_waiters.push(reply);
                        }
                        ExecutionCoordinatorMsg::Shutdown => break,
                    }
                    finish_drain_if_idle(&mut state);
                }
            }
        }
    });
    Ok(())
}

fn finish_drain_if_idle(state: &mut CoordinatorState) {
    if !state.draining
        || state
            .records
            .values()
            .any(|record| !record.execution.state().is_terminal())
    {
        return;
    }
    for waiter in std::mem::take(&mut state.drain_waiters) {
        let _ = waiter.send(());
    }
}

fn visible_record<'a>(
    state: &'a CoordinatorState,
    id: ExecutionId,
    named_session_id: Option<&str>,
) -> Option<&'a ExecutionRecord> {
    state
        .records
        .get(&id)
        .filter(|record| record.visible_to(named_session_id))
}

fn visible_record_mut<'a>(
    state: &'a mut CoordinatorState,
    id: ExecutionId,
    named_session_id: Option<&str>,
) -> Option<&'a mut ExecutionRecord> {
    state
        .records
        .get_mut(&id)
        .filter(|record| record.visible_to(named_session_id))
}

fn not_found() -> ResponsePayload {
    ResponsePayload::err(error_code::NOT_FOUND, "execution not found")
}

async fn submit(
    client_id: u64,
    request_id: Option<u32>,
    mut spec: cue_core::execution::ExecutionSpec,
    binding: SessionBinding,
    state: &mut CoordinatorState,
    sys: &ActorSystem,
    db: &storage::SharedConnection,
) {
    let Some(id) = state.alloc_execution() else {
        if let Some(request_id) = request_id {
            send_response(
                sys,
                client_id,
                request_id,
                ResponsePayload::err(error_code::INTERNAL, "execution id space exhausted"),
            )
            .await;
        }
        return;
    };
    let start_scope = spec.start_scope.unwrap_or(binding.scope);
    spec.start_scope = Some(start_scope);
    let adapter_required = spec.launch_context.spawn_adapter.is_some();
    let execution = match Execution::new(id, spec) {
        Ok(execution) => execution,
        Err(error) => {
            if let Some(request_id) = request_id {
                send_response(
                    sys,
                    client_id,
                    request_id,
                    ResponsePayload::err(error_code::INVALID_REQUEST, error.to_string()),
                )
                .await;
            }
            return;
        }
    };
    let record = ExecutionRecord {
        execution,
        current_scope: start_scope,
        session_id: binding.named_session_id,
        pty_default: binding.pty_default.unwrap_or(true),
        wrapper_default: binding
            .wrapper_default
            .unwrap_or(sys.config.wrapper.enabled),
        direct_output_client: request_id.map(|_| client_id),
        adapter_required,
        jobs: BTreeMap::new(),
        waiters: Vec::new(),
        finished_published: false,
    };
    if let Err(error) = persist_record(db, &record).await {
        if let Some(request_id) = request_id {
            send_response(
                sys,
                client_id,
                request_id,
                ResponsePayload::err(
                    error_code::INTERNAL,
                    format!("persist execution before admission: {error}"),
                ),
            )
            .await;
        }
        return;
    }
    let created = record.info();
    let owner = record.session_id.clone();
    state.records.insert(id, record);
    publish_session_event(
        "execution_coordinator",
        &sys.event_bus,
        EventChannel::Executions,
        cue_core::ipc::EventPayload::ExecutionCreated {
            execution: Box::new(created.clone()),
        },
        owner,
    )
    .await;
    if let Some(request_id) = request_id {
        send_response(
            sys,
            client_id,
            request_id,
            ResponsePayload::Ok(OkPayload::ExecutionCreated {
                execution: Box::new(created),
            }),
        )
        .await;
    }
    drive_execution(id, state, sys, db).await;
}

async fn drive_execution(
    id: ExecutionId,
    state: &mut CoordinatorState,
    sys: &ActorSystem,
    db: &storage::SharedConnection,
) {
    let Some(mut record) = state.records.remove(&id) else {
        return;
    };
    if record.execution.state().is_terminal() {
        finish_if_terminal(&mut record, sys).await;
        if let Err(error) = persist_record(db, &record).await {
            error!(%id, %error, "execution coordinator: failed to persist terminal state");
        }
        state.records.insert(id, record);
        return;
    }

    loop {
        let old_step_states = step_states(&record);
        let transition = record.execution.advance();
        publish_changed_step_states(&record, &old_step_states, sys).await;
        if transition.newly_ready.is_empty() && transition.to_cancel.is_empty() {
            break;
        }
        stop_cancelled_steps(&record, transition.to_cancel, sys).await;

        let mut made_progress = false;
        for node_id in transition.newly_ready {
            let Some(node) = record.execution.node(node_id) else {
                continue;
            };
            match node.action {
                ExecutionAction::ContextDelta(delta) => {
                    made_progress = true;
                    let old_state = record.execution.state();
                    if record.execution.mark_running(node_id).is_err() {
                        continue;
                    }
                    let completion = match derive_scope(sys, record.current_scope, delta).await {
                        Ok(scope) => {
                            record.current_scope = scope;
                            record
                                .execution
                                .mark_finished(node_id, NodeOutcome::Succeeded)
                        }
                        Err(message) => record.execution.mark_finished(
                            node_id,
                            NodeOutcome::Failed(StepFailure::Infrastructure { message }),
                        ),
                    };
                    if let Ok(transition) = completion {
                        stop_cancelled_steps(&record, transition.to_cancel, sys).await;
                    }
                    publish_execution_state_change(&record, old_state, sys).await;
                }
                ExecutionAction::Pipeline(pipeline) => {
                    let Some(step_id) = node.step_id else {
                        continue;
                    };
                    let Some(job_id) = state.alloc_process_job() else {
                        mark_step_infrastructure_failure(
                            &mut record,
                            node_id,
                            "execution process id space exhausted".into(),
                            sys,
                        )
                        .await;
                        made_progress = true;
                        continue;
                    };
                    let grants = match sys
                        .resources
                        .try_reserve(job_id, &record.execution.spec().launch_context.needs)
                    {
                        Ok(grants) => grants,
                        Err(_) => continue,
                    };
                    made_progress = true;
                    let spawn_scope = if grants.iter().all(|grant| grant.env.is_empty()) {
                        Ok(record.current_scope)
                    } else {
                        let mut set = BTreeMap::new();
                        for grant in grants {
                            set.extend(grant.env);
                        }
                        derive_scope(
                            sys,
                            record.current_scope,
                            EnvDelta {
                                set,
                                unset: Vec::new(),
                                cwd: None,
                            },
                        )
                        .await
                    };
                    let spawn_scope = match spawn_scope {
                        Ok(scope) => scope,
                        Err(message) => {
                            sys.resources.release(job_id);
                            mark_step_infrastructure_failure(&mut record, node_id, message, sys)
                                .await;
                            continue;
                        }
                    };
                    let old_execution_state = record.execution.state();
                    let old_step_state = record
                        .execution
                        .step_state(step_id)
                        .cloned()
                        .unwrap_or(StepState::Queued);
                    if record.execution.mark_running(node_id).is_err() {
                        sys.resources.release(job_id);
                        continue;
                    }
                    record.jobs.insert(step_id, job_id);
                    publish_step_state_change(
                        &record,
                        step_id,
                        old_step_state,
                        StepState::Running,
                        sys,
                    )
                    .await;
                    publish_execution_state_change(&record, old_execution_state, sys).await;
                    if let Err(error) = persist_record(db, &record).await {
                        sys.resources.release(job_id);
                        record.jobs.remove(&step_id);
                        let old_execution_state = record.execution.state();
                        let failure = NodeOutcome::Failed(StepFailure::Infrastructure {
                            message: format!("persist running step before spawn: {error}"),
                        });
                        if let Ok(transition) = record.execution.mark_finished(node_id, failure) {
                            stop_cancelled_steps(&record, transition.to_cancel, sys).await;
                        }
                        let new_step_state = record
                            .execution
                            .step_state(step_id)
                            .cloned()
                            .expect("failed step state");
                        publish_step_state_change(
                            &record,
                            step_id,
                            StepState::Running,
                            new_step_state,
                            sys,
                        )
                        .await;
                        publish_execution_state_change(&record, old_execution_state, sys).await;
                        continue;
                    }
                    let launch = &record.execution.spec().launch_context;
                    let options = ProcessJobOptions {
                        cwd_override: None,
                        sandbox: launch
                            .workspace_view
                            .as_ref()
                            .map(crate::sandbox::SandboxConfig::from),
                        wrapper_enabled: launch.wrapper_enabled.unwrap_or(record.wrapper_default),
                        pty_enabled: launch.pty.unwrap_or(record.pty_default),
                        direct_output_client: record.direct_output_client,
                        session_id: record.session_id.clone(),
                        spawn_adapter: launch.spawn_adapter.clone().map(|handle| {
                            ProcessSpawnAdapter {
                                handle,
                                execution_id: id,
                                step_id,
                            }
                        }),
                        execution_step: Some(step_id),
                    };
                    if sys
                        .process_mgr
                        .send(ProcessMgrMsg::SpawnJob {
                            job_id,
                            plan: cue_core::pipeline::JobPlan::Pipeline(pipeline),
                            scope_hash: spawn_scope,
                            options: Box::new(options),
                        })
                        .await
                        .is_err()
                    {
                        sys.resources.release(job_id);
                        let old = record
                            .execution
                            .step_state(step_id)
                            .cloned()
                            .unwrap_or(StepState::Running);
                        let old_execution_state = record.execution.state();
                        if let Ok(transition) = record.execution.mark_finished(
                            node_id,
                            NodeOutcome::Failed(StepFailure::Infrastructure {
                                message: "process manager unavailable".into(),
                            }),
                        ) {
                            stop_cancelled_steps(&record, transition.to_cancel, sys).await;
                        }
                        let new = record
                            .execution
                            .step_state(step_id)
                            .cloned()
                            .expect("finished step state");
                        publish_step_state_change(&record, step_id, old, new, sys).await;
                        publish_execution_state_change(&record, old_execution_state, sys).await;
                    }
                }
            }
        }
        if !made_progress {
            break;
        }
    }

    finish_if_terminal(&mut record, sys).await;
    if let Err(error) = persist_record(db, &record).await {
        error!(%id, %error, "execution coordinator: failed to persist reducer state");
    }
    state.records.insert(id, record);
}

async fn mark_step_infrastructure_failure(
    record: &mut ExecutionRecord,
    node_id: PlanNodeId,
    message: String,
    sys: &ActorSystem,
) {
    let Some(node) = record.execution.node(node_id) else {
        return;
    };
    let old_execution_state = record.execution.state();
    if record.execution.mark_running(node_id).is_err() {
        return;
    }
    if let Some(step_id) = node.step_id {
        publish_step_state_change(record, step_id, StepState::Queued, StepState::Running, sys)
            .await;
    }
    let completion = record.execution.mark_finished(
        node_id,
        NodeOutcome::Failed(StepFailure::Infrastructure { message }),
    );
    if let Ok(transition) = completion {
        stop_cancelled_steps(record, transition.to_cancel, sys).await;
    }
    if let Some(step_id) = node.step_id {
        let state = record
            .execution
            .step_state(step_id)
            .cloned()
            .expect("failed step state");
        publish_step_state_change(record, step_id, StepState::Running, state, sys).await;
    }
    publish_execution_state_change(record, old_execution_state, sys).await;
}

async fn step_finished(
    step_id: StepId,
    exit_code: i32,
    state: &mut CoordinatorState,
    sys: &ActorSystem,
    db: &storage::SharedConnection,
) {
    let id = step_id.execution;
    let Some(mut record) = state.records.remove(&id) else {
        warn!(%step_id, "execution coordinator: completion for unknown step");
        return;
    };
    if let Some(job_id) = record.jobs.get(&step_id).copied() {
        sys.resources.release(job_id);
    }
    if !record.execution.state().is_terminal()
        && let Some(node) = record
            .execution
            .nodes()
            .into_iter()
            .find(|node| node.step_id == Some(step_id))
    {
        let old_execution_state = record.execution.state();
        let old_step_states = step_states(&record);
        let outcome = if exit_code == 0 {
            NodeOutcome::Succeeded
        } else if exit_code == cue_core::job::EXIT_CODE_UNAVAILABLE {
            NodeOutcome::Failed(StepFailure::Infrastructure {
                message: "process result unavailable".into(),
            })
        } else {
            NodeOutcome::Failed(StepFailure::Exit { code: exit_code })
        };
        if let Ok(transition) = record.execution.mark_finished(node.id, outcome) {
            publish_changed_step_states(&record, &old_step_states, sys).await;
            stop_cancelled_steps(&record, transition.to_cancel, sys).await;
            publish_execution_state_change(&record, old_execution_state, sys).await;
        }
    }
    state.records.insert(id, record);
    drive_execution(id, state, sys, db).await;
}

async fn cancel(
    response: ResponseTarget,
    id: ExecutionId,
    mode: cue_core::execution::CancelMode,
    named_session_id: Option<&str>,
    state: &mut CoordinatorState,
    sys: &ActorSystem,
    db: &storage::SharedConnection,
) {
    let ResponseTarget {
        client_id,
        request_id,
    } = response;
    let Some(mut record) = state.records.remove(&id) else {
        send_response(sys, client_id, request_id, not_found()).await;
        return;
    };
    if !record.visible_to(named_session_id) {
        state.records.insert(id, record);
        send_response(sys, client_id, request_id, not_found()).await;
        return;
    }
    let old_state = record.execution.state();
    let old_step_states = step_states(&record);
    let transition = record.execution.cancel(mode);
    stop_cancelled_steps(&record, transition.to_cancel, sys).await;
    publish_changed_step_states(&record, &old_step_states, sys).await;
    publish_execution_state_change(&record, old_state, sys).await;
    finish_if_terminal(&mut record, sys).await;
    if let Err(error) = persist_record(db, &record).await {
        error!(%id, %error, "execution coordinator: failed to persist cancellation");
    }
    state.records.insert(id, record);
    send_response(sys, client_id, request_id, ResponsePayload::ack()).await;
}

async fn attach_step(
    client_id: u64,
    request_id: u32,
    id: StepId,
    role: ForegroundRole,
    named_session_id: Option<&str>,
    state: &CoordinatorState,
    sys: &ActorSystem,
) {
    let Some(record) = visible_record(state, id.execution, named_session_id) else {
        send_response(sys, client_id, request_id, not_found()).await;
        return;
    };
    if record.execution.step_state(id).is_none() {
        send_response(
            sys,
            client_id,
            request_id,
            ResponsePayload::err(error_code::NOT_FOUND, "execution step not found"),
        )
        .await;
        return;
    }
    let job_id = {
        matches!(record.execution.step_state(id), Some(StepState::Running))
            .then(|| record.jobs.get(&id).copied())
            .flatten()
    };
    let Some(job_id) = job_id else {
        send_response(
            sys,
            client_id,
            request_id,
            ResponsePayload::err(error_code::INVALID_STATE, "execution step is not running"),
        )
        .await;
        return;
    };
    let (reply, attached) = oneshot::channel();
    if sys
        .process_mgr
        .send(ProcessMgrMsg::AttachFg {
            client_id,
            job_id,
            role,
            legacy_snapshot_event: false,
            reply,
        })
        .await
        .is_err()
    {
        send_response(
            sys,
            client_id,
            request_id,
            ResponsePayload::err(error_code::INTERNAL, "process manager unavailable"),
        )
        .await;
        return;
    }
    let response = match attached.await {
        Ok(Ok(info)) => ResponsePayload::Ok(OkPayload::FgAttached(Box::new(info))),
        Ok(Err(message)) => ResponsePayload::err(error_code::INVALID_STATE, message),
        Err(error) => ResponsePayload::err(
            error_code::INTERNAL,
            format!("process manager dropped step attach reply: {error}"),
        ),
    };
    send_response(sys, client_id, request_id, response).await;
}

async fn kill_job(sys: &ActorSystem, job_id: JobId) {
    let (reply, stopped) = oneshot::channel();
    if sys
        .process_mgr
        .send(ProcessMgrMsg::KillJob { job_id, reply })
        .await
        .is_err()
    {
        return;
    }
    if let Err(error) = stopped.await {
        warn!(%job_id, %error, "execution coordinator: process stop acknowledgement dropped");
    }
}

async fn stop_cancelled_steps(record: &ExecutionRecord, steps: Vec<StepId>, sys: &ActorSystem) {
    for step_id in steps {
        if let Some(job_id) = record.jobs.get(&step_id).copied() {
            kill_job(sys, job_id).await;
            sys.resources.release(job_id);
        }
    }
}

async fn derive_scope(
    sys: &ActorSystem,
    base: ScopeHash,
    delta: EnvDelta,
) -> Result<ScopeHash, String> {
    let (reply, result) = oneshot::channel();
    sys.scope_store
        .send(ScopeStoreMsg::Derive { base, delta, reply })
        .await
        .map_err(|_| "scope store unavailable".to_string())?;
    result
        .await
        .map_err(|_| "scope store reply dropped".to_string())?
        .map_err(|error| error.to_string())
}

async fn finish_if_terminal(record: &mut ExecutionRecord, sys: &ActorSystem) {
    if !record.execution.state().is_terminal() || record.finished_published {
        return;
    }
    record.finished_published = true;
    let info = record.info();
    publish_session_event(
        "execution_coordinator",
        &sys.event_bus,
        EventChannel::Executions,
        cue_core::ipc::EventPayload::ExecutionFinished {
            execution: Box::new(info.clone()),
        },
        record.session_id.clone(),
    )
    .await;
    for waiter in std::mem::take(&mut record.waiters) {
        send_response(
            sys,
            waiter.client_id,
            waiter.request_id,
            ResponsePayload::Ok(OkPayload::ExecutionInfo(Box::new(info.clone()))),
        )
        .await;
    }
}

async fn publish_execution_state_change(
    record: &ExecutionRecord,
    old_state: ExecutionState,
    sys: &ActorSystem,
) {
    let new_state = record.execution.state();
    if new_state == old_state {
        return;
    }
    publish_session_event(
        "execution_coordinator",
        &sys.event_bus,
        EventChannel::Executions,
        cue_core::ipc::EventPayload::ExecutionStateChanged {
            id: record.execution.id(),
            old_state,
            new_state,
        },
        record.session_id.clone(),
    )
    .await;
}

async fn publish_step_state_change(
    record: &ExecutionRecord,
    id: StepId,
    old_state: StepState,
    new_state: StepState,
    sys: &ActorSystem,
) {
    if old_state == new_state {
        return;
    }
    publish_session_event(
        "execution_coordinator",
        &sys.event_bus,
        EventChannel::Executions,
        cue_core::ipc::EventPayload::StepStateChanged {
            id,
            old_state,
            new_state,
        },
        record.session_id.clone(),
    )
    .await;
}

fn step_states(record: &ExecutionRecord) -> BTreeMap<StepId, StepState> {
    record
        .execution
        .nodes()
        .into_iter()
        .filter_map(|node| {
            let id = node.step_id?;
            Some((
                id,
                record
                    .execution
                    .step_state(id)
                    .cloned()
                    .expect("execution step has state"),
            ))
        })
        .collect()
}

async fn publish_changed_step_states(
    record: &ExecutionRecord,
    old_states: &BTreeMap<StepId, StepState>,
    sys: &ActorSystem,
) {
    for (id, new_state) in step_states(record) {
        let Some(old_state) = old_states.get(&id) else {
            continue;
        };
        if old_state != &new_state {
            publish_step_state_change(record, id, old_state.clone(), new_state, sys).await;
        }
    }
}

async fn read_output(
    state: &CoordinatorState,
    id: ExecutionId,
    requested_step: Option<StepId>,
    stdout_bytes: Option<usize>,
    stderr_bytes: Option<usize>,
    named_session_id: Option<&str>,
) -> ResponsePayload {
    let Some(record) = visible_record(state, id, named_session_id) else {
        return not_found();
    };
    if requested_step.is_some_and(|step| step.execution != id) {
        return ResponsePayload::err(
            error_code::INVALID_REQUEST,
            "step does not belong to execution",
        );
    }
    let selected = record
        .info()
        .steps
        .into_iter()
        .map(|step| step.id)
        .filter(|step| requested_step.is_none_or(|requested| requested == *step))
        .collect::<Vec<_>>();
    if requested_step.is_some() && selected.is_empty() {
        return ResponsePayload::err(error_code::NOT_FOUND, "step output not found");
    }
    let output_dir = match crate::dirs::output_dir() {
        Ok(path) => path,
        Err(error) => return ResponsePayload::err(error_code::INTERNAL, error.to_string()),
    };
    let mut steps = Vec::with_capacity(selected.len());
    for step_id in selected {
        let stem = super::process_output_stem(JobId(0), Some(step_id));
        let stdout = read_stream_tail(
            output_dir.join(format!("{stem}.log")),
            stdout_bytes.unwrap_or(DEFAULT_OUTPUT_TAIL),
        )
        .await;
        let stderr = read_stream_tail(
            output_dir.join(format!("{stem}.stderr")),
            stderr_bytes.unwrap_or(DEFAULT_OUTPUT_TAIL),
        )
        .await;
        steps.push(StepOutput {
            id: step_id,
            stdout,
            stderr,
            stderr_pty_merged: record
                .execution
                .spec()
                .launch_context
                .pty
                .unwrap_or(record.pty_default),
        });
    }
    ResponsePayload::Ok(OkPayload::ExecutionOutput { id, steps })
}

async fn persist_record(
    db: &storage::SharedConnection,
    record: &ExecutionRecord,
) -> anyhow::Result<()> {
    let stored = record.stored();
    let steps = record.info().steps;
    storage::with_connection(db, move |connection| {
        storage::store_execution(connection, &stored, &steps)
    })
    .await
}

async fn read_stream_tail(path: std::path::PathBuf, limit: usize) -> StreamText {
    let bytes = tokio::task::spawn_blocking(move || std::fs::read(path))
        .await
        .ok()
        .and_then(Result::ok)
        .unwrap_or_default();
    let truncated = bytes.len() > limit;
    let tail = if truncated {
        &bytes[bytes.len() - limit..]
    } else {
        &bytes
    };
    match std::str::from_utf8(tail) {
        Ok(data) => StreamText {
            data: data.to_string(),
            truncated,
            encoding: OutputEncoding::Utf8,
            base64: None,
        },
        Err(_) => StreamText {
            data: String::from_utf8_lossy(tail).into_owned(),
            truncated,
            encoding: OutputEncoding::Base64,
            base64: Some(BASE64_STANDARD.encode(tail)),
        },
    }
}

async fn send_response(
    sys: &ActorSystem,
    client_id: u64,
    request_id: u32,
    payload: ResponsePayload,
) {
    if let Err(error) = sys
        .gateway
        .send(GatewayMsg::SendResponse {
            client_id,
            request_id,
            payload,
        })
        .await
    {
        error!(%client_id, %request_id, %error, "execution coordinator: failed to send response");
    }
}
