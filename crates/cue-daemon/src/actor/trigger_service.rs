//! Sole owner for durable schedules and timer wakeups.
//!
//! A trigger owns no execution state. When its timer fires it submits a fresh
//! typed execution to the execution coordinator.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use cue_core::ScheduleId;
use cue_core::cron::CronStatus;
use cue_core::ipc::{OkPayload, ResponsePayload, ScheduleInfo, error_code};
use tokio::sync::mpsc;
use tokio::time::Instant;
use tracing::{error, warn};

use super::cron_schedule::next_trigger_instant;
use super::{
    ActorSystem, ExecutionCoordinatorMsg, GatewayMsg, ResponseTarget, SessionBinding,
    TriggerServiceMsg,
};
use crate::storage::{self, StoredSchedule};

#[derive(Clone)]
struct TriggerEntry {
    stored: StoredSchedule,
    next_trigger: Option<Instant>,
}

impl TriggerEntry {
    fn visible_to(&self, named_session_id: Option<&str>) -> bool {
        named_session_id.is_none() || self.stored.session_id.as_deref() == named_session_id
    }

    fn info(&self) -> ScheduleInfo {
        ScheduleInfo {
            id: self.stored.id,
            schedule: self.stored.schedule.clone(),
            execution: self.stored.execution.clone(),
            status: self.stored.status,
            next_trigger_at_ms: self.stored.next_trigger_at_ms,
        }
    }

    fn binding(&self) -> SessionBinding {
        SessionBinding {
            session_id: self
                .stored
                .session_id
                .clone()
                .unwrap_or_else(|| format!("trigger:{}", self.stored.id)),
            named_session_id: self.stored.session_id.clone(),
            scope: self.stored.scope_hash,
            incarnation: 0,
            pty_default: Some(self.stored.pty_default),
            wrapper_default: Some(self.stored.wrapper_default),
        }
    }
}

struct TriggerState {
    next_id: u64,
    entries: BTreeMap<ScheduleId, TriggerEntry>,
    draining: bool,
}

pub(super) async fn spawn(
    mut rx: mpsc::Receiver<TriggerServiceMsg>,
    sys: ActorSystem,
    db: storage::SharedConnection,
    lifecycle: Arc<crate::lifecycle::DaemonLifecycle>,
) -> anyhow::Result<()> {
    let loaded = storage::with_connection(&db, storage::load_schedules).await?;
    let mut entries = BTreeMap::new();
    let mut next_id = 1;
    for stored in loaded {
        next_id = next_id.max(stored.id.0.saturating_add(1));
        let next_trigger = stored.next_trigger_at_ms.map(instant_from_unix_ms);
        entries.insert(
            stored.id,
            TriggerEntry {
                stored,
                next_trigger,
            },
        );
    }

    tokio::spawn(async move {
        let mut state = TriggerState {
            next_id,
            entries,
            draining: false,
        };
        let mut tick = tokio::time::interval(Duration::from_millis(100));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                _ = tick.tick() => {
                    if !state.draining && lifecycle.is_execution_ready() {
                        fire_due(&mut state, &sys, &db).await;
                    }
                }
                message = rx.recv() => {
                    let Some(message) = message else { break };
                    match message {
                        TriggerServiceMsg::Create { client_id, request_id, schedule, execution, binding } => {
                            create(ResponseTarget { client_id, request_id }, *schedule, *execution, binding, &mut state, &sys, &db).await;
                        }
                        TriggerServiceMsg::List { client_id, request_id, limit, named_session_id } => {
                            let mut schedules = state.entries.values()
                                .filter(|entry| entry.visible_to(named_session_id.as_deref()))
                                .map(TriggerEntry::info)
                                .collect::<Vec<_>>();
                            schedules.sort_by_key(|schedule| std::cmp::Reverse(schedule.id));
                            if let Some(limit) = limit { schedules.truncate(limit); }
                            send_response(&sys, client_id, request_id, ResponsePayload::Ok(OkPayload::ScheduleList(schedules))).await;
                        }
                        TriggerServiceMsg::Pause { client_id, request_id, id, named_session_id } => {
                            mutate(ResponseTarget { client_id, request_id }, id, named_session_id.as_deref(), Mutation::Pause, &mut state, &sys, &db).await;
                        }
                        TriggerServiceMsg::Resume { client_id, request_id, id, named_session_id } => {
                            mutate(ResponseTarget { client_id, request_id }, id, named_session_id.as_deref(), Mutation::Resume, &mut state, &sys, &db).await;
                        }
                        TriggerServiceMsg::Remove { client_id, request_id, id, named_session_id } => {
                            remove(client_id, request_id, id, named_session_id.as_deref(), &mut state, &sys, &db).await;
                        }
                        TriggerServiceMsg::BeginDrain { reply } => {
                            state.draining = true;
                            let _ = reply.send(());
                        }
                        TriggerServiceMsg::Shutdown => break,
                    }
                }
            }
        }
    });
    Ok(())
}

async fn create(
    response: ResponseTarget,
    schedule: cue_core::cron::CronSchedule,
    mut execution: cue_core::execution::ExecutionSpec,
    binding: SessionBinding,
    state: &mut TriggerState,
    sys: &ActorSystem,
    db: &storage::SharedConnection,
) {
    let ResponseTarget {
        client_id,
        request_id,
    } = response;
    if execution.launch_context.spawn_adapter.is_some() {
        send_response(
            sys,
            client_id,
            request_id,
            ResponsePayload::err(
                error_code::INVALID_REQUEST,
                "scheduled executions cannot carry an ephemeral spawn adapter",
            ),
        )
        .await;
        return;
    }
    if let Err(error) = execution.plan.validate() {
        send_response(
            sys,
            client_id,
            request_id,
            ResponsePayload::err(error_code::INVALID_REQUEST, error.to_string()),
        )
        .await;
        return;
    }
    let Some(next_trigger) = next_trigger_instant(&schedule, Duration::ZERO) else {
        send_response(
            sys,
            client_id,
            request_id,
            ResponsePayload::err(
                error_code::INVALID_REQUEST,
                "schedule has no future trigger",
            ),
        )
        .await;
        return;
    };
    let id = ScheduleId(state.next_id);
    let Some(next_id) = state.next_id.checked_add(1) else {
        send_response(
            sys,
            client_id,
            request_id,
            ResponsePayload::err(error_code::INTERNAL, "schedule id space exhausted"),
        )
        .await;
        return;
    };
    execution.start_scope = Some(binding.scope);
    let stored = StoredSchedule {
        id,
        schedule,
        execution,
        status: CronStatus::Scheduled,
        next_trigger_at_ms: Some(unix_ms_from_instant(next_trigger)),
        scope_hash: binding.scope,
        session_id: binding.named_session_id,
        pty_default: binding.pty_default.unwrap_or(true),
        wrapper_default: binding
            .wrapper_default
            .unwrap_or(sys.config.wrapper.enabled),
    };
    let entry = TriggerEntry {
        stored,
        next_trigger: Some(next_trigger),
    };
    if let Err(error) = persist_entry(db, &entry).await {
        send_response(
            sys,
            client_id,
            request_id,
            ResponsePayload::err(error_code::INTERNAL, format!("persist schedule: {error}")),
        )
        .await;
        return;
    }
    state.next_id = next_id;
    let info = entry.info();
    state.entries.insert(id, entry);
    send_response(
        sys,
        client_id,
        request_id,
        ResponsePayload::Ok(OkPayload::ScheduleCreated {
            schedule: Box::new(info),
        }),
    )
    .await;
}

#[derive(Clone, Copy)]
enum Mutation {
    Pause,
    Resume,
}

async fn mutate(
    response: ResponseTarget,
    id: ScheduleId,
    named_session_id: Option<&str>,
    mutation: Mutation,
    state: &mut TriggerState,
    sys: &ActorSystem,
    db: &storage::SharedConnection,
) {
    let ResponseTarget {
        client_id,
        request_id,
    } = response;
    let Some(entry) = state
        .entries
        .get_mut(&id)
        .filter(|entry| entry.visible_to(named_session_id))
    else {
        send_response(sys, client_id, request_id, not_found()).await;
        return;
    };
    if entry.stored.status.is_terminal() {
        send_response(
            sys,
            client_id,
            request_id,
            ResponsePayload::err(error_code::INVALID_STATE, "schedule is terminal"),
        )
        .await;
        return;
    }
    let original = entry.clone();
    match mutation {
        Mutation::Pause => {
            entry.stored.status = CronStatus::Paused;
            entry.stored.next_trigger_at_ms = None;
            entry.next_trigger = None;
        }
        Mutation::Resume => {
            let Some(next) = next_trigger_instant(&entry.stored.schedule, Duration::ZERO) else {
                send_response(
                    sys,
                    client_id,
                    request_id,
                    ResponsePayload::err(error_code::INVALID_STATE, "schedule cannot be resumed"),
                )
                .await;
                return;
            };
            entry.stored.status = CronStatus::Scheduled;
            entry.stored.next_trigger_at_ms = Some(unix_ms_from_instant(next));
            entry.next_trigger = Some(next);
        }
    }
    let response = match persist_entry(db, entry).await {
        Ok(()) => ResponsePayload::ack(),
        Err(error) => {
            *entry = original;
            ResponsePayload::err(error_code::INTERNAL, error.to_string())
        }
    };
    send_response(sys, client_id, request_id, response).await;
}

async fn remove(
    client_id: u64,
    request_id: u32,
    id: ScheduleId,
    named_session_id: Option<&str>,
    state: &mut TriggerState,
    sys: &ActorSystem,
    db: &storage::SharedConnection,
) {
    let visible = state
        .entries
        .get(&id)
        .is_some_and(|entry| entry.visible_to(named_session_id));
    if !visible {
        send_response(sys, client_id, request_id, not_found()).await;
        return;
    }
    let result = storage::with_connection(db, move |connection| {
        storage::remove_schedule(connection, id)
    })
    .await;
    match result {
        Ok(true) => {
            state.entries.remove(&id);
            send_response(sys, client_id, request_id, ResponsePayload::ack()).await;
        }
        Ok(false) => send_response(sys, client_id, request_id, not_found()).await,
        Err(error) => {
            send_response(
                sys,
                client_id,
                request_id,
                ResponsePayload::err(error_code::INTERNAL, error.to_string()),
            )
            .await;
        }
    }
}

async fn fire_due(state: &mut TriggerState, sys: &ActorSystem, db: &storage::SharedConnection) {
    let now = Instant::now();
    let due = state
        .entries
        .iter()
        .filter(|(_, entry)| {
            entry.stored.status.is_runnable()
                && entry.next_trigger.is_some_and(|deadline| deadline <= now)
        })
        .map(|(id, _)| *id)
        .collect::<Vec<_>>();
    for id in due {
        let Some(entry) = state.entries.get_mut(&id) else {
            continue;
        };
        let execution = entry.stored.execution.clone();
        let binding = entry.binding();
        if sys
            .execution
            .send(ExecutionCoordinatorMsg::SubmitTriggered {
                spec: Box::new(execution),
                binding,
            })
            .await
            .is_err()
        {
            entry.stored.status = CronStatus::Failed;
            entry.stored.next_trigger_at_ms = None;
            entry.next_trigger = None;
        } else if entry.stored.schedule.is_oneshot() {
            entry.stored.status = CronStatus::Completed;
            entry.stored.next_trigger_at_ms = None;
            entry.next_trigger = None;
        } else if let Some(next) = next_trigger_instant(&entry.stored.schedule, Duration::ZERO) {
            entry.stored.next_trigger_at_ms = Some(unix_ms_from_instant(next));
            entry.next_trigger = Some(next);
        } else {
            entry.stored.status = CronStatus::Failed;
            entry.stored.next_trigger_at_ms = None;
            entry.next_trigger = None;
        }
        if let Err(error) = persist_entry(db, entry).await {
            error!(%id, %error, "trigger service: failed to persist fired schedule");
            entry.stored.status = CronStatus::Failed;
            entry.next_trigger = None;
        }
    }
}

async fn persist_entry(db: &storage::SharedConnection, entry: &TriggerEntry) -> anyhow::Result<()> {
    let stored = entry.stored.clone();
    storage::with_connection(db, move |connection| {
        storage::store_schedule(connection, &stored)
    })
    .await
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
        warn!(%client_id, %request_id, %error, "trigger service: response dropped");
    }
}

fn not_found() -> ResponsePayload {
    ResponsePayload::err(error_code::NOT_FOUND, "schedule not found")
}

fn unix_ms_from_instant(instant: Instant) -> i64 {
    let delay = instant.saturating_duration_since(Instant::now());
    let target = SystemTime::now() + delay;
    target
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| i64::try_from(duration.as_millis()).ok())
        .unwrap_or(i64::MAX)
}

fn instant_from_unix_ms(timestamp: i64) -> Instant {
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| i64::try_from(duration.as_millis()).ok())
        .unwrap_or(timestamp);
    let delay_ms = timestamp.saturating_sub(now_ms);
    Instant::now() + Duration::from_millis(u64::try_from(delay_ms).unwrap_or(0))
}

#[cfg(test)]
mod tests {
    use super::*;
    use cue_core::ScopeHash;

    #[test]
    fn restored_past_deadline_fires_immediately() {
        let past = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
            - 10;
        assert!(instant_from_unix_ms(past) <= Instant::now());
    }

    #[test]
    fn schedule_binding_retains_scope_and_owner() {
        let entry = TriggerEntry {
            stored: StoredSchedule {
                id: ScheduleId(3),
                schedule: cue_core::cron::CronSchedule::Delay(Duration::from_secs(1)),
                execution: cue_core::execution::ExecutionSpec {
                    plan: cue_core::execution::ExecutionPlan::pipeline(
                        cue_core::pipeline::Pipeline::simple(vec!["true".into()]),
                    ),
                    start_scope: None,
                    launch_context: Default::default(),
                    source: None,
                    retry_of: None,
                },
                status: CronStatus::Scheduled,
                next_trigger_at_ms: None,
                scope_hash: ScopeHash([7; 32]),
                session_id: Some("SS-1".into()),
                pty_default: false,
                wrapper_default: true,
            },
            next_trigger: None,
        };

        let binding = entry.binding();

        assert_eq!(binding.scope, ScopeHash([7; 32]));
        assert_eq!(binding.named_session_id.as_deref(), Some("SS-1"));
        assert_eq!(binding.pty_default, Some(false));
        assert_eq!(binding.wrapper_default, Some(true));
    }
}
