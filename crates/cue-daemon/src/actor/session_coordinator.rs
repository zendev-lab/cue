//! Sole owner for client bindings, durable named sessions, and scope cursors.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use cue_core::ScopeHash;
use cue_core::ipc::{OkPayload, ResponsePayload, SessionInfo, SessionScopeState, error_code};
use cue_core::scope::{EnvDelta, EnvSnapshot, Scope};
use tokio::sync::mpsc;
use tracing::{debug, info};

use super::{
    ActorSystem, EventBusMsg, ScopeStoreMsg, SessionBinding, SessionCommand, SessionCommandResult,
    SessionCoordinatorMsg,
};
use crate::storage;

const ANONYMOUS_SESSION_TTL: Duration = Duration::from_secs(300);

#[derive(Clone, Default)]
struct LaunchDefaults {
    pty: Option<bool>,
    wrapper_enabled: Option<bool>,
}

struct SessionState {
    scope: ScopeHash,
    incarnation: u64,
    defaults: LaunchDefaults,
    connected_clients: usize,
    disconnected_at: Option<Instant>,
    named: Option<NamedSessionMeta>,
}

#[derive(Clone)]
struct NamedSessionMeta {
    id: String,
    name: String,
    scope_durable: bool,
    created_at_ms: i64,
    updated_at_ms: i64,
    archived_at_ms: Option<i64>,
}

#[derive(Clone)]
struct UnavailableNamedSession {
    meta: NamedSessionMeta,
    defaults: LaunchDefaults,
}

struct SessionStateOwner {
    next_incarnation: u64,
    sessions: HashMap<String, SessionState>,
    unavailable: HashMap<String, UnavailableNamedSession>,
    client_sessions: HashMap<u64, String>,
}

impl SessionStateOwner {
    fn new() -> Self {
        Self {
            next_incarnation: 1,
            sessions: HashMap::new(),
            unavailable: HashMap::new(),
            client_sessions: HashMap::new(),
        }
    }

    fn alloc_incarnation(&mut self) -> anyhow::Result<u64> {
        let incarnation = self.next_incarnation;
        self.next_incarnation = incarnation
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("session incarnation space exhausted"))?;
        Ok(incarnation)
    }

    fn session_for_client(&self, client_id: u64) -> Option<&SessionState> {
        self.client_sessions
            .get(&client_id)
            .and_then(|key| self.sessions.get(key))
    }

    fn client_scope(&self, client_id: u64) -> Option<ScopeHash> {
        self.session_for_client(client_id)
            .map(|session| session.scope)
    }

    fn client_binding(&self, client_id: u64) -> Option<SessionBinding> {
        let key = self.client_sessions.get(&client_id)?;
        let session = self.sessions.get(key)?;
        let (session_id, named_session_id) = session.named.as_ref().map_or_else(
            || {
                (
                    key.strip_prefix("ephemeral:").unwrap_or(key).to_owned(),
                    None,
                )
            },
            |named| (named.id.clone(), Some(named.id.clone())),
        );
        Some(SessionBinding {
            session_id,
            named_session_id,
            scope: session.scope,
            incarnation: session.incarnation,
            pty_default: session.defaults.pty,
            wrapper_default: session.defaults.wrapper_enabled,
        })
    }
}

#[derive(Clone)]
enum NamedSessionLocation {
    Ready(String),
    NeedsRefresh(String),
}

#[derive(Clone, Copy)]
enum SessionListFilter {
    Active,
    Archived,
    All,
}

pub(super) async fn spawn(
    mut rx: mpsc::Receiver<SessionCoordinatorMsg>,
    db: storage::SharedConnection,
    sys: ActorSystem,
    lifecycle: Arc<crate::lifecycle::DaemonLifecycle>,
) -> anyhow::Result<()> {
    let mut state = SessionStateOwner::new();
    restore_named_sessions(&db, &mut state).await?;

    tokio::spawn(async move {
        let mut startup_waiting = lifecycle.is_starting();
        let mut activation_armed = false;
        let mut gc = tokio::time::interval(Duration::from_secs(30));
        gc.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        debug!("session coordinator: started");
        loop {
            tokio::select! {
                biased;
                _ = lifecycle.wait_for_startup_activation(), if startup_waiting && activation_armed => {
                    startup_waiting = false;
                    lifecycle.mark_startup_execution_ready();
                    activation_armed = false;
                }
                message = rx.recv() => {
                    let Some(message) = message else { break };
                    match message {
                        SessionCoordinatorMsg::Activate { reply } => {
                            let result = if !startup_waiting || activation_armed {
                                Err(anyhow::anyhow!("session coordinator cannot arm startup activation in its current state"))
                            } else {
                                activation_armed = true;
                                Ok(())
                            };
                            let _ = reply.send(result);
                        }
                        SessionCoordinatorMsg::Connect { client_id, session_id, snapshot, refresh, reply } => {
                            let result = connect_session(client_id, session_id, snapshot, refresh, &mut state, &sys).await;
                            let result = match result {
                                Ok(binding) => set_client_event_session(&sys, client_id, &binding).await.map(|()| binding),
                                Err(error) => Err(error),
                            };
                            let _ = reply.send(result);
                        }
                        SessionCoordinatorMsg::Session { client_id, command, reply } => {
                            let mut result = handle_session_command(client_id, command, &mut state, &db, &sys).await;
                            if let Some(binding) = result.binding.as_ref()
                                && let Err(error) = set_client_event_session(&sys, client_id, binding).await
                            {
                                result = SessionCommandResult {
                                    payload: ResponsePayload::err(error_code::INTERNAL, error.to_string()),
                                    binding: None,
                                };
                            }
                            let _ = reply.send(result);
                        }
                        SessionCoordinatorMsg::Disconnect { client_id } => disconnect_session(client_id, &mut state),
                        SessionCoordinatorMsg::CurrentBinding { client_id, reply } => {
                            let _ = reply.send(state.client_binding(client_id));
                        }
                        SessionCoordinatorMsg::ApplyScopeDelta { client_id, base, delta, reply } => {
                            let response = apply_scope_delta(client_id, base, delta, &mut state, &db, &sys).await;
                            let _ = reply.send(response);
                        }
                        SessionCoordinatorMsg::Shutdown => break,
                    }
                }
                _ = gc.tick() => {
                    sweep_anonymous_sessions(&mut state);
                }
            }
        }
        debug!("session coordinator: stopped");
    });
    Ok(())
}

async fn restore_named_sessions(
    db: &storage::SharedConnection,
    state: &mut SessionStateOwner,
) -> anyhow::Result<()> {
    let restored = storage::with_connection(db, |connection| {
        let sessions = storage::load_sessions(connection)?;
        for session in &sessions {
            if let Some(scope) = session.scope_hash
                && storage::get_scope(connection, &scope)?.is_none()
            {
                return Err(anyhow::anyhow!(
                    "named session {} references missing scope {}",
                    session.id,
                    scope
                ));
            }
        }
        Ok(sessions)
    })
    .await
    .map_err(|error| anyhow::anyhow!("load persisted named sessions: {error}"))?;

    for session in restored {
        let incarnation = state.alloc_incarnation()?;
        let meta = NamedSessionMeta {
            id: session.id.clone(),
            name: session.name,
            scope_durable: session.scope_hash.is_some(),
            created_at_ms: session.created_at_ms,
            updated_at_ms: session.updated_at_ms,
            archived_at_ms: session.archived_at_ms,
        };
        let defaults = LaunchDefaults {
            pty: session.pty_default,
            wrapper_enabled: session.wrapper_enabled,
        };
        if let Some(scope) = session.scope_hash {
            state.sessions.insert(
                named_session_key(&session.id),
                SessionState {
                    scope,
                    incarnation,
                    defaults,
                    connected_clients: 0,
                    disconnected_at: None,
                    named: Some(meta),
                },
            );
        } else {
            state
                .unavailable
                .insert(session.id, UnavailableNamedSession { meta, defaults });
        }
    }
    if !state.sessions.is_empty() || !state.unavailable.is_empty() {
        info!(
            ready = state.sessions.len(),
            needs_refresh = state.unavailable.len(),
            "session coordinator: restored named sessions"
        );
    }
    Ok(())
}

async fn connect_session(
    client_id: u64,
    public_id: String,
    snapshot: EnvSnapshot,
    refresh: bool,
    state: &mut SessionStateOwner,
    sys: &ActorSystem,
) -> anyhow::Result<SessionBinding> {
    let key = ephemeral_session_key(&public_id);
    let old_key = state.client_sessions.get(&client_id).cloned();
    let same = old_key.as_deref() == Some(&key);
    if state.sessions.contains_key(&key) {
        let refreshed_scope = if refresh {
            Some(insert_scope(sys, Scope::root(snapshot)).await?)
        } else {
            None
        };
        let session = state.sessions.get_mut(&key).expect("session exists");
        if !same {
            session.connected_clients += 1;
        }
        session.disconnected_at = None;
        if let Some(scope) = refreshed_scope {
            session.scope = scope;
        }
        state.client_sessions.insert(client_id, key.clone());
        mark_replaced_session_disconnected(state, old_key, &key);
        return state
            .client_binding(client_id)
            .ok_or_else(|| anyhow::anyhow!("connected session disappeared"));
    }

    let incarnation = state.alloc_incarnation()?;
    let scope = insert_scope(sys, Scope::root(snapshot)).await?;
    state.sessions.insert(
        key.clone(),
        SessionState {
            scope,
            incarnation,
            defaults: LaunchDefaults::default(),
            connected_clients: 1,
            disconnected_at: None,
            named: None,
        },
    );
    state.client_sessions.insert(client_id, key.clone());
    mark_replaced_session_disconnected(state, old_key, &key);
    state
        .client_binding(client_id)
        .ok_or_else(|| anyhow::anyhow!("connected session disappeared"))
}

async fn handle_session_command(
    client_id: u64,
    command: SessionCommand,
    state: &mut SessionStateOwner,
    db: &storage::SharedConnection,
    sys: &ActorSystem,
) -> SessionCommandResult {
    if state.session_for_client(client_id).is_none() {
        return response(missing_session());
    }
    match command {
        SessionCommand::Create { name } => create_named_session(client_id, name, state, db).await,
        SessionCommand::List => response(ResponsePayload::Ok(OkPayload::SessionList(
            session_list(state, client_id, SessionListFilter::Active),
        ))),
        SessionCommand::ListArchived => response(ResponsePayload::Ok(OkPayload::SessionList(
            session_list(state, client_id, SessionListFilter::Archived),
        ))),
        SessionCommand::ListAll => response(ResponsePayload::Ok(OkPayload::SessionList(
            session_list(state, client_id, SessionListFilter::All),
        ))),
        SessionCommand::Archive { selector } => {
            set_archived(client_id, &selector, true, state, db, sys).await
        }
        SessionCommand::Restore { selector } => {
            set_archived(client_id, &selector, false, state, db, sys).await
        }
        SessionCommand::Attach { selector, refresh } => {
            attach_named_session(client_id, &selector, refresh, state, db).await
        }
        SessionCommand::Info { selector } => {
            let location = selector
                .as_deref()
                .and_then(|value| find_named(state, value))
                .or_else(|| {
                    let key = state.client_sessions.get(&client_id)?;
                    state.sessions.get(key)?.named.as_ref()?;
                    Some(NamedSessionLocation::Ready(key.clone()))
                });
            location
                .and_then(|location| session_info(state, &location, client_id))
                .map_or_else(
                    || {
                        response(ResponsePayload::err(
                            error_code::NOT_FOUND,
                            "named session not found",
                        ))
                    },
                    |info| response(ResponsePayload::Ok(OkPayload::SessionInfo(Box::new(info)))),
                )
        }
    }
}

async fn create_named_session(
    client_id: u64,
    name: String,
    state: &mut SessionStateOwner,
    db: &storage::SharedConnection,
) -> SessionCommandResult {
    if let Err(message) = validate_session_name(&name) {
        return response(ResponsePayload::err(error_code::INVALID_REQUEST, message));
    }
    if find_named(state, &name).is_some() {
        return response(ResponsePayload::err(
            error_code::ALREADY_EXISTS,
            format!("named session `{name}` already exists"),
        ));
    }
    let current = state
        .session_for_client(client_id)
        .expect("handshake checked");
    let scope = current.scope;
    let defaults = current.defaults.clone();
    let now = unix_time_ms();
    let id = format!("SS-{}", uuid::Uuid::new_v4());
    let mut meta = NamedSessionMeta {
        id: id.clone(),
        name,
        scope_durable: false,
        created_at_ms: now,
        updated_at_ms: now,
        archived_at_ms: None,
    };
    let incarnation = match state.alloc_incarnation() {
        Ok(value) => value,
        Err(error) => return response(internal(error)),
    };
    meta.scope_durable = match persist_named_session(db, &meta, scope, &defaults).await {
        Ok(value) => value,
        Err(error) => return response(internal(error)),
    };
    let key = named_session_key(&id);
    state.sessions.insert(
        key.clone(),
        SessionState {
            scope,
            incarnation,
            defaults,
            connected_clients: 0,
            disconnected_at: None,
            named: Some(meta),
        },
    );
    let binding = bind_named(state, client_id, &key).expect("inserted named session");
    let info = ready_info(state, &key, client_id).expect("inserted named metadata");
    SessionCommandResult {
        payload: ResponsePayload::Ok(OkPayload::SessionInfo(Box::new(info))),
        binding: Some(binding),
    }
}

async fn attach_named_session(
    client_id: u64,
    selector: &str,
    refresh: bool,
    state: &mut SessionStateOwner,
    db: &storage::SharedConnection,
) -> SessionCommandResult {
    let Some(location) = find_named(state, selector) else {
        return response(ResponsePayload::err(
            error_code::NOT_FOUND,
            format!("named session `{selector}` not found"),
        ));
    };
    let archived = match &location {
        NamedSessionLocation::Ready(key) => state.sessions[key]
            .named
            .as_ref()
            .and_then(|meta| meta.archived_at_ms),
        NamedSessionLocation::NeedsRefresh(id) => state.unavailable[id].meta.archived_at_ms,
    };
    if archived.is_some() {
        return response(ResponsePayload::err(
            error_code::INVALID_STATE,
            format!("named session `{selector}` is archived; restore it before attaching"),
        ));
    }
    match location {
        NamedSessionLocation::Ready(key) => {
            let scope = if refresh {
                state.client_scope(client_id)
            } else {
                state.sessions.get(&key).map(|session| session.scope)
            };
            let Some(scope) = scope else {
                return response(missing_session());
            };
            let target = &state.sessions[&key];
            let mut meta = target.named.clone().expect("ready named metadata");
            let defaults = target.defaults.clone();
            meta.updated_at_ms = unix_time_ms();
            meta.scope_durable = match persist_named_session(db, &meta, scope, &defaults).await {
                Ok(value) => value,
                Err(error) => return response(internal(error)),
            };
            let target = state.sessions.get_mut(&key).expect("ready named session");
            target.scope = scope;
            target.named = Some(meta);
            let binding = bind_named(state, client_id, &key).expect("ready named session");
            let info = ready_info(state, &key, client_id).expect("ready named metadata");
            SessionCommandResult {
                payload: ResponsePayload::Ok(OkPayload::SessionInfo(Box::new(info))),
                binding: Some(binding),
            }
        }
        NamedSessionLocation::NeedsRefresh(id) => {
            if !refresh {
                return response(ResponsePayload::err(
                    error_code::INVALID_STATE,
                    format!(
                        "named session `{selector}` lost its volatile scope during daemon restart; attach with refresh=true to replace it explicitly"
                    ),
                ));
            }
            let Some(scope) = state.client_scope(client_id) else {
                return response(missing_session());
            };
            let unavailable = state
                .unavailable
                .get(&id)
                .cloned()
                .expect("unavailable named session");
            let mut meta = unavailable.meta;
            meta.updated_at_ms = unix_time_ms();
            let incarnation = match state.alloc_incarnation() {
                Ok(value) => value,
                Err(error) => return response(internal(error)),
            };
            meta.scope_durable =
                match persist_named_session(db, &meta, scope, &unavailable.defaults).await {
                    Ok(value) => value,
                    Err(error) => return response(internal(error)),
                };
            state.unavailable.remove(&id);
            let key = named_session_key(&id);
            state.sessions.insert(
                key.clone(),
                SessionState {
                    scope,
                    incarnation,
                    defaults: unavailable.defaults,
                    connected_clients: 0,
                    disconnected_at: None,
                    named: Some(meta),
                },
            );
            let binding = bind_named(state, client_id, &key).expect("refreshed named session");
            let info = ready_info(state, &key, client_id).expect("refreshed named metadata");
            SessionCommandResult {
                payload: ResponsePayload::Ok(OkPayload::SessionInfo(Box::new(info))),
                binding: Some(binding),
            }
        }
    }
}

async fn set_archived(
    client_id: u64,
    selector: &str,
    archive: bool,
    state: &mut SessionStateOwner,
    db: &storage::SharedConnection,
    sys: &ActorSystem,
) -> SessionCommandResult {
    let Some(location) = find_named(state, selector) else {
        return response(ResponsePayload::err(
            error_code::NOT_FOUND,
            "named session not found",
        ));
    };
    let info = session_info(state, &location, client_id).expect("located session");
    if archive == info.archived_at_ms.is_some() {
        return response(ResponsePayload::Ok(OkPayload::SessionInfo(Box::new(info))));
    }
    if archive {
        if info.connected_clients > 0 {
            return response(ResponsePayload::err(
                error_code::INVALID_STATE,
                format!(
                    "named session has {} connected client(s); detach them before archiving",
                    info.connected_clients
                ),
            ));
        }
        if let Some(blocker) = session_resource_blocker(sys, &info.id).await {
            return response(ResponsePayload::err(error_code::INVALID_STATE, blocker));
        }
    }
    let now = unix_time_ms();
    let archived_at = archive.then_some(now);
    let id = info.id;
    let stored_id = id.clone();
    if let Err(error) = storage::with_connection(db, move |connection| {
        storage::set_session_archived_at(connection, &stored_id, archived_at, now)
    })
    .await
    {
        return response(internal(error));
    }
    match &location {
        NamedSessionLocation::Ready(key) => {
            let meta = state
                .sessions
                .get_mut(key)
                .and_then(|session| session.named.as_mut())
                .expect("ready metadata");
            meta.archived_at_ms = archived_at;
            meta.updated_at_ms = now;
        }
        NamedSessionLocation::NeedsRefresh(id) => {
            let meta = &mut state
                .unavailable
                .get_mut(id)
                .expect("unavailable metadata")
                .meta;
            meta.archived_at_ms = archived_at;
            meta.updated_at_ms = now;
        }
    }
    let info = session_info(state, &location, client_id).expect("updated session");
    response(ResponsePayload::Ok(OkPayload::SessionInfo(Box::new(info))))
}

async fn session_resource_blocker(sys: &ActorSystem, session_id: &str) -> Option<String> {
    let (execution_reply, execution) = tokio::sync::oneshot::channel();
    if sys
        .execution
        .send(super::ExecutionCoordinatorMsg::SessionArchiveBlocker {
            session_id: session_id.to_owned(),
            reply: execution_reply,
        })
        .await
        .is_err()
    {
        return Some("execution coordinator unavailable while checking session archive".into());
    }
    match execution.await {
        Ok(Some(blocker)) => return Some(blocker),
        Ok(None) => {}
        Err(_) => return Some("execution coordinator dropped session archive check".into()),
    }
    let (schedule_reply, schedule) = tokio::sync::oneshot::channel();
    if sys
        .triggers
        .send(super::TriggerServiceMsg::SessionArchiveBlocker {
            session_id: session_id.to_owned(),
            reply: schedule_reply,
        })
        .await
        .is_err()
    {
        return Some("trigger service unavailable while checking session archive".into());
    }
    match schedule.await {
        Ok(blocker) => blocker,
        Err(_) => Some("trigger service dropped session archive check".into()),
    }
}

async fn apply_scope_delta(
    client_id: u64,
    requested_base: Option<ScopeHash>,
    delta: EnvDelta,
    state: &mut SessionStateOwner,
    db: &storage::SharedConnection,
    sys: &ActorSystem,
) -> ResponsePayload {
    let Some(current_scope) = state.client_scope(client_id) else {
        return missing_session();
    };
    let base = requested_base.unwrap_or(current_scope);
    let before = match get_scope_snapshot(sys, base).await {
        Ok(snapshot) => snapshot,
        Err(message) => return ResponsePayload::err(error_code::NOT_FOUND, message),
    };
    let hash = match derive_scope(sys, base, delta).await {
        Ok(hash) => hash,
        Err(message) => return ResponsePayload::err(error_code::INTERNAL, message),
    };
    if requested_base.is_none()
        && let Err(error) = update_client_scope(state, client_id, hash, db).await
    {
        return internal(error);
    }
    match get_scope_snapshot(sys, hash).await {
        Ok(after) => ResponsePayload::Ok(OkPayload::ScopeCreated {
            hash: hash.to_string(),
            summary: format_scope_change(hash, &before, &after),
        }),
        Err(message) => ResponsePayload::err(error_code::INTERNAL, message),
    }
}

async fn update_client_scope(
    state: &mut SessionStateOwner,
    client_id: u64,
    scope: ScopeHash,
    db: &storage::SharedConnection,
) -> anyhow::Result<()> {
    let key = state
        .client_sessions
        .get(&client_id)
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("client session handshake required"))?;
    let session = state
        .sessions
        .get(&key)
        .ok_or_else(|| anyhow::anyhow!("client session unavailable"))?;
    let Some(mut meta) = session.named.clone() else {
        state
            .sessions
            .get_mut(&key)
            .expect("anonymous session")
            .scope = scope;
        return Ok(());
    };
    let defaults = session.defaults.clone();
    meta.updated_at_ms = unix_time_ms();
    meta.scope_durable = persist_named_session(db, &meta, scope, &defaults).await?;
    let session = state.sessions.get_mut(&key).expect("named session");
    session.scope = scope;
    session.named = Some(meta);
    Ok(())
}

fn bind_named(state: &mut SessionStateOwner, client_id: u64, key: &str) -> Option<SessionBinding> {
    let old_key = state.client_sessions.get(&client_id).cloned();
    let same = old_key.as_deref() == Some(key);
    let target = state.sessions.get_mut(key)?;
    if !same {
        target.connected_clients += 1;
    }
    target.disconnected_at = None;
    state.client_sessions.insert(client_id, key.to_owned());
    mark_replaced_session_disconnected(state, old_key, key);
    state.client_binding(client_id)
}

fn disconnect_session(client_id: u64, state: &mut SessionStateOwner) {
    let Some(key) = state.client_sessions.remove(&client_id) else {
        return;
    };
    let Some(session) = state.sessions.get_mut(&key) else {
        return;
    };
    session.connected_clients = session.connected_clients.saturating_sub(1);
    if session.connected_clients == 0 {
        session.disconnected_at = Some(Instant::now());
    }
}

fn mark_replaced_session_disconnected(
    state: &mut SessionStateOwner,
    old_key: Option<String>,
    new_key: &str,
) {
    if let Some(old_key) = old_key
        && old_key != new_key
        && let Some(session) = state.sessions.get_mut(&old_key)
    {
        session.connected_clients = session.connected_clients.saturating_sub(1);
        if session.connected_clients == 0 {
            session.disconnected_at = Some(Instant::now());
        }
    }
}

fn sweep_anonymous_sessions(state: &mut SessionStateOwner) {
    let now = Instant::now();
    state.sessions.retain(|_, session| {
        session.named.is_some()
            || session.connected_clients > 0
            || session
                .disconnected_at
                .is_none_or(|at| now.duration_since(at) < ANONYMOUS_SESSION_TTL)
    });
}

fn session_list(
    state: &SessionStateOwner,
    client_id: u64,
    filter: SessionListFilter,
) -> Vec<SessionInfo> {
    let mut sessions = state
        .sessions
        .keys()
        .filter_map(|key| ready_info(state, key, client_id))
        .collect::<Vec<_>>();
    sessions.extend(
        state
            .unavailable
            .keys()
            .filter_map(|id| unavailable_info(state, id)),
    );
    sessions.retain(|session| match filter {
        SessionListFilter::Active => session.archived_at_ms.is_none(),
        SessionListFilter::Archived => session.archived_at_ms.is_some(),
        SessionListFilter::All => true,
    });
    sessions.sort_by(|left, right| {
        left.created_at_ms
            .cmp(&right.created_at_ms)
            .then_with(|| left.name.cmp(&right.name))
            .then_with(|| left.id.cmp(&right.id))
    });
    sessions
}

fn session_info(
    state: &SessionStateOwner,
    location: &NamedSessionLocation,
    client_id: u64,
) -> Option<SessionInfo> {
    match location {
        NamedSessionLocation::Ready(key) => ready_info(state, key, client_id),
        NamedSessionLocation::NeedsRefresh(id) => unavailable_info(state, id),
    }
}

fn ready_info(state: &SessionStateOwner, key: &str, client_id: u64) -> Option<SessionInfo> {
    let session = state.sessions.get(key)?;
    let named = session.named.as_ref()?;
    Some(SessionInfo {
        id: named.id.clone(),
        name: named.name.clone(),
        scope_state: if named.scope_durable {
            SessionScopeState::ReadyDurable
        } else {
            SessionScopeState::ReadyVolatile
        },
        scope_hash: Some(session.scope.to_string()),
        connected_clients: session.connected_clients,
        restart_safe: named.scope_durable,
        current: state
            .client_sessions
            .get(&client_id)
            .is_some_and(|current| current == key),
        created_at_ms: named.created_at_ms,
        updated_at_ms: named.updated_at_ms,
        archived_at_ms: named.archived_at_ms,
    })
}

fn unavailable_info(state: &SessionStateOwner, id: &str) -> Option<SessionInfo> {
    let session = state.unavailable.get(id)?;
    Some(SessionInfo {
        id: session.meta.id.clone(),
        name: session.meta.name.clone(),
        scope_state: SessionScopeState::NeedsRefresh,
        scope_hash: None,
        connected_clients: 0,
        restart_safe: false,
        current: false,
        created_at_ms: session.meta.created_at_ms,
        updated_at_ms: session.meta.updated_at_ms,
        archived_at_ms: session.meta.archived_at_ms,
    })
}

fn find_named(state: &SessionStateOwner, selector: &str) -> Option<NamedSessionLocation> {
    state
        .sessions
        .iter()
        .find_map(|(key, session)| {
            session
                .named
                .as_ref()
                .filter(|meta| meta.id == selector || meta.name == selector)
                .map(|_| NamedSessionLocation::Ready(key.clone()))
        })
        .or_else(|| {
            state.unavailable.iter().find_map(|(id, session)| {
                (id == selector || session.meta.name == selector)
                    .then(|| NamedSessionLocation::NeedsRefresh(id.clone()))
            })
        })
}

async fn persist_named_session(
    db: &storage::SharedConnection,
    meta: &NamedSessionMeta,
    scope: ScopeHash,
    defaults: &LaunchDefaults,
) -> anyhow::Result<bool> {
    let stored = storage::StoredSession {
        id: meta.id.clone(),
        name: meta.name.clone(),
        scope_hash: Some(scope),
        pty_default: defaults.pty,
        wrapper_enabled: defaults.wrapper_enabled,
        created_at_ms: meta.created_at_ms,
        updated_at_ms: meta.updated_at_ms,
        archived_at_ms: meta.archived_at_ms,
    };
    storage::with_connection(db, move |connection| {
        storage::upsert_session(connection, &stored)
    })
    .await
}

async fn set_client_event_session(
    sys: &ActorSystem,
    client_id: u64,
    binding: &SessionBinding,
) -> anyhow::Result<()> {
    sys.event_bus
        .send(EventBusMsg::SetClientSession {
            client_id,
            named_session_id: binding.named_session_id.clone(),
        })
        .await
        .map_err(|error| anyhow::anyhow!("event bus unavailable during session bind: {error}"))
}

async fn insert_scope(sys: &ActorSystem, scope: Scope) -> anyhow::Result<ScopeHash> {
    let (reply, result) = tokio::sync::oneshot::channel();
    sys.scope_store
        .send(ScopeStoreMsg::Insert { scope, reply })
        .await
        .map_err(|_| anyhow::anyhow!("scope store unavailable"))?;
    result
        .await
        .map_err(|_| anyhow::anyhow!("scope store reply dropped"))?
}

async fn derive_scope(
    sys: &ActorSystem,
    base: ScopeHash,
    delta: EnvDelta,
) -> Result<ScopeHash, String> {
    let (reply, result) = tokio::sync::oneshot::channel();
    sys.scope_store
        .send(ScopeStoreMsg::Derive { base, delta, reply })
        .await
        .map_err(|_| "scope store unavailable".to_owned())?;
    result
        .await
        .map_err(|_| "scope store reply dropped".to_owned())?
        .map_err(|error| error.to_string())
}

async fn get_scope_snapshot(sys: &ActorSystem, hash: ScopeHash) -> Result<EnvSnapshot, String> {
    let (reply, result) = tokio::sync::oneshot::channel();
    sys.scope_store
        .send(ScopeStoreMsg::GetScope { hash, reply })
        .await
        .map_err(|_| "scope store unavailable".to_owned())?;
    match result.await {
        Ok(Ok(Some(scope))) => scope
            .snapshot
            .ok_or_else(|| format!("scope {hash} has no snapshot")),
        Ok(Ok(None)) => Err(format!("scope {hash} not found")),
        Ok(Err(error)) => Err(error.to_string()),
        Err(_) => Err("scope store reply dropped".into()),
    }
}

fn format_scope_change(hash: ScopeHash, before: &EnvSnapshot, after: &EnvSnapshot) -> String {
    let mut lines = vec![hash.to_string()];
    if before.cwd != after.cwd {
        lines.push(format!(
            "cwd: {} -> {}",
            before.cwd.display(),
            after.cwd.display()
        ));
    }
    for (key, value) in &after.env {
        if before.env.get(key) != Some(value) {
            lines.push(format!(
                "env: {key}: {} -> {}",
                before
                    .env
                    .get(key)
                    .map(|old| old.escape_default().to_string())
                    .unwrap_or_else(|| "<unset>".into()),
                value.escape_default()
            ));
        }
    }
    for (key, value) in &before.env {
        if !after.env.contains_key(key) {
            lines.push(format!("env: {key}: {} -> <unset>", value.escape_default()));
        }
    }
    if lines.len() == 1 {
        lines.push("no persistent scope changes".into());
    }
    lines.join("\n")
}

fn validate_session_name(name: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err("session name must not be empty".into());
    }
    if name.trim() != name {
        return Err("session name must not have leading or trailing whitespace".into());
    }
    if name.chars().count() > 64 {
        return Err("session name must be at most 64 characters".into());
    }
    if name.chars().any(char::is_control) {
        return Err("session name must not contain control characters".into());
    }
    if name.starts_with("SS-") {
        return Err("session names beginning with `SS-` are reserved for session ids".into());
    }
    Ok(())
}

fn named_session_key(id: &str) -> String {
    format!("named:{id}")
}
fn ephemeral_session_key(id: &str) -> String {
    format!("ephemeral:{id}")
}
fn unix_time_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}
fn missing_session() -> ResponsePayload {
    ResponsePayload::err(
        error_code::INVALID_REQUEST,
        "client session handshake required",
    )
}
fn internal(error: impl std::fmt::Display) -> ResponsePayload {
    ResponsePayload::err(error_code::INTERNAL, error.to_string())
}
fn response(payload: ResponsePayload) -> SessionCommandResult {
    SessionCommandResult {
        payload,
        binding: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_public_session_names() {
        assert!(validate_session_name("build").is_ok());
        assert!(validate_session_name("").is_err());
        assert!(validate_session_name(" build").is_err());
        assert!(validate_session_name("SS-reserved").is_err());
    }

    #[test]
    fn anonymous_sessions_expire_but_named_sessions_do_not() {
        let scope = ScopeHash([1; 32]);
        let mut state = SessionStateOwner::new();
        state.sessions.insert(
            "anonymous".into(),
            SessionState {
                scope,
                incarnation: 1,
                defaults: LaunchDefaults::default(),
                connected_clients: 0,
                disconnected_at: Some(Instant::now() - ANONYMOUS_SESSION_TTL),
                named: None,
            },
        );
        state.sessions.insert(
            "named".into(),
            SessionState {
                scope,
                incarnation: 2,
                defaults: LaunchDefaults::default(),
                connected_clients: 0,
                disconnected_at: Some(Instant::now() - ANONYMOUS_SESSION_TTL),
                named: Some(NamedSessionMeta {
                    id: "SS-1".into(),
                    name: "keep".into(),
                    scope_durable: true,
                    created_at_ms: 0,
                    updated_at_ms: 0,
                    archived_at_ms: None,
                }),
            },
        );
        sweep_anonymous_sessions(&mut state);
        assert!(!state.sessions.contains_key("anonymous"));
        assert!(state.sessions.contains_key("named"));
    }
}
