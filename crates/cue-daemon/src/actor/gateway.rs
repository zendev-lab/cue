//! Gateway actor — Unix socket listener, per-client handlers, message framing.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use anyhow::{Context, Result, bail};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{mpsc, watch};
use tracing::{debug, error, info, warn};

use cue_core::EventChannel;
#[cfg(test)]
use cue_core::ipc::EventPayload;
use cue_core::ipc::{
    ForegroundRole, IPC_PROTOCOL_VERSION, MAX_MESSAGE_SIZE, Message, OkPayload, OutputEncoding,
    PageInfo, RequestPayload, ResponsePayload, current_protocol_capabilities, encode_message,
    error_code,
};
use cue_core::scope::EnvSnapshot;

use super::operation_ledger::{BeginOutcome, OperationLedger, OperationWaiter};
use super::{
    ActorSystem, CLIENT_EVENT_CAP, ClientEvent, ClientEventAudience, EventBusMsg,
    ExecutionCoordinatorMsg, ForegroundRoleUpdate, GatewayMsg, ScopeStoreMsg, SessionBinding,
    SessionCommand, SessionCoordinatorMsg, TriggerServiceMsg,
};

/// Next client id counter (global, atomic).
static NEXT_CLIENT_ID: AtomicU64 = AtomicU64::new(1);

// ── Message framing ──

/// Read one length-prefixed JSON message from the stream.
pub(crate) async fn read_message<R>(stream: &mut R) -> Result<Message>
where
    R: AsyncRead + Unpin,
{
    let len = stream.read_u32().await.context("read length prefix")?;
    if len as usize > MAX_MESSAGE_SIZE {
        bail!("message too large: {len} bytes");
    }
    let mut buf = vec![0u8; len as usize];
    stream.read_exact(&mut buf).await.context("read body")?;
    let msg: Message = serde_json::from_slice(&buf).context("deserialize message")?;
    Ok(msg)
}

/// Write one length-prefixed JSON message to the stream.
pub(crate) async fn write_message<W>(stream: &mut W, msg: &Message) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    let encoded = encode_message(msg)?;
    stream.write_all(&encoded).await.context("write message")?;
    stream.flush().await.context("flush")?;
    Ok(())
}

async fn write_client_message<W>(stream: &mut W, msg: &Message) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    tokio::time::timeout(CLIENT_WRITE_TIMEOUT, write_message(stream, msg))
        .await
        .context("client write timed out")?
}

const CLIENT_RESPONSE_CAP: usize = 64;
const MAX_INFLIGHT_REQUESTS_PER_CLIENT: usize = 1_024;
const CLIENT_WRITE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
const MAX_OUTPUT_TAIL_BYTES: usize = MAX_MESSAGE_SIZE / 4;

#[derive(Clone)]
struct ClientQueues {
    responses: mpsc::Sender<(u32, ResponsePayload)>,
    events: mpsc::Sender<ClientEvent>,
    disconnect: watch::Sender<bool>,
    event_state: SharedClientEventState,
}

/// Gateway-owned transport audience and the response fence for one binding
/// transition. The outer `Option` distinguishes an unhandshaken transport from
/// the legacy anonymous binding represented by `Some(None)`.
#[derive(Default)]
struct ClientEventState {
    binding: Option<Option<String>>,
    fence: Option<ClientEventFence>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ClientEventFence {
    request_id: u32,
    policy: ClientEventFencePolicy,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ClientEventFencePolicy {
    /// A named-session switch invalidates every resource event queued before
    /// the binding response becomes visible.
    DropQueued,
    /// Foreground attach establishes an atomic snapshot/live boundary. Events
    /// produced after registration wait behind the response and are then kept.
    HoldQueued,
}

type SharedClientEventState = Arc<Mutex<ClientEventState>>;

/// Shared registry for each client's bounded outbound queues.
type ClientMap = Arc<Mutex<HashMap<u64, ClientQueues>>>;
type SharedOperationLedger = Arc<Mutex<OperationLedger>>;

/// Outbound event plumbing owned by one client transport.
///
/// Keeping these two channels together avoids growing the request router's
/// argument list every time transport event handling gains another concern.
struct ClientEventSink<'a> {
    sender: &'a mpsc::Sender<ClientEvent>,
    disconnect: &'a watch::Sender<bool>,
}

fn client_registry(clients: &ClientMap) -> MutexGuard<'_, HashMap<u64, ClientQueues>> {
    clients
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn operation_ledger(operations: &SharedOperationLedger) -> MutexGuard<'_, OperationLedger> {
    operations
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn client_event_state(state: &SharedClientEventState) -> MutexGuard<'_, ClientEventState> {
    state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn client_event_fenced(state: &SharedClientEventState) -> bool {
    client_event_state(state).fence.is_some()
}

fn event_audience_matches(
    binding: &Option<Option<String>>,
    audience: &ClientEventAudience,
) -> bool {
    match audience {
        ClientEventAudience::Global => true,
        ClientEventAudience::Session(owner) => match binding {
            // Resource events are never delivered before the handshake has
            // established the transport's compatibility mode.
            None => false,
            // Anonymous transports intentionally retain the legacy global
            // resource view.
            Some(None) => true,
            Some(Some(attached)) => owner.as_deref() == Some(attached.as_str()),
        },
    }
}

fn client_event_is_deliverable(state: &SharedClientEventState, event: &ClientEvent) -> bool {
    let state = client_event_state(state);
    state.fence.is_none() && event_audience_matches(&state.binding, &event.audience)
}

fn client_event_can_enqueue(state: &SharedClientEventState, event: &ClientEvent) -> bool {
    let state = client_event_state(state);
    event_audience_matches(&state.binding, &event.audience)
        && !matches!(
            state.fence,
            Some(ClientEventFence {
                policy: ClientEventFencePolicy::DropQueued,
                ..
            })
        )
}

/// Start the outbound half of a binding transition before any await can let a
/// direct event overtake it. Returns whether the binding itself changed.
fn begin_client_event_fence(
    state: &SharedClientEventState,
    request_id: u32,
    named_session_id: Option<String>,
) -> bool {
    let mut state = client_event_state(state);
    let changed = state.binding.as_ref() != Some(&named_session_id);
    state.binding = Some(named_session_id);
    state.fence = Some(ClientEventFence {
        request_id,
        policy: ClientEventFencePolicy::DropQueued,
    });
    changed
}

/// Hold matching events until a foreground response has been written. Unlike
/// a binding switch, these events happened after the attachment's atomic
/// snapshot cut and must be delivered rather than drained.
fn begin_client_event_hold_fence(state: &SharedClientEventState, request_id: u32) {
    let mut state = client_event_state(state);
    debug_assert!(state.fence.is_none(), "client already has an event fence");
    state.fence = Some(ClientEventFence {
        request_id,
        policy: ClientEventFencePolicy::HoldQueued,
    });
}

/// A binding response is the visibility barrier. Discard everything that was
/// queued while the transition was in flight, then reopen delivery. Audience
/// metadata on later events protects the small enqueue race around this drain.
fn complete_client_event_fence(
    state: &SharedClientEventState,
    request_id: u32,
    events: &mut mpsc::Receiver<ClientEvent>,
) {
    let mut state = client_event_state(state);
    let Some(fence) = state.fence else {
        return;
    };
    if fence.request_id != request_id {
        return;
    }
    if fence.policy == ClientEventFencePolicy::DropQueued {
        while events.try_recv().is_ok() {}
    }
    state.fence = None;
}

fn reserve_request_id(inflight: &Arc<Mutex<HashSet<u32>>>, request_id: u32) -> bool {
    let mut inflight = inflight
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if inflight.len() >= MAX_INFLIGHT_REQUESTS_PER_CLIENT {
        return false;
    }
    inflight.insert(request_id)
}

fn release_request_id(inflight: &Arc<Mutex<HashSet<u32>>>, request_id: u32) {
    inflight
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .remove(&request_id);
}

/// Spawn the Gateway actor.
///
/// This creates a Unix socket listener and spawns a task that accepts connections.
/// Per-client handler tasks are spawned for each connection.
pub(super) async fn spawn(
    mut rx: mpsc::Receiver<GatewayMsg>,
    socket_path: PathBuf,
    sys: ActorSystem,
    runtime_db: crate::storage::SharedConnection,
    lifecycle: Arc<crate::lifecycle::DaemonLifecycle>,
) -> Result<()> {
    // Startup owns stale-socket cleanup while holding the socket-specific
    // instance lock. The gateway must never unlink a path that may belong to a
    // live listener.
    let listener = bind_private_listener(&socket_path)?;

    info!(path = %socket_path.display(), "gateway: listening");

    // Shared state: client_id → bounded outbound queues and eviction signal.
    let clients: ClientMap = Arc::new(Mutex::new(HashMap::new()));
    // One authoritative in-memory ledger spans every transport connection and
    // is hydrated from durable completion facts before accepting requests.
    let facts = crate::storage::with_connection(&runtime_db, crate::storage::load_operation_facts)
        .await
        .context("load durable operation facts")?;
    let operations: SharedOperationLedger = Arc::new(Mutex::new(
        OperationLedger::restore(facts).context("restore operation ledger")?,
    ));

    let clients_for_dispatch = Arc::clone(&clients);
    let operations_for_dispatch = Arc::clone(&operations);
    let runtime_db_for_dispatch = runtime_db.clone();

    // Accept loop — runs in its own task.
    let sys_accept = sys.clone();
    let operations_for_accept = Arc::clone(&operations);
    let lifecycle_for_accept = Arc::clone(&lifecycle);
    let accept_handle = tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((stream, _addr)) => {
                    let client_id = NEXT_CLIENT_ID.fetch_add(1, Ordering::Relaxed);
                    info!(%client_id, "gateway: client connected");
                    let sys_clone = sys_accept.clone();
                    let clients_clone = Arc::clone(&clients_for_dispatch);
                    let operations_clone = Arc::clone(&operations_for_accept);
                    let lifecycle_clone = Arc::clone(&lifecycle_for_accept);
                    tokio::spawn(handle_client(
                        client_id,
                        stream,
                        sys_clone,
                        clients_clone,
                        operations_clone,
                        lifecycle_clone,
                    ));
                }
                Err(e) => {
                    error!("gateway: accept error: {e}");
                }
            }
        }
    });

    // Dispatch loop — routes responses/events back to clients.
    tokio::spawn(async move {
        let mut accept_handle = Some(accept_handle);
        while let Some(msg) = rx.recv().await {
            match msg {
                GatewayMsg::SendResponse {
                    client_id,
                    request_id,
                    payload,
                } => {
                    let routed_request = OperationWaiter {
                        client_id,
                        request_id,
                    };
                    let completion = operation_ledger(&operations_for_dispatch)
                        .complete(routed_request, payload.clone());
                    if let Some(completion) = completion {
                        let fact = completion.fact;
                        if let Err(error) =
                            crate::storage::with_connection(&runtime_db_for_dispatch, move |conn| {
                                crate::storage::store_operation_fact(conn, &fact)
                            })
                            .await
                        {
                            error!(%error, "gateway: failed to persist operation fact");
                        }
                        for waiter in completion.waiters {
                            queue_response_for_client(
                                &clients,
                                waiter.client_id,
                                waiter.request_id,
                                completion.response.clone(),
                            );
                        }
                    } else {
                        queue_response_for_client(&clients, client_id, request_id, payload);
                    }
                }

                GatewayMsg::SendEvent {
                    client_id,
                    payload,
                    session_id,
                } => {
                    queue_event_for_client(
                        &clients,
                        client_id,
                        ClientEvent::session(payload, session_id),
                    );
                }

                GatewayMsg::Shutdown => {
                    info!("gateway: shutdown signal received");
                    if let Some(handle) = accept_handle.take() {
                        handle.abort();
                        let _ = handle.await;
                    }
                    break;
                }
            }
        }

        if let Some(handle) = accept_handle.take() {
            handle.abort();
            let _ = handle.await;
        }

        debug!("gateway: dispatch loop stopped");
    });

    Ok(())
}

fn bind_private_listener(socket_path: &Path) -> Result<UnixListener> {
    let listener = UnixListener::bind(socket_path)
        .with_context(|| format!("bind socket {}", socket_path.display()))?;
    if let Err(error) = crate::dirs::secure_private_file(socket_path) {
        drop(listener);
        let _ = std::fs::remove_file(socket_path);
        return Err(error).with_context(|| format!("secure socket {}", socket_path.display()));
    }
    Ok(listener)
}

/// Handle one client connection.
async fn handle_client(
    client_id: u64,
    stream: UnixStream,
    sys: ActorSystem,
    clients: ClientMap,
    operations: SharedOperationLedger,
    lifecycle: Arc<crate::lifecycle::DaemonLifecycle>,
) {
    // Per-client response channel.
    let (resp_tx, mut resp_rx) = mpsc::channel::<(u32, ResponsePayload)>(CLIENT_RESPONSE_CAP);
    // Per-client event channel.
    let (evt_tx, mut evt_rx) = mpsc::channel::<ClientEvent>(CLIENT_EVENT_CAP);
    let (disconnect_tx, mut disconnect_rx) = watch::channel(false);
    let inflight_request_ids = Arc::new(Mutex::new(HashSet::new()));
    let event_state = Arc::new(Mutex::new(ClientEventState::default()));

    // Register.
    client_registry(&clients).insert(
        client_id,
        ClientQueues {
            responses: resp_tx,
            events: evt_tx.clone(),
            disconnect: disconnect_tx.clone(),
            event_state: Arc::clone(&event_state),
        },
    );
    let mut session_namespace = None;

    // Framing reads are not cancellation-safe. A dedicated reader owns each
    // full length-prefix/body read so outbound traffic can never drop a
    // partially consumed inbound frame.
    let (mut reader, mut writer) = stream.into_split();
    let (incoming_tx, mut incoming_rx) = mpsc::channel(CLIENT_RESPONSE_CAP);
    let reader_handle = tokio::spawn(async move {
        loop {
            let message = read_message(&mut reader)
                .await
                .map_err(|error| error.to_string());
            let terminal = message.is_err();
            if incoming_tx.send(message).await.is_err() || terminal {
                break;
            }
        }
    });

    loop {
        tokio::select! {
            // Receive complete frames from the non-cancellable reader loop.
            msg_result = incoming_rx.recv(), if !client_event_fenced(&event_state) => {
                let Some(msg_result) = msg_result else {
                    break;
                };
                match msg_result {
                    Ok(Message::Request {
                        id,
                        operation_id,
                        payload,
                    }) => {
                        if !reserve_request_id(&inflight_request_ids, id) {
                            warn!(
                                %client_id,
                                request_id = id,
                                "gateway: disconnecting client after duplicate or excessive in-flight request ids"
                            );
                            break;
                        }
                        let waiter = OperationWaiter {
                            client_id,
                            request_id: id,
                        };
                        let outcome = idempotency_outcome(
                            &operations,
                            session_namespace,
                            operation_id.as_deref(),
                            &payload,
                            waiter,
                        );
                        let should_route = match outcome {
                            Ok(BeginOutcome::Route) => true,
                            Ok(BeginOutcome::Wait) => false,
                            Ok(BeginOutcome::Respond(payload)) => {
                                if sys.gateway.send(GatewayMsg::SendResponse {
                                    client_id,
                                    request_id: id,
                                    payload: *payload,
                                }).await.is_err() {
                                    break;
                                }
                                false
                            }
                            Err(error) => {
                                if sys.gateway.send(GatewayMsg::SendResponse {
                                    client_id,
                                    request_id: id,
                                    payload: ResponsePayload::err(
                                        error_code::INTERNAL,
                                        error.to_string(),
                                    ),
                                }).await.is_err() {
                                    break;
                                }
                                false
                            }
                        };
                        if should_route {
                            match route_request(
                                client_id,
                                id,
                                payload,
                                &sys,
                                ClientEventSink {
                                    sender: &evt_tx,
                                    disconnect: &disconnect_tx,
                                },
                                &lifecycle,
                                &event_state,
                            ).await {
                                Ok(Some(established_namespace)) => {
                                    session_namespace = Some(established_namespace);
                                }
                                Ok(None) => {}
                                Err(e) => {
                                    warn!(%client_id, "gateway: route error: {e}");
                                    if sys.gateway.send(GatewayMsg::SendResponse {
                                        client_id,
                                        request_id: id,
                                        payload: ResponsePayload::err(
                                            error_code::INTERNAL,
                                            e.to_string(),
                                        ),
                                    }).await.is_err() {
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    Ok(_) => {
                        // Clients should only send Request messages.
                        warn!(%client_id, "gateway: unexpected non-request message");
                    }
                    Err(e) => {
                        debug!(%client_id, "gateway: read error (disconnect?): {e}");
                        break;
                    }
                }
            }

            // Deliver response back to client.
            Some((request_id, payload)) = resp_rx.recv() => {
                let is_restart_response = matches!(
                    &payload,
                    ResponsePayload::Ok(OkPayload::RestartAccepted { .. })
                );
                let msg = Message::Response { id: request_id, payload };
                let write_result = write_client_message(&mut writer, &msg).await;
                if is_restart_response {
                    // Successful flush satisfies ACK-before-teardown. A failed
                    // flush resolves the response gate too: the restart remains
                    // accepted, but the caller must treat the result as ambiguous.
                    lifecycle.mark_restart_response_complete(client_id, request_id);
                }
                if write_result.is_err() {
                    break;
                }
                complete_client_event_fence(&event_state, request_id, &mut evt_rx);
                // Keep the fence until bytes are written, not merely queued.
                release_request_id(&inflight_request_ids, request_id);
            }

            // Deliver pushed event to client.
            Some(event) = evt_rx.recv(), if !client_event_fenced(&event_state) => {
                if !client_event_is_deliverable(&event_state, &event) {
                    debug!(%client_id, "gateway: dropping event outside current binding or response fence");
                    continue;
                }
                let msg = Message::Event {
                    payload: event.payload,
                };
                if write_client_message(&mut writer, &msg).await.is_err() {
                    break;
                }
            }

            changed = disconnect_rx.changed() => {
                if changed.is_ok() && *disconnect_rx.borrow() {
                    warn!(%client_id, "gateway: disconnecting evicted client");
                }
                break;
            }
        }
    }

    // Cleanup.
    lifecycle.resolve_restart_response_disconnect(client_id);
    reader_handle.abort();
    let _ = reader_handle.await;
    info!(%client_id, "gateway: client disconnected");
    client_registry(&clients).remove(&client_id);
    operation_ledger(&operations).remove_waiters_for_client(client_id);
    if sys
        .event_bus
        .send(EventBusMsg::UnsubscribeAll { client_id })
        .await
        .is_err()
    {
        debug!(%client_id, "gateway: event bus unavailable during client cleanup");
    }
    if let Err(error) = detach_client_foreground(&sys, client_id, "client disconnected").await {
        debug!(%client_id, "gateway: foreground cleanup failed: {error}");
    }
    if sys
        .sessions
        .send(SessionCoordinatorMsg::Disconnect { client_id })
        .await
        .is_err()
    {
        debug!(%client_id, "gateway: session coordinator unavailable during client cleanup");
    }
}

fn idempotency_outcome(
    operations: &SharedOperationLedger,
    session_namespace: Option<[u8; 32]>,
    operation_id: Option<&str>,
    payload: &RequestPayload,
    waiter: OperationWaiter,
) -> Result<BeginOutcome> {
    let Some(operation_id) = operation_id else {
        return Ok(BeginOutcome::Route);
    };
    if !is_side_effecting_request(payload) {
        return Ok(BeginOutcome::respond(ResponsePayload::err(
            error_code::INVALID_REQUEST,
            "operation_id is supported only for daemon-global side-effecting requests",
        )));
    }
    let Some(session_namespace) = session_namespace else {
        return Ok(BeginOutcome::respond(ResponsePayload::err(
            error_code::INVALID_REQUEST,
            "operation_id requires a successful session handshake",
        )));
    };
    let fingerprint = OperationLedger::fingerprint(payload).context("fingerprint IPC request")?;
    Ok(operation_ledger(operations).begin(session_namespace, operation_id, fingerprint, waiter))
}

fn is_side_effecting_request(payload: &RequestPayload) -> bool {
    matches!(
        payload,
        RequestPayload::SubmitExecution { .. }
            | RequestPayload::CancelExecution { .. }
            | RequestPayload::ApplyScopeDelta { .. }
            | RequestPayload::CreateSchedule { .. }
            | RequestPayload::PauseSchedule { .. }
            | RequestPayload::ResumeSchedule { .. }
            | RequestPayload::RemoveSchedule { .. }
            | RequestPayload::ArchiveSession { .. }
            | RequestPayload::RestoreSession { .. }
            | RequestPayload::Restart {}
            | RequestPayload::Shutdown {}
    )
}

fn draining_response() -> ResponsePayload {
    ResponsePayload::err(
        error_code::DAEMON_DRAINING,
        "daemon startup/restart handoff is in progress; new execution admission is closed",
    )
}

fn foreground_role_response(
    result: Result<Result<ForegroundRoleUpdate, String>, tokio::sync::oneshot::error::RecvError>,
    operation: &str,
) -> ResponsePayload {
    match result {
        Ok(Ok(update)) => ResponsePayload::Ok(OkPayload::FgRoleChanged {
            id: update.id,
            attachment_id: update.attachment_id,
            role: update.role,
            control_available: update.control_available,
        }),
        Ok(Err(message)) => ResponsePayload::err(error_code::INVALID_STATE, message),
        Err(error) => ResponsePayload::err(
            error_code::INTERNAL,
            format!("process manager dropped {operation} reply: {error}"),
        ),
    }
}

/// Route an incoming request to the appropriate actor.
async fn route_request(
    client_id: u64,
    request_id: u32,
    payload: RequestPayload,
    sys: &ActorSystem,
    event_sink: ClientEventSink<'_>,
    lifecycle: &crate::lifecycle::DaemonLifecycle,
    event_state: &SharedClientEventState,
) -> Result<Option<[u8; 32]>> {
    match payload {
        RequestPayload::Handshake {
            protocol_version,
            session_id,
            cwd,
            env,
            refresh,
        } => {
            if protocol_version != IPC_PROTOCOL_VERSION {
                sys.gateway
                    .send(GatewayMsg::SendResponse {
                        client_id,
                        request_id,
                        payload: ResponsePayload::err(
                            error_code::PROTOCOL_UPGRADE_REQUIRED,
                            format!(
                                "client IPC protocol {protocol_version} is unsupported; upgrade to protocol {IPC_PROTOCOL_VERSION}"
                            ),
                        ),
                    })
                    .await
                    .context("send protocol upgrade error")?;
                return Ok(None);
            }
            let snapshot = EnvSnapshot {
                env,
                cwd: PathBuf::from(cwd),
            };
            let (reply, result) = tokio::sync::oneshot::channel();
            sys.sessions
                .send(SessionCoordinatorMsg::Connect {
                    client_id,
                    session_id,
                    snapshot,
                    refresh,
                    reply,
                })
                .await
                .context("send session handshake")?;
            match result.await {
                Ok(Ok(binding)) => {
                    prepare_client_binding(sys, event_state, client_id, request_id, &binding)
                        .await?;
                    sys.gateway
                        .send(GatewayMsg::SendResponse {
                            client_id,
                            request_id,
                            payload: ResponsePayload::ack(),
                        })
                        .await
                        .context("send handshake ack")?;
                    return Ok(Some(OperationLedger::session_incarnation_namespace(
                        &binding.session_id,
                        binding.incarnation,
                    )));
                }
                Ok(Err(error)) => {
                    sys.gateway
                        .send(GatewayMsg::SendResponse {
                            client_id,
                            request_id,
                            payload: ResponsePayload::err(error_code::INTERNAL, error.to_string()),
                        })
                        .await
                        .context("send handshake error")?;
                }
                Err(_) => {
                    sys.gateway
                        .send(GatewayMsg::SendResponse {
                            client_id,
                            request_id,
                            payload: ResponsePayload::err(
                                error_code::INTERNAL,
                                "scheduler session reply dropped",
                            ),
                        })
                        .await
                        .context("send handshake dropped error")?;
                }
            }
        }

        RequestPayload::CreateSession { name } => {
            return route_session_command(
                client_id,
                request_id,
                SessionCommand::Create { name },
                sys,
                event_state,
            )
            .await;
        }

        RequestPayload::ListSessions {} => {
            return route_session_command(
                client_id,
                request_id,
                SessionCommand::List,
                sys,
                event_state,
            )
            .await;
        }

        RequestPayload::ListArchivedSessions {} => {
            return route_session_command(
                client_id,
                request_id,
                SessionCommand::ListArchived,
                sys,
                event_state,
            )
            .await;
        }

        RequestPayload::ListAllSessions {} => {
            return route_session_command(
                client_id,
                request_id,
                SessionCommand::ListAll,
                sys,
                event_state,
            )
            .await;
        }

        RequestPayload::ArchiveSession { selector } => {
            return route_session_command(
                client_id,
                request_id,
                SessionCommand::Archive { selector },
                sys,
                event_state,
            )
            .await;
        }

        RequestPayload::RestoreSession { selector } => {
            return route_session_command(
                client_id,
                request_id,
                SessionCommand::Restore { selector },
                sys,
                event_state,
            )
            .await;
        }

        RequestPayload::AttachSession { selector, refresh } => {
            return route_session_command(
                client_id,
                request_id,
                SessionCommand::Attach { selector, refresh },
                sys,
                event_state,
            )
            .await;
        }

        RequestPayload::SessionInfo { selector } => {
            return route_session_command(
                client_id,
                request_id,
                SessionCommand::Info { selector },
                sys,
                event_state,
            )
            .await;
        }

        RequestPayload::ListScopes { limit } => {
            if current_binding(sys, client_id).await?.is_none() {
                send_handshake_required(sys, client_id, request_id).await?;
                return Ok(None);
            }
            let (reply, scopes) = tokio::sync::oneshot::channel();
            sys.scope_store
                .send(ScopeStoreMsg::ListScopes { reply })
                .await
                .context("send typed scope list")?;
            let payload = match scopes.await.context("typed scope list reply dropped")? {
                Ok(scopes) => {
                    let total = scopes.len();
                    let shown = limit.map_or(total, |limit| limit.min(total));
                    ResponsePayload::Ok(OkPayload::ScopeListPage {
                        scopes: scopes.into_iter().take(shown).collect(),
                        page: PageInfo {
                            total,
                            shown,
                            limit,
                            truncated: shown < total,
                        },
                    })
                }
                Err(error) => ResponsePayload::err(error_code::INTERNAL, error.to_string()),
            };
            send_typed_response(
                sys,
                client_id,
                request_id,
                payload,
                "send typed scope list response",
            )
            .await?;
        }

        RequestPayload::ListResources {} => {
            if current_binding(sys, client_id).await?.is_none() {
                send_handshake_required(sys, client_id, request_id).await?;
                return Ok(None);
            }
            let routes = sys.resources.key_routes();
            let reservations: std::collections::BTreeMap<_, _> = sys
                .resources
                .active_reservation_counts()
                .into_iter()
                .collect();
            let providers = sys
                .resources
                .snapshot()
                .into_iter()
                .map(|(id, snapshot)| cue_core::ipc::ResourceProviderInfo {
                    keys: routes
                        .iter()
                        .filter_map(|(key, provider)| (provider == &id).then_some(key.clone()))
                        .collect(),
                    active_reservations: reservations.get(&id).copied().unwrap_or(0),
                    captured_at_ms: snapshot
                        .captured_at
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis()
                        .try_into()
                        .unwrap_or(u64::MAX),
                    units: snapshot.units,
                    id: id.to_string(),
                })
                .collect();
            send_typed_response(
                sys,
                client_id,
                request_id,
                ResponsePayload::Ok(OkPayload::ResourceList(providers)),
                "send typed resource list response",
            )
            .await?;
        }

        RequestPayload::SubmitExecution { spec } => {
            if lifecycle.execution_admission_closed() {
                sys.gateway
                    .send(GatewayMsg::SendResponse {
                        client_id,
                        request_id,
                        payload: draining_response(),
                    })
                    .await
                    .context("send draining execution rejection")?;
                return Ok(None);
            }
            let Some(binding) = current_binding(sys, client_id).await? else {
                send_handshake_required(sys, client_id, request_id).await?;
                return Ok(None);
            };
            sys.execution
                .send(ExecutionCoordinatorMsg::Submit {
                    client_id,
                    request_id,
                    spec,
                    binding,
                })
                .await
                .context("send execution submission")?;
        }
        RequestPayload::GetExecution { id } => {
            let Some(binding) = current_binding(sys, client_id).await? else {
                send_handshake_required(sys, client_id, request_id).await?;
                return Ok(None);
            };
            sys.execution
                .send(ExecutionCoordinatorMsg::Get {
                    client_id,
                    request_id,
                    id,
                    named_session_id: binding.named_session_id,
                })
                .await
                .context("send execution query")?;
        }
        RequestPayload::ListExecutions { limit } => {
            let Some(binding) = current_binding(sys, client_id).await? else {
                send_handshake_required(sys, client_id, request_id).await?;
                return Ok(None);
            };
            sys.execution
                .send(ExecutionCoordinatorMsg::List {
                    client_id,
                    request_id,
                    limit,
                    named_session_id: binding.named_session_id,
                })
                .await
                .context("send execution list")?;
        }
        RequestPayload::WaitExecution { id } => {
            let Some(binding) = current_binding(sys, client_id).await? else {
                send_handshake_required(sys, client_id, request_id).await?;
                return Ok(None);
            };
            sys.execution
                .send(ExecutionCoordinatorMsg::Wait {
                    client_id,
                    request_id,
                    id,
                    named_session_id: binding.named_session_id,
                })
                .await
                .context("send execution wait")?;
        }
        RequestPayload::CancelExecution { id, mode } => {
            let Some(binding) = current_binding(sys, client_id).await? else {
                send_handshake_required(sys, client_id, request_id).await?;
                return Ok(None);
            };
            sys.execution
                .send(ExecutionCoordinatorMsg::Cancel {
                    client_id,
                    request_id,
                    id,
                    mode,
                    named_session_id: binding.named_session_id,
                })
                .await
                .context("send execution cancellation")?;
        }
        RequestPayload::ReadExecutionOutput {
            id,
            step_id,
            stdout_bytes,
            stderr_bytes,
        } => {
            let Some(binding) = current_binding(sys, client_id).await? else {
                send_handshake_required(sys, client_id, request_id).await?;
                return Ok(None);
            };
            sys.execution
                .send(ExecutionCoordinatorMsg::ReadOutput {
                    client_id,
                    request_id,
                    id,
                    step_id,
                    stdout_bytes,
                    stderr_bytes,
                    named_session_id: binding.named_session_id,
                })
                .await
                .context("send execution output query")?;
        }

        RequestPayload::StepAttach { id } => {
            let Some(binding) = current_binding(sys, client_id).await? else {
                send_handshake_required(sys, client_id, request_id).await?;
                return Ok(None);
            };
            begin_client_event_hold_fence(event_state, request_id);
            sys.execution
                .send(ExecutionCoordinatorMsg::AttachStep {
                    client_id,
                    request_id,
                    id,
                    role: ForegroundRole::Controller,
                    named_session_id: binding.named_session_id,
                })
                .await
                .context("send execution step attach")?;
        }

        RequestPayload::StepWatch { id } => {
            let Some(binding) = current_binding(sys, client_id).await? else {
                send_handshake_required(sys, client_id, request_id).await?;
                return Ok(None);
            };
            begin_client_event_hold_fence(event_state, request_id);
            sys.execution
                .send(ExecutionCoordinatorMsg::AttachStep {
                    client_id,
                    request_id,
                    id,
                    role: ForegroundRole::Observer,
                    named_session_id: binding.named_session_id,
                })
                .await
                .context("send execution step watch")?;
        }

        RequestPayload::ApplyScopeDelta { base, delta } => {
            let (reply, response) = tokio::sync::oneshot::channel();
            sys.sessions
                .send(SessionCoordinatorMsg::ApplyScopeDelta {
                    client_id,
                    base,
                    delta,
                    reply,
                })
                .await
                .context("send typed scope delta")?;
            let payload = response.await.context("typed scope delta reply dropped")?;
            sys.gateway
                .send(GatewayMsg::SendResponse {
                    client_id,
                    request_id,
                    payload,
                })
                .await
                .context("send typed scope delta response")?;
        }

        RequestPayload::GetScope { hash } => {
            if current_binding(sys, client_id).await?.is_none() {
                send_handshake_required(sys, client_id, request_id).await?;
                return Ok(None);
            }
            let (reply, scope) = tokio::sync::oneshot::channel();
            sys.scope_store
                .send(ScopeStoreMsg::GetScope { hash, reply })
                .await
                .context("send typed scope query")?;
            let payload = match scope.await.context("typed scope query reply dropped")? {
                Ok(Some(scope)) => match scope.snapshot {
                    Some(snapshot) => {
                        ResponsePayload::Ok(OkPayload::ScopeInfo(cue_core::ipc::ScopeInfo {
                            hash: scope.hash.to_string(),
                            parent: scope.parent.map(|parent| parent.to_string()),
                            cwd: snapshot.cwd.display().to_string(),
                            env_count: snapshot.env.len(),
                        }))
                    }
                    None => ResponsePayload::err(
                        error_code::INVALID_STATE,
                        format!("scope {hash} has no snapshot"),
                    ),
                },
                Ok(None) => {
                    ResponsePayload::err(error_code::NOT_FOUND, format!("scope {hash} not found"))
                }
                Err(error) => ResponsePayload::err(error_code::INTERNAL, error.to_string()),
            };
            sys.gateway
                .send(GatewayMsg::SendResponse {
                    client_id,
                    request_id,
                    payload,
                })
                .await
                .context("send typed scope query response")?;
        }

        RequestPayload::CreateSchedule {
            schedule,
            execution,
        } => {
            if lifecycle.execution_admission_closed() {
                sys.gateway
                    .send(GatewayMsg::SendResponse {
                        client_id,
                        request_id,
                        payload: draining_response(),
                    })
                    .await
                    .context("send draining schedule rejection")?;
                return Ok(None);
            }
            let Some(binding) = current_binding(sys, client_id).await? else {
                send_handshake_required(sys, client_id, request_id).await?;
                return Ok(None);
            };
            sys.triggers
                .send(TriggerServiceMsg::Create {
                    client_id,
                    request_id,
                    schedule: Box::new(schedule),
                    execution,
                    binding,
                })
                .await
                .context("send schedule creation")?;
        }
        RequestPayload::ListSchedules { limit } => {
            let Some(binding) = current_binding(sys, client_id).await? else {
                send_handshake_required(sys, client_id, request_id).await?;
                return Ok(None);
            };
            sys.triggers
                .send(TriggerServiceMsg::List {
                    client_id,
                    request_id,
                    limit,
                    named_session_id: binding.named_session_id,
                })
                .await
                .context("send schedule list")?;
        }
        RequestPayload::PauseSchedule { id } => {
            let Some(binding) = current_binding(sys, client_id).await? else {
                send_handshake_required(sys, client_id, request_id).await?;
                return Ok(None);
            };
            sys.triggers
                .send(TriggerServiceMsg::Pause {
                    client_id,
                    request_id,
                    id,
                    named_session_id: binding.named_session_id,
                })
                .await
                .context("send schedule pause")?;
        }
        RequestPayload::ResumeSchedule { id } => {
            if lifecycle.execution_admission_closed() {
                sys.gateway
                    .send(GatewayMsg::SendResponse {
                        client_id,
                        request_id,
                        payload: draining_response(),
                    })
                    .await
                    .context("send draining schedule resume rejection")?;
                return Ok(None);
            }
            let Some(binding) = current_binding(sys, client_id).await? else {
                send_handshake_required(sys, client_id, request_id).await?;
                return Ok(None);
            };
            sys.triggers
                .send(TriggerServiceMsg::Resume {
                    client_id,
                    request_id,
                    id,
                    named_session_id: binding.named_session_id,
                })
                .await
                .context("send schedule resume")?;
        }
        RequestPayload::RemoveSchedule { id } => {
            let Some(binding) = current_binding(sys, client_id).await? else {
                send_handshake_required(sys, client_id, request_id).await?;
                return Ok(None);
            };
            sys.triggers
                .send(TriggerServiceMsg::Remove {
                    client_id,
                    request_id,
                    id,
                    named_session_id: binding.named_session_id,
                })
                .await
                .context("send schedule removal")?;
        }

        RequestPayload::ShowEnv { tail_bytes } => {
            let payload = if let Some(response) = invalid_tail_bytes_response(tail_bytes) {
                response
            } else if let Some(binding) = current_binding(sys, client_id).await? {
                let (reply, scope) = tokio::sync::oneshot::channel();
                sys.scope_store
                    .send(ScopeStoreMsg::GetScope {
                        hash: binding.scope,
                        reply,
                    })
                    .await
                    .context("send typed environment scope query")?;
                match scope
                    .await
                    .context("typed environment scope reply dropped")?
                {
                    Ok(Some(scope)) => match scope.snapshot {
                        Some(snapshot) => {
                            text_output_response(format_snapshot_env(&snapshot), tail_bytes)
                        }
                        None => ResponsePayload::err(
                            error_code::INVALID_STATE,
                            format!("scope {} has no snapshot", binding.scope),
                        ),
                    },
                    Ok(None) => ResponsePayload::err(
                        error_code::NOT_FOUND,
                        format!("scope {} not found", binding.scope),
                    ),
                    Err(error) => ResponsePayload::err(error_code::INTERNAL, error.to_string()),
                }
            } else {
                handshake_required_response()
            };
            send_typed_response(
                sys,
                client_id,
                request_id,
                payload,
                "send typed environment response",
            )
            .await?;
        }

        RequestPayload::ShowConfig { tail_bytes } => {
            let payload = if current_binding(sys, client_id).await?.is_none() {
                handshake_required_response()
            } else if let Some(response) = invalid_tail_bytes_response(tail_bytes) {
                response
            } else {
                text_output_response(sys.config.display_text(), tail_bytes)
            };
            send_typed_response(
                sys,
                client_id,
                request_id,
                payload,
                "send typed config response",
            )
            .await?;
        }

        RequestPayload::Subscribe { channels } => {
            let channels = match EventChannel::parse_list(&channels) {
                Ok(channels) => channels,
                Err(error) => {
                    sys.gateway
                        .send(GatewayMsg::SendResponse {
                            client_id,
                            request_id,
                            payload: invalid_event_channel_response(error.input()),
                        })
                        .await
                        .context("send invalid subscribe response")?;
                    return Ok(None);
                }
            };
            for channel in channels {
                sys.event_bus
                    .send(EventBusMsg::Subscribe {
                        client_id,
                        channel,
                        sender: event_sink.sender.clone(),
                        disconnect: event_sink.disconnect.clone(),
                    })
                    .await?;
            }
            sys.gateway
                .send(GatewayMsg::SendResponse {
                    client_id,
                    request_id,
                    payload: ResponsePayload::ack(),
                })
                .await?;
        }

        RequestPayload::Unsubscribe { channels } => {
            let channels = match EventChannel::parse_list(&channels) {
                Ok(channels) => channels,
                Err(error) => {
                    sys.gateway
                        .send(GatewayMsg::SendResponse {
                            client_id,
                            request_id,
                            payload: invalid_event_channel_response(error.input()),
                        })
                        .await
                        .context("send invalid unsubscribe response")?;
                    return Ok(None);
                }
            };
            for channel in channels {
                sys.event_bus
                    .send(EventBusMsg::Unsubscribe { client_id, channel })
                    .await?;
            }
            sys.gateway
                .send(GatewayMsg::SendResponse {
                    client_id,
                    request_id,
                    payload: ResponsePayload::ack(),
                })
                .await?;
        }

        RequestPayload::StepClaimControl {} => {
            begin_client_event_hold_fence(event_state, request_id);
            let (tx, rx) = tokio::sync::oneshot::channel();
            sys.process_mgr
                .send(super::ProcessMgrMsg::ClaimFgControl {
                    client_id,
                    reply: tx,
                })
                .await
                .context("claim foreground control")?;
            let payload = foreground_role_response(rx.await, "claim foreground control");
            sys.gateway
                .send(GatewayMsg::SendResponse {
                    client_id,
                    request_id,
                    payload,
                })
                .await?;
        }

        RequestPayload::StepReleaseControl {} => {
            begin_client_event_hold_fence(event_state, request_id);
            let (tx, rx) = tokio::sync::oneshot::channel();
            sys.process_mgr
                .send(super::ProcessMgrMsg::ReleaseFgControl {
                    client_id,
                    reply: tx,
                })
                .await
                .context("release foreground control")?;
            let payload = foreground_role_response(rx.await, "release foreground control");
            sys.gateway
                .send(GatewayMsg::SendResponse {
                    client_id,
                    request_id,
                    payload,
                })
                .await?;
        }

        RequestPayload::StepDetach {} => {
            detach_client_foreground(sys, client_id, "detached").await?;
            sys.gateway
                .send(GatewayMsg::SendResponse {
                    client_id,
                    request_id,
                    payload: ResponsePayload::ack(),
                })
                .await?;
        }

        RequestPayload::StepInput { data } => {
            let (tx, rx) = tokio::sync::oneshot::channel();
            sys.process_mgr
                .send(super::ProcessMgrMsg::FgInput {
                    client_id,
                    data,
                    reply: tx,
                })
                .await
                .context("send fg input to process_mgr")?;
            let payload = match rx.await {
                Ok(Ok(())) => ResponsePayload::ack(),
                Ok(Err(message)) => ResponsePayload::err(error_code::INVALID_STATE, message),
                Err(_) => ResponsePayload::err(error_code::INTERNAL, "process_mgr unreachable"),
            };
            sys.gateway
                .send(GatewayMsg::SendResponse {
                    client_id,
                    request_id,
                    payload,
                })
                .await?;
        }

        RequestPayload::StepResize { cols, rows } => {
            let (tx, rx) = tokio::sync::oneshot::channel();
            sys.process_mgr
                .send(super::ProcessMgrMsg::FgResize {
                    client_id,
                    cols,
                    rows,
                    reply: tx,
                })
                .await
                .context("send fg resize to process_mgr")?;
            let payload = match rx.await {
                Ok(Ok(())) => ResponsePayload::ack(),
                Ok(Err(message)) => ResponsePayload::err(error_code::INVALID_STATE, message),
                Err(_) => ResponsePayload::err(error_code::INTERNAL, "process_mgr unreachable"),
            };
            sys.gateway
                .send(GatewayMsg::SendResponse {
                    client_id,
                    request_id,
                    payload,
                })
                .await?;
        }

        RequestPayload::Ping {} => {
            sys.gateway
                .send(GatewayMsg::SendResponse {
                    client_id,
                    request_id,
                    payload: ResponsePayload::Ok(OkPayload::Pong {
                        version: crate::version().to_string(),
                        instance_id: crate::daemon_instance_id().to_string(),
                        generation_id: crate::daemon_generation_id().to_string(),
                        ready: lifecycle.is_execution_ready(),
                        protocol_version: IPC_PROTOCOL_VERSION,
                        capabilities: current_protocol_capabilities(),
                    }),
                })
                .await?;
        }

        RequestPayload::Restart {} => {
            if !lifecycle.is_execution_ready() {
                sys.gateway
                    .send(GatewayMsg::SendResponse {
                        client_id,
                        request_id,
                        payload: draining_response(),
                    })
                    .await
                    .context("send starting restart rejection")?;
                return Ok(None);
            }
            let ticket = lifecycle.request_restart(client_id, request_id)?;
            if ticket.first_request {
                let (execution_reply, execution_accepted) = tokio::sync::oneshot::channel();
                let execution_closed = sys
                    .execution
                    .send(ExecutionCoordinatorMsg::BeginDrain {
                        reply: execution_reply,
                    })
                    .await
                    .is_err()
                    || execution_accepted.await.is_err();
                let (trigger_reply, trigger_accepted) = tokio::sync::oneshot::channel();
                let triggers_closed = sys
                    .triggers
                    .send(TriggerServiceMsg::BeginDrain {
                        reply: trigger_reply,
                    })
                    .await
                    .is_err()
                    || trigger_accepted.await.is_err();
                if execution_closed || triggers_closed {
                    // An execution owner may already have closed admission before
                    // dropping its acknowledgement. Never reopen only one side
                    // of that boundary: cancel the durable successor fence and
                    // fail-stop the daemon through the coordinated signal path.
                    lifecycle.fail_stop_restart()?;
                    sys.gateway
                        .send(GatewayMsg::SendResponse {
                            client_id,
                            request_id,
                            payload: ResponsePayload::err(
                                error_code::INTERNAL,
                                "execution owners could not begin daemon drain",
                            ),
                        })
                        .await?;
                    return Ok(None);
                }
                lifecycle.mark_drained();
            }
            sys.gateway
                .send(GatewayMsg::SendResponse {
                    client_id,
                    request_id,
                    payload: ResponsePayload::Ok(OkPayload::RestartAccepted {
                        restart_id: ticket.restart_id,
                        daemon_instance_id: ticket.daemon_instance_id,
                        target_generation: ticket.target_generation,
                    }),
                })
                .await?;
        }

        RequestPayload::Shutdown {} => {
            info!("gateway: shutdown request from client {client_id}");
            lifecycle.cancel_restart_for_shutdown()?;
            sys.gateway
                .send(GatewayMsg::SendResponse {
                    client_id,
                    request_id,
                    payload: ResponsePayload::ack(),
                })
                .await?;
            // Signal the main process so async_main performs a full coordinated shutdown.
            unsafe {
                libc::kill(std::process::id() as i32, libc::SIGTERM);
            }
        }
    }

    Ok(None)
}

async fn current_binding(sys: &ActorSystem, client_id: u64) -> Result<Option<SessionBinding>> {
    let (reply, binding) = tokio::sync::oneshot::channel();
    sys.sessions
        .send(SessionCoordinatorMsg::CurrentBinding { client_id, reply })
        .await
        .context("query current session binding")?;
    binding.await.context("session binding reply dropped")
}

async fn send_handshake_required(sys: &ActorSystem, client_id: u64, request_id: u32) -> Result<()> {
    sys.gateway
        .send(GatewayMsg::SendResponse {
            client_id,
            request_id,
            payload: ResponsePayload::err(
                error_code::INVALID_REQUEST,
                "client session handshake required",
            ),
        })
        .await
        .context("send handshake-required response")
}

async fn route_session_command(
    client_id: u64,
    request_id: u32,
    command: SessionCommand,
    sys: &ActorSystem,
    event_state: &SharedClientEventState,
) -> Result<Option<[u8; 32]>> {
    let (reply, result) = tokio::sync::oneshot::channel();
    sys.sessions
        .send(SessionCoordinatorMsg::Session {
            client_id,
            command,
            reply,
        })
        .await
        .context("send named-session request")?;
    let result = result.await.context("session coordinator reply dropped")?;
    let namespace = result.binding.as_ref().map(|binding| {
        OperationLedger::session_incarnation_namespace(&binding.session_id, binding.incarnation)
    });
    if let Some(binding) = result.binding.as_ref() {
        prepare_client_binding(sys, event_state, client_id, request_id, binding).await?;
    }
    sys.gateway
        .send(GatewayMsg::SendResponse {
            client_id,
            request_id,
            payload: result.payload,
        })
        .await
        .context("send named-session response")?;
    Ok(namespace)
}

async fn prepare_client_binding(
    sys: &ActorSystem,
    event_state: &SharedClientEventState,
    client_id: u64,
    request_id: u32,
    binding: &SessionBinding,
) -> Result<()> {
    // This update must happen before the first await after the scheduler has
    // accepted the binding, otherwise a direct event can overtake the fence.
    let binding_changed =
        begin_client_event_fence(event_state, request_id, binding.named_session_id.clone());
    bind_client_event_session(sys, client_id, binding.named_session_id.clone()).await?;
    if binding_changed {
        detach_client_foreground(sys, client_id, "session binding changed").await?;
    }
    Ok(())
}

async fn detach_client_foreground(sys: &ActorSystem, client_id: u64, reason: &str) -> Result<()> {
    let (reply, detached) = tokio::sync::oneshot::channel();
    sys.process_mgr
        .send(super::ProcessMgrMsg::DetachFg {
            client_id,
            reason: reason.to_string(),
            reply: Some(reply),
        })
        .await
        .context("send foreground detach to process manager")?;
    detached
        .await
        .context("process manager dropped foreground detach acknowledgement")
}

async fn bind_client_event_session(
    sys: &ActorSystem,
    client_id: u64,
    named_session_id: Option<String>,
) -> Result<()> {
    sys.event_bus
        .send(EventBusMsg::SetClientSession {
            client_id,
            named_session_id,
        })
        .await
        .context("bind client event session")
}

async fn send_typed_response(
    sys: &ActorSystem,
    client_id: u64,
    request_id: u32,
    payload: ResponsePayload,
    context: &'static str,
) -> Result<()> {
    sys.gateway
        .send(GatewayMsg::SendResponse {
            client_id,
            request_id,
            payload,
        })
        .await
        .context(context)
}

fn handshake_required_response() -> ResponsePayload {
    ResponsePayload::err(
        error_code::INVALID_REQUEST,
        "client session handshake required",
    )
}

fn invalid_tail_bytes_response(tail_bytes: Option<usize>) -> Option<ResponsePayload> {
    tail_bytes
        .filter(|bytes| *bytes > MAX_OUTPUT_TAIL_BYTES)
        .map(|_| {
            ResponsePayload::err(
                error_code::INVALID_SYNTAX,
                format!("tail_bytes must be <= {MAX_OUTPUT_TAIL_BYTES} bytes"),
            )
        })
}

fn text_output_response(text: String, tail_bytes: Option<usize>) -> ResponsePayload {
    let (text, truncated) = match tail_bytes {
        Some(limit) => tail_utf8(&text, limit),
        None => (text, false),
    };
    ResponsePayload::Ok(OkPayload::TextOutput {
        text,
        truncated,
        encoding: OutputEncoding::Utf8,
        base64: None,
    })
}

fn tail_utf8(text: &str, max_bytes: usize) -> (String, bool) {
    if max_bytes == 0 {
        return (String::new(), !text.is_empty());
    }
    if text.len() <= max_bytes {
        return (text.to_owned(), false);
    }
    let mut start = text.len() - max_bytes;
    while !text.is_char_boundary(start) {
        start += 1;
    }
    (text[start..].to_owned(), true)
}

fn format_snapshot_env(snapshot: &EnvSnapshot) -> String {
    let mut lines = vec![format!("cwd={}", snapshot.cwd.display())];
    lines.extend(
        snapshot
            .env
            .iter()
            .map(|(key, value)| format!("{key}={}", value.escape_default())),
    );
    lines.join("\n")
}

fn queue_response_for_client(
    clients: &ClientMap,
    client_id: u64,
    request_id: u32,
    payload: ResponsePayload,
) {
    let client = client_registry(clients).get(&client_id).cloned();

    if let Some(client) = client {
        match client.responses.try_send((request_id, payload)) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                warn!(%client_id, "gateway: evicting lagging client with full response queue");
                evict_client(clients, client_id);
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                warn!(%client_id, "gateway: evicting client with closed response queue");
                evict_client(clients, client_id);
            }
        }
    } else {
        warn!(%client_id, "gateway: no such client for response");
    }
}

fn queue_event_for_client(clients: &ClientMap, client_id: u64, event: ClientEvent) {
    let client = client_registry(clients).get(&client_id).cloned();

    if let Some(client) = client {
        if !client_event_can_enqueue(&client.event_state, &event) {
            debug!(%client_id, "gateway: filtered direct event outside current binding or response fence");
            return;
        }
        match client.events.try_send(event) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                warn!(%client_id, "gateway: evicting lagging client with full direct-event queue");
                evict_client(clients, client_id);
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                warn!(%client_id, "gateway: evicting client with closed direct-event queue");
                evict_client(clients, client_id);
            }
        }
    } else {
        warn!(%client_id, "gateway: no such client for direct event");
    }
}

fn evict_client(clients: &ClientMap, client_id: u64) {
    let client = client_registry(clients).remove(&client_id);
    if let Some(client) = client {
        let _ = client.disconnect.send(true);
    }
}

fn invalid_event_channel_response(channel: &str) -> ResponsePayload {
    ResponsePayload::err(
        error_code::INVALID_REQUEST,
        format!(
            "invalid event channel `{channel}`; expected {}",
            EventChannel::EXPECTED
        ),
    )
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt as _;

    use super::*;

    #[tokio::test]
    async fn custom_socket_is_private_after_bind() {
        let socket = PathBuf::from(format!(
            "/tmp/cue-gateway-permissions-{}-{}.sock",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system time")
                .as_nanos()
        ));

        let listener = bind_private_listener(&socket).expect("bind private listener");

        assert_eq!(
            std::fs::metadata(&socket)
                .expect("stat socket")
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        drop(listener);
        std::fs::remove_file(socket).expect("remove socket");
    }

    #[tokio::test]
    async fn existing_live_socket_is_rejected_without_unlinking_it() {
        let socket = PathBuf::from(format!(
            "/tmp/cue-gateway-live-{}-{}.sock",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system time")
                .as_nanos()
        ));
        let listener = bind_private_listener(&socket).expect("bind first listener");

        let error = bind_private_listener(&socket).expect_err("second bind must fail");
        assert!(
            error.to_string().contains("bind socket"),
            "unexpected error: {error:#}"
        );
        assert!(socket.exists(), "live socket path must remain in place");
        let _client = UnixStream::connect(&socket)
            .await
            .expect("first listener remains reachable");

        drop(listener);
        std::fs::remove_file(socket).expect("remove socket");
    }

    #[tokio::test]
    async fn message_framing_roundtrip() {
        // Create a connected pair.
        let (mut client, mut server) = UnixStream::pair().unwrap();

        let msg = Message::Request {
            id: 42,
            operation_id: None,
            payload: RequestPayload::Ping {},
        };

        write_message(&mut client, &msg).await.unwrap();
        let decoded = read_message(&mut server).await.unwrap();

        if let Message::Request {
            id,
            payload: RequestPayload::Ping {},
            ..
        } = decoded
        {
            assert_eq!(id, 42);
        } else {
            panic!("wrong message variant");
        }
    }

    #[tokio::test]
    async fn partial_request_frame_survives_concurrent_outbound_event() {
        let (mut client, server) = UnixStream::pair().expect("socket pair");
        let clients: ClientMap = Arc::new(Mutex::new(HashMap::new()));
        let operations: SharedOperationLedger = Arc::new(Mutex::new(OperationLedger::default()));
        let (event_bus_tx, _event_bus_rx) = mpsc::channel(1);
        let (gateway_tx, mut gateway_rx) = mpsc::channel(2);
        let sys = test_actor_system(event_bus_tx, gateway_tx);
        let handler = tokio::spawn(handle_client(
            77,
            server,
            sys,
            Arc::clone(&clients),
            operations,
            Arc::new(crate::lifecycle::DaemonLifecycle::new(
                PathBuf::from("/tmp/cued-gateway-partial-frame.sock"),
                crate::lifecycle::RestartOwnership::Standalone,
            )),
        ));
        while !client_registry(&clients).contains_key(&77) {
            tokio::task::yield_now().await;
        }

        let request = encode_message(&Message::Request {
            id: 9,
            operation_id: None,
            payload: RequestPayload::Ping {},
        })
        .expect("encode request");
        let split_at = 8.min(request.len() - 1);
        client
            .write_all(&request[..split_at])
            .await
            .expect("write partial frame");
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;

        queue_event_for_client(
            &clients,
            77,
            ClientEvent::global(EventPayload::ShuttingDown {
                reason: "concurrent event".into(),
            }),
        );
        assert!(matches!(
            read_message(&mut client).await.expect("read event"),
            Message::Event {
                payload: EventPayload::ShuttingDown { .. }
            }
        ));

        client
            .write_all(&request[split_at..])
            .await
            .expect("finish request frame");
        let GatewayMsg::SendResponse {
            client_id,
            request_id,
            payload,
        } = gateway_rx.recv().await.expect("ping response")
        else {
            panic!("expected ping response");
        };
        queue_response_for_client(&clients, client_id, request_id, payload);
        assert!(matches!(
            read_message(&mut client).await.expect("read ping response"),
            Message::Response { id: 9, .. }
        ));

        drop(client);
        tokio::time::timeout(std::time::Duration::from_secs(1), handler)
            .await
            .expect("handler exits")
            .expect("handler task");
    }

    #[tokio::test]
    async fn response_roundtrip() {
        let (mut a, mut b) = UnixStream::pair().unwrap();
        let msg = Message::Response {
            id: 1,
            payload: ResponsePayload::Ok(OkPayload::Pong {
                version: "0.1.0".into(),
                instance_id: "00000000-0000-4000-8000-000000000000".into(),
                generation_id: "generation-1".into(),
                ready: true,
                protocol_version: IPC_PROTOCOL_VERSION,
                capabilities: current_protocol_capabilities(),
            }),
        };
        write_message(&mut a, &msg).await.unwrap();
        let decoded = read_message(&mut b).await.unwrap();
        assert!(matches!(
            decoded,
            Message::Response {
                id: 1,
                payload: ResponsePayload::Ok(OkPayload::Pong { version, .. }),
            } if version == "0.1.0"
        ));
    }

    struct TestClientQueues {
        queues: ClientQueues,
        responses: mpsc::Receiver<(u32, ResponsePayload)>,
        events: mpsc::Receiver<ClientEvent>,
        disconnect: watch::Receiver<bool>,
    }

    fn test_client_queues(capacity: usize) -> TestClientQueues {
        let (response_tx, responses) = mpsc::channel(capacity);
        let (event_tx, events) = mpsc::channel(capacity);
        let (disconnect_tx, disconnect) = watch::channel(false);
        let event_state = Arc::new(Mutex::new(ClientEventState {
            binding: Some(None),
            fence: None,
        }));
        TestClientQueues {
            queues: ClientQueues {
                responses: response_tx,
                events: event_tx,
                disconnect: disconnect_tx,
                event_state,
            },
            responses,
            events,
            disconnect,
        }
    }

    #[test]
    fn request_id_fence_rejects_reuse_until_response_is_written() {
        let inflight = Arc::new(Mutex::new(HashSet::new()));

        assert!(reserve_request_id(&inflight, 7));
        assert!(!reserve_request_id(&inflight, 7));
        release_request_id(&inflight, 7);
        assert!(reserve_request_id(&inflight, 7));
    }

    #[test]
    fn binding_response_fence_discards_queued_events_and_revalidates_owner() {
        let state = Arc::new(Mutex::new(ClientEventState {
            binding: Some(Some("SS-alpha".into())),
            fence: None,
        }));
        let (tx, mut rx) = mpsc::channel(4);
        tx.try_send(ClientEvent::session(
            EventPayload::ShuttingDown {
                reason: "queued alpha".into(),
            },
            Some("SS-alpha".into()),
        ))
        .unwrap();

        assert!(begin_client_event_fence(&state, 41, Some("SS-beta".into())));
        // EventBus can enqueue directly while the socket writer is fenced.
        tx.try_send(ClientEvent::session(
            EventPayload::ShuttingDown {
                reason: "early beta".into(),
            },
            Some("SS-beta".into()),
        ))
        .unwrap();
        complete_client_event_fence(&state, 41, &mut rx);

        assert!(rx.try_recv().is_err());
        assert!(client_event_is_deliverable(
            &state,
            &ClientEvent::session(
                EventPayload::ShuttingDown {
                    reason: "current beta".into(),
                },
                Some("SS-beta".into()),
            )
        ));
        assert!(!client_event_is_deliverable(
            &state,
            &ClientEvent::session(
                EventPayload::ShuttingDown {
                    reason: "stale alpha".into(),
                },
                Some("SS-alpha".into()),
            )
        ));
    }

    #[test]
    fn foreground_response_fence_holds_matching_events_until_response_is_written() {
        let state = Arc::new(Mutex::new(ClientEventState {
            binding: Some(Some("SS-alpha".into())),
            fence: None,
        }));
        let (tx, mut rx) = mpsc::channel(2);

        begin_client_event_hold_fence(&state, 42);
        let event = ClientEvent::session(
            EventPayload::FgOutput {
                id: cue_core::StepId {
                    execution: cue_core::ExecutionId(1),
                    index: 1,
                },
                attachment_id: 1,
                data: b"after-cut".to_vec(),
            },
            Some("SS-alpha".into()),
        );
        assert!(client_event_can_enqueue(&state, &event));
        assert!(!client_event_is_deliverable(&state, &event));
        tx.try_send(event).unwrap();
        complete_client_event_fence(&state, 42, &mut rx);

        let retained = rx.try_recv().expect("retained foreground event");
        assert!(client_event_is_deliverable(&state, &retained));
    }

    #[test]
    fn direct_event_dispatch_filters_named_owner_and_preserves_anonymous_compatibility() {
        let clients: ClientMap = Arc::new(Mutex::new(HashMap::new()));
        let mut named = test_client_queues(2);
        client_event_state(&named.queues.event_state).binding = Some(Some("SS-alpha".into()));
        client_registry(&clients).insert(7, named.queues.clone());

        queue_event_for_client(
            &clients,
            7,
            ClientEvent::session(
                EventPayload::ShuttingDown {
                    reason: "foreign".into(),
                },
                Some("SS-beta".into()),
            ),
        );
        assert!(named.events.try_recv().is_err());
        queue_event_for_client(
            &clients,
            7,
            ClientEvent::session(
                EventPayload::ShuttingDown {
                    reason: "matching".into(),
                },
                Some("SS-alpha".into()),
            ),
        );
        assert!(named.events.try_recv().is_ok());

        client_event_state(&named.queues.event_state).binding = Some(None);
        queue_event_for_client(
            &clients,
            7,
            ClientEvent::session(
                EventPayload::ShuttingDown {
                    reason: "legacy global".into(),
                },
                Some("SS-beta".into()),
            ),
        );
        assert!(named.events.try_recv().is_ok());
    }

    #[test]
    fn response_dispatch_evicts_lagging_client_without_blocking_healthy_client() {
        let clients: ClientMap = Arc::new(Mutex::new(HashMap::new()));
        let mut slow = test_client_queues(1);
        let mut healthy = test_client_queues(1);
        slow.queues
            .responses
            .try_send((1, ResponsePayload::ack()))
            .unwrap();
        client_registry(&clients).insert(7, slow.queues.clone());
        client_registry(&clients).insert(8, healthy.queues.clone());

        queue_response_for_client(&clients, 7, 2, ResponsePayload::ack());
        queue_response_for_client(&clients, 8, 3, ResponsePayload::ack());

        assert!(*slow.disconnect.borrow_and_update());
        assert!(!client_registry(&clients).contains_key(&7));
        assert_eq!(healthy.responses.try_recv().unwrap().0, 3);
        assert!(client_registry(&clients).contains_key(&8));
    }

    #[test]
    fn direct_event_dispatch_evicts_lagging_client_without_blocking_healthy_client() {
        let clients: ClientMap = Arc::new(Mutex::new(HashMap::new()));
        let mut slow = test_client_queues(1);
        let mut healthy = test_client_queues(1);
        slow.queues
            .events
            .try_send(ClientEvent::global(EventPayload::ShuttingDown {
                reason: "first".into(),
            }))
            .unwrap();
        client_registry(&clients).insert(7, slow.queues.clone());
        client_registry(&clients).insert(8, healthy.queues.clone());

        queue_event_for_client(
            &clients,
            7,
            ClientEvent::global(EventPayload::ShuttingDown {
                reason: "second".into(),
            }),
        );
        queue_event_for_client(
            &clients,
            8,
            ClientEvent::global(EventPayload::ShuttingDown {
                reason: "healthy".into(),
            }),
        );

        assert!(*slow.disconnect.borrow_and_update());
        assert!(!client_registry(&clients).contains_key(&7));
        assert!(matches!(
            healthy.events.try_recv().unwrap().payload,
            EventPayload::ShuttingDown { reason } if reason == "healthy"
        ));
        assert!(client_registry(&clients).contains_key(&8));
    }

    fn test_actor_system(
        event_bus: mpsc::Sender<EventBusMsg>,
        gateway: mpsc::Sender<GatewayMsg>,
    ) -> ActorSystem {
        let (sessions, _sessions_rx) = mpsc::channel(super::super::ACTOR_CHANNEL_CAP);
        let (process_mgr, _process_rx) = mpsc::channel(super::super::ACTOR_CHANNEL_CAP);
        let (scope_store, _scope_rx) = mpsc::channel(super::super::ACTOR_CHANNEL_CAP);
        ActorSystem {
            gateway,
            sessions,
            execution: mpsc::channel(1).0,
            triggers: mpsc::channel(1).0,
            process_mgr,
            scope_store,
            event_bus,
            config: crate::config::Config::default(),
            resources: std::sync::Arc::new(crate::resource::ProviderRegistry::empty()),
        }
    }

    #[tokio::test]
    async fn subscribe_request_registers_only_requested_channels() {
        let (event_bus_tx, mut event_bus_rx) = mpsc::channel(2);
        let (gateway_tx, mut gateway_rx) = mpsc::channel(1);
        let sys = test_actor_system(event_bus_tx, gateway_tx);
        let (evt_tx, mut evt_rx) = mpsc::channel(1);
        let (disconnect_tx, _disconnect_rx) = watch::channel(false);
        let event_state = Arc::new(Mutex::new(ClientEventState::default()));
        let lifecycle = crate::lifecycle::DaemonLifecycle::new(
            PathBuf::from("/tmp/cued-gateway-subscribe.sock"),
            crate::lifecycle::RestartOwnership::Standalone,
        );

        route_request(
            7,
            42,
            RequestPayload::subscribe(&[EventChannel::System]),
            &sys,
            ClientEventSink {
                sender: &evt_tx,
                disconnect: &disconnect_tx,
            },
            &lifecycle,
            &event_state,
        )
        .await
        .unwrap();

        match event_bus_rx.recv().await.unwrap() {
            EventBusMsg::Subscribe {
                client_id,
                channel,
                sender,
                disconnect: _,
            } => {
                assert_eq!(client_id, 7);
                assert_eq!(channel, EventChannel::System);
                sender
                    .try_send(ClientEvent::global(EventPayload::ShuttingDown {
                        reason: "test".into(),
                    }))
                    .unwrap();
                assert!(matches!(
                    evt_rx.try_recv().unwrap().payload,
                    EventPayload::ShuttingDown { .. }
                ));
            }
            _ => panic!("expected explicit system subscription"),
        }

        match gateway_rx.recv().await.unwrap() {
            GatewayMsg::SendResponse {
                client_id,
                request_id,
                payload,
            } => {
                assert_eq!(client_id, 7);
                assert_eq!(request_id, 42);
                assert!(matches!(payload, ResponsePayload::Ok(OkPayload::Ack {})));
            }
            _ => panic!("expected subscribe ack"),
        }
        assert!(event_bus_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn subscribe_rejects_unknown_event_channels() {
        let (event_bus_tx, mut event_bus_rx) = mpsc::channel(1);
        let (gateway_tx, mut gateway_rx) = mpsc::channel(1);
        let sys = test_actor_system(event_bus_tx, gateway_tx);
        let (evt_tx, _evt_rx) = mpsc::channel(1);
        let (disconnect_tx, _disconnect_rx) = watch::channel(false);
        let event_state = Arc::new(Mutex::new(ClientEventState::default()));
        let lifecycle = crate::lifecycle::DaemonLifecycle::new(
            PathBuf::from("/tmp/cued-gateway-invalid-subscribe.sock"),
            crate::lifecycle::RestartOwnership::Standalone,
        );

        route_request(
            7,
            42,
            RequestPayload::Subscribe {
                channels: vec!["output:C1".into()],
            },
            &sys,
            ClientEventSink {
                sender: &evt_tx,
                disconnect: &disconnect_tx,
            },
            &lifecycle,
            &event_state,
        )
        .await
        .unwrap();

        assert!(event_bus_rx.try_recv().is_err());
        match gateway_rx.recv().await.unwrap() {
            GatewayMsg::SendResponse {
                client_id,
                request_id,
                payload: ResponsePayload::Err { code, message },
            } => {
                assert_eq!(client_id, 7);
                assert_eq!(request_id, 42);
                assert_eq!(code, error_code::INVALID_REQUEST);
                assert!(message.contains("invalid event channel `output:C1`"));
            }
            _ => panic!("expected invalid subscription response"),
        }
    }
}
