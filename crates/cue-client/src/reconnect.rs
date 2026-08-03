use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use tokio::sync::mpsc;

use cue_core::ipc::Message;

use crate::client::{ClientReader, CuedClient, WriterHandle, spawn_writer_task};

type ConnectFuture = Pin<Box<dyn Future<Output = Result<CuedClient>> + Send + 'static>>;

/// Commands sent to the connection manager task.
enum ReconnectCmd {
    /// Drop the current transport and immediately attempt to reconnect using
    /// the supplied connector.  If the first attempt fails the manager falls
    /// back to its normal periodic retry loop.
    SwitchTarget(ClientConnector),
    /// Shut the connection manager down cleanly.
    Shutdown,
}

/// Failure while sending a control command to the connection manager.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionControlError {
    Full,
    Closed,
}

impl std::fmt::Display for ConnectionControlError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Full => f.write_str("connection control queue is full"),
            Self::Closed => f.write_str("connection manager is closed"),
        }
    }
}

impl std::error::Error for ConnectionControlError {}

/// Cloneable control handle for the connection manager.
#[derive(Clone)]
pub struct ConnectionController {
    tx: mpsc::Sender<ReconnectCmd>,
}

impl ConnectionController {
    pub fn switch_target(&self, connector: ClientConnector) -> Result<(), ConnectionControlError> {
        self.try_send(ReconnectCmd::SwitchTarget(connector))
    }

    pub fn shutdown(&self) -> Result<(), ConnectionControlError> {
        self.try_send(ReconnectCmd::Shutdown)
    }

    fn try_send(&self, command: ReconnectCmd) -> Result<(), ConnectionControlError> {
        self.tx.try_send(command).map_err(|error| match error {
            mpsc::error::TrySendError::Full(_) => ConnectionControlError::Full,
            mpsc::error::TrySendError::Closed(_) => ConnectionControlError::Closed,
        })
    }
}

/// Cloneable connector used by the shared reconnect loop.
#[derive(Clone)]
pub struct ClientConnector {
    connect: Arc<dyn Fn() -> ConnectFuture + Send + Sync>,
}

impl ClientConnector {
    pub fn new<F, Fut>(connect: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<CuedClient>> + Send + 'static,
    {
        Self {
            connect: Arc::new(move || Box::pin(connect())),
        }
    }

    pub fn unix(socket_path: PathBuf) -> Self {
        Self::new(move || {
            let socket_path = socket_path.clone();
            async move { CuedClient::connect(&socket_path).await }
        })
    }

    pub async fn connect(&self) -> Result<CuedClient> {
        (self.connect)().await
    }
}

/// Default reconnect interval after the daemon disconnects.
const DEFAULT_RECONNECT_DELAY: Duration = Duration::from_secs(3);
/// Maximum duration of one connection-manager attempt.
///
/// The attempt covers the connector's complete transaction: transport setup,
/// protocol handshake, and any named-session attachment performed by a
/// decorated connector. `SwitchTarget` and `Shutdown` preempt this deadline.
const CONNECT_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(30);
/// Bound transport events awaiting a frontend consumer.
///
/// Each IPC frame is independently capped by `MAX_MESSAGE_SIZE`; bounding the
/// message count here prevents a slow renderer from turning sustained process
/// output into an unbounded client-side queue.
const CONNECTION_EVENT_CAPACITY: usize = 64;

/// Events produced by the shared connection manager.
pub enum ConnectionEvent {
    Incoming(Message),
    Disconnected,
    ReconnectFailed { message: String },
    Reconnected { writer: WriterHandle },
}

/// Spawn the connection manager with a control handle.
///
/// Returns `(event_rx, controller)`:
/// - `event_rx` delivers [`ConnectionEvent`]s to the caller.
/// - `controller` lets the caller request target switches or shutdown.
pub fn spawn_connection_manager_controllable(
    initial_reader: Option<ClientReader>,
    connector: ClientConnector,
) -> (mpsc::Receiver<ConnectionEvent>, ConnectionController) {
    spawn_connection_manager_controllable_with_delay(
        initial_reader,
        connector,
        DEFAULT_RECONNECT_DELAY,
    )
}

/// Spawn the connection manager with a control channel and a custom reconnect
/// interval.
fn spawn_connection_manager_controllable_with_delay(
    initial_reader: Option<ClientReader>,
    connector: ClientConnector,
    reconnect_delay: Duration,
) -> (mpsc::Receiver<ConnectionEvent>, ConnectionController) {
    spawn_connection_manager_controllable_with_timing(
        initial_reader,
        connector,
        reconnect_delay,
        CONNECT_ATTEMPT_TIMEOUT,
    )
}

fn spawn_connection_manager_controllable_with_timing(
    initial_reader: Option<ClientReader>,
    connector: ClientConnector,
    reconnect_delay: Duration,
    connect_attempt_timeout: Duration,
) -> (mpsc::Receiver<ConnectionEvent>, ConnectionController) {
    let (event_tx, event_rx) = mpsc::channel(CONNECTION_EVENT_CAPACITY);
    let (cmd_tx, cmd_rx) = mpsc::channel(8);
    let controller = ConnectionController { tx: cmd_tx };
    tokio::spawn(run_controllable_connection_manager(
        initial_reader,
        connector,
        reconnect_delay,
        connect_attempt_timeout,
        event_tx,
        cmd_rx,
    ));
    (event_rx, controller)
}

enum ConnectAttemptOutcome {
    Connected(CuedClient),
    Failed {
        error: anyhow::Error,
        target_replaced: bool,
    },
    Shutdown,
}

/// Run one cancellable connection attempt.
///
/// A target switch drops the in-flight connector future and immediately starts
/// the replacement. A shutdown (or all control handles being dropped) cancels
/// the attempt and exits without waiting for the deadline.
async fn connect_with_control(
    connector: &mut ClientConnector,
    cmd_rx: &mut mpsc::Receiver<ReconnectCmd>,
    attempt_timeout: Duration,
) -> ConnectAttemptOutcome {
    let mut target_replaced = false;
    loop {
        let attempt_connector = connector.clone();
        let attempt = attempt_connector.connect();
        let deadline = tokio::time::sleep(attempt_timeout);
        tokio::pin!(attempt);
        tokio::pin!(deadline);

        tokio::select! {
            // If control and connection completion become ready together, an
            // explicit user command wins and the stale result is discarded.
            biased;

            command = cmd_rx.recv() => {
                match command {
                    Some(ReconnectCmd::SwitchTarget(new_connector)) => {
                        *connector = new_connector;
                        target_replaced = true;
                    }
                    Some(ReconnectCmd::Shutdown) | None => {
                        return ConnectAttemptOutcome::Shutdown;
                    }
                }
            }
            result = &mut attempt => {
                return match result {
                    Ok(client) => ConnectAttemptOutcome::Connected(client),
                    Err(error) => ConnectAttemptOutcome::Failed {
                        error,
                        target_replaced,
                    },
                };
            }
            _ = &mut deadline => {
                return ConnectAttemptOutcome::Failed {
                    error: anyhow::anyhow!(
                        "connection attempt timed out after {attempt_timeout:?}"
                    ),
                    target_replaced,
                };
            }
        }
    }
}

/// Long-lived controllable task.  Reads from the active connection, forwards
/// messages and handles control commands concurrently.
async fn run_controllable_connection_manager(
    initial_reader: Option<ClientReader>,
    mut connector: ClientConnector,
    reconnect_delay: Duration,
    connect_attempt_timeout: Duration,
    tx: mpsc::Sender<ConnectionEvent>,
    mut cmd_rx: mpsc::Receiver<ReconnectCmd>,
) {
    let mut reader_opt = initial_reader;

    'outer: loop {
        let mut failure_reported = false;

        // ── Reading phase ──────────────────────────────────────────────────
        if let Some(mut reader) = reader_opt.take() {
            loop {
                tokio::select! {
                    result = reader.recv() => {
                        match result {
                            Ok(msg) => {
                                if tx.send(ConnectionEvent::Incoming(msg)).await.is_err() {
                                    return;
                                }
                            }
                            Err(_) => {
                                // Natural disconnect — fall through to reconnect.
                                if tx.send(ConnectionEvent::Disconnected).await.is_err() {
                                    return;
                                }
                                break;
                            }
                        }
                    }
                    cmd = cmd_rx.recv() => {
                        match cmd {
                            Some(ReconnectCmd::SwitchTarget(new_connector)) => {
                                // Drop current connection and switch connector.
                                drop(reader);
                                connector = new_connector;
                                if tx.send(ConnectionEvent::Disconnected).await.is_err() {
                                    return;
                                }
                                // Attempt immediate connection before falling back
                                // to the periodic retry loop.
                                match connect_with_control(
                                    &mut connector,
                                    &mut cmd_rx,
                                    connect_attempt_timeout,
                                )
                                .await
                                {
                                    ConnectAttemptOutcome::Connected(client) => {
                                        let (new_reader, writer) = client.into_split();
                                        let writer = spawn_writer_task(writer);
                                        if tx.send(ConnectionEvent::Reconnected { writer }).await.is_err() {
                                            return;
                                        }
                                        reader_opt = Some(new_reader);
                                        continue 'outer;
                                    }
                                    ConnectAttemptOutcome::Failed { error, .. } => {
                                        if send_reconnect_failed(&tx, error).await.is_err() {
                                            return;
                                        }
                                        failure_reported = true;
                                    }
                                    ConnectAttemptOutcome::Shutdown => return,
                                }
                                break;
                            }
                            Some(ReconnectCmd::Shutdown) | None => return,
                        }
                    }
                }
            }
        }

        // ── Reconnect phase ────────────────────────────────────────────────
        loop {
            tokio::select! {
                _ = tokio::time::sleep(reconnect_delay) => {}
                cmd = cmd_rx.recv() => {
                    match cmd {
                        Some(ReconnectCmd::SwitchTarget(new_connector)) => {
                            connector = new_connector;
                            failure_reported = false;
                        }
                        Some(ReconnectCmd::Shutdown) | None => return,
                    }
                }
            }

            match connect_with_control(&mut connector, &mut cmd_rx, connect_attempt_timeout).await {
                ConnectAttemptOutcome::Connected(client) => {
                    let (new_reader, writer) = client.into_split();
                    let writer = spawn_writer_task(writer);
                    if tx
                        .send(ConnectionEvent::Reconnected { writer })
                        .await
                        .is_err()
                    {
                        return;
                    }
                    reader_opt = Some(new_reader);
                    continue 'outer;
                }
                ConnectAttemptOutcome::Failed {
                    error,
                    target_replaced,
                } => {
                    if target_replaced {
                        failure_reported = false;
                    }
                    if !failure_reported {
                        if send_reconnect_failed(&tx, error).await.is_err() {
                            return;
                        }
                        failure_reported = true;
                    }
                }
                ConnectAttemptOutcome::Shutdown => return,
            }
        }
    }
}

async fn send_reconnect_failed(
    tx: &mpsc::Sender<ConnectionEvent>,
    error: anyhow::Error,
) -> Result<(), ()> {
    tx.send(ConnectionEvent::ReconnectFailed {
        message: format!("{error:#}"),
    })
    .await
    .map_err(|_| ())
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use tokio::io::{AsyncReadExt, AsyncWriteExt, duplex};
    use tokio::time::{Duration, timeout};

    use cue_core::ipc::{EventPayload, RequestPayload};

    use super::*;

    struct AttemptDropSignal(mpsc::UnboundedSender<()>);

    impl Drop for AttemptDropSignal {
        fn drop(&mut self) {
            let _ = self.0.send(());
        }
    }

    fn pending_connector() -> (
        ClientConnector,
        mpsc::UnboundedReceiver<()>,
        mpsc::UnboundedReceiver<()>,
    ) {
        let (started_tx, started_rx) = mpsc::unbounded_channel();
        let (dropped_tx, dropped_rx) = mpsc::unbounded_channel();
        let connector = ClientConnector::new(move || {
            let started_tx = started_tx.clone();
            let dropped_tx = dropped_tx.clone();
            async move {
                let _drop_signal = AttemptDropSignal(dropped_tx);
                let _ = started_tx.send(());
                std::future::pending::<anyhow::Result<CuedClient>>().await
            }
        });
        (connector, started_rx, dropped_rx)
    }

    #[tokio::test]
    async fn connection_event_queue_backpressures_transport_reads() {
        let (client_stream, mut daemon_stream) = duplex(256);
        let client = CuedClient::from_stream(client_stream);
        let (reader, _writer) = client.into_split();
        let connector = ClientConnector::new(|| async { anyhow::bail!("unused connector") });
        let (mut event_rx, _controller) = spawn_connection_manager_controllable_with_delay(
            Some(reader),
            connector,
            Duration::from_secs(60),
        );
        assert_eq!(event_rx.max_capacity(), CONNECTION_EVENT_CAPACITY);

        let message_count = CONNECTION_EVENT_CAPACITY + 8;
        let mut writer = tokio::spawn(async move {
            for index in 0..message_count {
                let message = Message::Event {
                    payload: EventPayload::ShuttingDown {
                        reason: format!("event-{index}"),
                    },
                };
                let body = serde_json::to_vec(&message).expect("serialize event");
                daemon_stream
                    .write_all(&(body.len() as u32).to_be_bytes())
                    .await
                    .expect("write event length");
                daemon_stream
                    .write_all(&body)
                    .await
                    .expect("write event body");
            }
        });

        timeout(Duration::from_secs(1), async {
            while event_rx.len() < CONNECTION_EVENT_CAPACITY {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("connection event queue did not fill");
        assert!(
            timeout(Duration::from_millis(20), &mut writer)
                .await
                .is_err(),
            "transport writer should remain backpressured while the event queue is full"
        );

        for expected in 0..message_count {
            let event = timeout(Duration::from_secs(1), event_rx.recv())
                .await
                .expect("event receive timeout")
                .expect("connection event");
            match event {
                ConnectionEvent::Incoming(Message::Event {
                    payload: EventPayload::ShuttingDown { reason },
                }) => assert_eq!(reason, format!("event-{expected}")),
                _ => panic!("unexpected connection event"),
            }
        }
        timeout(Duration::from_secs(1), writer)
            .await
            .expect("transport writer stayed blocked")
            .expect("transport writer task failed");
    }

    #[tokio::test]
    async fn custom_connector_reconnects_and_provides_writer() {
        let (initial_client_stream, initial_daemon_stream) = duplex(256);
        let initial_client = CuedClient::from_stream(initial_client_stream);
        let (initial_reader, _initial_writer) = initial_client.into_split();

        let (daemon_tx, mut daemon_rx) = mpsc::unbounded_channel();
        let attempts = Arc::new(AtomicUsize::new(0));
        let connector = ClientConnector::new({
            let daemon_tx = daemon_tx.clone();
            let attempts = attempts.clone();
            move || {
                let daemon_tx = daemon_tx.clone();
                let attempts = attempts.clone();
                async move {
                    attempts.fetch_add(1, Ordering::Relaxed);
                    let (client_stream, daemon_stream) = duplex(256);
                    daemon_tx.send(daemon_stream).expect("send daemon stream");
                    Ok(CuedClient::from_stream(client_stream))
                }
            }
        });

        let (mut rx, _controller) = spawn_connection_manager_controllable_with_delay(
            Some(initial_reader),
            connector,
            Duration::from_millis(10),
        );

        drop(initial_daemon_stream);

        let disconnected = timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("disconnect timeout")
            .expect("disconnect event");
        assert!(matches!(disconnected, ConnectionEvent::Disconnected));

        let reconnected = timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("reconnect timeout")
            .expect("reconnect event");

        let daemon_stream = daemon_rx.recv().await.expect("daemon stream");
        let mut daemon_stream = daemon_stream;
        match reconnected {
            ConnectionEvent::Reconnected { writer } => {
                writer
                    .try_send(RequestPayload::Ping {})
                    .expect("queue ping request");
            }
            _ => panic!("expected reconnect event"),
        }

        let mut len_prefix = [0u8; 4];
        timeout(
            Duration::from_secs(1),
            daemon_stream.read_exact(&mut len_prefix),
        )
        .await
        .expect("writer timeout")
        .expect("read request");
        assert!(u32::from_be_bytes(len_prefix) > 0);
        assert_eq!(attempts.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn switch_target_cmd_reconnects_to_new_connector() {
        let (initial_client_stream, initial_daemon_stream) = duplex(256);
        let initial_client = CuedClient::from_stream(initial_client_stream);
        let (initial_reader, _initial_writer) = initial_client.into_split();

        // Connector for the new target.
        let (new_daemon_tx, mut new_daemon_rx) = mpsc::unbounded_channel();
        let new_connector = ClientConnector::new({
            let new_daemon_tx = new_daemon_tx.clone();
            move || {
                let new_daemon_tx = new_daemon_tx.clone();
                async move {
                    let (client_stream, daemon_stream) = duplex(256);
                    new_daemon_tx
                        .send(daemon_stream)
                        .expect("send daemon stream");
                    Ok(CuedClient::from_stream(client_stream))
                }
            }
        });

        // Stub initial connector — must not be called after SwitchTarget.
        let initial_connector = ClientConnector::new(move || async move {
            let (client_stream, _daemon) = duplex(256);
            Ok(CuedClient::from_stream(client_stream))
        });

        let (mut event_rx, controller) = spawn_connection_manager_controllable_with_delay(
            Some(initial_reader),
            initial_connector,
            Duration::from_millis(10),
        );

        // Trigger a target switch while connected.
        controller
            .switch_target(new_connector)
            .expect("send SwitchTarget");

        // Expect Disconnected then Reconnected.
        let ev1 = timeout(Duration::from_secs(2), event_rx.recv())
            .await
            .expect("timeout waiting for Disconnected")
            .expect("channel closed");
        assert!(
            matches!(ev1, ConnectionEvent::Disconnected),
            "expected Disconnected, got other event"
        );

        let ev2 = timeout(Duration::from_secs(2), event_rx.recv())
            .await
            .expect("timeout waiting for Reconnected")
            .expect("channel closed");
        assert!(
            matches!(ev2, ConnectionEvent::Reconnected { .. }),
            "expected Reconnected, got other event"
        );

        // The new daemon stream should have been accepted.
        let _new_daemon_stream = new_daemon_rx
            .recv()
            .await
            .expect("new daemon stream not received");

        // Drop the initial daemon stream (already detached after SwitchTarget).
        drop(initial_daemon_stream);
    }

    #[tokio::test]
    async fn switch_target_reports_failed_attempt_and_keeps_retrying() {
        let (initial_client_stream, initial_daemon_stream) = duplex(256);
        let initial_client = CuedClient::from_stream(initial_client_stream);
        let (initial_reader, _initial_writer) = initial_client.into_split();

        let (new_daemon_tx, mut new_daemon_rx) = mpsc::unbounded_channel();
        let attempts = Arc::new(AtomicUsize::new(0));
        let new_connector = ClientConnector::new({
            let attempts = attempts.clone();
            let new_daemon_tx = new_daemon_tx.clone();
            move || {
                let attempts = attempts.clone();
                let new_daemon_tx = new_daemon_tx.clone();
                async move {
                    if attempts.fetch_add(1, Ordering::Relaxed) == 0 {
                        anyhow::bail!("dial failed");
                    }
                    let (client_stream, daemon_stream) = duplex(256);
                    new_daemon_tx
                        .send(daemon_stream)
                        .expect("send daemon stream");
                    Ok(CuedClient::from_stream(client_stream))
                }
            }
        });

        let initial_connector = ClientConnector::new(move || async move {
            let (client_stream, _daemon) = duplex(256);
            Ok(CuedClient::from_stream(client_stream))
        });

        let (mut event_rx, controller) = spawn_connection_manager_controllable_with_delay(
            Some(initial_reader),
            initial_connector,
            Duration::from_millis(10),
        );

        controller
            .switch_target(new_connector)
            .expect("send SwitchTarget");

        let disconnected = timeout(Duration::from_secs(2), event_rx.recv())
            .await
            .expect("timeout waiting for Disconnected")
            .expect("channel closed");
        assert!(matches!(disconnected, ConnectionEvent::Disconnected));

        let failed = timeout(Duration::from_secs(2), event_rx.recv())
            .await
            .expect("timeout waiting for ReconnectFailed")
            .expect("channel closed");
        match failed {
            ConnectionEvent::ReconnectFailed { message } => {
                assert!(message.contains("dial failed"), "{message}");
            }
            _ => panic!("expected ReconnectFailed"),
        }

        let reconnected = timeout(Duration::from_secs(2), event_rx.recv())
            .await
            .expect("timeout waiting for Reconnected")
            .expect("channel closed");
        assert!(matches!(reconnected, ConnectionEvent::Reconnected { .. }));
        let _new_daemon_stream = new_daemon_rx
            .recv()
            .await
            .expect("new daemon stream not received");
        assert_eq!(attempts.load(Ordering::Relaxed), 2);

        drop(initial_daemon_stream);
    }

    #[tokio::test]
    async fn controller_shutdown_closes_connection_manager() {
        let connector = ClientConnector::new(|| async { anyhow::bail!("should not connect") });
        let (mut event_rx, controller) = spawn_connection_manager_controllable_with_delay(
            None,
            connector,
            Duration::from_secs(60),
        );

        controller.shutdown().expect("send shutdown");

        let event = timeout(Duration::from_secs(1), event_rx.recv())
            .await
            .expect("connection manager did not shut down");
        assert!(event.is_none(), "shutdown should close event stream");
    }

    #[tokio::test]
    async fn switch_target_cancels_a_pending_connection_attempt() {
        let (connector, mut started_rx, mut dropped_rx) = pending_connector();
        let (new_daemon_tx, mut new_daemon_rx) = mpsc::unbounded_channel();
        let new_connector = ClientConnector::new(move || {
            let new_daemon_tx = new_daemon_tx.clone();
            async move {
                let (client_stream, daemon_stream) = duplex(256);
                new_daemon_tx
                    .send(daemon_stream)
                    .expect("send replacement daemon stream");
                Ok(CuedClient::from_stream(client_stream))
            }
        });
        let (mut event_rx, controller) = spawn_connection_manager_controllable_with_timing(
            None,
            connector,
            Duration::ZERO,
            Duration::from_secs(60),
        );

        timeout(Duration::from_secs(1), started_rx.recv())
            .await
            .expect("pending attempt did not start")
            .expect("pending attempt start channel closed");
        controller
            .switch_target(new_connector)
            .expect("switch pending target");
        timeout(Duration::from_secs(1), dropped_rx.recv())
            .await
            .expect("replaced attempt was not cancelled")
            .expect("attempt drop channel closed");

        let replacement_daemon = timeout(Duration::from_secs(1), new_daemon_rx.recv())
            .await
            .expect("replacement connector did not run")
            .expect("replacement daemon channel closed");
        let event = timeout(Duration::from_secs(1), event_rx.recv())
            .await
            .expect("replacement event timeout")
            .expect("connection event stream closed");
        assert!(matches!(event, ConnectionEvent::Reconnected { .. }));

        drop(replacement_daemon);
    }

    #[tokio::test]
    async fn shutdown_cancels_a_pending_connection_attempt() {
        let (connector, mut started_rx, mut dropped_rx) = pending_connector();
        let (mut event_rx, controller) = spawn_connection_manager_controllable_with_timing(
            None,
            connector,
            Duration::ZERO,
            Duration::from_secs(60),
        );

        timeout(Duration::from_secs(1), started_rx.recv())
            .await
            .expect("pending attempt did not start")
            .expect("pending attempt start channel closed");
        controller.shutdown().expect("shutdown pending attempt");
        timeout(Duration::from_secs(1), dropped_rx.recv())
            .await
            .expect("shutdown did not cancel pending attempt")
            .expect("attempt drop channel closed");
        let event = timeout(Duration::from_secs(1), event_rx.recv())
            .await
            .expect("connection manager did not shut down");
        assert!(event.is_none(), "shutdown should close event stream");
    }

    #[tokio::test]
    async fn pending_connection_attempt_obeys_the_attempt_timeout() {
        let (mut connector, mut started_rx, mut dropped_rx) = pending_connector();
        let (_cmd_tx, mut cmd_rx) = mpsc::channel(1);
        let attempt_timeout = Duration::from_millis(20);

        let outcome = timeout(
            Duration::from_secs(1),
            connect_with_control(&mut connector, &mut cmd_rx, attempt_timeout),
        )
        .await
        .expect("connection attempt did not time out");

        match outcome {
            ConnectAttemptOutcome::Failed {
                error,
                target_replaced,
            } => {
                assert!(!target_replaced);
                assert!(
                    error.to_string().contains("timed out after 20ms"),
                    "{error:#}"
                );
            }
            _ => panic!("expected timed out connection attempt"),
        }
        started_rx
            .try_recv()
            .expect("pending connector should have started");
        timeout(Duration::from_secs(1), dropped_rx.recv())
            .await
            .expect("timed out attempt was not cancelled")
            .expect("attempt drop channel closed");
    }
}
