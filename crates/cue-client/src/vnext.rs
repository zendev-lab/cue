//! IPC v4 client and frontend-owned Scope submission flow.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::ffi::OsString;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex, OnceLock};

use anyhow::{Context as _, Result, bail};
use cue_core::vnext::{
    AbsolutePath, CancelMode, EnvKey, EnvValue, ExecutionSpec, FileModeMask, OutputStream, Scope,
};
use cue_core::{ExecutionId, ScopeHash, StepId};
use cue_language::{
    Mode, VnextCommand, VnextFrontendAction, compile_vnext_command, compile_vnext_file,
};
use cue_protocol::{
    AttachmentId, Capability, ClientId, Command, EventPayload, ExecutionView, Hello,
    MAX_MESSAGE_SIZE, Message, OperationId, OutputChunk, OutputRange, PROTOCOL_VERSION, Query,
    RequestId, ResponsePayload, ResultPayload, encode_message,
};
use tokio::io::{self, AsyncRead, AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::UnixStream;
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::client::ClientStream;

type BoxedStream = Box<dyn ClientStream>;

/// Result of compiling and dispatching one surface command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SurfaceOutcome {
    Response(ResultPayload),
    Frontend(VnextFrontendAction),
}

/// Sequential IPC v4 connection. Convert it into a multiplexed client before
/// sharing it between an interactive frontend's request and event loops.
pub struct VnextClient {
    stream: BoxedStream,
    client_id: ClientId,
    operation_prefix: String,
    next_request: u64,
    next_operation: u64,
    capabilities: Vec<Capability>,
    pending_events: VecDeque<EventPayload>,
}

impl VnextClient {
    pub async fn connect(socket: &Path) -> Result<Self> {
        let stream = UnixStream::connect(socket)
            .await
            .with_context(|| format!("connect to {}", socket.display()))?;
        let client_id = generated_client_id()?;
        Self::connect_stream(stream, client_id).await
    }

    pub async fn connect_stream<S>(stream: S, client_id: ClientId) -> Result<Self>
    where
        S: ClientStream + 'static,
    {
        let operation_prefix = format!("{}:{}", client_id.as_str(), uuid::Uuid::new_v4());
        let mut client = Self {
            stream: Box::new(stream),
            client_id,
            operation_prefix,
            next_request: 1,
            next_operation: 1,
            capabilities: Vec::new(),
            pending_events: VecDeque::new(),
        };
        client.hello().await?;
        Ok(client)
    }

    pub fn client_id(&self) -> &ClientId {
        &self.client_id
    }

    pub fn capabilities(&self) -> &[Capability] {
        &self.capabilities
    }

    pub fn supports(&self, capability: Capability) -> bool {
        self.capabilities.contains(&capability)
    }

    async fn hello(&mut self) -> Result<()> {
        let client_id = self.client_id.clone();
        let result = self
            .query(Query::Hello(Hello {
                protocol_version: PROTOCOL_VERSION,
                client_id,
            }))
            .await?;
        let ResultPayload::Hello {
            protocol_version,
            capabilities,
            ..
        } = result
        else {
            bail!("daemon returned an unexpected Hello response: {result:?}");
        };
        if protocol_version != PROTOCOL_VERSION {
            bail!(
                "daemon protocol is v{protocol_version}; this client requires v{PROTOCOL_VERSION}"
            );
        }
        self.capabilities = capabilities;
        Ok(())
    }

    pub async fn query(&mut self, query: Query) -> Result<ResultPayload> {
        let request_id = self.allocate_request_id();
        self.send(&Message::Query { request_id, query }).await?;
        self.wait_response(request_id).await
    }

    pub async fn command(&mut self, command: Command) -> Result<ResultPayload> {
        let request_id = self.allocate_request_id();
        let operation_id = self.allocate_operation_id()?;
        self.send(&Message::Command {
            request_id,
            operation_id,
            command,
        })
        .await?;
        self.wait_response(request_id).await
    }

    pub async fn put_scope(&mut self, scope: Scope) -> Result<(ScopeHash, bool)> {
        match self
            .command(Command::PutScope {
                scope: Box::new(scope),
            })
            .await?
        {
            ResultPayload::ScopeStored { hash, durable } => Ok((hash, durable)),
            other => bail!("daemon returned an unexpected PutScope response: {other:?}"),
        }
    }

    pub async fn submit(&mut self, spec: ExecutionSpec) -> Result<ExecutionView> {
        match self
            .command(Command::SubmitExecution {
                spec: Box::new(spec),
            })
            .await?
        {
            ResultPayload::ExecutionSubmitted { execution } => Ok(*execution),
            other => bail!("daemon returned an unexpected Submit response: {other:?}"),
        }
    }

    /// Store the exact frontend Scope, compile against its content hash, and
    /// only then submit the fully resolved execution.
    pub async fn submit_file(&mut self, scope: Scope, source: &str) -> Result<ExecutionView> {
        let (hash, _) = self.put_scope(scope).await?;
        let spec = compile_vnext_file(source, hash).context("compile Cue file for IPC v4")?;
        self.submit(spec).await
    }

    /// Dispatch one interactive surface command without introducing an
    /// ambient daemon cursor. Submit commands always perform PutScope first.
    pub async fn execute_surface(
        &mut self,
        scope: Scope,
        source: &str,
        mode: Mode,
    ) -> Result<SurfaceOutcome> {
        let (hash, _) = self.put_scope(scope).await?;
        let command =
            compile_vnext_command(source, mode, hash).context("compile Cue command for IPC v4")?;
        let result = match command {
            VnextCommand::Submit(spec) => ResultPayload::ExecutionSubmitted {
                execution: Box::new(self.submit(spec).await?),
            },
            VnextCommand::ListExecutions => {
                self.query(Query::ListExecutions {
                    before: None,
                    limit: 100,
                })
                .await?
            }
            VnextCommand::GetExecution { id } => self.query(Query::GetExecution { id }).await?,
            VnextCommand::WaitExecution { id } => self.query(Query::WaitExecution { id }).await?,
            VnextCommand::ReadOutput {
                target,
                stream,
                tail_bytes,
            } => {
                let step = resolve_output_step(self, target).await?;
                let maximum = tail_bytes.unwrap_or(1024 * 1024).min(u32::MAX as usize) as u32;
                let range = OutputRange {
                    offset: 0,
                    max_bytes: maximum,
                };
                let empty = OutputRange {
                    offset: 0,
                    max_bytes: 0,
                };
                self.query(Query::ReadOutput {
                    step,
                    stdout: if matches!(stream, cue_language::OutputSelection::Stdout) {
                        range.clone()
                    } else {
                        empty.clone()
                    },
                    stderr: if matches!(stream, cue_language::OutputSelection::Stderr) {
                        range
                    } else {
                        empty.clone()
                    },
                    terminal: empty,
                })
                .await?
            }
            VnextCommand::CancelExecution { id, force } => {
                self.command(Command::CancelExecution {
                    id,
                    mode: if force {
                        CancelMode::Force
                    } else {
                        CancelMode::Graceful
                    },
                })
                .await?
            }
            VnextCommand::AttachPty {
                step,
                claim_control,
            } => {
                let attached = self
                    .command(Command::AttachPty {
                        step,
                        replay_bytes: 64 * 1024,
                    })
                    .await?;
                if claim_control {
                    let ResultPayload::PtyAttached { attachment, .. } = attached else {
                        bail!("daemon returned an unexpected AttachPty response: {attached:?}");
                    };
                    self.command(Command::ClaimPtyControl { attachment })
                        .await?
                } else {
                    attached
                }
            }
            VnextCommand::Frontend(action) => return Ok(SurfaceOutcome::Frontend(action)),
        };
        Ok(SurfaceOutcome::Response(result))
    }

    pub async fn next_event(&mut self) -> Result<EventPayload> {
        if let Some(event) = self.pending_events.pop_front() {
            return Ok(event);
        }
        match read_message(&mut self.stream).await? {
            Message::Event { payload } => Ok(payload),
            Message::Response { request_id, .. } => {
                bail!(
                    "received unclaimed response for request {}",
                    request_id.get()
                )
            }
            Message::Query { .. } | Message::Command { .. } => {
                bail!("daemon sent a client-only IPC v4 message")
            }
        }
    }

    pub fn into_multiplexed(self) -> VnextMultiplexedClient {
        VnextMultiplexedClient::new(self)
    }

    fn allocate_request_id(&mut self) -> RequestId {
        let value = nonzero_counter(&mut self.next_request);
        RequestId::new(value).unwrap_or_else(|_| unreachable!("counter never returns zero"))
    }

    fn allocate_operation_id(&mut self) -> Result<OperationId> {
        let value = nonzero_counter(&mut self.next_operation);
        OperationId::new(format!("{}:{value}", self.operation_prefix))
            .context("construct operation identity")
    }

    async fn send(&mut self, message: &Message) -> Result<()> {
        self.stream
            .write_all(&encode_message(message).context("encode IPC v4 message")?)
            .await
            .context("write IPC v4 message")?;
        self.stream.flush().await.context("flush IPC v4 message")
    }

    async fn wait_response(&mut self, expected: RequestId) -> Result<ResultPayload> {
        loop {
            match read_message(&mut self.stream).await? {
                Message::Response {
                    request_id,
                    payload,
                } if request_id == expected => return result(payload),
                Message::Response { request_id, .. } => bail!(
                    "received response {} while waiting for {}",
                    request_id.get(),
                    expected.get()
                ),
                Message::Event { payload } => self.pending_events.push_back(payload),
                Message::Query { .. } | Message::Command { .. } => {
                    bail!("daemon sent a client-only IPC v4 message")
                }
            }
        }
    }
}

/// Concurrent v4 client for TUI and other event-driven frontends.
pub struct VnextMultiplexedClient {
    writer: Arc<Mutex<io::WriteHalf<BoxedStream>>>,
    operation_prefix: String,
    next_request: AtomicU64,
    next_operation: AtomicU64,
    pending: Arc<StdMutex<HashMap<RequestId, oneshot::Sender<Result<ResponsePayload>>>>>,
    events: Mutex<mpsc::UnboundedReceiver<EventPayload>>,
    capabilities: Vec<Capability>,
    reader_task: JoinHandle<()>,
}

impl VnextMultiplexedClient {
    fn new(client: VnextClient) -> Self {
        let VnextClient {
            stream,
            operation_prefix,
            next_request,
            next_operation,
            capabilities,
            pending_events,
            ..
        } = client;
        let (reader, writer) = io::split(stream);
        let pending = Arc::new(StdMutex::new(HashMap::new()));
        let (event_tx, event_rx) = mpsc::unbounded_channel();
        for event in pending_events {
            let _ = event_tx.send(event);
        }
        let reader_task = tokio::spawn(run_reader(reader, pending.clone(), event_tx));
        Self {
            writer: Arc::new(Mutex::new(writer)),
            operation_prefix,
            next_request: AtomicU64::new(next_request),
            next_operation: AtomicU64::new(next_operation),
            pending,
            events: Mutex::new(event_rx),
            capabilities,
            reader_task,
        }
    }

    pub fn supports(&self, capability: Capability) -> bool {
        self.capabilities.contains(&capability)
    }

    pub async fn query(&self, query: Query) -> Result<ResultPayload> {
        let request_id = atomic_id(&self.next_request);
        self.call(request_id, Message::Query { request_id, query })
            .await
    }

    pub async fn command(&self, command: Command) -> Result<ResultPayload> {
        let request_id = atomic_id(&self.next_request);
        let operation = atomic_counter(&self.next_operation);
        let operation_id = OperationId::new(format!("{}:{operation}", self.operation_prefix))
            .context("construct operation identity")?;
        self.call(
            request_id,
            Message::Command {
                request_id,
                operation_id,
                command,
            },
        )
        .await
    }

    pub async fn next_event(&self) -> Option<EventPayload> {
        self.events.lock().await.recv().await
    }

    async fn call(&self, request_id: RequestId, message: Message) -> Result<ResultPayload> {
        let (tx, rx) = oneshot::channel();
        self.pending
            .lock()
            .map_err(|_| anyhow::anyhow!("lock pending v4 responses"))?
            .insert(request_id, tx);
        let send = async {
            let mut writer = self.writer.lock().await;
            writer
                .write_all(&encode_message(&message).context("encode IPC v4 message")?)
                .await
                .context("write IPC v4 message")?;
            writer.flush().await.context("flush IPC v4 message")
        }
        .await;
        if let Err(error) = send {
            if let Ok(mut pending) = self.pending.lock() {
                pending.remove(&request_id);
            }
            return Err(error);
        }
        let payload = rx
            .await
            .with_context(|| format!("response waiter {} closed", request_id.get()))??;
        result(payload)
    }
}

impl Drop for VnextMultiplexedClient {
    fn drop(&mut self) {
        self.reader_task.abort();
        fail_pending(&self.pending, "IPC v4 client dropped");
    }
}

async fn run_reader(
    mut reader: io::ReadHalf<BoxedStream>,
    pending: Arc<StdMutex<HashMap<RequestId, oneshot::Sender<Result<ResponsePayload>>>>>,
    events: mpsc::UnboundedSender<EventPayload>,
) {
    let reason = loop {
        match read_message(&mut reader).await {
            Ok(Message::Response {
                request_id,
                payload,
            }) => {
                let waiter = pending
                    .lock()
                    .ok()
                    .and_then(|mut pending| pending.remove(&request_id));
                if let Some(waiter) = waiter {
                    let _ = waiter.send(Ok(payload));
                }
            }
            Ok(Message::Event { payload }) => {
                if events.send(payload).is_err() {
                    return;
                }
            }
            Ok(Message::Query { .. } | Message::Command { .. }) => {
                break "daemon sent a client-only IPC v4 message".to_owned();
            }
            Err(error) => break error.to_string(),
        }
    };
    fail_pending(&pending, &reason);
}

fn fail_pending(
    pending: &StdMutex<HashMap<RequestId, oneshot::Sender<Result<ResponsePayload>>>>,
    reason: &str,
) {
    let Ok(mut pending) = pending.lock() else {
        return;
    };
    for (_, waiter) in pending.drain() {
        let _ = waiter.send(Err(anyhow::anyhow!(reason.to_owned())));
    }
}

async fn read_message<R>(reader: &mut R) -> Result<Message>
where
    R: AsyncRead + Unpin,
{
    let mut header = [0u8; 4];
    reader
        .read_exact(&mut header)
        .await
        .context("read IPC v4 length prefix")?;
    let length = u32::from_be_bytes(header) as usize;
    if length > MAX_MESSAGE_SIZE {
        bail!("IPC v4 message is {length} bytes; maximum is {MAX_MESSAGE_SIZE}");
    }
    let mut frame = Vec::with_capacity(4 + length);
    frame.extend_from_slice(&header);
    frame.resize(4 + length, 0);
    reader
        .read_exact(&mut frame[4..])
        .await
        .context("read IPC v4 message body")?;
    cue_protocol::decode_message(&frame).context("decode IPC v4 message")
}

fn result(payload: ResponsePayload) -> Result<ResultPayload> {
    match payload {
        ResponsePayload::Ok(result) => Ok(result),
        ResponsePayload::Error(error) => {
            bail!(
                "daemon rejected IPC v4 request [{:?}]: {}",
                error.code,
                error.message
            )
        }
    }
}

fn nonzero_counter(counter: &mut u64) -> u64 {
    let current = (*counter).max(1);
    *counter = current.checked_add(1).unwrap_or(1);
    current
}

fn atomic_id(counter: &AtomicU64) -> RequestId {
    let current = atomic_counter(counter);
    RequestId::new(current).unwrap_or_else(|_| unreachable!("nonzero request ID was checked"))
}

fn atomic_counter(counter: &AtomicU64) -> u64 {
    loop {
        let current = counter.fetch_add(1, Ordering::Relaxed);
        if current != 0 {
            return current;
        }
    }
}

fn generated_client_id() -> Result<ClientId> {
    ClientId::new(format!(
        "cue-client:{}:{}",
        std::process::id(),
        uuid::Uuid::new_v4()
    ))
    .context("construct client identity")
}

/// Snapshot the process-owned cwd, environment, and umask for an explicit
/// submission. Non-Unicode environment entries are rejected, not discarded.
pub fn process_scope() -> Result<Scope> {
    let cwd = std::env::current_dir().context("read current directory")?;
    let cwd = AbsolutePath::new(cwd).context("construct absolute current directory")?;
    let mut env = BTreeMap::new();
    for (key, value) in std::env::vars_os() {
        let key = os_string(key, "environment name")?;
        let value = os_string(value, &format!("environment value for {key}"))?;
        env.insert(
            EnvKey::new(key).context("validate environment name")?,
            EnvValue::new(value).context("validate environment value")?,
        );
    }
    Ok(Scope::new(cwd, env, current_umask()?))
}

fn os_string(value: OsString, label: &str) -> Result<String> {
    value
        .into_string()
        .map_err(|_| anyhow::anyhow!("{label} is not valid UTF-8"))
}

fn current_umask() -> Result<FileModeMask> {
    static LOCK: OnceLock<StdMutex<()>> = OnceLock::new();
    let _guard = LOCK
        .get_or_init(|| StdMutex::new(()))
        .lock()
        .map_err(|_| anyhow::anyhow!("lock process umask"))?;
    // SAFETY: the process-global mutation is held only for the two adjacent
    // calls and serialized among Cue callers. The original value is restored.
    let mask = unsafe {
        let mask = libc::umask(0);
        libc::umask(mask);
        mask
    };
    FileModeMask::new(mask as u16).context("validate process umask")
}

async fn resolve_output_step(
    client: &mut VnextClient,
    target: cue_language::OutputTarget,
) -> Result<StepId> {
    match target {
        cue_language::OutputTarget::Step(step) => Ok(step),
        cue_language::OutputTarget::Execution(id) => {
            let ResultPayload::Execution { execution } =
                client.query(Query::GetExecution { id }).await?
            else {
                bail!("daemon returned an unexpected GetExecution response");
            };
            execution
                .snapshot
                .steps
                .last()
                .map(|step| step.id())
                .ok_or_else(|| anyhow::anyhow!("execution {id} has no output-producing steps"))
        }
    }
}

pub fn output_bytes(chunks: &[OutputChunk], stream: OutputStream) -> Vec<u8> {
    chunks
        .iter()
        .filter(|chunk| chunk.stream == stream)
        .flat_map(|chunk| chunk.data.iter().copied())
        .collect()
}

pub async fn wait_execution(client: &mut VnextClient, id: ExecutionId) -> Result<ExecutionView> {
    match client.query(Query::WaitExecution { id }).await? {
        ResultPayload::Execution { execution } => Ok(*execution),
        other => bail!("daemon returned an unexpected WaitExecution response: {other:?}"),
    }
}

pub async fn attach_pty(
    client: &mut VnextClient,
    step: StepId,
    replay_bytes: u32,
) -> Result<(AttachmentId, ResultPayload)> {
    let response = client
        .command(Command::AttachPty { step, replay_bytes })
        .await?;
    let ResultPayload::PtyAttached { attachment, .. } = response else {
        bail!("daemon returned an unexpected AttachPty response: {response:?}");
    };
    Ok((attachment, response))
}

#[cfg(test)]
mod tests {
    use super::*;
    use cue_core::vnext::{Argv, ExecutionPlan, IoMode, Pipeline, Process};
    use cue_daemon::vnext::{VnextService, serve_stream};

    fn scope() -> Scope {
        Scope::new(
            AbsolutePath::new("/tmp").unwrap(),
            BTreeMap::new(),
            FileModeMask::new(0o022).unwrap(),
        )
    }

    fn client_id() -> ClientId {
        ClientId::new("client-test").unwrap()
    }

    #[tokio::test]
    async fn explicit_scope_submit_wait_and_output_roundtrip() {
        let service = VnextService::in_memory().unwrap();
        let (client_stream, server_stream) = tokio::io::duplex(128 * 1024);
        let server = tokio::spawn(serve_stream(service, server_stream));
        let mut client = VnextClient::connect_stream(client_stream, client_id())
            .await
            .unwrap();
        assert!(client.supports(Capability::OperationIdempotency));

        let (scope_hash, durable) = client.put_scope(scope()).await.unwrap();
        assert!(durable);
        let spec = ExecutionSpec::new(
            scope_hash,
            ExecutionPlan::run(
                Pipeline::simple(Process::new(
                    Argv::new("/bin/echo", vec!["client-v4".into()]).unwrap(),
                )),
                IoMode::Captured,
            ),
        )
        .unwrap();
        let submitted = client.submit(spec).await.unwrap();
        let finished = wait_execution(&mut client, submitted.snapshot.id)
            .await
            .unwrap();
        assert!(finished.state.is_terminal());
        let step = finished.snapshot.steps[0].id();
        let output = client
            .query(Query::ReadOutput {
                step,
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
            })
            .await
            .unwrap();
        let ResultPayload::Output { chunks } = output else {
            panic!("unexpected output response");
        };
        assert_eq!(output_bytes(&chunks, OutputStream::Stdout), b"client-v4\n");
        drop(client);
        server.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn multiplexed_client_routes_concurrent_responses_and_events() {
        let service = VnextService::in_memory().unwrap();
        let (client_stream, server_stream) = tokio::io::duplex(128 * 1024);
        let server = tokio::spawn(serve_stream(service, server_stream));
        let client = VnextClient::connect_stream(client_stream, client_id())
            .await
            .unwrap()
            .into_multiplexed();
        let (ping, list) = tokio::join!(
            client.query(Query::Ping),
            client.query(Query::ListExecutions {
                before: None,
                limit: 10,
            })
        );
        assert!(matches!(ping.unwrap(), ResultPayload::Ack));
        assert!(matches!(list.unwrap(), ResultPayload::Executions { .. }));
        drop(client);
        server.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn file_submission_orders_put_scope_compile_and_submit() {
        let service = VnextService::in_memory().unwrap();
        let (client_stream, server_stream) = tokio::io::duplex(128 * 1024);
        let server = tokio::spawn(serve_stream(service, server_stream));
        let mut client = VnextClient::connect_stream(client_stream, client_id())
            .await
            .unwrap();
        let execution = client.submit_file(scope(), "/bin/true").await.unwrap();
        assert_eq!(execution.snapshot.spec.scope(), scope().compute_hash());
        drop(client);
        server.await.unwrap().unwrap();
    }
}
