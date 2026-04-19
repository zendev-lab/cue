use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use tokio::io::{self, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::UnixStream;
use tokio::sync::mpsc;

use cue_core::Mode;
use cue_core::ipc::{MAX_MESSAGE_SIZE, Message, RequestPayload, encode_message};

/// Client handle for a single connection to the cued daemon.
pub struct CuedClient {
    stream: BoxedClientStream,
    next_id: u32,
}

impl CuedClient {
    /// Build a client from any bidirectional byte stream that speaks the cue IPC.
    pub fn from_stream<S>(stream: S) -> Self
    where
        S: ClientStream + 'static,
    {
        Self {
            stream: Box::new(stream),
            next_id: 1,
        }
    }

    /// Connect to the daemon at `socket_path`.
    pub async fn connect(socket_path: &Path) -> Result<Self> {
        let stream = UnixStream::connect(socket_path)
            .await
            .with_context(|| format!("connect to {}", socket_path.display()))?;
        Ok(Self::from_stream(stream))
    }

    /// Send a request and return the assigned request ID.
    pub async fn send(&mut self, payload: RequestPayload) -> Result<u32> {
        send_request(&mut self.stream, &mut self.next_id, payload).await
    }

    /// Read the next message from the daemon.
    pub async fn recv(&mut self) -> Result<Message> {
        read_message(&mut self.stream).await
    }

    /// Convenience: send an Eval request.
    pub async fn eval(&mut self, input: &str, mode: Mode) -> Result<u32> {
        self.send(RequestPayload::Eval {
            input: input.to_string(),
            mode,
        })
        .await
    }

    /// Convenience: subscribe to event channels.
    pub async fn subscribe(&mut self, channels: &[&str]) -> Result<()> {
        self.send(RequestPayload::Subscribe {
            channels: channels.iter().map(|s| (*s).to_string()).collect(),
        })
        .await?;
        Ok(())
    }

    /// Convenience: send a Ping request.
    pub async fn ping(&mut self) -> Result<u32> {
        self.send(RequestPayload::Ping {}).await
    }

    /// Split the client into read/write halves for concurrent use.
    ///
    /// Returns `(reader, writer_stream)` where the reader can call `recv()`
    /// and the writer keeps the `next_id` counter.
    pub fn into_split(self) -> (ClientReader, ClientWriter) {
        let (read_half, write_half) = io::split(self.stream);
        (
            ClientReader { stream: read_half },
            ClientWriter {
                stream: write_half,
                next_id: self.next_id,
            },
        )
    }
}

/// Read half of a split client connection.
pub struct ClientReader {
    stream: io::ReadHalf<BoxedClientStream>,
}

impl ClientReader {
    /// Read the next message from the daemon.
    pub async fn recv(&mut self) -> Result<Message> {
        read_message(&mut self.stream).await
    }
}

/// Write half of a split client connection.
pub struct ClientWriter {
    stream: io::WriteHalf<BoxedClientStream>,
    next_id: u32,
}

impl ClientWriter {
    /// Send a request and return the assigned request ID.
    pub async fn send(&mut self, payload: RequestPayload) -> Result<u32> {
        send_request(&mut self.stream, &mut self.next_id, payload).await
    }
}

/// A cloneable handle for sending requests to the daemon.
///
/// Internally holds an [`mpsc::Sender`] that feeds a dedicated writer task
/// which owns the actual [`ClientWriter`].
#[derive(Clone)]
pub struct WriterHandle {
    tx: mpsc::Sender<RequestPayload>,
}

impl WriterHandle {
    /// Enqueue a request payload to be sent to the daemon (non-blocking).
    ///
    /// Returns `Ok(())` if the message was enqueued, or `Err` if the
    /// writer task has exited or the channel is full.
    pub fn try_send(
        &self,
        payload: RequestPayload,
    ) -> Result<(), mpsc::error::TrySendError<RequestPayload>> {
        self.tx.try_send(payload)
    }

    /// Enqueue a request asynchronously, waiting for buffer space if needed.
    pub fn send(&self, payload: RequestPayload) {
        let tx = self.tx.clone();
        tokio::spawn(async move {
            let _ = tx.send(payload).await;
        });
    }
}

/// Spawn a dedicated writer task that owns the [`ClientWriter`] and receives
/// messages from a bounded channel. Returns a [`WriterHandle`] for sending
/// requests.
///
/// The task exits when all [`WriterHandle`] clones are dropped.
pub fn spawn_writer_task(mut writer: ClientWriter) -> WriterHandle {
    let (tx, mut rx) = mpsc::channel::<RequestPayload>(64);
    tokio::spawn(async move {
        while let Some(payload) = rx.recv().await {
            if let Err(error) = writer.send(payload).await {
                tracing::error!(%error, "writer task send error");
                break;
            }
        }
        tracing::debug!("writer task exiting");
    });
    WriterHandle { tx }
}

const APP_DIR: &str = "cue-shell";

#[doc(hidden)]
pub trait ClientStream: AsyncRead + AsyncWrite + Send + Unpin {}

impl<T> ClientStream for T where T: AsyncRead + AsyncWrite + Send + Unpin {}

type BoxedClientStream = Box<dyn ClientStream>;

/// Resolve the default socket path: `$XDG_RUNTIME_DIR/cue-shell/cued.sock`.
pub fn default_socket_path() -> PathBuf {
    let runtime_dir = if let Ok(dir) = std::env::var("XDG_RUNTIME_DIR") {
        PathBuf::from(dir).join(APP_DIR)
    } else {
        std::env::temp_dir().join(APP_DIR)
    };
    runtime_dir.join("cued.sock")
}

async fn read_message<R>(stream: &mut R) -> Result<Message>
where
    R: AsyncRead + Unpin,
{
    let mut len_buf = [0u8; 4];
    stream
        .read_exact(&mut len_buf)
        .await
        .context("read length prefix")?;
    let len = u32::from_be_bytes(len_buf) as usize;

    if len > MAX_MESSAGE_SIZE {
        bail!("message too large: {len} bytes (max {MAX_MESSAGE_SIZE})");
    }

    let mut body = vec![0u8; len];
    stream
        .read_exact(&mut body)
        .await
        .context("read message body")?;

    serde_json::from_slice(&body).context("deserialize message")
}

async fn send_request<W>(stream: &mut W, next_id: &mut u32, payload: RequestPayload) -> Result<u32>
where
    W: AsyncWrite + Unpin,
{
    let id = *next_id;
    *next_id += 1;

    let msg = Message::Request { id, payload };
    let buf = encode_message(&msg).context("encode request")?;
    stream.write_all(&buf).await.context("write to socket")?;
    Ok(id)
}
