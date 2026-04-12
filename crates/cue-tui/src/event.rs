//! Event loop — merges terminal events, socket messages, and a tick timer
//! into a single stream of [`AppMsg`].
//!
//! The socket connection manager handles auto-reconnect: when the daemon
//! disconnects, it retries every 3 seconds and sends `Reconnected` with a
//! new [`WriterHandle`] on success.

use std::path::PathBuf;
use std::time::Duration;

use anyhow::Result;
use crossterm::event::{self, Event as CtEvent};
use tokio::sync::mpsc;

use crate::app::AppMsg;
use crate::client::{ClientReader, CuedClient, spawn_writer_task};
use cue_core::ipc::Message;

/// Spawn the event-producing tasks and return a receiver of [`AppMsg`].
///
/// Three sources feed the channel:
/// 1. **Terminal events** — crossterm key/mouse/resize (blocking thread)
/// 2. **Socket connection manager** — read + auto-reconnect (async task)
/// 3. **Tick timer** — periodic refresh for the status bar clock (async task)
pub fn spawn_event_loop(
    socket_reader: Option<ClientReader>,
    socket_path: PathBuf,
) -> Result<mpsc::UnboundedReceiver<AppMsg>> {
    let (tx, rx) = mpsc::unbounded_channel();

    // 1. Terminal events (blocking thread)
    let tx_term = tx.clone();
    std::thread::Builder::new()
        .name("tui-events".into())
        .spawn(move || {
            loop {
                match event::poll(Duration::from_millis(100)) {
                    Ok(true) => match event::read() {
                        Ok(ev) => {
                            let msg = match ev {
                                CtEvent::Key(key) => AppMsg::KeyEvent(key),
                                CtEvent::Mouse(mouse) => AppMsg::MouseEvent(mouse),
                                CtEvent::Resize(w, h) => AppMsg::Resize(w, h),
                                _ => continue,
                            };
                            if tx_term.send(msg).is_err() {
                                break;
                            }
                        }
                        Err(_) => break,
                    },
                    Ok(false) => continue,
                    Err(_) => break,
                }
            }
        })?;

    // 2. Socket connection manager (read + auto-reconnect)
    let tx_sock = tx.clone();
    tokio::spawn(socket_manager(socket_reader, socket_path, tx_sock));

    // 3. Tick timer
    let tx_tick = tx;
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        loop {
            interval.tick().await;
            if tx_tick.send(AppMsg::Tick).is_err() {
                break;
            }
        }
    });

    Ok(rx)
}

/// Long-lived task that reads from the daemon socket and auto-reconnects.
///
/// Lifecycle: read → disconnect → wait 3s → reconnect → read → ...
async fn socket_manager(
    initial_reader: Option<ClientReader>,
    socket_path: PathBuf,
    tx: mpsc::UnboundedSender<AppMsg>,
) {
    let mut reader_opt = initial_reader;

    loop {
        // Phase 1: Read from current connection until EOF/error.
        if let Some(mut reader) = reader_opt.take() {
            if read_until_disconnect(&mut reader, &tx).await.is_err() {
                return; // Channel closed — TUI is shutting down.
            }
            // Disconnected.
            if tx.send(AppMsg::Disconnected).is_err() {
                return;
            }
        }

        // Phase 2: Reconnect loop with 3s interval.
        loop {
            tokio::time::sleep(Duration::from_secs(3)).await;

            match CuedClient::connect(&socket_path).await {
                Ok(client) => {
                    let (reader, writer) = client.into_split();
                    let writer_handle = spawn_writer_task(writer);
                    if tx
                        .send(AppMsg::Reconnected {
                            writer: writer_handle,
                        })
                        .is_err()
                    {
                        return;
                    }
                    reader_opt = Some(reader);
                    break; // Back to read phase.
                }
                Err(_) => continue,
            }
        }
    }
}

/// Read messages from the daemon and forward as [`AppMsg`].
///
/// Returns `Ok(())` on disconnect, `Err(())` if the event channel is closed.
async fn read_until_disconnect(
    reader: &mut ClientReader,
    tx: &mpsc::UnboundedSender<AppMsg>,
) -> Result<(), ()> {
    loop {
        match reader.recv().await {
            Ok(msg) => {
                let app_msg = match msg {
                    Message::Response { id, payload } => AppMsg::Response { id, payload },
                    Message::Event { payload } => AppMsg::ServerEvent(payload),
                    Message::Request { .. } => continue,
                };
                if tx.send(app_msg).is_err() {
                    return Err(()); // TUI shut down.
                }
            }
            Err(_) => return Ok(()), // Daemon disconnected.
        }
    }
}
