//! Validated local transport for ephemeral spawn adapters.

use std::os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::time::Duration;

use cue_core::spawn_adapter::{SpawnAdapterHandle, SpawnAdapterRequest, SpawnAdapterResponse};
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;

const MAX_ADAPTER_MESSAGE_SIZE: usize = 1024 * 1024;
const ADAPTER_CALL_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone)]
pub(crate) struct SpawnAdapterClient {
    handle: SpawnAdapterHandle,
    adapter_dir: PathBuf,
}

impl SpawnAdapterClient {
    pub(crate) fn new(handle: SpawnAdapterHandle) -> Self {
        Self {
            handle,
            adapter_dir: crate::dirs::runtime_adapter_dir(),
        }
    }

    #[cfg(test)]
    fn with_adapter_dir(handle: SpawnAdapterHandle, adapter_dir: PathBuf) -> Self {
        Self {
            handle,
            adapter_dir,
        }
    }

    pub(crate) async fn prepare(
        &self,
        request: SpawnAdapterRequest,
    ) -> Result<Vec<String>, SpawnAdapterError> {
        match self.call(request).await? {
            SpawnAdapterResponse::Prepared { argv }
                if argv.first().is_some_and(|program| !program.is_empty()) =>
            {
                Ok(argv)
            }
            SpawnAdapterResponse::Prepared { .. } => Err(SpawnAdapterError::Protocol(
                "adapter returned an empty argv".into(),
            )),
            SpawnAdapterResponse::Rejected { message } => Err(SpawnAdapterError::Rejected(message)),
            response => Err(SpawnAdapterError::Protocol(format!(
                "unexpected prepare response {response:?}"
            ))),
        }
    }

    pub(crate) async fn settle(
        &self,
        request: SpawnAdapterRequest,
    ) -> Result<(), SpawnAdapterError> {
        match self.call(request).await? {
            SpawnAdapterResponse::Settled => Ok(()),
            SpawnAdapterResponse::InfrastructureFailure { message } => {
                Err(SpawnAdapterError::Infrastructure(message))
            }
            response => Err(SpawnAdapterError::Protocol(format!(
                "unexpected settle response {response:?}"
            ))),
        }
    }

    async fn call(
        &self,
        request: SpawnAdapterRequest,
    ) -> Result<SpawnAdapterResponse, SpawnAdapterError> {
        validate_endpoint(&self.handle.endpoint, &self.adapter_dir, current_uid())?;
        let body = serde_json::to_vec(&request)
            .map_err(|error| SpawnAdapterError::Protocol(error.to_string()))?;
        if body.len() > MAX_ADAPTER_MESSAGE_SIZE {
            return Err(SpawnAdapterError::Protocol(
                "spawn adapter request is too large".into(),
            ));
        }

        tokio::time::timeout(ADAPTER_CALL_TIMEOUT, async {
            let mut stream = UnixStream::connect(&self.handle.endpoint)
                .await
                .map_err(|error| SpawnAdapterError::Unavailable(error.to_string()))?;
            stream
                .write_u32(body.len() as u32)
                .await
                .map_err(|error| SpawnAdapterError::Unavailable(error.to_string()))?;
            stream
                .write_all(&body)
                .await
                .map_err(|error| SpawnAdapterError::Unavailable(error.to_string()))?;
            stream
                .flush()
                .await
                .map_err(|error| SpawnAdapterError::Unavailable(error.to_string()))?;

            let len = stream
                .read_u32()
                .await
                .map_err(|error| SpawnAdapterError::Unavailable(error.to_string()))?
                as usize;
            if len > MAX_ADAPTER_MESSAGE_SIZE {
                return Err(SpawnAdapterError::Protocol(
                    "spawn adapter response is too large".into(),
                ));
            }
            let mut response = vec![0; len];
            stream
                .read_exact(&mut response)
                .await
                .map_err(|error| SpawnAdapterError::Unavailable(error.to_string()))?;
            serde_json::from_slice(&response)
                .map_err(|error| SpawnAdapterError::Protocol(error.to_string()))
        })
        .await
        .map_err(|_| SpawnAdapterError::Unavailable("spawn adapter timed out".into()))?
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub(crate) enum SpawnAdapterError {
    #[error("spawn adapter unavailable: {0}")]
    Unavailable(String),
    #[error("spawn rejected: {0}")]
    Rejected(String),
    #[error("spawn adapter infrastructure failure: {0}")]
    Infrastructure(String),
    #[error("invalid spawn adapter protocol: {0}")]
    Protocol(String),
    #[error("invalid spawn adapter endpoint: {0}")]
    InvalidEndpoint(String),
}

fn current_uid() -> u32 {
    // SAFETY: geteuid has no preconditions and does not mutate memory.
    unsafe { libc::geteuid() }
}

fn validate_endpoint(
    endpoint: &Path,
    adapter_dir: &Path,
    expected_uid: u32,
) -> Result<(), SpawnAdapterError> {
    if !endpoint.is_absolute() || endpoint.parent() != Some(adapter_dir) {
        return Err(SpawnAdapterError::InvalidEndpoint(format!(
            "{} is not directly inside {}",
            endpoint.display(),
            adapter_dir.display()
        )));
    }

    let dir_metadata = std::fs::symlink_metadata(adapter_dir).map_err(|error| {
        SpawnAdapterError::InvalidEndpoint(format!(
            "cannot inspect adapter directory {}: {error}",
            adapter_dir.display()
        ))
    })?;
    if !dir_metadata.is_dir()
        || dir_metadata.file_type().is_symlink()
        || dir_metadata.uid() != expected_uid
        || dir_metadata.permissions().mode() & 0o077 != 0
    {
        return Err(SpawnAdapterError::InvalidEndpoint(format!(
            "adapter directory {} is not a private directory owned by the current uid",
            adapter_dir.display()
        )));
    }

    let endpoint_metadata = std::fs::symlink_metadata(endpoint).map_err(|error| {
        SpawnAdapterError::InvalidEndpoint(format!(
            "cannot inspect adapter endpoint {}: {error}",
            endpoint.display()
        ))
    })?;
    if !endpoint_metadata.file_type().is_socket()
        || endpoint_metadata.file_type().is_symlink()
        || endpoint_metadata.uid() != expected_uid
        || endpoint_metadata.permissions().mode() & 0o077 != 0
    {
        return Err(SpawnAdapterError::InvalidEndpoint(format!(
            "adapter endpoint {} is not a private Unix socket owned by the current uid",
            endpoint.display()
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use cue_core::{ExecutionId, SecretToken, StepId};
    use tokio::net::UnixListener;

    use super::*;

    static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn temp_adapter_dir() -> PathBuf {
        std::env::temp_dir().join(format!(
            "cue-spawn-adapter-{}-{}",
            std::process::id(),
            TEMP_COUNTER.fetch_add(1, Ordering::Relaxed)
        ))
    }

    fn handle(endpoint: PathBuf) -> SpawnAdapterHandle {
        SpawnAdapterHandle {
            endpoint,
            token: SecretToken::new("lease-token"),
        }
    }

    fn prepare_request() -> SpawnAdapterRequest {
        SpawnAdapterRequest::Prepare {
            token: SecretToken::new("lease-token"),
            execution_id: ExecutionId(1),
            step_id: StepId {
                execution: ExecutionId(1),
                index: 1,
            },
            segment_index: 0,
            argv: vec!["true".into()],
            cwd: PathBuf::from("/tmp"),
        }
    }

    fn settle_request() -> SpawnAdapterRequest {
        SpawnAdapterRequest::Settle {
            token: SecretToken::new("lease-token"),
            execution_id: ExecutionId(1),
            step_id: StepId {
                execution: ExecutionId(1),
                index: 1,
            },
            segment_index: 0,
            result: cue_core::spawn_adapter::SpawnResult::Exited { code: 0 },
            diagnostic_tail: "diagnostic".into(),
            diagnostic_truncated: false,
        }
    }

    #[tokio::test]
    async fn prepare_roundtrips_over_validated_private_socket() {
        let adapter_dir = temp_adapter_dir();
        std::fs::create_dir(&adapter_dir).unwrap();
        std::fs::set_permissions(&adapter_dir, std::fs::Permissions::from_mode(0o700)).unwrap();
        let endpoint = adapter_dir.join("broker.sock");
        let listener = UnixListener::bind(&endpoint).unwrap();
        std::fs::set_permissions(&endpoint, std::fs::Permissions::from_mode(0o600)).unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let len = stream.read_u32().await.unwrap() as usize;
            let mut body = vec![0; len];
            stream.read_exact(&mut body).await.unwrap();
            let request: SpawnAdapterRequest = serde_json::from_slice(&body).unwrap();
            assert!(matches!(request, SpawnAdapterRequest::Prepare { .. }));
            let response = serde_json::to_vec(&SpawnAdapterResponse::Prepared {
                argv: vec!["sandbox-runner".into(), "true".into()],
            })
            .unwrap();
            stream.write_u32(response.len() as u32).await.unwrap();
            stream.write_all(&response).await.unwrap();
        });

        let client =
            SpawnAdapterClient::with_adapter_dir(handle(endpoint.clone()), adapter_dir.clone());
        let argv = client.prepare(prepare_request()).await.unwrap();

        assert_eq!(argv, vec!["sandbox-runner", "true"]);
        server.await.unwrap();
        std::fs::remove_file(endpoint).unwrap();
        std::fs::remove_dir(adapter_dir).unwrap();
    }

    #[tokio::test]
    async fn prepare_rejects_socket_outside_adapter_directory() {
        let adapter_dir = temp_adapter_dir();
        std::fs::create_dir(&adapter_dir).unwrap();
        std::fs::set_permissions(&adapter_dir, std::fs::Permissions::from_mode(0o700)).unwrap();
        let outside = adapter_dir.with_extension("sock");
        let listener = UnixListener::bind(&outside).unwrap();
        std::fs::set_permissions(&outside, std::fs::Permissions::from_mode(0o600)).unwrap();

        let client =
            SpawnAdapterClient::with_adapter_dir(handle(outside.clone()), adapter_dir.clone());
        let error = client.prepare(prepare_request()).await.unwrap_err();

        assert!(matches!(error, SpawnAdapterError::InvalidEndpoint(_)));
        drop(listener);
        std::fs::remove_file(outside).unwrap();
        std::fs::remove_dir(adapter_dir).unwrap();
    }

    #[tokio::test]
    async fn prepare_rejects_group_accessible_socket() {
        let adapter_dir = temp_adapter_dir();
        std::fs::create_dir(&adapter_dir).unwrap();
        std::fs::set_permissions(&adapter_dir, std::fs::Permissions::from_mode(0o700)).unwrap();
        let endpoint = adapter_dir.join("broker.sock");
        let listener = UnixListener::bind(&endpoint).unwrap();
        std::fs::set_permissions(&endpoint, std::fs::Permissions::from_mode(0o660)).unwrap();

        let client =
            SpawnAdapterClient::with_adapter_dir(handle(endpoint.clone()), adapter_dir.clone());
        let error = client.prepare(prepare_request()).await.unwrap_err();

        assert!(matches!(error, SpawnAdapterError::InvalidEndpoint(_)));
        drop(listener);
        std::fs::remove_file(endpoint).unwrap();
        std::fs::remove_dir(adapter_dir).unwrap();
    }

    #[tokio::test]
    async fn settle_reports_broker_infrastructure_failure() {
        let adapter_dir = temp_adapter_dir();
        std::fs::create_dir(&adapter_dir).unwrap();
        std::fs::set_permissions(&adapter_dir, std::fs::Permissions::from_mode(0o700)).unwrap();
        let endpoint = adapter_dir.join("broker.sock");
        let listener = UnixListener::bind(&endpoint).unwrap();
        std::fs::set_permissions(&endpoint, std::fs::Permissions::from_mode(0o600)).unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let len = stream.read_u32().await.unwrap() as usize;
            let mut body = vec![0; len];
            stream.read_exact(&mut body).await.unwrap();
            let request: SpawnAdapterRequest = serde_json::from_slice(&body).unwrap();
            assert!(matches!(
                request,
                SpawnAdapterRequest::Settle {
                    diagnostic_tail,
                    diagnostic_truncated: false,
                    ..
                } if diagnostic_tail == "diagnostic"
            ));
            let response = serde_json::to_vec(&SpawnAdapterResponse::InfrastructureFailure {
                message: "classification failed".into(),
            })
            .unwrap();
            stream.write_u32(response.len() as u32).await.unwrap();
            stream.write_all(&response).await.unwrap();
        });

        let client =
            SpawnAdapterClient::with_adapter_dir(handle(endpoint.clone()), adapter_dir.clone());
        let error = client.settle(settle_request()).await.unwrap_err();

        assert_eq!(
            error,
            SpawnAdapterError::Infrastructure("classification failed".into())
        );
        server.await.unwrap();
        std::fs::remove_file(endpoint).unwrap();
        std::fs::remove_dir(adapter_dir).unwrap();
    }

    #[tokio::test]
    async fn settle_fails_closed_after_broker_disappears() {
        let adapter_dir = temp_adapter_dir();
        std::fs::create_dir(&adapter_dir).unwrap();
        std::fs::set_permissions(&adapter_dir, std::fs::Permissions::from_mode(0o700)).unwrap();
        let endpoint = adapter_dir.join("broker.sock");
        let listener = UnixListener::bind(&endpoint).unwrap();
        std::fs::set_permissions(&endpoint, std::fs::Permissions::from_mode(0o600)).unwrap();
        let client =
            SpawnAdapterClient::with_adapter_dir(handle(endpoint.clone()), adapter_dir.clone());
        drop(listener);
        std::fs::remove_file(&endpoint).unwrap();

        let error = client.settle(settle_request()).await.unwrap_err();

        assert!(matches!(error, SpawnAdapterError::InvalidEndpoint(_)));
        std::fs::remove_dir(adapter_dir).unwrap();
    }
}
