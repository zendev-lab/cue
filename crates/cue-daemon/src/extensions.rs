//! Host extension dispatch and transactional submission hooks, independent of Core semantics.
use cue_core::{ExecutionId, ExecutionSpec};
use cue_protocol::ExtensionRequest;
use cue_runtime::{RuntimeError, RuntimeErrorKind, RuntimeFuture};
use cue_store_sqlite::StoreError;
use rusqlite::Connection;
use serde::Deserialize;
use serde_json::{Value, json};
use std::sync::Arc;

pub trait SubmissionEffect: Send + Sync {
    fn insert(&self, connection: &Connection, execution: ExecutionId) -> Result<(), StoreError>;
}
pub struct PreparedSubmission {
    pub spec: ExecutionSpec,
    pub effect: Box<dyn SubmissionEffect>,
}
pub trait Extension: Send + Sync {
    fn namespace(&self) -> &'static str;
    fn version(&self) -> u32;
    fn query(
        self: Arc<Self>,
        method: String,
        data: Value,
    ) -> RuntimeFuture<Result<Value, RuntimeError>>;
    fn prepare(
        self: Arc<Self>,
        method: String,
        data: Value,
    ) -> RuntimeFuture<Result<PreparedSubmission, RuntimeError>>;
    fn admitted(&self, execution: ExecutionId) -> Result<bool, RuntimeError>;
    fn tick(self: Arc<Self>) -> RuntimeFuture<Result<(), RuntimeError>>;
    fn recover(self: Arc<Self>) -> RuntimeFuture<Result<(), RuntimeError>>;
}
#[derive(Default)]
pub struct Extensions {
    entries: Vec<Arc<dyn Extension>>,
}
impl Extensions {
    pub fn register(&mut self, extension: Arc<dyn Extension>) -> Result<(), RuntimeError> {
        if self
            .entries
            .iter()
            .any(|e| e.namespace() == extension.namespace())
        {
            return Err(invalid("duplicate extension namespace"));
        }
        self.entries.push(extension);
        Ok(())
    }
    pub fn resolve(&self, request: &ExtensionRequest) -> Result<Arc<dyn Extension>, RuntimeError> {
        let extension = self
            .entries
            .iter()
            .find(|e| e.namespace() == request.namespace)
            .ok_or_else(|| invalid("unknown extension namespace"))?;
        if extension.version() != request.version {
            return Err(invalid("unsupported extension version"));
        }
        Ok(extension.clone())
    }
    pub fn admitted(&self, id: ExecutionId) -> Result<bool, RuntimeError> {
        for entry in &self.entries {
            if !entry.admitted(id)? {
                return Ok(false);
            }
        }
        Ok(true)
    }
    pub async fn tick(&self) -> Result<(), RuntimeError> {
        for entry in &self.entries {
            entry.clone().tick().await?;
        }
        Ok(())
    }
    pub async fn recover(&self) -> Result<(), RuntimeError> {
        for entry in &self.entries {
            entry.clone().recover().await?;
        }
        Ok(())
    }
}
fn invalid(message: impl Into<String>) -> RuntimeError {
    RuntimeError::new(RuntimeErrorKind::InvalidInput, message)
}
fn failure(error: anyhow::Error) -> RuntimeError {
    RuntimeError::infrastructure(error.to_string())
}

pub struct ResourceExtension(pub Arc<cue_resources::Resources>);
struct ResourceEffect(Arc<cue_resources::Resources>, cue_resources::Needs);
impl SubmissionEffect for ResourceEffect {
    fn insert(&self, db: &Connection, id: ExecutionId) -> Result<(), StoreError> {
        self.0.insert(db, id, &self.1)
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Submit {
    spec: ExecutionSpec,
    needs: cue_resources::Needs,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Show {
    execution: ExecutionId,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Empty {}
impl Extension for ResourceExtension {
    fn namespace(&self) -> &'static str {
        "resources"
    }
    fn version(&self) -> u32 {
        1
    }
    fn prepare(
        self: Arc<Self>,
        method: String,
        data: Value,
    ) -> RuntimeFuture<Result<PreparedSubmission, RuntimeError>> {
        Box::pin(async move {
            if method != "submit" {
                return Err(invalid("unknown resource command"));
            }
            let submit: Submit =
                serde_json::from_value(data).map_err(|e| invalid(e.to_string()))?;
            self.0
                .validate(&submit.needs)
                .await
                .map_err(|e| invalid(e.to_string()))?;
            Ok(PreparedSubmission {
                spec: submit.spec,
                effect: Box::new(ResourceEffect(self.0.clone(), submit.needs)),
            })
        })
    }
    fn query(
        self: Arc<Self>,
        method: String,
        data: Value,
    ) -> RuntimeFuture<Result<Value, RuntimeError>> {
        Box::pin(async move {
            if method == "show" {
                let show: Show =
                    serde_json::from_value(data).map_err(|e| invalid(e.to_string()))?;
                return Ok(json!(self.0.record(show.execution).map_err(failure)?));
            }
            let _: Empty = serde_json::from_value(data).map_err(|e| invalid(e.to_string()))?;
            match method.as_str() {
                "providers" => self.0.inspect_providers().await.map_err(failure),
                "list" => Ok(
                    json!({"providers":self.0.inspect_providers().await.map_err(failure)?,"executions":self.0.records().map_err(failure)?}),
                ),
                _ => Err(invalid("unknown resource query")),
            }
        })
    }
    fn admitted(&self, id: ExecutionId) -> Result<bool, RuntimeError> {
        self.0.admitted(id).map_err(failure)
    }
    fn tick(self: Arc<Self>) -> RuntimeFuture<Result<(), RuntimeError>> {
        Box::pin(async move { self.0.tick().await.map_err(failure) })
    }
    fn recover(self: Arc<Self>) -> RuntimeFuture<Result<(), RuntimeError>> {
        Box::pin(async move { self.0.recover().await.map_err(failure) })
    }
}
