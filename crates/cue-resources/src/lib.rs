//! Execution-scoped resource policy; Core contains none of this state.
mod command;
mod nvidia;
mod types;
pub use types::*;

use anyhow::{Context, Result, bail};
use cue_core::ExecutionId;
use cue_runtime::{RuntimeError, RuntimeErrorKind, SpawnContext, SpawnRequest, SpawnTransform};
use cue_store_sqlite::Store;
use rusqlite::{Connection, OptionalExtension};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

pub struct Resources {
    store: Arc<Mutex<Store>>,
    providers: BTreeMap<String, ProviderConfig>,
    daemon_id: String,
    coordinator: tokio::sync::Mutex<()>,
}
impl Resources {
    pub fn new(store: Arc<Mutex<Store>>, config: ResourceConfig) -> Result<Arc<Self>> {
        let mut providers = BTreeMap::new();
        let mut keys = BTreeMap::new();
        for provider in config.providers {
            if provider.id.is_empty() || provider.keys().is_empty() {
                bail!("provider requires an ID and resource keys")
            }
            for key in provider.keys().keys() {
                if key.is_empty()
                    || !key
                        .bytes()
                        .all(|c| c.is_ascii_alphanumeric() || c == b'_' || c == b'-')
                {
                    bail!("invalid resource key {key}")
                }
                if keys.insert(key.clone(), provider.id.clone()).is_some() {
                    bail!("duplicate resource key {key}")
                }
            }
            match &provider.backend {
                Backend::Command {
                    argv, timeout_ms, ..
                } if argv.is_empty()
                    || argv[0].is_empty()
                    || *timeout_ms == 0
                    || *timeout_ms > 60_000 =>
                {
                    bail!("invalid provider command or timeout")
                }
                Backend::Nvidia { argv, .. } if argv.is_empty() || argv[0].is_empty() => {
                    bail!("empty NVIDIA command")
                }
                _ => {}
            }
            if providers.insert(provider.id.clone(), provider).is_some() {
                bail!("duplicate provider ID")
            }
        }
        let daemon_id = {
            let db = store
                .lock()
                .map_err(|_| anyhow::anyhow!("store lock poisoned"))?;
            let tx = db.connection().unchecked_transaction()?;
            tx.execute_batch("CREATE TABLE IF NOT EXISTS resource_executions (execution_id INTEGER PRIMARY KEY REFERENCES executions(id), record_json TEXT NOT NULL); CREATE TABLE IF NOT EXISTS resource_identity (id TEXT PRIMARY KEY);")?;
            let id: Option<String> = tx
                .query_row("SELECT id FROM resource_identity", [], |r| r.get(0))
                .optional()?;
            let id = match id {
                Some(id) => id,
                None => {
                    let id = uuid::Uuid::new_v4().to_string();
                    tx.execute("INSERT INTO resource_identity VALUES (?1)", [&id])?;
                    id
                }
            };
            tx.pragma_update(None, "user_version", 3)?;
            tx.commit()?;
            id
        };
        Ok(Arc::new(Self {
            store,
            providers,
            daemon_id,
            coordinator: tokio::sync::Mutex::new(()),
        }))
    }
    fn with_store<T>(&self, f: impl FnOnce(&Store) -> Result<T>) -> Result<T> {
        let store = self
            .store
            .lock()
            .map_err(|_| anyhow::anyhow!("store lock poisoned"))?;
        f(&store)
    }
    pub fn records(&self) -> Result<Vec<Record>> {
        self.with_store(|store| {
            let mut query = store
                .connection()
                .prepare("SELECT record_json FROM resource_executions ORDER BY execution_id")?;
            query
                .query_map([], |r| r.get::<_, String>(0))?
                .map(|row| Ok(serde_json::from_str(&row?)?))
                .collect()
        })
    }
    pub fn record(&self, id: ExecutionId) -> Result<Option<Record>> {
        self.with_store(|store| {
            let text: Option<String> = store
                .connection()
                .query_row(
                    "SELECT record_json FROM resource_executions WHERE execution_id=?1",
                    [i64::try_from(id.0)?],
                    |r| r.get(0),
                )
                .optional()?;
            text.map(|s| serde_json::from_str(&s).map_err(Into::into))
                .transpose()
        })
    }
    fn save(&self, record: &Record) -> Result<()> {
        self.with_store(|store| {
            store.connection().execute(
                "UPDATE resource_executions SET record_json=?2 WHERE execution_id=?1",
                rusqlite::params![
                    i64::try_from(record.execution.0)?,
                    serde_json::to_string(record)?
                ],
            )?;
            Ok(())
        })
    }
    pub fn insert(
        &self,
        connection: &Connection,
        id: ExecutionId,
        needs: &Needs,
    ) -> Result<(), cue_store_sqlite::StoreError> {
        let row = Record {
            execution: id,
            needs: needs.clone(),
            providers: self
                .providers
                .values()
                .filter(|p| p.keys().keys().any(|k| needs.contains_key(k)))
                .cloned()
                .collect(),
            status: Status::Waiting,
            attempts: Vec::new(),
            reason: None,
            environment: BTreeMap::new(),
        };
        connection.execute(
            "INSERT INTO resource_executions VALUES (?1,?2)",
            rusqlite::params![
                i64::try_from(id.0).map_err(|_| cue_store_sqlite::StoreError::IntegerOverflow {
                    kind: "execution",
                    value: id.0
                })?,
                serde_json::to_string(&row)?
            ],
        )?;
        Ok(())
    }
    pub async fn validate(&self, needs: &Needs) -> Result<()> {
        if needs.is_empty() {
            bail!("resource submission requires at least one need")
        }
        for (key, need) in needs {
            let provider = self
                .providers
                .values()
                .find(|p| p.keys().contains_key(key))
                .with_context(|| format!("unknown resource key {key}"))?;
            let unit = provider.keys()[key];
            if !matches!(
                (unit, need),
                (Unit::Count, Quantity::Count(_)) | (Unit::Bytes, Quantity::Bytes(_))
            ) {
                bail!("wrong quantity type for {key}")
            }
            match &provider.backend {
                Backend::Static { capacity } if need.value() > capacity[key].value() => {
                    bail!("{key} demand exceeds configured capacity")
                }
                Backend::Nvidia { argv, .. } => {
                    nvidia::probe(argv)
                        .await
                        .context("NVIDIA provider unavailable")?;
                }
                _ => {}
            }
        }
        Ok(())
    }
    pub fn admitted(&self, id: ExecutionId) -> Result<bool> {
        Ok(self
            .record(id)?
            .is_none_or(|r| r.status == Status::Allocated))
    }
    fn terminal(&self, id: ExecutionId) -> Result<bool> {
        self.with_store(|s| {
            Ok(s.get_execution(id)?
                .context("resource execution disappeared")?
                .state
                .is_terminal())
        })
    }
    fn attempts(&self, row: &Record) -> Vec<Attempt> {
        row.providers
            .iter()
            .filter_map(|provider| {
                let needs: Needs = row
                    .needs
                    .iter()
                    .filter(|(k, _)| provider.keys().contains_key(*k))
                    .map(|(k, v)| (k.clone(), *v))
                    .collect();
                if needs.is_empty() {
                    return None;
                }
                Some(Attempt {
                    provider: provider.clone(),
                    request: ProviderRequest {
                        version: 1,
                        method: "reserve".into(),
                        daemon_id: self.daemon_id.clone(),
                        request_id: uuid::Uuid::new_v4().to_string(),
                        execution: row.execution,
                        needs,
                    },
                    grant: None,
                    uncertain: false,
                    released: false,
                })
            })
            .collect()
    }
    async fn external(&self, attempt: &Attempt, method: &str) -> Result<ProviderReply> {
        let Backend::Command {
            argv, timeout_ms, ..
        } = &attempt.provider.backend
        else {
            bail!("not a command provider")
        };
        let mut request = attempt.request.clone();
        request.method = method.into();
        let out = command::run(argv, serde_json::to_vec(&request)?, *timeout_ms).await?;
        serde_json::from_slice(&out).context("invalid provider response")
    }
    async fn reserve(&self, attempt: &Attempt) -> Result<ProviderReply> {
        let held: Vec<Attempt> = self
            .records()?
            .into_iter()
            .flat_map(|r| r.attempts)
            .filter(|a| a.provider.id == attempt.provider.id && a.grant.is_some() && !a.released)
            .collect();
        match &attempt.provider.backend {
            Backend::Static { capacity } => {
                for (key, need) in &attempt.request.needs {
                    let used = held
                        .iter()
                        .filter_map(|a| a.request.needs.get(key))
                        .fold(0u64, |sum, n| sum.saturating_add(n.value()));
                    if capacity[key].value().saturating_sub(used) < need.value() {
                        return Ok(ProviderReply::Rejected {
                            reason: format!("waiting for {key}"),
                        });
                    }
                }
                Ok(ProviderReply::Granted {
                    grant: Grant {
                        id: attempt.request.request_id.clone(),
                        environment: BTreeMap::new(),
                        devices: Vec::new(),
                    },
                })
            }
            Backend::Nvidia {
                argv,
                safety_margin_bytes,
            } => {
                let devices = nvidia::probe(argv).await?;
                Ok(
                    match nvidia::select(
                        &devices,
                        &attempt.request.needs,
                        &held,
                        *safety_margin_bytes,
                        &attempt.request.request_id,
                    )? {
                        Some(grant) => ProviderReply::Granted { grant },
                        None => ProviderReply::Rejected {
                            reason: "waiting for NVIDIA capacity".into(),
                        },
                    },
                )
            }
            Backend::Command { .. } => {
                if attempt.uncertain {
                    match self.external(attempt, "lookup").await? {
                        ProviderReply::Absent => {}
                        reply => return Ok(reply),
                    }
                }
                self.external(attempt, "reserve").await
            }
        }
    }
    /// Validate persisted ownership before the daemon starts any execution.
    pub async fn recover(&self) -> Result<()> {
        for mut row in self.records()? {
            if row.status == Status::Released {
                continue;
            }
            if !self.terminal(row.execution)? {
                for provider in &row.providers {
                    let current = self
                        .providers
                        .get(&provider.id)
                        .context("pending resource provider was removed")?;
                    if serde_json::to_value(current)? != serde_json::to_value(provider)? {
                        bail!(
                            "pending provider {} changed for {}",
                            provider.id,
                            row.execution
                        )
                    }
                }
            }
            for (i, attempt) in row
                .attempts
                .clone()
                .iter()
                .enumerate()
                .filter(|(_, a)| !a.released)
            {
                let current = self
                    .providers
                    .get(&attempt.provider.id)
                    .context("resource provider was removed with unresolved allocations")?;
                if serde_json::to_value(current)? != serde_json::to_value(&attempt.provider)? {
                    bail!(
                        "provider {} changed with unresolved allocation {}",
                        current.id,
                        attempt.request.request_id
                    )
                }
                if matches!(attempt.provider.backend, Backend::Command { .. })
                    && (attempt.uncertain || attempt.grant.is_some())
                {
                    match self.external(attempt, "lookup").await? {
                        ProviderReply::Granted { grant }
                            if attempt.grant.as_ref().is_none_or(|old| old == &grant) =>
                        {
                            row.attempts[i].grant = Some(grant);
                            row.attempts[i].uncertain = false;
                            self.save(&row)?;
                        }
                        ProviderReply::Absent if attempt.grant.is_none() => {}
                        ProviderReply::Released
                            if matches!(row.status, Status::Cleaning | Status::Isolated)
                                || self.terminal(row.execution)? =>
                        {
                            row.attempts[i].released = true;
                            self.save(&row)?;
                        }
                        _ => bail!(
                            "cannot recover {} allocation {}: provider ownership is unknown",
                            row.execution,
                            attempt.request.request_id
                        ),
                    }
                }
                if let Backend::Nvidia { argv, .. } = &attempt.provider.backend {
                    let devices = nvidia::probe(argv).await?;
                    if let Some(grant) = &attempt.grant
                        && grant
                            .devices
                            .iter()
                            .any(|uuid| !devices.iter().any(|d| &d.uuid == uuid))
                    {
                        bail!("allocated NVIDIA device is missing for {}", row.execution)
                    }
                }
            }
            // Waiting demand must still have the original key routes.
            for key in row.needs.keys() {
                if !self.providers.values().any(|p| p.keys().contains_key(key)) {
                    bail!("resource {key} removed while {} is pending", row.execution)
                }
            }
        }
        Ok(())
    }
    async fn cleanup(&self, row: &mut Record) -> Result<bool> {
        row.status = Status::Cleaning;
        self.save(row)?;
        for i in (0..row.attempts.len()).rev() {
            let attempt = &row.attempts[i];
            if attempt.released {
                continue;
            }
            if matches!(attempt.provider.backend, Backend::Command { .. })
                && (attempt.grant.is_some() || attempt.uncertain)
            {
                if attempt.uncertain {
                    match self.external(attempt, "lookup").await {
                        Ok(ProviderReply::Released) => {
                            row.attempts[i].released = true;
                            row.attempts[i].uncertain = false;
                            self.save(row)?;
                            continue;
                        }
                        Ok(ProviderReply::Granted { grant }) => {
                            row.attempts[i].grant = Some(grant);
                            self.save(row)?;
                        }
                        Ok(ProviderReply::Absent) => {}
                        reply => {
                            row.status = Status::Isolated;
                            row.reason = Some(format!("lookup unresolved: {reply:?}"));
                            self.save(row)?;
                            return Ok(false);
                        }
                    }
                }
                match self.external(&row.attempts[i], "release").await {
                    Ok(ProviderReply::Released) => {}
                    reply => {
                        row.attempts[i].uncertain = true;
                        row.status = Status::Isolated;
                        row.reason = Some(format!("release unresolved: {reply:?}"));
                        self.save(row)?;
                        return Ok(false);
                    }
                }
            }
            row.attempts[i].released = true;
            row.attempts[i].uncertain = false;
            self.save(row)?;
        }
        row.environment.clear();
        if self.terminal(row.execution)? {
            row.status = Status::Released;
        } else {
            row.status = Status::Waiting;
            row.attempts.clear();
        }
        self.save(row)?;
        Ok(true)
    }
    pub async fn tick(&self) -> Result<()> {
        let _serial = self.coordinator.lock().await;
        for mut row in self.records()? {
            if row.status == Status::Released {
                continue;
            }
            if self.terminal(row.execution)?
                || matches!(row.status, Status::Cleaning | Status::Isolated)
            {
                self.cleanup(&mut row).await?;
                continue;
            }
            if row.status == Status::Allocated {
                continue;
            }
            if row.status == Status::Waiting {
                row.attempts = self.attempts(&row);
                row.status = Status::Allocating;
                self.save(&row)?;
            }
            let mut complete = true;
            for i in 0..row.attempts.len() {
                if row.attempts[i].grant.is_some() {
                    continue;
                }
                if self.terminal(row.execution)? {
                    complete = false;
                    break;
                }
                let original = row.attempts[i].clone();
                // Persist uncertainty before crossing the provider boundary.
                row.attempts[i].uncertain = true;
                self.save(&row)?;
                match self.reserve(&original).await {
                    Ok(ProviderReply::Granted { grant }) => {
                        row.attempts[i].grant = Some(grant.clone());
                        row.attempts[i].uncertain = false;
                        self.save(&row)?;
                    }
                    Ok(ProviderReply::Rejected { reason }) => {
                        row.attempts[i].uncertain = false;
                        row.reason = Some(reason);
                        row.status = Status::Cleaning;
                        complete = false;
                        break;
                    }
                    result => {
                        row.reason = Some(format!("provider result unresolved: {result:?}"));
                        row.status = Status::Cleaning;
                        complete = false;
                        break;
                    }
                }
            }
            if complete {
                row.environment.clear();
                for grant in row.attempts.iter().filter_map(|a| a.grant.as_ref()) {
                    for (key, value) in &grant.environment {
                        if key.is_empty()
                            || key.contains(['=', '\0'])
                            || value.contains('\0')
                            || row.environment.insert(key.clone(), value.clone()).is_some()
                        {
                            row.reason =
                                Some(format!("conflicting or invalid provider environment {key}"));
                            complete = false;
                        }
                    }
                }
                if complete {
                    row.status = Status::Allocated;
                    row.reason = None;
                } else {
                    row.status = Status::Cleaning;
                }
            }
            self.save(&row)?;
            if self.terminal(row.execution)?
                || matches!(row.status, Status::Cleaning | Status::Isolated)
            {
                self.cleanup(&mut row).await?;
            }
        }
        Ok(())
    }
    pub async fn inspect_providers(&self) -> Result<Value> {
        let rows = self.records()?;
        let mut result = Vec::new();
        for provider in self.providers.values() {
            let snapshot: Result<Value> = match &provider.backend {
                Backend::Static { capacity } => {
                    let mut available = capacity.clone();
                    for row in &rows {
                        for a in &row.attempts {
                            if a.provider.id == provider.id && !a.released && a.grant.is_some() {
                                for (key, q) in &a.request.needs {
                                    if let Some(cap) = available.get_mut(key) {
                                        let n = cap.value().saturating_sub(q.value());
                                        *cap = match cap {
                                            Quantity::Count(_) => Quantity::Count(n),
                                            Quantity::Bytes(_) => Quantity::Bytes(n),
                                        };
                                    }
                                }
                            }
                        }
                    }
                    Ok(json!({"capacity":capacity,"available":available}))
                }
                Backend::Nvidia { argv, .. } => nvidia::probe(argv)
                    .await
                    .and_then(|d| serde_json::to_value(d).map_err(Into::into)),
                Backend::Command { .. } => {
                    let attempt = Attempt {
                        provider: provider.clone(),
                        request: ProviderRequest {
                            version: 1,
                            method: "probe".into(),
                            daemon_id: self.daemon_id.clone(),
                            request_id: String::new(),
                            execution: ExecutionId(0),
                            needs: Needs::new(),
                        },
                        grant: None,
                        uncertain: false,
                        released: false,
                    };
                    match self.external(&attempt, "probe").await {
                        Ok(ProviderReply::Snapshot { data }) => Ok(data),
                        Ok(other) => Err(anyhow::anyhow!("unexpected probe response {other:?}")),
                        Err(e) => Err(e),
                    }
                }
            };
            result.push(match snapshot {Ok(data)=>json!({"id":provider.id,"keys":provider.keys(),"available":true,"snapshot":data}),Err(error)=>json!({"id":provider.id,"keys":provider.keys(),"available":false,"error":error.to_string()})});
        }
        Ok(json!(result))
    }
}
impl SpawnTransform for Resources {
    fn transform(
        &self,
        request: &SpawnRequest,
        context: &mut SpawnContext,
    ) -> Result<(), RuntimeError> {
        if let Some(row) = self.record(request.step.execution).map_err(runtime_error)? {
            if row.status != Status::Allocated {
                return Err(RuntimeError::new(
                    RuntimeErrorKind::Conflict,
                    "execution resources are not allocated",
                ));
            }
            for (k, v) in row.environment {
                if context.environment.insert(k, v).is_some() {
                    return Err(RuntimeError::new(
                        RuntimeErrorKind::Conflict,
                        "physical environment conflict",
                    ));
                }
            }
        }
        Ok(())
    }
}
fn runtime_error(error: anyhow::Error) -> RuntimeError {
    RuntimeError::infrastructure(error.to_string())
}
