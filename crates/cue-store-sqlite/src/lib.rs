//! SQLite-backed vNext execution, scope, fact, and idempotency store.
//!
//! This provider owns a fresh schema. It never opens, imports, or mutates the
//! IPC v3 database; the daemon hard cut archives that database separately.

use cue_core::vnext::{Execution, ExecutionSnapshot, ExecutionState, Scope, StepState};
use cue_core::{ExecutionId, ScopeHash, StepId};
use cue_protocol::{ClientId, Command, EventId, Fact, FactEvent, OperationId, ResponsePayload};
use rusqlite::{Connection, OptionalExtension as _};
use thiserror::Error;

const SCHEMA_VERSION: u32 = 2;
const STORE_SCHEMA: &str = r#"
CREATE TABLE scopes (
    hash            BLOB PRIMARY KEY,
    snapshot_json   TEXT NOT NULL,
    created_at_ms   INTEGER NOT NULL
) WITHOUT ROWID;

CREATE TABLE executions (
    id              INTEGER PRIMARY KEY,
    snapshot_json   TEXT NOT NULL,
    state_json      TEXT NOT NULL,
    created_at_ms   INTEGER NOT NULL,
    updated_at_ms   INTEGER NOT NULL
);

CREATE TABLE runtime_work (
    execution_id INTEGER NOT NULL REFERENCES executions(id),
    step_index INTEGER NOT NULL CHECK(step_index > 0),
    desired_generation INTEGER NOT NULL CHECK(desired_generation > 0),
    applied_generation INTEGER NOT NULL DEFAULT 0,
    claimed_generation INTEGER,
    attempt_started INTEGER NOT NULL DEFAULT 0 CHECK(attempt_started IN (0, 1)),
    PRIMARY KEY (execution_id, step_index),
    CHECK(applied_generation <= desired_generation)
) WITHOUT ROWID;

CREATE TABLE facts (
    event_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    execution_id   INTEGER REFERENCES executions(id),
    occurred_at_ms INTEGER NOT NULL,
    fact_json       TEXT NOT NULL
);

CREATE INDEX facts_execution_cursor
ON facts (execution_id, event_id);

CREATE TABLE operations (
    client_id_hash    BLOB NOT NULL,
    operation_id_hash BLOB NOT NULL,
    fingerprint       BLOB NOT NULL,
    response_json     TEXT,
    completed_at_ms   INTEGER NOT NULL,
    PRIMARY KEY (client_id_hash, operation_id_hash)
) WITHOUT ROWID;
"#;

const CLIENT_HASH_DOMAIN: &[u8] = b"cue-vnext-client-id\0";
const OPERATION_HASH_DOMAIN: &[u8] = b"cue-vnext-operation-id\0";
const COMMAND_HASH_DOMAIN: &[u8] = b"cue-vnext-command\0";

pub struct Store {
    connection: Connection,
}

impl Store {
    pub fn in_memory() -> Result<Self, StoreError> {
        Self::from_connection(Connection::open_in_memory()?)
    }

    /// Adopt a connection opened by the host's private-file policy.
    ///
    /// The store intentionally does not open filesystem paths itself: the
    /// daemon owns permissions, symlink rejection, and archive placement.
    pub fn from_connection(connection: Connection) -> Result<Self, StoreError> {
        connection.pragma_update(None, "journal_mode", "WAL")?;
        connection.pragma_update(None, "synchronous", "NORMAL")?;
        connection.pragma_update(None, "foreign_keys", "ON")?;
        migrate(&connection)?;
        Ok(Self { connection })
    }

    pub fn put_scope(
        &self,
        scope: &Scope,
        created_at_ms: i64,
    ) -> Result<ScopePersistence, StoreError> {
        if contains_sensitive_environment(scope) {
            return Err(StoreError::SensitiveEnvironmentUnsupported);
        }
        let hash = scope.compute_hash();
        self.connection.execute(
            "INSERT OR IGNORE INTO scopes (hash, snapshot_json, created_at_ms)
             VALUES (?1, ?2, ?3)",
            rusqlite::params![
                hash.0.as_slice(),
                serde_json::to_string(scope)?,
                created_at_ms
            ],
        )?;
        Ok(ScopePersistence::Persisted)
    }

    pub fn get_scope(&self, hash: ScopeHash) -> Result<Option<Scope>, StoreError> {
        let snapshot = self
            .connection
            .query_row(
                "SELECT snapshot_json FROM scopes WHERE hash = ?1",
                rusqlite::params![hash.0.as_slice()],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        let Some(snapshot) = snapshot else {
            return Ok(None);
        };
        let scope: Scope = serde_json::from_str(&snapshot)?;
        let actual = scope.compute_hash();
        if actual != hash {
            return Err(StoreError::ScopeHashMismatch {
                expected: hash,
                actual,
            });
        }
        Ok(Some(scope))
    }

    /// Commit a complete projection and its facts in one transaction.
    pub fn commit_execution(
        &self,
        execution: &StoredExecution,
        facts: &[FactDraft],
    ) -> Result<Vec<FactEvent>, StoreError> {
        let transaction = self.connection.unchecked_transaction()?;
        validate_commit(&transaction, execution, facts)?;
        let committed = commit_projection(&transaction, execution, facts)?;
        transaction.commit()?;
        Ok(committed)
    }

    /// Atomically claim a command identity and commit its execution effect.
    pub fn commit_execution_command(
        &self,
        operation: OperationCommit<'_>,
        execution: &StoredExecution,
        facts: &[FactDraft],
    ) -> Result<ExecutionCommandCommit, StoreError> {
        let transaction = self.connection.unchecked_transaction()?;
        match record_operation_on(&transaction, operation)? {
            OperationRecord::Inserted => {
                validate_commit(&transaction, execution, facts)?;
                let facts = commit_projection(&transaction, execution, facts)?;
                transaction.commit()?;
                Ok(ExecutionCommandCommit::Committed { facts })
            }
            OperationRecord::Replay { response } => Ok(ExecutionCommandCommit::Replay { response }),
            OperationRecord::Conflict { stored_fingerprint } => {
                Ok(ExecutionCommandCommit::Conflict { stored_fingerprint })
            }
        }
    }

    pub fn get_execution(&self, id: ExecutionId) -> Result<Option<StoredExecution>, StoreError> {
        let row = self
            .connection
            .query_row(
                "SELECT snapshot_json, state_json, created_at_ms, updated_at_ms
                 FROM executions WHERE id = ?1",
                rusqlite::params![sqlite_id(id)?],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, i64>(3)?,
                    ))
                },
            )
            .optional()?;
        row.map(decode_execution).transpose()
    }

    pub fn list_executions(
        &self,
        before: Option<ExecutionId>,
        limit: u16,
    ) -> Result<Vec<StoredExecution>, StoreError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let before = before.map(sqlite_id).transpose()?.unwrap_or(i64::MAX);
        let mut statement = self.connection.prepare(
            "SELECT snapshot_json, state_json, created_at_ms, updated_at_ms
             FROM executions WHERE id < ?1 ORDER BY id DESC LIMIT ?2",
        )?;
        let rows = statement.query_map(rusqlite::params![before, i64::from(limit)], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, i64>(3)?,
            ))
        })?;
        let mut executions = Vec::new();
        for row in rows {
            executions.push(decode_execution(row?)?);
        }
        Ok(executions)
    }

    /// Step references only. A consumer must read the latest committed snapshot.
    pub fn pending_runtime_steps(&self) -> Result<Vec<StepId>, StoreError> {
        let mut statement = self.connection.prepare(
            "SELECT execution_id, step_index FROM runtime_work
             WHERE desired_generation > applied_generation AND claimed_generation IS NULL
             ORDER BY execution_id, step_index",
        )?;
        let rows = statement.query_map([], |row| {
            Ok(StepId {
                execution: ExecutionId(read_u64(row, 0)?),
                index: row.get(1)?,
            })
        })?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(StoreError::from)
    }

    /// Claim one generation; concurrent consumers cannot claim the same Step.
    pub fn claim_runtime_step(&self, step: StepId) -> Result<Option<u64>, StoreError> {
        self.connection
            .query_row(
                "UPDATE runtime_work SET claimed_generation = desired_generation
             WHERE execution_id = ?1 AND step_index = ?2 AND claimed_generation IS NULL
               AND desired_generation > applied_generation
             RETURNING claimed_generation",
                rusqlite::params![sqlite_id(step.execution)?, step.index],
                |row| read_u64(row, 0),
            )
            .optional()
            .map_err(StoreError::from)
    }

    /// Acknowledge only this claim. A later generation remains pending.
    pub fn acknowledge_runtime_step(
        &self,
        step: StepId,
        generation: u64,
    ) -> Result<bool, StoreError> {
        Ok(self.connection.execute(
            "UPDATE runtime_work SET applied_generation = ?3, claimed_generation = NULL
             WHERE execution_id = ?1 AND step_index = ?2 AND claimed_generation = ?3",
            rusqlite::params![
                sqlite_id(step.execution)?,
                step.index,
                sqlite_u64(generation, "generation")?
            ],
        )? == 1)
    }

    /// Persist the uncertainty boundary before attempting any physical spawn.
    /// Once set, recovery cannot infer that replay is safe from a Running snapshot.
    pub fn begin_run_attempt(&self, step: StepId, generation: u64) -> Result<bool, StoreError> {
        let transaction = self.connection.unchecked_transaction()?;
        let projection = load_execution(&transaction, step.execution)?
            .ok_or(StoreError::MissingExecution(step.execution))?;
        let execution = Execution::restore(projection.snapshot)?;
        if !matches!(
            execution.step(step).map(|step| step.state()),
            Some(StepState::Running)
        ) || !matches!(
            execution.action(step),
            Some(cue_core::vnext::StepAction::Run { .. })
        ) {
            return Ok(false);
        }
        let started = transaction.execute(
            "UPDATE runtime_work SET attempt_started = 1
             WHERE execution_id = ?1 AND step_index = ?2 AND claimed_generation = ?3
               AND desired_generation = ?3 AND attempt_started = 0",
            rusqlite::params![
                sqlite_id(step.execution)?,
                step.index,
                sqlite_u64(generation, "generation")?
            ],
        )? == 1;
        transaction.commit()?;
        Ok(started)
    }

    /// Call only after acquiring exclusive daemon ownership and retiring old workers.
    /// This implementation cannot retake physical process ownership, so unknown
    /// active attempts fail closed; unstarted work and replayable builtins are requeued.
    pub fn recover_runtime_work(&self) -> Result<(), StoreError> {
        let transaction = self.connection.unchecked_transaction()?;
        let mut active = Vec::new();
        let mut statement = transaction.prepare("SELECT id FROM executions ORDER BY id")?;
        let ids = statement
            .query_map([], |row| read_u64(row, 0))?
            .collect::<Result<Vec<_>, _>>()?;
        drop(statement);
        for id in ids {
            let projection = load_execution(&transaction, ExecutionId(id))?
                .ok_or(StoreError::MissingExecution(ExecutionId(id)))?;
            for step in &projection.snapshot.steps {
                if matches!(
                    step.state(),
                    StepState::Running | StepState::Cancelling { .. }
                ) {
                    let attempted = transaction.query_row(
                        "SELECT attempt_started FROM runtime_work WHERE execution_id = ?1 AND step_index = ?2",
                        rusqlite::params![sqlite_id(step.id().execution)?, step.id().index], |row| row.get::<_, bool>(0),
                    ).optional()?.ok_or(StoreError::MissingRuntimeWork(step.id()))?;
                    if attempted {
                        return Err(StoreError::UncertainRunOwnership(step.id()));
                    }
                    active.push(step.id());
                }
            }
        }
        transaction.execute("UPDATE runtime_work SET claimed_generation = NULL", [])?;
        for step in active {
            transaction.execute(
                "UPDATE runtime_work SET desired_generation = desired_generation + 1
                WHERE execution_id = ?1 AND step_index = ?2",
                rusqlite::params![sqlite_id(step.execution)?, step.index],
            )?;
        }
        transaction.commit()?;
        Ok(())
    }

    pub fn facts_after(
        &self,
        execution: ExecutionId,
        after: Option<EventId>,
        limit: u16,
    ) -> Result<Vec<FactEvent>, StoreError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let after = after.map(EventId::get).unwrap_or(0);
        let mut statement = self.connection.prepare(
            "SELECT event_id, occurred_at_ms, fact_json
             FROM facts
             WHERE execution_id = ?1 AND event_id > ?2
             ORDER BY event_id LIMIT ?3",
        )?;
        let rows = statement.query_map(
            rusqlite::params![
                sqlite_id(execution)?,
                sqlite_u64(after, "event")?,
                i64::from(limit)
            ],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, String>(2)?,
                ))
            },
        )?;
        let mut facts = Vec::new();
        for row in rows {
            let (event_id, occurred_at_ms, fact_json) = row?;
            facts.push(FactEvent {
                id: EventId::new(u64::try_from(event_id).map_err(|_| {
                    StoreError::NegativeInteger {
                        kind: "event",
                        value: event_id,
                    }
                })?)
                .map_err(StoreError::InvalidProtocolId)?,
                occurred_at_ms,
                fact: serde_json::from_str(&fact_json)?,
            });
        }
        Ok(facts)
    }

    /// Insert one completed operation or replay the immutable prior outcome.
    pub fn record_operation(
        &self,
        client: &ClientId,
        operation: &OperationId,
        command: &Command,
        response: Option<&ResponsePayload>,
        completed_at_ms: i64,
    ) -> Result<OperationRecord, StoreError> {
        record_operation_on(
            &self.connection,
            OperationCommit {
                client,
                operation,
                command,
                response,
                completed_at_ms,
            },
        )
    }

    /// Drop replay payloads while preserving permanent at-most-once tombstones.
    pub fn tombstone_operation_responses_before(
        &self,
        completed_before_ms: i64,
    ) -> Result<usize, StoreError> {
        Ok(self.connection.execute(
            "UPDATE operations SET response_json = NULL
             WHERE completed_at_ms < ?1 AND response_json IS NOT NULL",
            [completed_before_ms],
        )?)
    }

    #[cfg(test)]
    fn connection(&self) -> &Connection {
        &self.connection
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredExecution {
    pub snapshot: ExecutionSnapshot,
    pub state: ExecutionState,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FactDraft {
    pub occurred_at_ms: i64,
    pub fact: Fact,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScopePersistence {
    Persisted,
    VolatileSensitiveEnvironment,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OperationRecord {
    Inserted,
    Replay { response: Option<ResponsePayload> },
    Conflict { stored_fingerprint: [u8; 32] },
}

#[derive(Debug, Clone, Copy)]
pub struct OperationCommit<'a> {
    pub client: &'a ClientId,
    pub operation: &'a OperationId,
    pub command: &'a Command,
    pub response: Option<&'a ResponsePayload>,
    pub completed_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutionCommandCommit {
    Committed { facts: Vec<FactEvent> },
    Replay { response: Option<ResponsePayload> },
    Conflict { stored_fingerprint: [u8; 32] },
}

pub fn command_fingerprint(command: &Command) -> Result<[u8; 32], StoreError> {
    let encoded = serde_json::to_vec(command)?;
    let mut hasher = blake3::Hasher::new();
    hasher.update(COMMAND_HASH_DOMAIN);
    hasher.update(&(encoded.len() as u64).to_le_bytes());
    hasher.update(&encoded);
    Ok(*hasher.finalize().as_bytes())
}

fn validate_commit(
    connection: &Connection,
    execution: &StoredExecution,
    facts: &[FactDraft],
) -> Result<(), StoreError> {
    validate_execution(execution)?;
    validate_scope_references(connection, execution)?;
    let previous = load_execution(connection, execution.snapshot.id)?;
    if let Some(previous) = &previous
        && previous.created_at_ms != execution.created_at_ms
    {
        return Err(StoreError::CreatedAtMismatch {
            id: execution.snapshot.id,
            existing: previous.created_at_ms,
            attempted: execution.created_at_ms,
        });
    }
    if let Some(previous) = &previous {
        if previous.snapshot.spec != execution.snapshot.spec {
            return Err(StoreError::ImmutableExecutionSpec(execution.snapshot.id));
        }
        if execution.updated_at_ms < previous.updated_at_ms {
            return Err(StoreError::UpdatedAtRegression {
                id: execution.snapshot.id,
                previous: previous.updated_at_ms,
                attempted: execution.updated_at_ms,
            });
        }
    }
    for draft in facts {
        if draft.fact.execution_id() != execution.snapshot.id {
            return Err(StoreError::FactExecutionMismatch {
                expected: execution.snapshot.id,
                actual: draft.fact.execution_id(),
            });
        }
    }
    validate_fact_projection(previous.as_ref(), execution, facts)?;
    Ok(())
}

fn load_execution(
    connection: &Connection,
    id: ExecutionId,
) -> Result<Option<StoredExecution>, StoreError> {
    connection
        .query_row(
            "SELECT snapshot_json, state_json, created_at_ms, updated_at_ms
             FROM executions WHERE id = ?1",
            rusqlite::params![sqlite_id(id)?],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                ))
            },
        )
        .optional()?
        .map(decode_execution)
        .transpose()
}

fn validate_scope_references(
    connection: &Connection,
    execution: &StoredExecution,
) -> Result<(), StoreError> {
    let mut scopes = vec![execution.snapshot.spec.scope()];
    for step in &execution.snapshot.steps {
        if let Some(hash) = step.input_scope() {
            scopes.push(hash);
        }
        if let Some(hash) = step.output_scope() {
            scopes.push(hash);
        }
    }
    scopes.sort_by_key(|hash| hash.0);
    scopes.dedup();
    for hash in scopes {
        let exists = connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM scopes WHERE hash = ?1)",
            rusqlite::params![hash.0.as_slice()],
            |row| row.get::<_, bool>(0),
        )?;
        if !exists {
            return Err(StoreError::MissingScopeReference {
                execution: execution.snapshot.id,
                scope: hash,
            });
        }
    }
    Ok(())
}

fn commit_projection(
    connection: &Connection,
    execution: &StoredExecution,
    facts: &[FactDraft],
) -> Result<Vec<FactEvent>, StoreError> {
    let id = sqlite_id(execution.snapshot.id)?;
    connection.execute(
        "INSERT INTO executions (
             id, snapshot_json, state_json, created_at_ms, updated_at_ms
         ) VALUES (?1, ?2, ?3, ?4, ?5)
         ON CONFLICT(id) DO UPDATE SET
             snapshot_json = excluded.snapshot_json,
             state_json = excluded.state_json,
             updated_at_ms = excluded.updated_at_ms",
        rusqlite::params![
            id,
            serde_json::to_string(&execution.snapshot)?,
            serde_json::to_string(&execution.state)?,
            execution.created_at_ms,
            execution.updated_at_ms,
        ],
    )?;

    for draft in facts {
        if let Fact::StepStateChanged {
            id: step,
            next: StepState::Running | StepState::Cancelling { .. },
            ..
        } = &draft.fact
        {
            connection.execute(
                "INSERT INTO runtime_work (execution_id, step_index, desired_generation)
                 VALUES (?1, ?2, 1)
                 ON CONFLICT(execution_id, step_index) DO UPDATE SET
                     desired_generation = desired_generation + 1",
                rusqlite::params![id, i64::from(step.index)],
            )?;
        }
    }

    let mut committed = Vec::with_capacity(facts.len());
    for draft in facts {
        connection.execute(
            "INSERT INTO facts (execution_id, occurred_at_ms, fact_json)
             VALUES (?1, ?2, ?3)",
            rusqlite::params![
                id,
                draft.occurred_at_ms,
                serde_json::to_string(&draft.fact)?
            ],
        )?;
        let raw_event_id = connection.last_insert_rowid();
        committed.push(FactEvent {
            id: EventId::new(u64::try_from(raw_event_id).map_err(|_| {
                StoreError::NegativeInteger {
                    kind: "event",
                    value: raw_event_id,
                }
            })?)
            .expect("SQLite AUTOINCREMENT event ids are non-zero"),
            occurred_at_ms: draft.occurred_at_ms,
            fact: draft.fact.clone(),
        });
    }
    Ok(committed)
}

fn record_operation_on(
    connection: &Connection,
    operation: OperationCommit<'_>,
) -> Result<OperationRecord, StoreError> {
    let client_hash = hash_text(CLIENT_HASH_DOMAIN, operation.client.as_str());
    let operation_hash = hash_text(OPERATION_HASH_DOMAIN, operation.operation.as_str());
    let fingerprint = command_fingerprint(operation.command)?;
    let response_json = operation.response.map(serde_json::to_string).transpose()?;
    let inserted = connection.execute(
        "INSERT OR IGNORE INTO operations (
             client_id_hash, operation_id_hash, fingerprint,
             response_json, completed_at_ms
         ) VALUES (?1, ?2, ?3, ?4, ?5)",
        rusqlite::params![
            client_hash.as_slice(),
            operation_hash.as_slice(),
            fingerprint.as_slice(),
            response_json,
            operation.completed_at_ms,
        ],
    )?;
    if inserted == 1 {
        return Ok(OperationRecord::Inserted);
    }

    let existing = connection
        .query_row(
            "SELECT fingerprint, response_json FROM operations
             WHERE client_id_hash = ?1 AND operation_id_hash = ?2",
            rusqlite::params![client_hash.as_slice(), operation_hash.as_slice()],
            |row| Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Option<String>>(1)?)),
        )
        .optional()?;
    let (stored_fingerprint, response_json) =
        existing.ok_or(StoreError::OperationLostAfterConflict)?;
    let stored_fingerprint = hash_blob(&stored_fingerprint, "operation fingerprint")?;
    if stored_fingerprint != fingerprint {
        return Ok(OperationRecord::Conflict { stored_fingerprint });
    }
    let response = response_json
        .map(|json| serde_json::from_str(&json))
        .transpose()?;
    Ok(OperationRecord::Replay { response })
}

fn migrate(connection: &Connection) -> Result<(), StoreError> {
    let version: u32 = connection.pragma_query_value(None, "user_version", |row| row.get(0))?;
    if version > SCHEMA_VERSION {
        return Err(StoreError::NewerSchema {
            actual: version,
            supported: SCHEMA_VERSION,
        });
    }
    if version != 0 && version != SCHEMA_VERSION {
        return Err(StoreError::IncompatibleSchema(version));
    }
    if version == 0 {
        let transaction = connection.unchecked_transaction()?;
        transaction.execute_batch(STORE_SCHEMA)?;
        transaction.pragma_update(None, "user_version", SCHEMA_VERSION)?;
        transaction.commit()?;
    }
    Ok(())
}

fn validate_execution(execution: &StoredExecution) -> Result<(), StoreError> {
    if execution.updated_at_ms < execution.created_at_ms {
        return Err(StoreError::InvalidTimestamps {
            created_at_ms: execution.created_at_ms,
            updated_at_ms: execution.updated_at_ms,
        });
    }
    let reducer = Execution::restore(execution.snapshot.clone())?;
    for step in reducer.steps() {
        use cue_core::vnext::{BuiltinCommand, EnvEdit, Sensitivity, StepAction};
        let sensitive_patch = |patch: &cue_core::vnext::EnvPatch| {
            patch.iter().any(|(_, edit)|
            matches!(edit, EnvEdit::Set(value) if value.sensitivity() == Sensitivity::Sensitive))
        };
        let sensitive = match reducer.action(step.id()) {
            Some(StepAction::Builtin(BuiltinCommand::Env(mutation))) => {
                sensitive_patch(mutation.patch())
            }
            Some(StepAction::Run { pipeline, .. }) => pipeline
                .processes()
                .any(|process| sensitive_patch(process.env())),
            _ => false,
        };
        if sensitive {
            return Err(StoreError::SensitiveEnvironmentUnsupported);
        }
    }
    let actual = reducer.state();
    if actual != execution.state {
        return Err(StoreError::ExecutionStateMismatch {
            declared: execution.state.clone(),
            actual,
        });
    }
    Ok(())
}

fn validate_fact_projection(
    previous: Option<&StoredExecution>,
    execution: &StoredExecution,
    facts: &[FactDraft],
) -> Result<(), StoreError> {
    let mismatch = |message: String| StoreError::FactProjectionMismatch { message };
    let baseline = previous
        .map(|stored| stored.snapshot.clone())
        .unwrap_or_else(|| {
            Execution::new(execution.snapshot.id, execution.snapshot.spec.clone()).snapshot()
        });
    let mut steps = baseline
        .steps
        .iter()
        .map(|step| {
            (
                step.id(),
                step.state().clone(),
                step.input_scope(),
                step.output_scope(),
            )
        })
        .collect::<Vec<_>>();
    let mut state = previous
        .map(|stored| stored.state.clone())
        .unwrap_or(ExecutionState::Pending);
    let mut created = false;
    let mut finished = false;

    for (offset, draft) in facts.iter().enumerate() {
        if finished {
            return Err(mismatch("facts follow execution_finished".into()));
        }
        match &draft.fact {
            Fact::ExecutionCreated { scope, .. } => {
                if previous.is_some() || created || offset != 0 {
                    return Err(mismatch(
                        "execution_created must be the first fact of a new execution".into(),
                    ));
                }
                if *scope != execution.snapshot.spec.scope() {
                    return Err(mismatch(format!(
                        "created scope {scope} differs from execution scope {}",
                        execution.snapshot.spec.scope()
                    )));
                }
                created = true;
            }
            Fact::StepStateChanged {
                id,
                previous,
                next,
                input_scope,
                output_scope,
            } => {
                let step = steps
                    .iter_mut()
                    .find(|step| step.0 == *id)
                    .ok_or_else(|| mismatch(format!("snapshot has no step {id}")))?;
                if &step.1 != previous {
                    return Err(mismatch(format!(
                        "step {id} fact starts at {previous:?}; projection is {:?}",
                        step.1
                    )));
                }
                if !valid_step_transition(previous, next) {
                    return Err(mismatch(format!(
                        "step {id} has invalid transition {previous:?} -> {next:?}"
                    )));
                }
                step.1 = next.clone();
                step.2 = *input_scope;
                step.3 = *output_scope;
            }
            Fact::ExecutionStateChanged { previous, next, .. } => {
                if &state != previous {
                    return Err(mismatch(format!(
                        "execution fact starts at {previous:?}; projection is {state:?}"
                    )));
                }
                if !valid_execution_transition(previous, next) {
                    return Err(mismatch(format!(
                        "execution has invalid transition {previous:?} -> {next:?}"
                    )));
                }
                state = next.clone();
            }
            Fact::OutputAppended {
                step,
                start_offset,
                end_offset,
                ..
            } => {
                if !steps.iter().any(|record| record.0 == *step) {
                    return Err(mismatch(format!("snapshot has no output step {step}")));
                }
                if end_offset < start_offset {
                    return Err(mismatch(format!(
                        "output fact for {step} reverses offsets {start_offset}..{end_offset}"
                    )));
                }
            }
            Fact::ExecutionFinished {
                state: final_state, ..
            } => {
                if final_state != &state || !state.is_terminal() {
                    return Err(mismatch(format!(
                        "finished fact declares {final_state:?}; projection is {state:?}"
                    )));
                }
                finished = true;
            }
        }
    }

    if previous.is_none() && !created {
        return Err(mismatch(
            "new execution is missing execution_created".into(),
        ));
    }
    for expected in &execution.snapshot.steps {
        let actual = steps
            .iter()
            .find(|step| step.0 == expected.id())
            .ok_or_else(|| mismatch(format!("fact projection has no step {}", expected.id())))?;
        if &actual.1 != expected.state()
            || actual.2 != expected.input_scope()
            || actual.3 != expected.output_scope()
        {
            return Err(mismatch(format!(
                "facts do not project to committed record for {}",
                expected.id()
            )));
        }
    }
    if state != execution.state {
        return Err(mismatch(format!(
            "facts project to {state:?}; committed execution is {:?}",
            execution.state
        )));
    }
    let became_terminal =
        previous.is_none_or(|stored| !stored.state.is_terminal()) && execution.state.is_terminal();
    if became_terminal != finished {
        return Err(mismatch(
            "execution_finished must occur exactly when the execution becomes terminal".into(),
        ));
    }
    Ok(())
}

fn valid_step_transition(previous: &StepState, next: &StepState) -> bool {
    if let (
        StepState::Cancelling {
            cause: previous_cause,
            mode: cue_core::vnext::CancelMode::Graceful,
        },
        StepState::Cancelling {
            cause: next_cause,
            mode: cue_core::vnext::CancelMode::Force,
        },
    ) = (previous, next)
    {
        return previous_cause == next_cause;
    }
    if let (
        StepState::Cancelling {
            cause: previous_cause,
            ..
        },
        StepState::Cancelled { cause: next_cause },
    ) = (previous, next)
    {
        return previous_cause == next_cause;
    }
    matches!(
        (previous, next),
        (StepState::Pending, StepState::Running)
            | (StepState::Pending, StepState::Skipped { .. })
            | (StepState::Pending, StepState::Cancelled { .. })
            | (StepState::Running, StepState::Succeeded)
            | (StepState::Running, StepState::Failed { .. })
            | (StepState::Running, StepState::Cancelling { .. })
            | (StepState::Cancelling { .. }, StepState::Succeeded)
            | (StepState::Cancelling { .. }, StepState::Failed { .. })
            | (StepState::Cancelling { .. }, StepState::Cancelled { .. })
    )
}

fn valid_execution_transition(previous: &ExecutionState, next: &ExecutionState) -> bool {
    matches!(
        (previous, next),
        (ExecutionState::Pending, ExecutionState::Running)
            | (ExecutionState::Pending, ExecutionState::Cancelled)
            | (ExecutionState::Running, ExecutionState::Cancelling)
            | (ExecutionState::Cancelling, ExecutionState::Succeeded)
            | (ExecutionState::Cancelling, ExecutionState::Failed)
            | (ExecutionState::Cancelling, ExecutionState::Cancelled)
            | (ExecutionState::Running, ExecutionState::Succeeded)
            | (ExecutionState::Running, ExecutionState::Failed)
            | (ExecutionState::Running, ExecutionState::Cancelled)
    )
}

fn decode_execution(row: (String, String, i64, i64)) -> Result<StoredExecution, StoreError> {
    let execution = StoredExecution {
        snapshot: serde_json::from_str(&row.0)?,
        state: serde_json::from_str(&row.1)?,
        created_at_ms: row.2,
        updated_at_ms: row.3,
    };
    validate_execution(&execution)?;
    Ok(execution)
}

fn contains_sensitive_environment(scope: &Scope) -> bool {
    scope
        .env()
        .values()
        .any(|value| value.sensitivity() == cue_core::vnext::Sensitivity::Sensitive)
}

fn read_u64(row: &rusqlite::Row<'_>, index: usize) -> rusqlite::Result<u64> {
    let value: i64 = row.get(index)?;
    u64::try_from(value).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            index,
            rusqlite::types::Type::Integer,
            Box::new(error),
        )
    })
}

fn hash_text(domain: &[u8], value: &str) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(domain);
    hasher.update(&(value.len() as u64).to_le_bytes());
    hasher.update(value.as_bytes());
    *hasher.finalize().as_bytes()
}

fn sqlite_id(id: ExecutionId) -> Result<i64, StoreError> {
    sqlite_u64(id.0, "execution")
}

fn sqlite_u64(value: u64, kind: &'static str) -> Result<i64, StoreError> {
    i64::try_from(value).map_err(|_| StoreError::IntegerOverflow { kind, value })
}

fn hash_blob(blob: &[u8], kind: &'static str) -> Result<[u8; 32], StoreError> {
    blob.try_into().map_err(|_| StoreError::InvalidHashLength {
        kind,
        actual: blob.len(),
    })
}

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("Sensitive environments are not supported by this store")]
    SensitiveEnvironmentUnsupported,
    #[error("incompatible pre-FP IPC v4 schema {0}; retain the database and use a fresh store")]
    IncompatibleSchema(u32),
    #[error("missing execution {0}")]
    MissingExecution(ExecutionId),
    #[error("active step {0} has no durable runtime follow-up")]
    MissingRuntimeWork(StepId),
    #[error("cannot prove the old run attempt for {0} quiescent or uniquely controlled")]
    UncertainRunOwnership(StepId),
    #[error(transparent)]
    Database(#[from] rusqlite::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    InvalidSnapshot(#[from] cue_core::vnext::ExecutionError),
    #[error(transparent)]
    InvalidProtocolId(#[from] cue_protocol::IdError),
    #[error("database schema {actual} is newer than supported schema {supported}")]
    NewerSchema { actual: u32, supported: u32 },
    #[error("scope row hash mismatch: expected {expected}, computed {actual}")]
    ScopeHashMismatch {
        expected: ScopeHash,
        actual: ScopeHash,
    },
    #[error("execution projection declares {declared:?}, reducer computes {actual:?}")]
    ExecutionStateMismatch {
        declared: ExecutionState,
        actual: ExecutionState,
    },
    #[error("execution timestamps are reversed: created {created_at_ms}, updated {updated_at_ms}")]
    InvalidTimestamps {
        created_at_ms: i64,
        updated_at_ms: i64,
    },
    #[error("execution {id} creation time changed from {existing} to {attempted}")]
    CreatedAtMismatch {
        id: ExecutionId,
        existing: i64,
        attempted: i64,
    },
    #[error("execution {0} specification is immutable")]
    ImmutableExecutionSpec(ExecutionId),
    #[error("execution {id} update time regressed from {previous} to {attempted}")]
    UpdatedAtRegression {
        id: ExecutionId,
        previous: i64,
        attempted: i64,
    },
    #[error("execution {execution} references unavailable scope {scope}")]
    MissingScopeReference {
        execution: ExecutionId,
        scope: ScopeHash,
    },
    #[error("fact belongs to {actual:?}; execution projection is {expected}")]
    FactExecutionMismatch {
        expected: ExecutionId,
        actual: ExecutionId,
    },
    #[error("fact does not match execution projection: {message}")]
    FactProjectionMismatch { message: String },
    #[error("{kind} value {value} exceeds SQLite INTEGER range")]
    IntegerOverflow { kind: &'static str, value: u64 },
    #[error("database returned negative {kind} value {value}")]
    NegativeInteger { kind: &'static str, value: i64 },
    #[error("{kind} blob must contain 32 bytes, got {actual}")]
    InvalidHashLength { kind: &'static str, actual: usize },
    #[error("operation uniqueness conflict was reported but the durable row disappeared")]
    OperationLostAfterConflict,
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use cue_core::StepId;
    use cue_core::vnext::{
        AbsolutePath, Argv, EnvKey, EnvValue, ExecutionPlan, ExecutionSpec, FileModeMask, IoMode,
        Pipeline, Process, StepFailure, StepState,
    };
    use cue_protocol::{ProtocolErrorCode, Query};

    use super::*;

    const EXECUTION_ID: ExecutionId = ExecutionId(7);

    fn key(value: &str) -> EnvKey {
        EnvKey::new(value).unwrap()
    }

    fn value(value: &str) -> EnvValue {
        EnvValue::new(value).unwrap()
    }

    fn scope(entries: &[(&str, &str)]) -> Scope {
        Scope::new(
            AbsolutePath::new("/workspace").unwrap(),
            entries
                .iter()
                .map(|(name, value)| (key(name), self::value(value)))
                .collect::<BTreeMap<_, _>>(),
            FileModeMask::new(0o022).unwrap(),
        )
    }

    fn initial_scope() -> Scope {
        scope(&[("PATH", "/bin")])
    }

    fn reducer_for_scope(initial: &Scope) -> Execution {
        let process = Process::new(Argv::new("true", Vec::new()).unwrap());
        let spec = ExecutionSpec::new(
            initial.compute_hash(),
            ExecutionPlan::run(Pipeline::simple(process), IoMode::Captured),
        )
        .unwrap();
        Execution::new(EXECUTION_ID, spec)
    }

    fn reducer() -> Execution {
        reducer_for_scope(&initial_scope())
    }

    fn persist_initial_scope(store: &Store) {
        assert_eq!(
            store.put_scope(&initial_scope(), 1).unwrap(),
            ScopePersistence::Persisted
        );
    }

    fn stored(execution: &Execution, created_at_ms: i64, updated_at_ms: i64) -> StoredExecution {
        StoredExecution {
            snapshot: execution.snapshot(),
            state: execution.state(),
            created_at_ms,
            updated_at_ms,
        }
    }

    #[test]
    fn fresh_schema_is_versioned_and_rejects_newer_database() {
        let store = Store::in_memory().unwrap();
        let version: u32 = store
            .connection()
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, SCHEMA_VERSION);
        for table in ["scopes", "executions", "facts", "operations"] {
            let exists: bool = store
                .connection()
                .query_row(
                    "SELECT EXISTS(
                         SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1
                     )",
                    [table],
                    |row| row.get(0),
                )
                .unwrap();
            assert!(exists, "missing {table}");
        }

        let connection = Connection::open_in_memory().unwrap();
        connection
            .pragma_update(None, "user_version", SCHEMA_VERSION + 1)
            .unwrap();
        assert!(matches!(
            Store::from_connection(connection),
            Err(StoreError::NewerSchema { .. })
        ));
    }

    #[test]
    fn safe_scopes_roundtrip_and_sensitive_scopes_never_reach_sqlite() {
        let store = Store::in_memory().unwrap();
        let safe = scope(&[("PATH", "/bin"), ("MODE", "release")]);
        assert_eq!(
            store.put_scope(&safe, 10).unwrap(),
            ScopePersistence::Persisted
        );
        assert_eq!(store.get_scope(safe.compute_hash()).unwrap(), Some(safe));

        let mut entries = initial_scope().env().clone();
        entries.insert(
            key("ORDINARY_NAME"),
            EnvValue::classified("do-not-persist", cue_core::vnext::Sensitivity::Sensitive)
                .unwrap(),
        );
        let sensitive = Scope::new(
            initial_scope().cwd().clone(),
            entries,
            initial_scope().umask(),
        );
        assert!(matches!(
            store.put_scope(&sensitive, 11),
            Err(StoreError::SensitiveEnvironmentUnsupported)
        ));
        assert_eq!(store.get_scope(sensitive.compute_hash()).unwrap(), None);
        let stored_json: String = store
            .connection()
            .query_row(
                "SELECT group_concat(snapshot_json) FROM scopes",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(!stored_json.contains("do-not-persist"));

        let execution = reducer_for_scope(&sensitive);
        let projection = stored(&execution, 11, 11);
        let created = FactDraft {
            occurred_at_ms: 11,
            fact: Fact::ExecutionCreated {
                id: EXECUTION_ID,
                scope: sensitive.compute_hash(),
            },
        };
        assert!(matches!(
            store.commit_execution(&projection, &[created]),
            Err(StoreError::MissingScopeReference { .. })
        ));
    }

    #[test]
    fn execution_projection_and_fact_cursor_commit_atomically() {
        let store = Store::in_memory().unwrap();
        persist_initial_scope(&store);
        let mut execution = reducer();
        let initial = stored(&execution, 10, 10);
        let created = FactDraft {
            occurred_at_ms: 10,
            fact: Fact::ExecutionCreated {
                id: EXECUTION_ID,
                scope: initial.snapshot.spec.scope(),
            },
        };
        let committed = store.commit_execution(&initial, &[created]).unwrap();
        assert_eq!(committed[0].id.get(), 1);
        assert_eq!(store.get_execution(EXECUTION_ID).unwrap(), Some(initial));

        execution.advance().unwrap();
        let step_id = StepId {
            execution: EXECUTION_ID,
            index: 1,
        };
        let running = stored(&execution, 10, 11);
        let running_record = execution.step(step_id).unwrap();
        let running_facts = [
            FactDraft {
                occurred_at_ms: 11,
                fact: Fact::StepStateChanged {
                    id: step_id,
                    previous: StepState::Pending,
                    next: StepState::Running,
                    input_scope: running_record.input_scope(),
                    output_scope: running_record.output_scope(),
                },
            },
            FactDraft {
                occurred_at_ms: 11,
                fact: Fact::ExecutionStateChanged {
                    id: EXECUTION_ID,
                    previous: ExecutionState::Pending,
                    next: ExecutionState::Running,
                },
            },
        ];
        let committed_running = store.commit_execution(&running, &running_facts).unwrap();
        assert_eq!(committed_running[0].id.get(), 2);
        assert_eq!(committed_running[1].id.get(), 3);

        execution
            .complete_run(step_id, cue_core::vnext::RunCompletion::Succeeded)
            .unwrap();
        let completed = stored(&execution, 10, 20);
        let record = execution.step(step_id).unwrap();
        let facts = [
            FactDraft {
                occurred_at_ms: 20,
                fact: Fact::StepStateChanged {
                    id: step_id,
                    previous: StepState::Running,
                    next: record.state().clone(),
                    input_scope: record.input_scope(),
                    output_scope: record.output_scope(),
                },
            },
            FactDraft {
                occurred_at_ms: 20,
                fact: Fact::ExecutionStateChanged {
                    id: EXECUTION_ID,
                    previous: ExecutionState::Running,
                    next: ExecutionState::Succeeded,
                },
            },
            FactDraft {
                occurred_at_ms: 20,
                fact: Fact::ExecutionFinished {
                    id: EXECUTION_ID,
                    state: ExecutionState::Succeeded,
                },
            },
        ];
        let committed = store.commit_execution(&completed, &facts).unwrap();
        assert_eq!(committed[0].id.get(), 4);
        assert_eq!(committed[1].id.get(), 5);
        assert_eq!(committed[2].id.get(), 6);

        let replay = store
            .facts_after(EXECUTION_ID, Some(EventId::new(3).unwrap()), 10)
            .unwrap();
        assert_eq!(replay, committed);
        assert_eq!(store.get_execution(EXECUTION_ID).unwrap(), Some(completed));
    }

    #[test]
    fn invalid_projection_or_fact_writes_nothing() {
        let store = Store::in_memory().unwrap();
        persist_initial_scope(&store);
        let execution = reducer();
        let mut projection = stored(&execution, 20, 10);
        assert!(matches!(
            store.commit_execution(&projection, &[]),
            Err(StoreError::InvalidTimestamps { .. })
        ));

        projection.created_at_ms = 10;
        projection.updated_at_ms = 10;
        projection.state = ExecutionState::Succeeded;
        assert!(matches!(
            store.commit_execution(&projection, &[]),
            Err(StoreError::ExecutionStateMismatch { .. })
        ));

        projection.state = ExecutionState::Pending;
        let mismatched = FactDraft {
            occurred_at_ms: 10,
            fact: Fact::ExecutionCreated {
                id: ExecutionId(8),
                scope: projection.snapshot.spec.scope(),
            },
        };
        assert!(matches!(
            store.commit_execution(&projection, &[mismatched]),
            Err(StoreError::FactExecutionMismatch { .. })
        ));
        assert_eq!(store.get_execution(EXECUTION_ID).unwrap(), None);
    }

    #[test]
    fn existing_execution_metadata_and_fact_history_are_immutable() {
        let store = Store::in_memory().unwrap();
        persist_initial_scope(&store);
        let mut execution = reducer();
        let initial = stored(&execution, 10, 10);
        store
            .commit_execution(
                &initial,
                &[FactDraft {
                    occurred_at_ms: 10,
                    fact: Fact::ExecutionCreated {
                        id: EXECUTION_ID,
                        scope: initial.snapshot.spec.scope(),
                    },
                }],
            )
            .unwrap();

        let changed_creation_time = stored(&execution, 11, 11);
        assert!(matches!(
            store.commit_execution(&changed_creation_time, &[]),
            Err(StoreError::CreatedAtMismatch { .. })
        ));

        execution.advance().unwrap();
        let step_id = StepId {
            execution: EXECUTION_ID,
            index: 1,
        };
        let running = stored(&execution, 10, 11);
        let record = execution.step(step_id).unwrap();
        let dishonest = FactDraft {
            occurred_at_ms: 11,
            fact: Fact::StepStateChanged {
                id: step_id,
                previous: StepState::Running,
                next: StepState::Running,
                input_scope: record.input_scope(),
                output_scope: record.output_scope(),
            },
        };
        assert!(matches!(
            store.commit_execution(&running, &[dishonest]),
            Err(StoreError::FactProjectionMismatch { .. })
        ));
        assert_eq!(store.get_execution(EXECUTION_ID).unwrap(), Some(initial));
    }

    #[test]
    fn operation_replay_conflict_and_tombstone_are_permanent() {
        let store = Store::in_memory().unwrap();
        let client = ClientId::new("client-1").unwrap();
        let operation = OperationId::new("tool-call:1").unwrap();
        let command = Command::Shutdown;
        let response = ResponsePayload::error(ProtocolErrorCode::Draining, "stopping");

        assert_eq!(
            store
                .record_operation(&client, &operation, &command, Some(&response), 10)
                .unwrap(),
            OperationRecord::Inserted
        );
        assert_eq!(
            store
                .record_operation(&client, &operation, &command, None, 11)
                .unwrap(),
            OperationRecord::Replay {
                response: Some(response.clone())
            }
        );
        assert!(matches!(
            store
                .record_operation(&client, &operation, &Command::Restart, None, 12)
                .unwrap(),
            OperationRecord::Conflict { .. }
        ));

        assert_eq!(store.tombstone_operation_responses_before(11).unwrap(), 1);
        assert_eq!(
            store
                .record_operation(&client, &operation, &command, Some(&response), 13)
                .unwrap(),
            OperationRecord::Replay { response: None }
        );
    }

    #[test]
    fn execution_submission_and_operation_claim_share_one_transaction() {
        let store = Store::in_memory().unwrap();
        persist_initial_scope(&store);
        let execution = reducer();
        let projection = stored(&execution, 10, 10);
        let command = Command::SubmitExecution {
            spec: Box::new(projection.snapshot.spec.clone()),
        };
        let client = ClientId::new("client-submit").unwrap();
        let operation = OperationId::new("submit:7").unwrap();
        let response = ResponsePayload::ack();
        let fact = FactDraft {
            occurred_at_ms: 10,
            fact: Fact::ExecutionCreated {
                id: EXECUTION_ID,
                scope: projection.snapshot.spec.scope(),
            },
        };

        let committed = store
            .commit_execution_command(
                OperationCommit {
                    client: &client,
                    operation: &operation,
                    command: &command,
                    response: Some(&response),
                    completed_at_ms: 10,
                },
                &projection,
                &[fact],
            )
            .unwrap();
        assert!(matches!(
            committed,
            ExecutionCommandCommit::Committed { facts } if facts.len() == 1
        ));
        assert!(store.get_execution(EXECUTION_ID).unwrap().is_some());

        let mut invalid_retry = projection.clone();
        invalid_retry.updated_at_ms = 0;
        assert_eq!(
            store
                .commit_execution_command(
                    OperationCommit {
                        client: &client,
                        operation: &operation,
                        command: &command,
                        response: None,
                        completed_at_ms: 11,
                    },
                    &invalid_retry,
                    &[],
                )
                .unwrap(),
            ExecutionCommandCommit::Replay {
                response: Some(response)
            }
        );

        let failed_operation = OperationId::new("submit:invalid").unwrap();
        assert!(matches!(
            store.commit_execution_command(
                OperationCommit {
                    client: &client,
                    operation: &failed_operation,
                    command: &command,
                    response: None,
                    completed_at_ms: 12,
                },
                &invalid_retry,
                &[],
            ),
            Err(StoreError::InvalidTimestamps { .. })
        ));
        let operation_count: i64 = store
            .connection()
            .query_row("SELECT COUNT(*) FROM operations", [], |row| row.get(0))
            .unwrap();
        assert_eq!(operation_count, 1, "failed effect must roll back its claim");
    }

    #[test]
    fn command_fingerprint_is_stable_and_covers_typed_payload() {
        let first = Command::UnwatchExecution { id: ExecutionId(1) };
        let same = Command::UnwatchExecution { id: ExecutionId(1) };
        let different = Command::UnwatchExecution { id: ExecutionId(2) };
        assert_eq!(
            command_fingerprint(&first).unwrap(),
            command_fingerprint(&same).unwrap()
        );
        assert_ne!(
            command_fingerprint(&first).unwrap(),
            command_fingerprint(&different).unwrap()
        );
        let query = Query::GetExecution { id: ExecutionId(1) };
        assert!(
            serde_json::to_string(&query)
                .unwrap()
                .contains("get_execution")
        );
    }

    #[test]
    fn failed_execution_roundtrips_structured_failure_without_debug_secrets() {
        let store = Store::in_memory().unwrap();
        persist_initial_scope(&store);
        let mut execution = reducer();
        execution.advance().unwrap();
        let step_id = StepId {
            execution: EXECUTION_ID,
            index: 1,
        };
        execution
            .complete_run(
                step_id,
                cue_core::vnext::RunCompletion::Failed(StepFailure::Spawn {
                    message: "program missing".into(),
                }),
            )
            .unwrap();
        let projection = stored(&execution, 10, 11);
        let record = execution.step(step_id).unwrap();
        let facts = [
            FactDraft {
                occurred_at_ms: 10,
                fact: Fact::ExecutionCreated {
                    id: EXECUTION_ID,
                    scope: projection.snapshot.spec.scope(),
                },
            },
            FactDraft {
                occurred_at_ms: 10,
                fact: Fact::StepStateChanged {
                    id: step_id,
                    previous: StepState::Pending,
                    next: StepState::Running,
                    input_scope: record.input_scope(),
                    output_scope: None,
                },
            },
            FactDraft {
                occurred_at_ms: 10,
                fact: Fact::ExecutionStateChanged {
                    id: EXECUTION_ID,
                    previous: ExecutionState::Pending,
                    next: ExecutionState::Running,
                },
            },
            FactDraft {
                occurred_at_ms: 11,
                fact: Fact::StepStateChanged {
                    id: step_id,
                    previous: StepState::Running,
                    next: record.state().clone(),
                    input_scope: record.input_scope(),
                    output_scope: record.output_scope(),
                },
            },
            FactDraft {
                occurred_at_ms: 11,
                fact: Fact::ExecutionStateChanged {
                    id: EXECUTION_ID,
                    previous: ExecutionState::Running,
                    next: ExecutionState::Failed,
                },
            },
            FactDraft {
                occurred_at_ms: 11,
                fact: Fact::ExecutionFinished {
                    id: EXECUTION_ID,
                    state: ExecutionState::Failed,
                },
            },
        ];
        store.commit_execution(&projection, &facts).unwrap();
        assert_eq!(store.get_execution(EXECUTION_ID).unwrap(), Some(projection));
    }
    fn commit_reducer(
        store: &Store,
        execution: &Execution,
        time: i64,
    ) -> Result<Vec<FactEvent>, StoreError> {
        let previous = store.get_execution(execution.id())?;
        let baseline = previous
            .as_ref()
            .map(|stored| stored.snapshot.clone())
            .unwrap_or_else(|| Execution::new(execution.id(), execution.spec().clone()).snapshot());
        let mut facts = Vec::new();
        if previous.is_none() {
            facts.push(FactDraft {
                occurred_at_ms: time,
                fact: Fact::ExecutionCreated {
                    id: execution.id(),
                    scope: execution.spec().scope(),
                },
            });
        }
        for (before, after) in baseline.steps.iter().zip(execution.steps()) {
            if before != after {
                facts.push(FactDraft {
                    occurred_at_ms: time,
                    fact: Fact::StepStateChanged {
                        id: after.id(),
                        previous: before.state().clone(),
                        next: after.state().clone(),
                        input_scope: after.input_scope(),
                        output_scope: after.output_scope(),
                    },
                });
            }
        }
        let before_state = previous
            .as_ref()
            .map(|stored| stored.state.clone())
            .unwrap_or(ExecutionState::Pending);
        if before_state != execution.state() {
            facts.push(FactDraft {
                occurred_at_ms: time,
                fact: Fact::ExecutionStateChanged {
                    id: execution.id(),
                    previous: before_state.clone(),
                    next: execution.state(),
                },
            });
        }
        if !before_state.is_terminal() && execution.state().is_terminal() {
            facts.push(FactDraft {
                occurred_at_ms: time,
                fact: Fact::ExecutionFinished {
                    id: execution.id(),
                    state: execution.state(),
                },
            });
        }
        let created = previous
            .as_ref()
            .map(|stored| stored.created_at_ms)
            .unwrap_or(time);
        store.commit_execution(&stored(execution, created, time), &facts)
    }

    #[test]
    fn failed_fact_insert_rolls_back_snapshot_and_runtime_follow_up() {
        let store = Store::in_memory().unwrap();
        persist_initial_scope(&store);
        let mut execution = reducer();
        commit_reducer(&store, &execution, 1).unwrap();
        let before = store.get_execution(EXECUTION_ID).unwrap();
        store.connection().execute_batch("CREATE TRIGGER fail_fact BEFORE INSERT ON facts BEGIN SELECT RAISE(ABORT, 'injected fact failure'); END").unwrap();
        execution.advance().unwrap();
        assert!(commit_reducer(&store, &execution, 2).is_err());
        assert_eq!(store.get_execution(EXECUTION_ID).unwrap(), before);
        assert!(store.pending_runtime_steps().unwrap().is_empty());
        assert_eq!(store.facts_after(EXECUTION_ID, None, 100).unwrap().len(), 1);
    }

    #[test]
    fn scope_must_be_readable_before_snapshot_and_failed_commit_leaves_only_unreferenced_scope() {
        use cue_core::vnext::{BuiltinCommand, BuiltinSuccess, EnvEdit, EnvPatch};
        let store = Store::in_memory().unwrap();
        persist_initial_scope(&store);
        let command = BuiltinCommand::env(EnvPatch::new(BTreeMap::from([(
            key("MODE"),
            EnvEdit::set("changed").unwrap(),
        )])))
        .unwrap();
        let mut execution = Execution::new(
            EXECUTION_ID,
            ExecutionSpec::new(
                initial_scope().compute_hash(),
                ExecutionPlan::builtin(command),
            )
            .unwrap(),
        );
        execution.advance().unwrap();
        commit_reducer(&store, &execution, 1).unwrap();
        let before = store.get_execution(EXECUTION_ID).unwrap();
        let id = execution.steps()[0].id();
        let transition = execution
            .complete_builtin(id, &initial_scope(), Ok(BuiltinSuccess::Env))
            .unwrap();
        let scope = &transition.new_scopes[0];
        assert!(matches!(
            commit_reducer(&store, &execution, 2),
            Err(StoreError::MissingScopeReference { .. })
        ));
        store.connection().execute_batch("CREATE TRIGGER fail_scope BEFORE INSERT ON scopes BEGIN SELECT RAISE(ABORT, 'injected scope failure'); END").unwrap();
        assert!(store.put_scope(scope, 2).is_err());
        assert_eq!(store.get_execution(EXECUTION_ID).unwrap(), before);
        store
            .connection()
            .execute_batch("DROP TRIGGER fail_scope")
            .unwrap();
        store.put_scope(scope, 2).unwrap();
        store.connection().execute_batch("CREATE TRIGGER fail_execution BEFORE UPDATE ON executions BEGIN SELECT RAISE(ABORT, 'injected execution failure'); END").unwrap();
        assert!(commit_reducer(&store, &execution, 2).is_err());
        assert_eq!(store.get_execution(EXECUTION_ID).unwrap(), before);
        assert_eq!(
            store.get_scope(scope.compute_hash()).unwrap().as_ref(),
            Some(scope)
        );
        assert_eq!(store.pending_runtime_steps().unwrap(), vec![id]);
        store
            .connection()
            .execute_batch("DROP TRIGGER fail_execution")
            .unwrap();
        commit_reducer(&store, &execution, 2).unwrap();
        assert_eq!(
            store.get_execution(EXECUTION_ID).unwrap().unwrap().state,
            ExecutionState::Succeeded
        );
    }

    #[test]
    fn old_claim_cannot_clear_new_cancel_generation_or_start_after_cancellation() {
        use cue_core::vnext::CancelMode;
        let store = Store::in_memory().unwrap();
        persist_initial_scope(&store);
        let mut execution = reducer();
        execution.advance().unwrap();
        commit_reducer(&store, &execution, 1).unwrap();
        let id = execution.steps()[0].id();
        let old = store.claim_runtime_step(id).unwrap().unwrap();
        assert!(store.claim_runtime_step(id).unwrap().is_none());
        execution.cancel(CancelMode::Graceful);
        commit_reducer(&store, &execution, 2).unwrap();
        execution.cancel(CancelMode::Force);
        commit_reducer(&store, &execution, 3).unwrap();
        assert!(!store.begin_run_attempt(id, old).unwrap());
        assert!(store.acknowledge_runtime_step(id, old).unwrap());
        let latest = store.claim_runtime_step(id).unwrap().unwrap();
        assert!(latest > old);
        assert!(!store.acknowledge_runtime_step(id, old).unwrap());
        assert!(matches!(
            store
                .get_execution(EXECUTION_ID)
                .unwrap()
                .unwrap()
                .snapshot
                .steps[0]
                .state(),
            StepState::Cancelling {
                mode: CancelMode::Force,
                ..
            }
        ));
        assert!(!store.begin_run_attempt(id, latest).unwrap());
        execution.complete_cancelled(id).unwrap();
        commit_reducer(&store, &execution, 4).unwrap();
        assert!(store.acknowledge_runtime_step(id, latest).unwrap());
        assert!(store.pending_runtime_steps().unwrap().is_empty());
    }

    #[test]
    fn recovery_requeues_unstarted_work_but_never_replays_an_uncertain_run() {
        let connection = Connection::open("file:cue-fp-recovery?mode=memory&cache=shared").unwrap();
        let store = Store::from_connection(connection).unwrap();
        persist_initial_scope(&store);
        let mut execution = reducer();
        execution.advance().unwrap();
        commit_reducer(&store, &execution, 1).unwrap();
        let id = execution.steps()[0].id();
        let abandoned = store.claim_runtime_step(id).unwrap().unwrap();
        let reopened = Store::from_connection(
            Connection::open("file:cue-fp-recovery?mode=memory&cache=shared").unwrap(),
        )
        .unwrap();
        assert!(reopened.claim_runtime_step(id).unwrap().is_none());
        reopened.recover_runtime_work().unwrap();
        let current = reopened.claim_runtime_step(id).unwrap().unwrap();
        assert!(current > abandoned);
        assert!(!store.acknowledge_runtime_step(id, abandoned).unwrap());
        assert!(reopened.begin_run_attempt(id, current).unwrap());
        assert!(!reopened.begin_run_attempt(id, current).unwrap());
        assert!(
            matches!(reopened.recover_runtime_work(), Err(StoreError::UncertainRunOwnership(step)) if step == id)
        );
        assert_eq!(
            reopened.get_execution(EXECUTION_ID).unwrap().unwrap().state,
            ExecutionState::Running
        );
        assert!(
            reopened
                .facts_after(EXECUTION_ID, None, 100)
                .unwrap()
                .iter()
                .all(|event| !matches!(event.fact, Fact::ExecutionFinished { .. }))
        );
    }

    #[test]
    fn sensitivity_is_classification_not_a_variable_name_heuristic() {
        use cue_core::vnext::{EnvEdit, EnvPatch, Sensitivity};
        let store = Store::in_memory().unwrap();
        let normal = scope(&[("API_TOKEN", "explicitly-normal")]);
        assert_eq!(
            store.put_scope(&normal, 1).unwrap(),
            ScopePersistence::Persisted
        );
        persist_initial_scope(&store);
        let secret = EnvValue::classified("classified-value", Sensitivity::Sensitive).unwrap();
        let process = Process::with_env(
            Argv::new("true", Vec::new()).unwrap(),
            EnvPatch::new(BTreeMap::from([(key("INNOCENT"), EnvEdit::Set(secret))])),
        );
        let execution = Execution::new(
            EXECUTION_ID,
            ExecutionSpec::new(
                initial_scope().compute_hash(),
                ExecutionPlan::run(Pipeline::simple(process), IoMode::Captured),
            )
            .unwrap(),
        );
        assert!(matches!(
            commit_reducer(&store, &execution, 1),
            Err(StoreError::SensitiveEnvironmentUnsupported)
        ));
        assert!(store.get_execution(EXECUTION_ID).unwrap().is_none());
    }
}
