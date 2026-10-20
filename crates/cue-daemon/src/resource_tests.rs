use super::tests::{hello, put_scope, scope, spec, submitted_id};
use super::*;
use cue_protocol::ExtensionRequest;
use cue_resources::{
    Backend, Needs, ProviderConfig, Quantity, ResourceConfig, Resources, Status, Unit,
};
use serde_json::json;
use std::time::Duration;

fn config() -> ResourceConfig {
    ResourceConfig {
        providers: vec![ProviderConfig {
            id: "slots".into(),
            backend: Backend::Static {
                capacity: Needs::from([("worker".into(), Quantity::Count(1))]),
            },
        }],
    }
}
fn extension(method: &str, data: serde_json::Value) -> ExtensionRequest {
    ExtensionRequest {
        namespace: "resources".into(),
        version: 1,
        method: method.into(),
        data,
    }
}
fn submit_request(scope: ScopeHash) -> Command {
    Command::Extension(extension(
        "submit",
        json!({"spec":spec(scope,"/usr/bin/printf", &["once"]),"needs":{"worker":"1"}}),
    ))
}
fn records(service: &DaemonService) -> Vec<cue_resources::Record> {
    let store = service.store.lock_store().unwrap();
    let mut query = store
        .connection()
        .prepare("SELECT record_json FROM resource_executions ORDER BY execution_id")
        .unwrap();
    query
        .query_map([], |r| r.get::<_, String>(0))
        .unwrap()
        .map(|r| serde_json::from_str(&r.unwrap()).unwrap())
        .collect()
}

#[tokio::test]
async fn extension_queries_are_read_only_and_reject_unknown_contracts() {
    let service =
        DaemonService::from_store_with_resources(Store::in_memory().unwrap(), config()).unwrap();
    let mut client = service.connection();
    hello(&mut client).await;
    let before = service
        .store
        .lock_store()
        .unwrap()
        .connection()
        .total_changes();
    for method in ["list", "providers"] {
        let result = client
            .handle_query(Query::Extension(extension(method, json!({}))))
            .await
            .unwrap();
        assert!(matches!(
            result,
            ResponsePayload::Ok(ResultPayload::Extension { .. })
        ));
    }
    for request in [
        extension("reserve", json!({})),
        extension("providers", json!({"extra":true})),
        ExtensionRequest {
            namespace: "missing".into(),
            ..extension("list", json!({}))
        },
        ExtensionRequest {
            version: 2,
            ..extension("list", json!({}))
        },
    ] {
        assert!(
            client
                .handle_query(Query::Extension(request))
                .await
                .is_err()
        );
    }
    assert_eq!(
        before,
        service
            .store
            .lock_store()
            .unwrap()
            .connection()
            .total_changes()
    );
    assert!(records(&service).is_empty());
}

#[tokio::test]
async fn resource_submission_is_atomic_and_replays_the_entire_extension_fingerprint() {
    let service =
        DaemonService::from_store_with_resources(Store::in_memory().unwrap(), config()).unwrap();
    let mut client = service.connection();
    hello(&mut client).await;
    let (hash, _) = put_scope(&mut client, scope(false)).await;
    let command = submit_request(hash);
    let operation = OperationId::new("resource-submit").unwrap();
    service.store.lock_store().unwrap().connection().execute_batch("CREATE TRIGGER reject_resource BEFORE INSERT ON resource_executions BEGIN SELECT RAISE(ABORT, 'fault'); END;").unwrap();
    assert!(
        client
            .handle_command(operation.clone(), command.clone())
            .await
            .is_err()
    );
    assert!(service.store.list(None, 10).unwrap().is_empty());
    assert!(
        service
            .store
            .lock_store()
            .unwrap()
            .get_operation(&ClientId::new("test-client").unwrap(), &operation)
            .unwrap()
            .is_none()
    );
    service
        .store
        .lock_store()
        .unwrap()
        .connection()
        .execute_batch("DROP TRIGGER reject_resource;")
        .unwrap();
    let first = client
        .handle_command(operation.clone(), command.clone())
        .await
        .unwrap();
    let replay = client
        .handle_command(operation.clone(), command.clone())
        .await
        .unwrap();
    assert_eq!(first, replay);
    let Command::Extension(mut different) = command else {
        unreachable!()
    };
    different.data["needs"]["worker"] = json!("2");
    assert!(
        client
            .handle_command(operation, Command::Extension(different))
            .await
            .is_err()
    );
    let result = tokio::time::timeout(
        Duration::from_secs(3),
        service.wait_execution(ExecutionId(1)),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(result.state, ExecutionState::Succeeded);
    assert_eq!(service.store.list(None, 10).unwrap().len(), 1);
    assert_eq!(records(&service).len(), 1);
    assert_eq!(
        service
            .output
            .tail(
                StepId {
                    execution: ExecutionId(1),
                    index: 1
                },
                OutputStream::Stdout,
                100
            )
            .unwrap()
            .data,
        b"once"
    );
    service.drain().await.unwrap();
}

struct ProviderFixture {
    root: std::path::PathBuf,
}
impl ProviderFixture {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!("cue-provider-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&root).unwrap();
        std::fs::write(
            root.join("provider.py"),
            include_str!("../tests/fixtures/resource_provider.py"),
        )
        .unwrap();
        Self { root }
    }
    fn config(&self, id: &str, key: &str, timeout_ms: u64) -> ProviderConfig {
        ProviderConfig {
            id: id.into(),
            backend: Backend::Command {
                keys: BTreeMap::from([(key.into(), Unit::Count)]),
                argv: vec![
                    "/usr/bin/env".into(),
                    "python3".into(),
                    self.root.join("provider.py").display().to_string(),
                    self.root.display().to_string(),
                ],
                timeout_ms,
            },
        }
    }
    fn flag(&self, name: &str) {
        std::fs::write(self.root.join(name), "").unwrap();
    }
    fn clear(&self, name: &str) {
        std::fs::remove_file(self.root.join(name)).unwrap();
    }
    fn log(&self) -> Vec<serde_json::Value> {
        std::fs::read_to_string(self.root.join("calls.jsonl"))
            .unwrap_or_default()
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect()
    }
}
impl Drop for ProviderFixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

#[test]
fn schema_two_history_is_unchanged_when_resource_storage_and_guard_are_added() {
    let uri = format!(
        "file:cue-upgrade-{}?mode=memory&cache=shared",
        uuid::Uuid::new_v4()
    );
    let legacy = Store::from_connection(rusqlite::Connection::open(&uri).unwrap()).unwrap();
    let initial_scope = scope(false);
    legacy.put_scope(&initial_scope, 1).unwrap();
    let execution = Execution::new(
        ExecutionId(1),
        spec(initial_scope.compute_hash(), "/bin/true", &[]),
    );
    legacy
        .commit_execution(
            &projection(&execution, 1, 1),
            &[FactDraft {
                occurred_at_ms: 1,
                fact: Fact::ExecutionCreated {
                    id: ExecutionId(1),
                    scope: initial_scope.compute_hash(),
                },
            }],
        )
        .unwrap();
    legacy
        .connection()
        .pragma_update(None, "user_version", 2)
        .unwrap();
    let before = legacy.get_execution(ExecutionId(1)).unwrap().unwrap();
    let facts = legacy.facts_after(ExecutionId(1), None, 100).unwrap();
    let store = Store::from_connection(rusqlite::Connection::open(&uri).unwrap()).unwrap();
    assert_eq!(
        store
            .connection()
            .pragma_query_value(None, "user_version", |r| r.get::<_, u32>(0))
            .unwrap(),
        2
    );
    let store = Arc::new(Mutex::new(store));
    Resources::new(store.clone(), ResourceConfig::default()).unwrap();
    let db = store.lock().unwrap();
    assert_eq!(
        db.connection()
            .pragma_query_value(None, "user_version", |r| r.get::<_, u32>(0))
            .unwrap(),
        3
    );
    assert_eq!(db.get_execution(ExecutionId(1)).unwrap().unwrap(), before);
    assert_eq!(db.facts_after(ExecutionId(1), None, 100).unwrap(), facts);
    assert_eq!(
        db.connection()
            .query_row("SELECT count(*) FROM resource_executions", [], |r| r
                .get::<_, i64>(0))
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn uncertain_physical_run_prevents_upgrade_before_resource_tables_exist() {
    let uri = format!(
        "file:cue-uncertain-upgrade-{}?mode=memory&cache=shared",
        uuid::Uuid::new_v4()
    );
    let store = Store::from_connection(rusqlite::Connection::open(&uri).unwrap()).unwrap();
    let service = DaemonService::from_store(store).unwrap();
    let task = super::tests::committed_running_task(
        &service,
        spec(scope(false).compute_hash(), "/bin/true", &[])
            .plan()
            .clone(),
    )
    .await;
    let step = StepId {
        execution: task.id,
        index: 1,
    };
    let db = service.store.lock_store().unwrap();
    let generation = db.claim_runtime_step(step).unwrap().unwrap();
    assert!(db.begin_run_attempt(step, generation).unwrap());
    db.connection()
        .execute_batch(
            "DROP TABLE resource_executions; DROP TABLE resource_identity; PRAGMA user_version=2;",
        )
        .unwrap();
    assert!(matches!(
        Store::from_connection(rusqlite::Connection::open(&uri).unwrap()),
        Err(StoreError::UncertainRunOwnership(_))
    ));
    assert_eq!(
        db.connection()
            .pragma_query_value(None, "user_version", |r| r.get::<_, u32>(0))
            .unwrap(),
        2
    );
    assert_eq!(
        db.connection()
            .query_row(
                "SELECT count(*) FROM sqlite_master WHERE name='resource_executions'",
                [],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
        0
    );
}

// Use the real provider subprocess with a durable ledger, and explicitly step the
// coordinator to stop at allocation/release boundaries without timing races.
fn resource_execution(config: ResourceConfig, needs: Needs) -> (Arc<Resources>, Arc<Mutex<Store>>) {
    let store = Arc::new(Mutex::new(Store::in_memory().unwrap()));
    let resources = Resources::new(store.clone(), config).unwrap();
    let db = store.lock().unwrap();
    let scope = scope(false);
    db.put_scope(&scope, 1).unwrap();
    let execution = Execution::new(ExecutionId(1), spec(scope.compute_hash(), "/bin/true", &[]));
    db.commit_execution(
        &projection(&execution, 1, 1),
        &[FactDraft {
            occurred_at_ms: 1,
            fact: Fact::ExecutionCreated {
                id: ExecutionId(1),
                scope: scope.compute_hash(),
            },
        }],
    )
    .unwrap();
    resources
        .insert(db.connection(), ExecutionId(1), &needs)
        .unwrap();
    drop(db);
    (resources, store)
}

#[tokio::test]
async fn lost_reserve_and_release_responses_keep_identity_until_confirmed_cleanup() {
    let p = ProviderFixture::new();
    p.flag("lose_reserve");
    p.flag("fail_release");
    let config = ResourceConfig {
        providers: vec![p.config("command", "worker", 1000)],
    };
    let (resources, store) = resource_execution(
        config.clone(),
        Needs::from([("worker".into(), Quantity::Count(1))]),
    );
    resources.tick().await.unwrap();
    let row = resources.record(ExecutionId(1)).unwrap().unwrap();
    assert_eq!(row.status, Status::Isolated);
    assert!(row.attempts[0].grant.is_some());
    let request = row.attempts[0].request.request_id.clone();
    resources.tick().await.unwrap();
    assert_eq!(
        p.log().iter().filter(|v| v["method"] == "reserve").count(),
        1
    );
    assert!(p.log().iter().all(|v| v["request_id"] == request));
    assert!(
        Resources::new(store.clone(), ResourceConfig::default())
            .unwrap()
            .recover()
            .await
            .is_err()
    );
    drop(resources);
    let recovered = Resources::new(store, config).unwrap();
    recovered.recover().await.unwrap();
    p.clear("fail_release");
    p.flag("lose_release");
    recovered.tick().await.unwrap();
    assert_eq!(
        recovered.record(ExecutionId(1)).unwrap().unwrap().status,
        Status::Isolated
    );
    recovered.tick().await.unwrap();
    assert_eq!(
        recovered.record(ExecutionId(1)).unwrap().unwrap().status,
        Status::Waiting
    );
    // Cleanup confirmation permits a fresh request, after the first tombstone.
    recovered.tick().await.unwrap();
    assert_eq!(
        recovered.record(ExecutionId(1)).unwrap().unwrap().status,
        Status::Allocated
    );
    let reserves = p
        .log()
        .into_iter()
        .filter(|v| v["method"] == "reserve")
        .collect::<Vec<_>>();
    assert_eq!(reserves.len(), 2);
    assert_ne!(reserves[0]["request_id"], reserves[1]["request_id"]);
}

#[tokio::test]
async fn grant_commit_failure_recovers_the_original_request_before_admission() {
    let p = ProviderFixture::new();
    let config = ResourceConfig {
        providers: vec![p.config("command", "worker", 1000)],
    };
    let (resources, store) = resource_execution(
        config.clone(),
        Needs::from([("worker".into(), Quantity::Count(1))]),
    );
    store.lock().unwrap().connection().execute_batch("CREATE TRIGGER reject_grant BEFORE UPDATE ON resource_executions WHEN json_extract(NEW.record_json,'$.attempts[0].grant') IS NOT NULL BEGIN SELECT RAISE(ABORT, 'grant fault'); END;").unwrap();
    assert!(resources.tick().await.is_err());
    assert!(!resources.admitted(ExecutionId(1)).unwrap());
    let row = resources.record(ExecutionId(1)).unwrap().unwrap();
    assert!(row.attempts[0].uncertain);
    assert!(row.attempts[0].grant.is_none());
    store
        .lock()
        .unwrap()
        .connection()
        .execute_batch("DROP TRIGGER reject_grant;")
        .unwrap();
    drop(resources);
    let recovered = Resources::new(store, config).unwrap();
    recovered.recover().await.unwrap();
    recovered.tick().await.unwrap();
    assert!(recovered.admitted(ExecutionId(1)).unwrap());
    let calls = p.log();
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[0]["method"], "reserve");
    assert_eq!(calls[1]["method"], "lookup");
    assert_eq!(calls[0]["request_id"], calls[1]["request_id"]);
    assert_eq!(
        recovered
            .record(ExecutionId(1))
            .unwrap()
            .unwrap()
            .environment["CUDA_VISIBLE_DEVICES"],
        "GPU-fixture"
    );
}

#[tokio::test]
async fn release_failure_does_not_change_success_or_repeat_the_program() {
    let p = ProviderFixture::new();
    p.flag("fail_release");
    let service = DaemonService::from_store_with_resources(
        Store::in_memory().unwrap(),
        ResourceConfig {
            providers: vec![p.config("command", "worker", 1000)],
        },
    )
    .unwrap();
    let mut client = service.connection();
    hello(&mut client).await;
    let (hash, _) = put_scope(&mut client, scope(false)).await;
    client
        .handle_command(OperationId::new("once").unwrap(), submit_request(hash))
        .await
        .unwrap();
    assert_eq!(
        tokio::time::timeout(
            Duration::from_secs(3),
            service.wait_execution(ExecutionId(1))
        )
        .await
        .unwrap()
        .unwrap()
        .state,
        ExecutionState::Succeeded
    );
    tokio::time::timeout(Duration::from_secs(3), async {
        while records(&service)[0].status != Status::Isolated {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        p.log().iter().filter(|v| v["method"] == "reserve").count(),
        1
    );
    assert_eq!(
        service
            .output
            .tail(
                StepId {
                    execution: ExecutionId(1),
                    index: 1
                },
                OutputStream::Stdout,
                100
            )
            .unwrap()
            .data,
        b"once"
    );
    p.clear("fail_release");
    service.drain().await.unwrap();
    assert_eq!(records(&service)[0].status, Status::Released);
}

#[tokio::test]
async fn partial_allocation_rolls_back_in_reverse_order_and_timeout_is_bounded() {
    let first = ProviderFixture::new();
    let second = ProviderFixture::new();
    second.flag("reject");
    let (resources, _) = resource_execution(
        ResourceConfig {
            providers: vec![
                first.config("a", "one", 1000),
                second.config("b", "two", 1000),
            ],
        },
        Needs::from([
            ("one".into(), Quantity::Count(1)),
            ("two".into(), Quantity::Count(1)),
        ]),
    );
    resources.tick().await.unwrap();
    assert_eq!(
        resources.record(ExecutionId(1)).unwrap().unwrap().status,
        Status::Waiting
    );
    assert_eq!(
        first
            .log()
            .iter()
            .map(|v| v["method"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["reserve", "release"]
    );
    assert_eq!(second.log().len(), 1);
    second.clear("reject");
    second.flag("timeout");
    tokio::time::timeout(Duration::from_secs(4), resources.tick())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        resources.record(ExecutionId(1)).unwrap().unwrap().status,
        Status::Waiting
    );
    assert!(second.log().iter().any(|v| v["method"] == "lookup"));
}

#[tokio::test]
async fn physical_gpu_environment_overrides_logical_changes_in_captured_and_pty_runs() {
    use cue_core::{
        Argv, EnvEdit, EnvKey, EnvPatch, ExecutionPlan, ExecutionSpec, IoMode, Pipeline, Process,
    };
    let p = ProviderFixture::new();
    for io in [IoMode::Captured, IoMode::Pty] {
        let service = DaemonService::from_store_with_resources(
            Store::in_memory().unwrap(),
            ResourceConfig {
                providers: vec![p.config("devices", "gpu", 1000)],
            },
        )
        .unwrap();
        let mut client = service.connection();
        hello(&mut client).await;
        let (hash, _) = put_scope(&mut client, scope(false)).await;
        let process = Process::with_env(
            Argv::new("/usr/bin/printenv", ["CUDA_VISIBLE_DEVICES".into()]).unwrap(),
            EnvPatch::new(BTreeMap::from([(
                EnvKey::new("CUDA_VISIBLE_DEVICES").unwrap(),
                EnvEdit::set("wrong").unwrap(),
            )])),
        );
        let spec =
            ExecutionSpec::new(hash, ExecutionPlan::run(Pipeline::simple(process), io)).unwrap();
        let response = client
            .handle(Message::Command {
                request_id: RequestId::new(10).unwrap(),
                operation_id: OperationId::new("gpu").unwrap(),
                command: Command::Extension(extension(
                    "submit",
                    json!({"spec":spec,"needs":{"gpu":"1"}}),
                )),
            })
            .await;
        let id = submitted_id(&response);
        let result = tokio::time::timeout(Duration::from_secs(3), service.wait_execution(id))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(result.state, ExecutionState::Succeeded);
        let stream = if io == IoMode::Pty {
            OutputStream::Terminal
        } else {
            OutputStream::Stdout
        };
        let out = service
            .output
            .tail(
                StepId {
                    execution: id,
                    index: 1,
                },
                stream,
                100,
            )
            .unwrap()
            .data;
        assert_eq!(String::from_utf8(out).unwrap().trim(), "GPU-fixture");
        service.drain().await.unwrap();
    }
}
